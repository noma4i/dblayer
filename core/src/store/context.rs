//! What the Store knows about the open database besides the SQL: the models, the files and the notice of the commits.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use crate::owners::contract::{CommittedBroadcast, Models};
use crate::store::{BatchPlan, CommitResult, EntityOp};
use crate::store::ddl::{table_name, ManifestVerdict, RecreateReason};
use crate::store::files::FileDirectories;
use crate::identity::{ModelKey, RowRef};
use crate::store::operation::{apply_fsck, fsck_plan, open_operations, rebuilt_rows, FsckAction, Orphan, Orphans};
use crate::owners::write::operation_outbox::OutboxQueue;
use crate::schema::{reserved_member_names, resolve_types, validate_models, AssociationKind, AssociationSpec, ModelSpec};
use crate::store::sync_state::rebuild_sync;

use super::db_error::CoreError;
use super::changed_log::ChangedLog;
use super::replaced_log::ReplacedLog;
use super::{Reader, Store, StoreError, StoreEvent};

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenReport {
    pub recreated: Option<RecreateReason>,
    pub changed_models: Vec<String>,
    pub reset_models: Vec<String>,
    pub dropped: Vec<DroppedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DroppedRecord {
    pub table: String,
    pub key: String,
    pub reason: String,
}

impl CommittedBroadcast {
    /// The one notice of a commit: `rows` are exactly the changed rows of the tables named in `tables`.
    pub(crate) fn of_commit(result: &CommitResult) -> Self {
        let models: std::collections::BTreeSet<&str> = result.rows.iter().map(|change| change.row.model.as_str()).chain(result.scopes.iter().map(|scope| scope.model.as_str())).chain(result.members.iter().map(|row| row.model.as_str())).chain(result.replaced.iter().map(|(from, _)| from.model.as_str())).collect();
        let queue = (!result.operations.is_empty() || result.uploads_changed).then(|| crate::store::operation::QUEUE_TABLE.to_string());
        let tables: Vec<String> = models.iter().map(|model| table_name(model)).chain(queue).collect();
        let mut rows = super::changed_log::table_rows(result);
        rows.retain(|(table, _)| tables.contains(table));
        Self { revision: u64::try_from(result.revision.unwrap_or_default()).unwrap_or_default(), tables, models: Models::Set(models.into_iter().map(str::to_string).collect()), position: result.position, rows: Some(rows) }
    }

    /// The notice of a reset of `models`, or of every model.
    pub(crate) fn of_reset(revision: u64, models: Option<&[String]>) -> Self {
        Self { revision, tables: models.into_iter().flatten().map(|model| table_name(model)).collect(), models: models.map_or(Models::All, |models| Models::Set(models.iter().cloned().collect())), position: 0, rows: None }
    }
}

/// Where the commits go: the owner `Deliveries` listens once `Life` started.
#[derive(Default)]
pub(crate) struct CommitSink(OnceLock<Arc<dyn Fn(CommittedBroadcast) + Send + Sync>>);

impl CommitSink {
    fn notify(&self, notice: CommittedBroadcast) {
        if let Some(sink) = self.0.get() { sink(notice); }
    }
}

pub(crate) struct Context {
    pub specs: BTreeMap<String, ModelSpec>,
    pub pointer_guards: BTreeMap<String, Vec<AssociationSpec>>,
    pub files: Arc<FileDirectories>,
    pub opened: OpenReport,
    pub replaced: Arc<ReplacedLog>,
    pub changed: Arc<ChangedLog>,
    pub commits: Arc<CommitSink>,
    pub observers: OnceLock<Arc<dyn crate::owners::life::ObserverPort>>,
}

impl Context {
    pub(super) fn empty() -> Self {
        Self {
            specs: BTreeMap::new(),
            pointer_guards: BTreeMap::new(),
            files: Arc::new(FileDirectories::scratch()),
            opened: OpenReport { recreated: None, changed_models: Vec::new(), reset_models: Vec::new(), dropped: Vec::new() },
            replaced: Arc::default(),
            changed: Arc::default(),
            commits: Arc::default(),
            observers: OnceLock::new(),
        }
    }
}

impl Store {
    /// Opens the file (or memory), boots the models, restores the queue and installs the notice of the commits.
    pub fn open_models(path: Option<&Path>, models: Vec<ModelSpec>, data_version: Option<&str>) -> Result<Store, CoreError> {
        let mut models = models;
        resolve_types(&mut models);
        let faults = validate_models(&models, &reserved_member_names());
        if !faults.is_empty() {
            return Err(CoreError::Invalid(faults));
        }
        let (mut store, unreadable) = match path {
            Some(path) => match Store::open(path) {
                Ok(store) => (store, false),
                Err(error) if super::is_unreadable(&error) => {
                    super::remove_database(path)?;
                    (Store::open(path)?, true)
                }
                Err(error) => return Err(error.into()),
            },
            None => (Store::open_in_memory()?, false),
        };
        let boot = store.boot(&models, data_version)?;
        let changed_models = match &boot.verdict {
            ManifestVerdict::CacheOnly { models } => models.clone(),
            _ => Vec::new(),
        };
        let dropped = boot.dropped.into_iter().map(|row| DroppedRecord { table: row.table, key: row.key, reason: row.reason.to_string() }).collect();
        let opened = OpenReport { recreated: if unreadable { Some(RecreateReason::UnreadableFile) } else { boot.recreated }, changed_models, reset_models: boot.reset_models, dropped };
        recover_operations(&store, &models, opened.recreated.is_none())?;
        store.publish_revision(store.revision_seq()?);
        store.mark_ready();
        let replaced = Arc::new(ReplacedLog::default());
        let commits = Arc::new(CommitSink::default());
        let changed = Arc::new(ChangedLog::default());
        changed.start(store.revision_seq()?);
        let (logged, journal, notified, reset_journal) = (replaced.clone(), changed.clone(), commits.clone(), changed.clone());
        store.set_commit_log(Box::new(move |event| {
            if let StoreEvent::Committed(result) = event {
                logged.log_replaced(result.revision.unwrap_or_default(), result.replaced.iter().map(|(from, to)| (from.model.to_string(), from.id.clone(), to.clone())));
                journal.log_commit(result);
            }
        }))?;
        store.set_hook(Box::new(move |event| match event {
            StoreEvent::Committed(result) => notified.notify(CommittedBroadcast::of_commit(result)),
            StoreEvent::Reset { revision, models, .. } => {
                reset_journal.log_unknown(revision);
                notified.notify(CommittedBroadcast::of_reset(u64::try_from(revision).unwrap_or_default(), models.as_deref()));
            }
            StoreEvent::Renewed => {}
        }))?;
        store.context = Context {
            pointer_guards: pointer_guards(&models),
            specs: models.into_iter().map(|spec| (spec.key.as_str().to_string(), spec)).collect(),
            files: Arc::new(path.map_or_else(FileDirectories::scratch, FileDirectories::beside)),
            opened,
            replaced,
            changed,
            commits,
            observers: OnceLock::new(),
        };
        Ok(store)
    }

    pub fn spec(&self, model: &str) -> Result<&ModelSpec, CoreError> {
        self.context.specs.get(model).ok_or_else(|| CoreError::UnknownModel(model.to_string()))
    }

    pub(crate) fn specs(&self) -> &BTreeMap<String, ModelSpec> { &self.context.specs }

    pub(crate) fn pointer_guards(&self) -> &BTreeMap<String, Vec<AssociationSpec>> { &self.context.pointer_guards }

    pub(crate) fn files(&self) -> &Arc<FileDirectories> { &self.context.files }

    pub(crate) fn replaced(&self) -> &Arc<ReplacedLog> { &self.context.replaced }

    pub(crate) fn observers(&self) -> &OnceLock<Arc<dyn crate::owners::life::ObserverPort>> { &self.context.observers }

    pub fn open_report(&self) -> &OpenReport { &self.context.opened }

    pub(crate) fn on_commit(&self, sink: impl Fn(CommittedBroadcast) + Send + Sync + 'static) -> Result<(), StoreError> {
        self.context.commits.0.set(Arc::new(sink)).map_err(|_| StoreError::AlreadyAttached("the commit sink"))
    }

    pub fn set_clock(&self, clock: Arc<dyn crate::runtime::Clock>) { self.lock().clock = clock; }

    /// Reads with the models named, the failure as the error of the database.
    pub(crate) fn read_models<T>(&self, models: &[&str], run: impl FnOnce(&Reader<'_>) -> Result<T, CoreError>) -> Result<T, CoreError> {
        self.read(models, |reader| Ok(run(reader)))?
    }
}

fn recover_operations(store: &Store, models: &[ModelSpec], restore_rows: bool) -> Result<(), StoreError> {
    let rebuilt = {
        let inner = store.lock();
        let operations = open_operations(&inner.connection)?;
        let known: HashSet<String> = models.iter().map(|spec| spec.key.as_str().to_string()).collect();
        let queue = OutboxQueue::new(operations.clone());
        let mut temp_rows: HashSet<RowRef> = HashSet::new();
        let mut open_row_ids: HashSet<RowRef> = HashSet::new();
        for spec in models {
            let table = table_name(spec.key.as_str());
            let mut statement = inner.connection.prepare(&format!(include_str!("sql/context/recover_operations.sql"), crate::identity::TEMP_ID_PREFIX, table = table))?;
            let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
            for id in ids {
                temp_rows.insert(RowRef::new(spec.key.clone(), crate::store::operation::stored_row_id(&id?)?));
            }
            open_row_ids.extend(queue.open_row_ids(&spec.key).into_iter().map(|id| RowRef::new(spec.key.clone(), id)));
        }
        let reader = Reader::new(&inner.connection, &inner.models);
        for operation in &operations {
            for (model, id) in super::plan::Prior::created_rows(&reader, operation.id.0)? {
                open_row_ids.insert(RowRef::new(ModelKey::new(&model), crate::store::operation::stored_row_id(&id)?));
            }
        }
        let plan = fsck_plan(&operations, &temp_rows, &temp_rows, &open_row_ids);
        let mut orphans = Orphans::new();
        for row in plan.iter().filter_map(|action| match action { FsckAction::QuarantineOrphan(row) => Some(row), _ => None }) {
            if let Some(stored) = reader.stored_row(row.model.as_str(), row.id.as_str())? {
                let payload = serde_json::Value::Object(stored.row.clone());
                let info = inner.models.get(row.model.as_str()).ok_or_else(|| StoreError::UnknownModel(row.model.as_str().to_string()))?;
                let tables = info.layout.child_tables().into_iter().chain([table_name(row.model.as_str())]).collect();
                let search = (!info.search.is_empty()).then(|| crate::store::ddl::fts_name(row.model.as_str()));
                orphans.insert(row.clone(), Orphan { tables, search, payload });
            }
        }
        apply_fsck(&inner.connection, &plan, &orphans)?;
        rebuild_sync(&inner.connection)?;
        if restore_rows { rebuilt_rows(&inner.connection, &plan, &operations, &known)? } else { Vec::new() }
    };
    if rebuilt.is_empty() {
        return Ok(());
    }
    let mut envelope = BatchPlan::new(store.generation());
    envelope.entity_ops = rebuilt.into_iter().map(|row| EntityOp::Upsert { model: ModelKey::new(&row.model), id: row.id, row: row.row }).collect();
    store.commit(envelope)?;
    Ok(())
}

fn pointer_guards(models: &[ModelSpec]) -> BTreeMap<String, Vec<AssociationSpec>> {
    let mut guards: BTreeMap<String, Vec<AssociationSpec>> = BTreeMap::new();
    for association in models.iter().flat_map(|spec| &spec.associations) {
        if association.kind == AssociationKind::BelongsTo && !association.sets.is_empty() && !association.newer.is_empty() {
            guards.entry(association.target.to_string()).or_default().push(association.clone());
        }
    }
    guards
}
