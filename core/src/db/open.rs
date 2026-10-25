use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::ddl::{table_name, ManifestVerdict, RecreateReason};
use crate::files::FileDirectories;
use crate::identity::{Generation, RowRef};
use crate::commit::{CommitEnvelope, EntityOp, RowChangeKind, WriteOrigin};
use crate::identity::ModelKey;
use crate::operation::{apply_fsck, fsck_plan, open_operations, rebuilt_rows, FsckAction, Orphan, Orphans, OutboxQueue};
use crate::syncstate::rebuild_sync_with;
use crate::pacing::{Clock, Counters};
use crate::schema::{resolve_types, validate_models, AssociationKind, AssociationSpec, ModelSpec};
use crate::store::{Reader, Store, StoreError, StoreEvent};

use super::cache::RowCache;
use super::warm::Warm;
use super::dispatch::{on_owner_thread, Dispatch, Event};
use super::ephemeral::Ephemerals;
use super::error::DbError;
use super::listeners::Executor;
use super::observe::{ObserverCensus, Observers};
use super::reads::ReadPool;

const RESERVED_MEMBERS: [&str; 12] = ["id", "find", "where", "save", "count", "exists", "first", "observe", "order", "sync", "retry", "discard"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbConfig {
    pub cache_rows: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self { cache_rows: 4096 }
    }
}

pub(crate) struct Shared {
    pub store: Store,
    pub opened: OpenReport,
    pub specs: BTreeMap<String, ModelSpec>,
    pub pointer_guards: BTreeMap<String, Vec<AssociationSpec>>,
    pub cache: Arc<Mutex<RowCache>>,
    pub observers: Mutex<Observers>,
    pub observer_commands: Mutex<Vec<super::observe::ObserverCommand>>,
    pub observed_reads: Mutex<super::observed_reads::ObservedReads>,
    pub warm: Mutex<Warm>,
    #[cfg(test)]
    pub row_build_hook: Mutex<Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>>,
    pub counters: Arc<Mutex<Counters>>,
    pub dispatch: Arc<Dispatch>,
    pub outbox_changes: Arc<tokio::sync::watch::Sender<u64>>,
    pub operation_queues: Arc<crate::operation::OperationQueues>,
    pub cloud_changes: Arc<tokio::sync::watch::Sender<u64>>,
    pub connectivity: Arc<crate::transport::Connectivity>,
    pub reset_listeners: Arc<Mutex<Vec<super::reset::ResetListener>>>,
    pub ephemeral: Arc<Ephemerals>,
    pub executor: Arc<Executor>,
    pub reads: Arc<ReadPool>,
    pub files: FileDirectories,
    pub files_gate: Mutex<()>,
    pub upload_accepted: Mutex<Option<super::upload_accepted::UploadAcceptedCallback>>,
    pub upload_round: Mutex<super::upload_round::UploadRound>,
    pub expiry_gate: Mutex<()>,
    #[cfg(test)]
    pub panic_key: Mutex<Option<String>>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

thread_local! {
    static OBSERVERS_HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) struct ObserversGuard<'a>(MutexGuard<'a, Observers>);

impl<'a> ObserversGuard<'a> {
    fn try_new(mutex: &'a Mutex<Observers>) -> Option<Self> {
        let guard = match mutex.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        OBSERVERS_HELD.with(|held| held.set(held.get() + 1));
        Some(Self(guard))
    }

    fn new(mutex: &'a Mutex<Observers>) -> Self {
        let guard = locked(mutex);
        OBSERVERS_HELD.with(|held| held.set(held.get() + 1));
        Self(guard)
    }
}

impl Drop for ObserversGuard<'_> {
    fn drop(&mut self) {
        OBSERVERS_HELD.with(|held| held.set(held.get() - 1));
    }
}

impl std::ops::Deref for ObserversGuard<'_> {
    type Target = Observers;

    fn deref(&self) -> &Observers {
        &self.0
    }
}

impl std::ops::DerefMut for ObserversGuard<'_> {
    fn deref_mut(&mut self) -> &mut Observers {
        &mut self.0
    }
}

fn observers_held() -> bool {
    OBSERVERS_HELD.with(|held| held.get() > 0)
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.dispatch.close();
        self.ephemeral.close();
        self.executor.close();
        self.reads.close();
    }
}

impl Shared {
    pub(crate) fn spec(&self, model: &str) -> Result<&ModelSpec, DbError> {
        self.specs.get(model).ok_or_else(|| DbError::UnknownModel(model.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn panic_at(&self, key: &str) {
        let mut armed = locked(&self.panic_key);
        if armed.as_deref() == Some(key) {
            *armed = None;
            drop(armed);
            panic!("injected panic for {key}");
        }
    }

    pub(crate) fn observers(&self) -> ObserversGuard<'_> {
        let mut guard = ObserversGuard::new(&self.observers);
        self.run_observer_commands(&mut guard);
        guard
    }

    pub(crate) fn try_observers(&self) -> Option<ObserversGuard<'_>> {
        let mut guard = ObserversGuard::try_new(&self.observers)?;
        self.run_observer_commands(&mut guard);
        Some(guard)
    }

    pub(crate) fn read<T>(&self, models: &[&str], run: impl FnOnce(&Reader<'_>) -> Result<T, DbError>) -> Result<T, DbError> {
        if on_owner_thread() || observers_held() {
            locked(&self.counters).sql_under_owner += 1;
        }
        let mut outcome = None;
        self.store.read(models, |reader| {
            outcome = Some(run(reader));
            Ok(())
        })?;
        outcome.expect("read ran")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenReport {
    pub recreated: Option<RecreateReason>,
    pub changed_models: Vec<String>,
    pub reset_models: Vec<String>,
}

#[derive(Clone)]
pub struct Db {
    pub(crate) shared: Arc<Shared>,
}

impl Db {
    #[cfg(test)]
    pub fn open(path: &Path, models: Vec<ModelSpec>, data_version: Option<&str>) -> Result<Db, DbError> {
        Db::open_with(Some(path), models, data_version, DbConfig::default())
    }

    pub fn open_in_memory(models: Vec<ModelSpec>, data_version: Option<&str>) -> Result<Db, DbError> {
        Db::open_with(None, models, data_version, DbConfig::default())
    }

    pub fn open_with(
        path: Option<&Path>,
        models: Vec<ModelSpec>,
        data_version: Option<&str>,
        config: DbConfig,
    ) -> Result<Db, DbError> {
        let mut models = models;
        resolve_types(&mut models);
        let faults = validate_models(&models, &RESERVED_MEMBERS);
        if !faults.is_empty() {
            return Err(DbError::Invalid(faults));
        }
        let store = match path {
            Some(path) => Store::open(path)?,
            None => Store::open_in_memory()?,
        };
        let boot = store.boot(&models, data_version)?;
        let changed_models = match &boot.verdict {
            ManifestVerdict::CacheOnly { models } => models.clone(),
            _ => Vec::new(),
        };
        let reset_models = boot.reset_models;
        let opened = OpenReport { recreated: store.recreated(), changed_models, reset_models };
        recover_operations(&store, &models)?;
        store.mark_ready();
        let ephemeral = Ephemerals::new(&models);
        let pointer_guards = pointer_guards(&models);
        let specs = models.into_iter().map(|spec| (spec.key.as_str().to_string(), spec)).collect();
        let cache = Arc::new(Mutex::new(RowCache::new(config.cache_rows)));
        let dispatch = Arc::new(Dispatch::new());
        let reads = ReadPool::new(dispatch.clone());
        let outbox_changes = Arc::new(tokio::sync::watch::channel(0).0);
        let operation_queues = Arc::new(crate::operation::OperationQueues::default());
        let cloud_changes = Arc::new(tokio::sync::watch::channel(0).0);
        let reset_listeners = Arc::new(Mutex::new(Vec::new()));
        let replaced_log = dispatch.clone();
        store.set_commit_log(Box::new(move |event| {
            if let StoreEvent::Committed(result) = event {
                replaced_log.log_replaced(result.revision.unwrap_or_default(), result.replaced.iter().map(|(from, to)| (from.id.clone(), to.clone())));
            }
        }));
        store.set_hook(commit_hook(cache.clone(), dispatch.clone(), ephemeral.clone(), outbox_changes.clone(), operation_queues.clone(), cloud_changes.clone(), reset_listeners.clone()));
        let shared = Arc::new(Shared {
            store,
            opened,
            specs,
            pointer_guards,
            cache,
            observers: Mutex::new(Observers::default()),
            observer_commands: Mutex::new(Vec::new()),
            observed_reads: Mutex::new(Default::default()),
            warm: Mutex::new(Warm::default()),
            #[cfg(test)]
            row_build_hook: Mutex::new(None),
            counters: Arc::new(Mutex::new(Counters::default())),
            executor: Executor::new(dispatch.progress.clone()),
            reads: reads.clone(),
            dispatch: dispatch.clone(),
            outbox_changes,
            operation_queues,
            cloud_changes,
            connectivity: crate::transport::Connectivity::new(true),
            reset_listeners,
            ephemeral: ephemeral.clone(),
            files: path.map_or_else(FileDirectories::scratch, FileDirectories::beside),
            files_gate: Mutex::new(()),
            upload_accepted: Mutex::new(None),
            upload_round: Mutex::new(super::upload_round::UploadRound::default()),
            expiry_gate: Mutex::new(()),
            #[cfg(test)]
            panic_key: Mutex::new(None),
        });
        Dispatch::start(dispatch, Arc::downgrade(&shared))?;
        ReadPool::start(&reads, Arc::downgrade(&shared))?;
        Ephemerals::start(ephemeral, Arc::downgrade(&shared))?;
        Ok(Db { shared })
    }

    pub fn connectivity(&self) -> Arc<crate::transport::Connectivity> {
        self.shared.connectivity.clone()
    }

    pub fn open_report(&self) -> &OpenReport {
        &self.shared.opened
    }

    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        self.shared.ephemeral.lock().clock = clock.clone();
        self.shared.store.lock().clock = clock;
    }

    pub fn generation(&self) -> Generation {
        self.shared.store.generation()
    }

    #[cfg(test)]
    pub fn is_ready(&self) -> bool {
        self.shared.store.is_ready()
    }

    pub fn revision_seq(&self) -> Result<i64, DbError> {
        Ok(self.shared.store.revision_seq()?)
    }

    pub fn spec(&self, model: &str) -> Result<&ModelSpec, DbError> {
        self.shared.spec(model)
    }

    pub fn counters(&self) -> Counters {
        self.shared.store.counters() + locked(&self.shared.counters).clone() + locked(&self.shared.cache).counters()
    }

    #[cfg(test)]
    pub(crate) fn keyed_rows(&self) -> usize {
        self.shared.observers().keyed_rows()
    }

    pub fn observer_census(&self) -> ObserverCensus {
        let observers = self.shared.observers();
        ObserverCensus { observed_queries: observers.query_count(), subscriptions: observers.subscription_count() }
    }
}

fn commit_hook(cache: Arc<Mutex<RowCache>>, dispatch: Arc<Dispatch>, ephemeral: Arc<Ephemerals>, outbox_changes: Arc<tokio::sync::watch::Sender<u64>>, operation_queues: Arc<crate::operation::OperationQueues>, cloud_changes: Arc<tokio::sync::watch::Sender<u64>>, reset_listeners: Arc<Mutex<Vec<super::reset::ResetListener>>>) -> crate::store::StoreHook {
    Box::new(move |event| match event {
        StoreEvent::Committed(result) => {
            operation_queues.changed(&result.queue_operations);
            {
                let mut cache = locked(&cache);
                if let Some(revision) = result.revision {
                    cache.committed(revision);
                }
                for change in &result.rows {
                    cache.evict(change.row.model.as_str(), change.row.id.as_str());
                }
            }
            {
                let mut values = ephemeral.lock();
                for (from, to) in &result.replaced {
                    values.rename(from.model.as_str(), from.id.as_str(), to.as_str());
                }
                for change in &result.rows {
                    let (model, id) = (change.row.model.as_str(), change.row.id.as_str());
                    if change.kind == RowChangeKind::Destroyed {
                        values.forget(model, id);
                    } else {
                        values.track(model, id);
                    }
                }
            }
            if !result.operations.is_empty() {
                outbox_changes.send_modify(|version| *version += 1);
            }
            if result.cloud_logged {
                cloud_changes.send_modify(|version| *version += 1);
            }
            ephemeral.nudge();
            dispatch.push(Event::of_commit(result));
        }
        StoreEvent::Reset { revision, models, operations } => {
            operation_queues.reset(&operations);
            {
                let mut cache = locked(&cache);
                cache.committed(revision);
                match &models {
                    Some(models) => cache.evict_models(models),
                    None => cache.clear(),
                }
            }
            ephemeral.lock().clear(models.as_deref());
            outbox_changes.send_modify(|version| *version += 1);
            for listener in locked(&reset_listeners).iter() {
                listener(models.as_deref());
            }
            dispatch.push(Event::of_reset(revision, models.as_deref()));
        }
    })
}

fn recover_operations(store: &Store, models: &[ModelSpec]) -> Result<(), StoreError> {
    let rebuilt = {
        let inner = store.lock();
        let operations = open_operations(&inner.connection)?;
        let known: HashSet<String> = models.iter().map(|spec| spec.key.as_str().to_string()).collect();
        let queue = OutboxQueue::new(operations.clone());
        let mut temp_rows: HashSet<RowRef> = HashSet::new();
        let mut open_row_ids: HashSet<RowRef> = HashSet::new();
        for spec in models {
            let table = table_name(spec.key.as_str());
            let mut statement = inner.connection.prepare(&format!("SELECT id FROM {table} WHERE id LIKE '{}%'", crate::identity::TEMP_ID_PREFIX))?;
            let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
            for id in ids {
                temp_rows.insert(RowRef::new(spec.key.clone(), crate::operation::stored_row_id(&id?)?));
            }
            open_row_ids.extend(queue.open_row_ids(&spec.key).into_iter().map(|id| RowRef::new(spec.key.clone(), id)));
        }
        let plan = fsck_plan(&operations, &known, &temp_rows, &temp_rows, &open_row_ids);
        let reader = Reader::new(&inner.connection, &inner.models);
        let mut orphans = Orphans::new();
        for row in plan.iter().filter_map(|action| match action { FsckAction::QuarantineOrphan(row) => Some(row), _ => None }) {
            if let Some(stored) = reader.stored_row(row.model.as_str(), row.id.as_str())? {
                let payload = serde_json::Value::Object(stored.row.clone());
                let info = inner.models.get(row.model.as_str()).ok_or_else(|| StoreError::UnknownModel(row.model.as_str().to_string()))?;
                let tables = info.layout.child_tables().into_iter().chain([table_name(row.model.as_str())]).collect();
                let search = (!info.search.is_empty()).then(|| crate::ddl::fts_name(row.model.as_str()));
                orphans.insert(row.clone(), Orphan { tables, search, payload });
            }
        }
        apply_fsck(&inner.connection, &plan, &orphans)?;
        rebuild_sync_with(&inner.connection, &|key| crate::operation::Operation::upload_action(key).is_some_and(|(model, action)| inner.models.get(model.as_str()).is_some_and(|info| info.spec.upload_action(&action).is_some_and(|declared| declared.batch.is_some()))))?;
        rebuilt_rows(&inner.connection, &plan, &operations, &known)?
    };
    if rebuilt.is_empty() {
        return Ok(());
    }
    let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, store.generation());
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
