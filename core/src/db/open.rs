use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::ddl::table_name;
use crate::files::FileDirectories;
use crate::identity::{Generation, RowId};
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use crate::operation::{apply_fsck, fsck_plan, open_operations, rebuilt_rows, OutboxQueue};
use crate::pacing::Clock;
use crate::schema::{validate_models, ModelSpec};
use crate::store::{Reader, Store, StoreError};

use super::cache::{CacheStats, RowCache};
use super::diagnostics::{Counters, Diagnostics};
use super::error::DbError;
use super::observe::Observers;

const RESERVED_MEMBERS: [&str; 9] = ["id", "find", "where", "save", "count", "exists", "first", "observe", "order"];

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
    pub specs: BTreeMap<String, ModelSpec>,
    pub cache: Mutex<RowCache>,
    pub observers: Mutex<Observers>,
    pub counters: Counters,
    pub files: FileDirectories,
    pub files_gate: Mutex<()>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Shared {
    pub(crate) fn spec(&self, model: &str) -> Result<&ModelSpec, DbError> {
        self.specs.get(model).ok_or_else(|| DbError::UnknownModel(model.to_string()))
    }

    pub(crate) fn read<T>(&self, run: impl FnOnce(&Reader<'_>) -> Result<T, DbError>) -> Result<T, DbError> {
        let mut outcome = None;
        self.store.read(|reader| {
            outcome = Some(run(reader));
            Ok(())
        })?;
        outcome.expect("read ran")
    }
}

#[derive(Clone)]
pub struct Db {
    pub(crate) shared: Arc<Shared>,
}

impl Db {
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
        let faults = validate_models(&models, &RESERVED_MEMBERS);
        if !faults.is_empty() {
            return Err(DbError::Invalid(faults));
        }
        let store = match path {
            Some(path) => Store::open(path)?,
            None => Store::open_in_memory()?,
        };
        store.boot(&models, data_version)?;
        recover_operations(&store, &models)?;
        store.mark_ready();
        let specs = models.into_iter().map(|spec| (spec.key.as_str().to_string(), spec)).collect();
        let shared = Shared {
            store,
            specs,
            cache: Mutex::new(RowCache::new(config.cache_rows)),
            observers: Mutex::new(Observers::default()),
            counters: Counters::default(),
            files: path.map_or_else(FileDirectories::scratch, FileDirectories::beside),
            files_gate: Mutex::new(()),
        };
        Ok(Db { shared: Arc::new(shared) })
    }

    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        self.shared.store.set_clock(clock);
    }

    pub fn generation(&self) -> Generation {
        self.shared.store.generation()
    }

    pub fn is_ready(&self) -> bool {
        self.shared.store.is_ready()
    }

    pub fn revision_seq(&self) -> Result<i64, DbError> {
        Ok(self.shared.store.revision_seq()?)
    }

    pub fn spec(&self, model: &str) -> Result<&ModelSpec, DbError> {
        self.shared.spec(model)
    }

    pub fn cache_stats(&self) -> CacheStats {
        locked(&self.shared.cache).stats()
    }

    pub fn diagnostics(&self) -> Diagnostics {
        let observers = locked(&self.shared.observers);
        Diagnostics {
            observation_reads: self.shared.counters.observation_reads(),
            observer_calls: self.shared.counters.observer_calls(),
            observed_queries: observers.query_count(),
            subscriptions: observers.subscription_count(),
        }
    }
}

fn recover_operations(store: &Store, models: &[ModelSpec]) -> Result<(), StoreError> {
    let rebuilt = {
        let inner = store.lock();
        let operations = open_operations(&inner.connection)?;
        let known: HashSet<String> = models.iter().map(|spec| spec.key.as_str().to_string()).collect();
        let queue = OutboxQueue::new(operations.clone());
        let mut temp_rows: HashSet<RowId> = HashSet::new();
        let mut open_row_ids: HashSet<RowId> = HashSet::new();
        let mut tables: Vec<(String, String)> = Vec::new();
        for spec in models {
            let table = table_name(spec.key.as_str());
            let mut statement = inner.connection.prepare(&format!("SELECT id FROM {table} WHERE id LIKE 'temp-%'"))?;
            let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
            for id in ids {
                temp_rows.extend(RowId::new(&id?).ok());
            }
            open_row_ids.extend(queue.open_row_ids(&spec.key));
            tables.push((spec.key.as_str().to_string(), table));
        }
        let plan = fsck_plan(&operations, &known, &temp_rows, &temp_rows, &open_row_ids);
        apply_fsck(&inner.connection, &plan, &tables)?;
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
