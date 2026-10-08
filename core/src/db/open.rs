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
use crate::store::{Reader, Store, StoreError, StoreEvent};

use super::cache::{CacheStats, RowCache};
use super::diagnostics::{Counters, Diagnostics};
use super::dispatch::{Dispatch, Event};
use super::error::DbError;
use super::listeners::Executor;
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
    pub cache: Arc<Mutex<RowCache>>,
    pub observers: Mutex<Observers>,
    pub counters: Arc<Counters>,
    pub dispatch: Arc<Dispatch>,
    pub executor: Arc<Executor>,
    pub files: FileDirectories,
    pub files_gate: Mutex<()>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.dispatch.close();
        self.executor.close();
    }
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
        let cache = Arc::new(Mutex::new(RowCache::new(config.cache_rows)));
        let dispatch = Arc::new(Dispatch::new());
        store.set_hook(commit_hook(cache.clone(), dispatch.clone()));
        let shared = Arc::new(Shared {
            store,
            specs,
            cache,
            observers: Mutex::new(Observers::default()),
            counters: Arc::new(Counters::default()),
            executor: Executor::new(dispatch.busy.clone()),
            dispatch: dispatch.clone(),
            files: path.map_or_else(FileDirectories::scratch, FileDirectories::beside),
            files_gate: Mutex::new(()),
        });
        Dispatch::start(dispatch, Arc::downgrade(&shared));
        Ok(Db { shared })
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

fn commit_hook(cache: Arc<Mutex<RowCache>>, dispatch: Arc<Dispatch>) -> crate::store::StoreHook {
    Box::new(move |event| match event {
        StoreEvent::Committed(result) => {
            {
                let mut cache = locked(&cache);
                for change in &result.rows {
                    cache.evict(change.row.model.as_str(), change.row.id.as_str());
                }
            }
            dispatch.push(Event::of_commit(result));
        }
        StoreEvent::Reset { revision, models } => {
            {
                let mut cache = locked(&cache);
                match &models {
                    Some(models) => cache.evict_models(models),
                    None => cache.clear(),
                }
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
