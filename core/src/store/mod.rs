use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use rusqlite::Connection;

use crate::ddl::{layout, table_name, Layout, RecreateReason};
use crate::identity::Generation;
use crate::pacing::{Clock, Counters, SystemClock};
use crate::schema::{ModelSpec, SearchPath};

mod admission;
mod apply;
mod boot;
mod error;
mod file_ops;
mod maintenance;
mod membership;
mod open;
mod reset;
mod rows;
mod scopes;
mod search;
mod shape;
mod tombstones;
mod windows;

pub use admission::{admit_upsert, UpsertAdmission};
pub use apply::PlanCommitError;
pub use boot::BootReport;
pub use error::StoreError;
pub use maintenance::{MaintenanceReport, TempRowPolicy};
pub use membership::{MembershipChange, MembershipPlan};
pub use rows::{Reader, StoredRow};
#[cfg(test)]
pub use rows::ScopeRowsResult;
pub use tombstones::TombstoneRetention;
pub use windows::{family_prefix, window_key, PageInfo, WindowFreshness, WindowRecord};

pub(crate) struct ModelInfo {
    pub table: String,
    pub layout: Layout,
    pub search: Vec<SearchPath>,
    pub spec: ModelSpec,
}

impl ModelInfo {
    pub(crate) fn new(spec: &ModelSpec) -> Self {
        Self { table: table_name(spec.key.as_str()), layout: layout(spec), search: spec.search_paths(), spec: spec.clone() }
    }
}

pub(crate) struct Inner {
    pub connection: Connection,
    pub models: BTreeMap<String, ModelInfo>,
    pub clock: Arc<dyn Clock>,
    pub generation: Generation,
    pub ready: bool,
    pub poison: Option<String>,
    pub data_version: Option<String>,
    pub counters: Counters,
}

impl Inner {
    pub(crate) fn now_ms(&self) -> i64 {
        i64::try_from(self.clock.now().as_millis()).unwrap_or(i64::MAX)
    }

    pub(crate) fn model(&self, key: &str) -> Result<&ModelInfo, StoreError> {
        self.models.get(key).ok_or_else(|| StoreError::UnknownModel(key.to_string()))
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), StoreError> {
        match &self.poison {
            Some(reason) => Err(StoreError::Poisoned(reason.clone())),
            None => Ok(()),
        }
    }
}

pub enum StoreEvent<'a> {
    Committed(&'a crate::commit::CommitResult),
    Reset { revision: i64, models: Option<Vec<String>> },
}

pub type StoreHook = Box<dyn Fn(StoreEvent<'_>) + Send + Sync>;

pub struct Store {
    inner: Mutex<Inner>,
    hook: OnceLock<StoreHook>,
    recreated: Option<RecreateReason>,
    #[cfg(test)]
    failing_entity_reads: std::sync::atomic::AtomicUsize,
}

impl Store {
    pub(crate) fn from_connection(connection: Connection) -> Self {
        let inner = Inner {
            connection,
            models: BTreeMap::new(),
            clock: Arc::new(SystemClock),
            generation: Generation::INITIAL,
            ready: false,
            poison: None,
            data_version: None,
            counters: Counters::default(),
        };
        Self {
            inner: Mutex::new(inner),
            hook: OnceLock::new(),
            recreated: None,
            #[cfg(test)]
            failing_entity_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_entity_reads(&self, count: usize) {
        self.failing_entity_reads.store(count, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn injected_entity_read_failure(&self) -> Option<StoreError> {
        use std::sync::atomic::Ordering;
        self.failing_entity_reads
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1))
            .ok()
            .map(|_| StoreError::Sqlite(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR), None)))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn set_hook(&self, hook: StoreHook) {
        let _ = self.hook.set(hook);
    }

    pub(crate) fn announce(&self, event: StoreEvent<'_>) {
        if let Some(hook) = self.hook.get() {
            hook(event);
        }
    }

    pub fn generation(&self) -> Generation {
        self.lock().generation
    }

    #[cfg(test)]
    pub fn is_ready(&self) -> bool {
        self.lock().ready
    }

    pub fn mark_ready(&self) {
        self.lock().ready = true;
    }

    pub fn counters(&self) -> Counters {
        self.lock().counters.clone()
    }

    #[cfg(test)]
    pub fn apply_failure(&self) -> Option<String> {
        self.lock().poison.clone()
    }

    pub fn read<T>(&self, run: impl FnOnce(&Reader<'_>) -> Result<T, StoreError>) -> Result<T, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        run(&Reader::new(&inner.connection, &inner.models))
    }

    pub fn revision_seq(&self) -> Result<i64, StoreError> {
        self.read(|reader| reader.revision_seq())
    }
}

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod apply_tests;
#[cfg(test)]
mod boot_tests;
#[cfg(test)]
mod file_ops_tests;
#[cfg(test)]
mod maintenance_tests;
#[cfg(test)]
mod membership_tests;
#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod reset_tests;
#[cfg(test)]
mod rows_tests;
#[cfg(test)]
mod shape_tests;
#[cfg(test)]
mod tombstones_tests;
#[cfg(test)]
mod windows_tests;
