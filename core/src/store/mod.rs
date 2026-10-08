use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use rusqlite::Connection;

use crate::ddl::{column_name, column_type, table_name, ColumnType, RECORD_VERSION};
use crate::identity::Generation;
use crate::pacing::{Clock, Counters, SystemClock};
use crate::schema::ModelSpec;

mod admission;
mod apply;
mod boot;
mod diagnostics;
mod error;
mod file_ops;
mod intake;
mod location;
mod maintenance;
mod membership;
mod open;
mod reset;
mod rows;
mod scopes;
mod tombstones;
mod windows;

pub use admission::{
    admit_destroy, admit_patch, admit_upsert, admitted, drop_count, dropped, next_revision, DestroyAdmission,
    PatchAdmission, RevisionVerdict, RowRevision, UpsertAdmission,
};
pub use apply::PlanCommitError;
pub use boot::{boot_steps, BootConditions, BootReport, BootStep};
pub use diagnostics::{sync_tags, SyncContext, SyncFault};
pub use error::StoreError;
pub use intake::{accepted_rows, row_intake, tombstone_drop_count, IntakeOutcome, IntakeReason, RowPatch};
pub use location::StoreLocation;
pub use maintenance::{MaintenanceReport, TempRowPolicy};
pub use membership::{MembershipChange, MembershipPlan, ScopeRecord};
pub use reset::{ResetMode, ResetPlan, ResetStep};
pub use rows::{HydrationVerdict, Reader, ScopeRowsResult, StoredRow};
pub use tombstones::{SeamDrops, TombstoneFilter, TombstoneRetention, WriteSeam};
pub use windows::{
    family_prefix, window_key, InvalidationRequest, InvalidationScope, PageInfo, Pagination, QueryWindow,
    WindowFreshness, WindowRecord,
};

pub(crate) struct Promoted {
    pub field: String,
    pub column: String,
    pub kind: ColumnType,
}

pub(crate) struct ModelInfo {
    pub table: String,
    pub promoted: Vec<Promoted>,
    pub upsert_sql: String,
    pub spec: ModelSpec,
}

impl ModelInfo {
    pub(crate) fn new(spec: &ModelSpec) -> Self {
        let table = table_name(spec.key.as_str());
        let promoted: Vec<Promoted> = spec
            .fields
            .iter()
            .filter(|field| field.indexed)
            .filter_map(|field| {
                column_type(&field.kind).map(|kind| Promoted {
                    field: field.name.clone(),
                    column: column_name(&field.name),
                    kind,
                })
            })
            .collect();
        let mut columns = vec!["id", "revision", "born_rev", "field_rev", "record_version", "payload"];
        columns.extend(promoted.iter().map(|column| column.column.as_str()));
        let placeholders: Vec<String> = (1..=columns.len()).map(|index| format!("?{index}")).collect();
        let upsert_sql = format!(
            "INSERT OR REPLACE INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            placeholders.join(", ")
        );
        Self { table, promoted, upsert_sql, spec: spec.clone() }
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
    path: Option<PathBuf>,
    hook: OnceLock<StoreHook>,
}

impl Store {
    pub(crate) fn from_connection(connection: Connection, path: Option<PathBuf>) -> Self {
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
        Self { inner: Mutex::new(inner), path, hook: OnceLock::new() }
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

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn generation(&self) -> Generation {
        self.lock().generation
    }

    pub fn is_ready(&self) -> bool {
        self.lock().ready
    }

    pub fn mark_ready(&self) {
        self.lock().ready = true;
    }

    pub fn counters(&self) -> Counters {
        self.lock().counters.clone()
    }

    pub fn apply_failure(&self) -> Option<String> {
        self.lock().poison.clone()
    }

    pub fn record_version(&self) -> i64 {
        RECORD_VERSION
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
mod diagnostics_tests;
#[cfg(test)]
mod file_ops_tests;
#[cfg(test)]
mod intake_tests;
#[cfg(test)]
mod location_tests;
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
mod scopes_tests;
#[cfg(test)]
mod tombstones_tests;
#[cfg(test)]
mod windows_tests;
