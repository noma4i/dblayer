use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock};

use rusqlite::Connection;

use crate::commit::CommitResult;
use crate::ddl::{layout, table_name, Layout};
use crate::identity::{Generation, RowRef};
use crate::pacing::{Clock, Counters, SystemClock};
use crate::schema::{ModelSpec, SearchPath};

mod admission;
mod apply;
mod upload;
pub(crate) mod blob;
mod boot;
mod error;
mod file_ops;
pub(crate) mod merge;
mod open;
mod restore;
mod rollback;
#[cfg(test)]
mod restore_tests;
pub(crate) use open::{is_unreadable, remove_database};
pub use open::DroppedRow;
mod readers;
mod references;
pub(crate) mod reads;
mod reset;
pub(crate) use reset::{identity_of, RowOp};
mod rows;
mod response;
pub(crate) mod replace;
mod scopes;
mod search;
mod shape;
mod stable;
pub(crate) mod swap;
mod tombstones;
mod windows;
mod write;

pub use admission::{admit_upsert, UpsertAdmission};
pub use apply::PlanCommitError;
pub(crate) use apply::save_pending;
pub use boot::BootReport;
pub use error::StoreError;
pub use rows::{EdgeRead, Reader, StoredRow};
pub(crate) use response::coalesce_rows;
#[cfg(test)]
pub use rows::ScopeRowsResult;
pub use tombstones::TombstoneRetention;
pub(crate) use tombstones::TombstonePrune;
pub use windows::{family_prefix, window_key, PageInfo, WindowFreshness, WindowRecord};

#[cfg(test)]
type CommitCallback = Box<dyn Fn(&crate::commit::CommitResult) + Send + Sync>;

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
    pub models: Arc<BTreeMap<String, ModelInfo>>,
    pub clock: Arc<dyn Clock>,
    pub generation: Generation,
    pub ready: bool,
    pub poison: Arc<Mutex<Option<String>>>,
    pub data_version: Option<String>,
    pub counters: Counters,
}

impl Inner {
    pub(crate) fn ensure_generation(&self, authority: &crate::lifecycle::authority::Authority, generation: Generation) -> Result<(), StoreError> {
        if let crate::lifecycle::authority::Authority::Admitted(admitted) = authority {
            if !admitted.life().admits_epoch(admitted.value()) { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration)); }
        }
        if generation != self.generation {
            return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration));
        }
        Ok(())
    }

    pub(crate) fn now_ms(&self) -> i64 {
        i64::try_from(self.clock.now().as_millis()).unwrap_or(i64::MAX)
    }

    pub(crate) fn model(&self, key: &str) -> Result<&ModelInfo, StoreError> {
        self.models.get(key).ok_or_else(|| StoreError::UnknownModel(key.to_string()))
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), StoreError> {
        match &*self.poison.lock().unwrap_or_else(|error| error.into_inner()) {
            Some(reason) => Err(StoreError::Poisoned(reason.clone())),
            None => Ok(()),
        }
    }
}

pub(crate) enum StoreEvent<'a> {
    Committed(&'a crate::commit::CommitResult),
    Renewed(&'a [RowRef]),
    Reset { revision: i64, models: Option<Vec<String>>, operations: Vec<crate::operation::QueueOperation> },
}

pub(crate) type StoreHook = Box<dyn Fn(StoreEvent<'_>) + Send + Sync>;
pub(crate) type CommitLog = Box<dyn Fn(&StoreEvent<'_>) + Send + Sync>;

/// Hands out places in the order of the commits, so the hook runs after the writer lock is released and still sees events in commit order.
#[derive(Default)]
struct AnnounceLine {
    issued: AtomicU64,
    next: Mutex<u64>,
    turn: Condvar,
}

impl AnnounceLine {
    fn ticket(&self) -> u64 {
        self.issued.fetch_add(1, Ordering::SeqCst)
    }

    fn run(&self, ticket: u64, announce: impl FnOnce()) {
        struct Done<'a>(&'a AnnounceLine);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                *self.0.next.lock().unwrap_or_else(|error| error.into_inner()) += 1;
                self.0.turn.notify_all();
            }
        }
        let mut next = crate::db::lock::acquire(&self.next);
        if *next != ticket {
            next = crate::blocking::blocking_unit(|| {
                let mut next = next;
                while *next != ticket {
                    next = self.turn.wait(next).unwrap_or_else(|error| error.into_inner());
                }
                next
            });
        }
        drop(next);
        let _done = Done(self);
        announce();
    }
}

/// A finished commit whose announcement has not run yet. Its place in the announcement order is reserved, so a caller that holds a guard can finish the commit, release the guard, and then call `publish`. Dropping it publishes too: later commits wait for this place in the order.
#[must_use]
pub(crate) struct Committed<'a> {
    result: CommitResult,
    turn: Option<(&'a Store, u64)>,
}

impl Committed<'_> {
    fn unannounced(result: CommitResult) -> Self {
        Self { result, turn: None }
    }

    fn event(result: &CommitResult) -> StoreEvent<'_> {
        if result.is_noop() { StoreEvent::Renewed(&result.renewed) } else { StoreEvent::Committed(result) }
    }

    pub(crate) fn result(&self) -> &CommitResult {
        &self.result
    }

    pub(crate) fn publish(mut self) -> CommitResult {
        self.announce();
        let tx_id = self.result.tx_id;
        std::mem::replace(&mut self.result, CommitResult::noop(tx_id))
    }

    fn announce(&mut self) {
        if let Some((store, ticket)) = self.turn.take() {
            store.line.run(ticket, || store.announce(Self::event(&self.result)));
        }
    }
}

impl Drop for Committed<'_> {
    fn drop(&mut self) {
        self.announce();
    }
}

pub struct Store {
    inner: Mutex<Inner>,
    pub(crate) lifecycle: Arc<crate::ffi::admission::Leases>,
    models: RwLock<Arc<BTreeMap<String, ModelInfo>>>,
    poison: Arc<Mutex<Option<String>>>,
    readers: Option<readers::Readers>,
    reads: Arc<reads::Reads>,
    hook: OnceLock<StoreHook>,
    commit_log: OnceLock<CommitLog>,
    newest_operation: std::sync::atomic::AtomicI64,
    committed_revision: Arc<std::sync::atomic::AtomicI64>,
    line: AnnounceLine,
    session_end_replay: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) read_threads: Mutex<Vec<std::thread::ThreadId>>,
    #[cfg(test)]
    pub(crate) require_unlocked_responses: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) before_write: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    pub(crate) after_epoch_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    pub(crate) after_network_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    pub(crate) after_commit: Mutex<Option<CommitCallback>>,
}

impl Store {
    pub(crate) fn from_connection(connection: Connection) -> Self {
        let models = Arc::new(BTreeMap::new());
        let poison = Arc::new(Mutex::new(None));
        let inner = Inner {
            connection,
            models: models.clone(),
            clock: Arc::new(SystemClock),
            generation: Generation::INITIAL,
            ready: false,
            poison: poison.clone(),
            data_version: None,
            counters: Counters::default(),
        };
        Self {
            inner: Mutex::new(inner),
            lifecycle: Arc::default(),
            models: RwLock::new(models),
            poison,
            readers: None,
            reads: reads::Reads::new(),
            hook: OnceLock::new(),
            commit_log: OnceLock::new(),
            newest_operation: std::sync::atomic::AtomicI64::new(0),
            committed_revision: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            line: AnnounceLine::default(),
            #[cfg(test)]
            read_threads: Mutex::new(Vec::new()),
            #[cfg(test)]
            require_unlocked_responses: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            before_write: Mutex::new(None),
            #[cfg(test)]
            after_epoch_check: Mutex::new(None),
            #[cfg(test)]
            after_network_check: Mutex::new(None),
            #[cfg(test)]
            after_commit: Mutex::new(None),
            session_end_replay: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        crate::db::lock::acquire(&self.inner)
    }

    pub(crate) fn set_hook(&self, hook: StoreHook) {
        let _ = self.hook.set(hook);
    }

    pub(crate) fn set_commit_log(&self, log: CommitLog) {
        let _ = self.commit_log.set(log);
    }

    pub(crate) fn announce(&self, event: StoreEvent<'_>) {
        #[cfg(test)]
        if let (StoreEvent::Committed(result), Some(probe)) = (&event, self.after_commit.lock().unwrap_or_else(|error| error.into_inner()).as_ref()) {
            probe(result);
        }
        if let Some(hook) = self.hook.get() {
            hook(event);
        }
    }

    fn commit_announcement(&self, inner: MutexGuard<'_, Inner>, result: CommitResult) -> Committed<'_> {
        if let Some(revision) = result.revision { self.committed_revision.store(revision, std::sync::atomic::Ordering::Release); }
        let renewal = result.is_noop();
        if renewal && result.renewed.is_empty() {
            return Committed::unannounced(result);
        }
        if let Some(log) = self.commit_log.get() {
            log(&Committed::event(&result));
        }
        let ticket = self.line.ticket();
        drop(inner);
        Committed { result, turn: Some((self, ticket)) }
    }

    /// The generation the boot opened the database with: the envelopes of the writes name it, and a database that boots in a later generation must accept them.
    pub(crate) fn adopt_generation(&self, generation: Generation) { self.lock().generation = generation; }

    pub fn generation(&self) -> Generation {
        self.lifecycle.snapshot().generation
    }

    #[cfg(test)]
    pub fn is_ready(&self) -> bool {
        self.lock().ready
    }

    pub fn mark_ready(&self) {
        self.lock().ready = true;
    }

    #[cfg(test)]
    pub fn writer_is_free(&self) -> bool {
        self.inner.try_lock().is_ok()
    }

    pub fn counters(&self) -> Counters {
        self.lock().counters.clone()
    }

    #[cfg(test)]
    pub fn apply_failure(&self) -> Option<String> {
        self.poison.lock().unwrap_or_else(|error| error.into_inner()).clone()
    }

    fn needs_writer(models: &std::collections::BTreeMap<String, ModelInfo>, required: &[&str]) -> bool {
        if required.is_empty() {
            models.values().any(|model| model.spec.is_memory())
        } else {
            required.iter().any(|key| models.get(*key).is_some_and(|model| model.spec.is_memory()))
        }
    }

    fn reads_off_writer(&self) -> bool {
        let models = self.models.read().unwrap_or_else(|error| error.into_inner());
        self.readers.is_some() && !Self::needs_writer(&models, &[])
    }

    pub(crate) fn reads(&self) -> &Arc<reads::Reads> { &self.reads }

    pub fn read<T>(&self, required: &[&str], run: impl FnOnce(&Reader<'_>) -> Result<T, StoreError>) -> Result<T, StoreError> {
        let _admitted = self.reads.admit().ok_or(StoreError::Refused)?;
        let models = self.models.read().unwrap_or_else(|error| error.into_inner());
        if let Some(readers) = self.readers.as_ref().filter(|_| !Self::needs_writer(&models, required)) {
            let connection = readers.lease();
            if let Some(reason) = &*self.poison.lock().unwrap_or_else(|error| error.into_inner()) {
                return Err(StoreError::Poisoned(reason.clone()));
            }
            let transaction = connection.unchecked_transaction()?;
            #[cfg(test)]
            self.read_threads.lock().unwrap_or_else(|error| error.into_inner()).push(std::thread::current().id());
            let result = run(&Reader::new(&transaction, &models))?;
            transaction.commit()?;
            return Ok(result);
        }
        let inner = self.lock();
        inner.ensure_healthy()?;
        #[cfg(test)]
        self.read_threads.lock().unwrap_or_else(|error| error.into_inner()).push(std::thread::current().id());
        run(&Reader::new(&inner.connection, &inner.models))
    }

    pub fn revision_seq(&self) -> Result<i64, StoreError> {
        self.read(&[], |reader| reader.revision_seq())
    }

    pub(crate) fn published_revision(&self) -> Arc<std::sync::atomic::AtomicI64> { self.committed_revision.clone() }

    pub(crate) fn publish_revision(&self, revision: i64) { self.committed_revision.store(revision, std::sync::atomic::Ordering::Release); }
}

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod apply_tests;
#[cfg(test)]
mod batch_tests;
#[cfg(test)]
mod members_among_tests;
#[cfg(test)]
mod boot_tests;
#[cfg(test)]
mod write_tests;
#[cfg(test)]
mod file_ops_tests;
#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod readers_tests;
#[cfg(test)]
mod reads_tests;
#[cfg(test)]
mod hook_tests;
#[cfg(test)]
mod reset_tests;
#[cfg(test)]
mod rows_tests;
#[cfg(test)]
mod shape_tests;
#[cfg(test)]
mod value_tests;
#[cfg(test)]
mod union_tests;
#[cfg(test)]
mod outbox_storage_tests;
#[cfg(test)]
mod cache_storage_tests;
#[cfg(test)]
mod tombstones_tests;
#[cfg(test)]
mod tombstones_pin_tests;
#[cfg(test)]
mod windows_tests;
#[cfg(test)]
mod interface_tests;
