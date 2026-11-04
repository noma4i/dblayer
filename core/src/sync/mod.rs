mod entity_fetch;
#[cfg(test)]
pub(crate) type FailureProbe = Box<dyn Fn(bool) + Send>;

#[cfg(test)]
mod dsl_tests;
mod error;
mod extract;
mod freshness;
mod invalidate;
pub(crate) use invalidate::{WindowEvent, WindowMark};
mod land;
mod live;
mod load;
mod load_all;
mod load_machine;
mod load_owner;
#[cfg(test)]
mod load_machine_tests;
#[cfg(test)]
mod load_phase_tests;
#[cfg(test)]
mod load_owner_tests;
#[cfg(test)]
mod load_forget_tests;
#[cfg(test)]
mod load_owner_panic_tests;
#[cfg(test)]
mod load_wake_tests;
#[cfg(test)]
mod load_machine_oracle_tests;
#[cfg(test)]
mod load_wiring_tests;
mod model_event;
mod media;
#[cfg(test)]
mod media_tests;
mod node;
mod normalize;
mod online;
mod preload;
#[cfg(test)]
mod preload_tests;
#[cfg(test)]
mod lifecycle_tests;
mod registry;
mod state;
#[cfg(test)]
mod state_tests;
#[cfg(test)]
mod registry_tests;
mod resume;
mod resume_machine;
mod run;
mod send;
mod subscribe;
mod upload;
mod variables;

pub use entity_fetch::{EntityFetch, RefreshError, RefreshSink};
pub use error::SyncError;
pub use load::{DiscardReason, LoadOptions, LoadOutcome, LoadStatus};
pub use registry::{OperationDef, DEFAULT_PAGE_SIZE};
pub use load_machine::{LoadError, LoadPhase, RelationState};
pub use load_owner::RelationWatch;
pub use preload::PreloadFailure;
pub(crate) use preload::FirstScreenHold;
pub(crate) use load_machine::snapshot_phase;
pub use model_event::{ModelEvent, ModelEvents};
pub(crate) use model_event::EventReader;
pub use resume::{ResumeReport, Watch};
pub(crate) use resume::Failures;
pub use send::{FlushReport, SendResult, SentOperation, SkipReason};
pub use upload::{ItemKey, ItemOutcome, ItemReceipt, OperationOutcome, OperationReceipt};
pub(crate) use send::{DeliveryEffects, FinishedOperation, OutcomeTicket, Outcomes};
pub use subscribe::{EventEffect, Updates, SubscriptionDef, SubscriptionStatus};

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::watch;

use crate::db::Db;
use crate::pacing::{Backoff, Clock, Counters, FetchCoalescing, ResumeDrain, RetryPolicy};
use crate::transport::{Connectivity, Transport};
use crate::upload::{UploadConfig, UploadOperations};

pub const LIVE_LINGER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub resume: ResumeDrain,
    pub live_linger: Duration,
    pub upload: UploadConfig,
    pub uploads: Option<UploadOperations>,
    pub send_backoff: Backoff,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { resume: ResumeDrain::default(), live_linger: LIVE_LINGER, upload: UploadConfig::default(), uploads: None, send_backoff: RetryPolicy::Outbox.backoff().randomized() }
    }
}

pub(crate) type FlightResult = Result<LoadOutcome, SyncError>;

#[derive(Clone)]
pub(crate) enum FlightEnd {
    Settled(FlightResult),
    Abandoned,
}
pub(crate) type SubscriptionFailureNotice = Option<(String, crate::transport::SubscriptionFailure)>;

#[derive(Default)]
pub(crate) struct Flights {
    pub coalescing: FetchCoalescing,
    pub started: u64,
    pub waiting: HashMap<String, watch::Sender<Option<FlightEnd>>>,
    pub node_rereads: std::collections::HashSet<String>,
    pub owners: HashMap<String, (u64, crate::identity::Generation)>,
    pub interest: HashMap<String, usize>,
    pub tasks: HashMap<String, (u64, tokio::task::AbortHandle)>,
    pub pending: usize,
    #[cfg(test)]
    pub joined: JoinedLoads,
}

type Abandoned = (tokio::task::AbortHandle, Option<watch::Sender<Option<FlightEnd>>>);

impl Flights {
    /// Stops the flight of a key that no caller wants: it leaves the table at once, so the next caller starts a new flight instead of joining the stopped one.
    fn abandon(&mut self, key: &str) -> Option<Abandoned> {
        let (token, task) = self.tasks.remove(key)?;
        let mut waiting = None;
        if self.owners.get(key).is_some_and(|(owner, _)| *owner == token) {
            self.owners.remove(key);
            self.coalescing.finish(key);
            self.node_rereads.remove(key);
            waiting = self.waiting.remove(key);
        }
        Some((task, waiting))
    }

    /// The flights no caller wants, once no caller waits to name its key.
    fn unwanted(&mut self) -> Vec<Abandoned> {
        if self.pending > 0 { return Vec::new(); }
        let keys: Vec<String> = self.tasks.keys().filter(|key| self.interest.get(*key).is_none_or(|count| *count == 0)).cloned().collect();
        keys.iter().filter_map(|key| self.abandon(key)).collect()
    }
}

#[cfg(test)]
pub(crate) struct JoinedLoads(pub watch::Sender<usize>);

#[cfg(test)]
impl Default for JoinedLoads {
    fn default() -> Self { Self(watch::channel(0).0) }
}

pub(crate) struct Inner {
    stopped: watch::Receiver<bool>,
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
    registered: std::sync::atomic::AtomicBool,
    this: std::sync::OnceLock<std::sync::Weak<Inner>>,
    pub db: Db,
    pub transport: Arc<dyn Transport>,
    pub clock: Arc<dyn Clock>,
    pub config: SyncConfig,
    pub operations: Mutex<HashMap<String, Arc<OperationDef>>>,
    pub change_feed: Option<(String, crate::schema::ChangeFeed)>,
    pub change_targets: BTreeMap<String, BTreeMap<String, Vec<crate::schema::ChangeRead>>>,
    pub flights: Mutex<Flights>,
    pub counters: Mutex<Counters>,
    pub landing: Mutex<()>,
    pub issued: AtomicI64,
    pub issued_floor: std::sync::OnceLock<Result<i64, String>>,
    pub(crate) upload_housekeeping_due: std::sync::atomic::AtomicBool,
    pub(crate) upload_orphan_gate: tokio::sync::Mutex<()>,
    pub lives: Mutex<HashMap<String, live::Live>>,
    pub(crate) live_effects: crate::db::dispatch::OrderedEffects,
    pub subscription_failures: tokio::sync::broadcast::Sender<SubscriptionFailureNotice>,
    pub(crate) model_events: Mutex<Vec<(u64, std::sync::Weak<dyn model_event::EventReader>)>>,
    pub owner: Arc<load_owner::LoadOwner>,
    pub outbox_gate: tokio::sync::Mutex<()>,
    pub queue: Result<Arc<Mutex<crate::operation::OperationQueue>>, String>,
    /// The newest operation the outbox held when this database opened. A pending action that is not durable and is this old or older does not survive the open.
    pub(crate) reopened_through: i64,
    /// The identity of the session now and the newest pending operation when another identity took its place: an operation of that age belongs to the account that left and never goes out.
    pub(crate) account: Arc<Mutex<(Option<String>, i64)>>,
    /// The newest pending operation when the network was last lost: an action that is not a durable command and is not newer than it never goes out.
    pub(crate) network_lost: Arc<Mutex<i64>>,
    pub(crate) network_line: crate::commit::NetworkLine,
    #[cfg(test)]
    pub(crate) after_network_change: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// The operations that waited for a retry when the network came back. They do not wait for their backoff once: the first attempt after the return takes them out, and a second failure waits by the normal backoff.
    pub(crate) backoff_released: Mutex<std::collections::BTreeSet<i64>>,
    /// The row the answer of an operation carried at its root, by operation: the call that waits for the answer returns it.
    pub uploads: Mutex<upload::UploadRuns>,
    pub(crate) upload_effects: crate::db::dispatch::OrderedEffects,
    pub(crate) file_tasks: Arc<tokio::sync::watch::Sender<usize>>,
    pub connectivity: Arc<Connectivity>,
    pub(crate) wake: Mutex<WakeSlot>,
    pub(crate) resume: resume::ResumeOwner,
    pub(crate) preload: preload::PreloadOwner,
    pub(crate) entity_due: Mutex<Vec<entity_fetch::DueRefresh>>,
    pub outcomes: Arc<Mutex<send::Outcomes>>,
    #[cfg(test)]
    pub(crate) failure_probe: Mutex<Option<FailureProbe>>,
    #[cfg(test)]
    pub(crate) refusal_probe: Mutex<Option<Box<dyn Fn() + Send + std::marker::Sync>>>,
    pub final_attempts: Mutex<send::FinalAttempts>,
}

/// The retry wake of the outbox while it sleeps. A wake that woke up leaves the slot before its pass, so a newer wake never cancels a pass that runs.
#[derive(Default)]
pub(crate) struct WakeSlot {
    generation: u64,
    sleeping: Option<(u64, tokio::task::AbortHandle)>,
}

impl WakeSlot {
    pub(crate) fn take_sleeping(&mut self) -> Option<tokio::task::AbortHandle> {
        self.sleeping.take().map(|(_, handle)| handle)
    }
}

impl Inner {
    pub(crate) fn until_closed(&self, work: impl std::future::Future<Output = ()> + Send + 'static) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut stopped = self.stopped.clone();
        async move {
            tokio::select! {
                biased;
                _ = stopped.wait_for(|stopped| *stopped) => {}
                () = work => {}
            }
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        *self.stopped.borrow() || self.stopped.has_changed().is_err()
    }

    pub(crate) fn now_ms(&self) -> i64 {
        i64::try_from(self.clock.now().as_millis()).unwrap_or(i64::MAX)
    }
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
pub struct Sync {
    pub(crate) inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}

struct Lifetime {
    lifecycle: Arc<crate::lifecycle::Lifecycle>,
    closer: Arc<Closer>,
}

/// Stops the background tasks and the window machines when the database lifecycle closes. The lifecycle is the only place that decides a close.
struct Closer {
    stop: watch::Sender<bool>,
    owner: Arc<load_owner::LoadOwner>,
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
}

impl crate::lifecycle::Holder for Closer {
    fn close(&self) {
        crate::db::lock::effect(|| self.stop.send_replace(true));
        self.owner.submit(load_machine::Event::Close);
    }

    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        let _keep = &self.closer;
        self.lifecycle.close();
    }
}

#[derive(Clone)]
pub(crate) struct WeakSync {
    inner: std::sync::Weak<Inner>,
    lifetime: std::sync::Weak<Lifetime>,
}

impl WeakSync {
    pub(crate) fn upgrade(&self) -> Option<Sync> {
        let sync = Sync { inner: self.inner.upgrade()?, _lifetime: self.lifetime.upgrade()? };
        (!sync.is_closed()).then_some(sync)
    }
}

impl Sync {
    pub fn new(db: Db, transport: Arc<dyn Transport>, clock: Arc<dyn Clock>, config: SyncConfig) -> Self {
        db.set_clock(clock.clone());
        let outcomes = db.shared.outcomes.clone();
        db.shared.blob_failed_ttl_ms.store(config.upload.failed_ttl.as_millis().try_into().unwrap_or(i64::MAX), std::sync::atomic::Ordering::Relaxed);
        let queue = db.operation_queue().map_err(|error| error.to_string());
        let reopened_through = queue.as_ref().ok().and_then(|queue| locked(queue).pending.iter().next_back().copied()).unwrap_or(i64::MIN);
        let max_files = config.upload.max_files;
        let (stop, stopped) = watch::channel(false);
        let owner = Arc::new(load_owner::LoadOwner::default());
        let closer = Arc::new(Closer { stop, owner: owner.clone(), registration: std::sync::OnceLock::new() });
        let lifetime = Lifetime { lifecycle: db.shared.store.lifecycle.clone(), closer: closer.clone() };
        db.shared.store.lifecycle.enroll(&closer);
        let connectivity = db.connectivity();
        let change_feed = db.shared.specs.values().find_map(|spec| spec.change_feed.as_ref().map(|feed| (spec.key.to_string(), feed.clone())));
        let mut change_targets: BTreeMap<String, BTreeMap<String, Vec<crate::schema::ChangeRead>>> = BTreeMap::new();
        for spec in db.shared.specs.values() {
            for read in &spec.changes {
                change_targets.entry(read.area.clone()).or_default().entry(spec.key.to_string()).or_default().push(read.clone());
            }
        }
        let inner = Inner {
            stopped,
            registration: std::sync::OnceLock::new(),
            registered: std::sync::atomic::AtomicBool::new(false),
            this: std::sync::OnceLock::new(),
            db,
            transport,
            clock,
            config,
            operations: Mutex::new(HashMap::new()),
            change_feed,
            change_targets,
            flights: Mutex::new(Flights::default()),
            counters: Mutex::new(Counters::default()),
            landing: Mutex::new(()),
            issued: AtomicI64::new(0),
            issued_floor: std::sync::OnceLock::new(),
            upload_housekeeping_due: std::sync::atomic::AtomicBool::new(true),
            upload_orphan_gate: tokio::sync::Mutex::new(()),
            lives: Mutex::new(HashMap::new()),
            live_effects: crate::db::dispatch::OrderedEffects::default(),
            subscription_failures: tokio::sync::broadcast::channel(32).0,
            model_events: Mutex::new(Vec::new()),
            owner,
            outbox_gate: tokio::sync::Mutex::new(()),
            queue,
            reopened_through,
            account: Arc::new(Mutex::new((None, i64::MIN))),
            network_lost: Arc::new(Mutex::new(i64::MIN)),
            network_line: crate::commit::NetworkLine::default(),
            #[cfg(test)]
            after_network_change: Mutex::new(None),
            backoff_released: Mutex::new(std::collections::BTreeSet::new()),
            uploads: Mutex::new(upload::UploadRuns::new(max_files)),
            upload_effects: crate::db::dispatch::OrderedEffects::default(),
            file_tasks: Arc::new(tokio::sync::watch::channel(0).0),
            connectivity,
            wake: Mutex::new(WakeSlot::default()),
            resume: resume::ResumeOwner::new(),
            preload: preload::PreloadOwner::default(),
            entity_due: Mutex::new(Vec::new()),
            outcomes,
            #[cfg(test)]
            failure_probe: Mutex::new(None),
            #[cfg(test)]
            refusal_probe: Mutex::new(None),
            final_attempts: Mutex::new(send::FinalAttempts::default()),
        };
        let this = Self { inner: Arc::new(inner), _lifetime: Arc::new(lifetime) };
        let preloads = Arc::downgrade(&this.inner);
        this.inner.preload.attach(preloads.clone());
        let _ = this.inner.this.set(preloads.clone());
        this.inner.owner.on_reload(Arc::new(move |target, since| { if let Some(inner) = preloads.upgrade() { inner.spawn_retry(target, load_machine::LoadKind::Refresh, Some(since)); } }));
        if let Ok(session) = this.inner.transport.session() {
            if let Ok(admitted) = this.inner.db.admit() {
                let _ = this.inner.db.shared.store.lifecycle.session_from(session, admitted);
            }
        }
        locked(&this.inner.account).0 = this.inner.db.shared.store.lifecycle.snapshot().session;
        this.inner.db.shared.store.lifecycle.enroll(&this.inner);
        this
    }

    pub fn close(&self) -> bool {
        self._lifetime.lifecycle.close()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self._lifetime.lifecycle.snapshot().closed
    }

    pub(crate) fn downgrade(&self) -> WeakSync {
        WeakSync { inner: Arc::downgrade(&self.inner), lifetime: Arc::downgrade(&self._lifetime) }
    }

    pub fn register(&self, mut def: OperationDef) {
        if let Some(root) = self.inner.config.uploads.as_ref().and_then(|uploads| uploads.root_of(&def.name)) { def.root_path = root.to_string(); }
        locked(&self.inner.operations).insert(def.name.clone(), Arc::new(def));
    }

    #[cfg(test)]
    pub fn counters(&self) -> Counters {
        locked(&self.inner.counters).clone()
    }
}

impl crate::lifecycle::Holder for Inner {
    fn close(&self) {}

    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }

    fn changed(&self, snapshot: &crate::lifecycle::Snapshot) {
        let Some(inner) = self.this.get().and_then(std::sync::Weak::upgrade) else { return };
        match &snapshot.change {
            crate::lifecycle::Change::Cleared(models) => {
                self.owner.reset(models.as_deref());
                self.reset_live_ready();
                self.reopen_live();
                inner.drop_due_refreshes(models.as_deref());
                if models.is_none() { inner.end_uploads_of_reset(); }
                inner.preload_on_event();
            }
            crate::lifecycle::Change::SessionStarted(_) | crate::lifecycle::Change::SessionEnded => {
                if matches!(snapshot.change, crate::lifecycle::Change::SessionEnded) { inner.cancel_uploads(); }
                inner.preload_session();
                inner.account_changed(snapshot.session.as_deref());
                if snapshot.session.is_some() {
                    inner.refresh_due_entities(true);
                    inner.preload_on_event();
                    if let Ok(admitted) = self.db.shared.store.lifecycle.admit_session_at(crate::lifecycle::Origin::Session, "resume", snapshot.session_epoch) {
                        let _ = inner.request_resume(&admitted, resume_machine::Cause::Explicit, None);
                    }
                    inner.spawn_flush(Duration::ZERO);
                }
            }
            _ => {}
        }
    }
}

#[cfg(any(test, feature = "stress"))]
pub mod rig;
#[cfg(feature = "stress")]
#[allow(clippy::disallowed_methods)]
pub mod stress;

#[cfg(test)]
mod edge_tests;

#[cfg(test)]
mod extract_tests;
#[cfg(test)]
mod freshness_tests;
#[cfg(test)]
mod invariants;
#[cfg(test)]
mod land_tests;
#[cfg(test)]
mod thread_window_tests;
#[cfg(test)]
mod row_rules_tests;
#[cfg(test)]
pub(crate) mod stable_order;
#[cfg(test)]
mod load_all_tests;
#[cfg(test)]
mod load_more_tests;
#[cfg(test)]
mod scalar_params_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod merge_tests;
#[cfg(test)]
mod list_merge_tests;
#[cfg(test)]
mod members_tests;
#[cfg(test)]
mod members_order_tests;
#[cfg(test)]
mod total_from_tests;
#[cfg(test)]
mod enum_scope_tests;
#[cfg(test)]
mod node_tests;
#[cfg(test)]
mod entity_fetch_tests;
#[cfg(test)]
mod generation_tests;
#[cfg(test)]
mod opponent_null_tests;
#[cfg(test)]
mod null_object_tests;
#[cfg(test)]
mod normalize_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod change_source_tests;
#[cfg(test)]
mod event_payload_tests;
#[cfg(test)]
mod subscribe_tests;
#[cfg(test)]
mod model_event_tests;
#[cfg(test)]
mod subscribe_clock_tests;
#[cfg(test)]
mod variables_tests;
#[cfg(test)]
mod response_paths_tests;
#[cfg(test)]
mod write_consistency_tests;
#[cfg(test)]
mod blocking_tests;
