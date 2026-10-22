mod error;
mod extract;
mod freshness;
mod invalidate;
mod land;
mod live;
mod load;
mod load_machine;
mod load_owner;
#[cfg(test)]
mod load_machine_tests;
#[cfg(test)]
mod load_owner_tests;
#[cfg(test)]
mod load_machine_oracle_tests;
mod node;
mod normalize;
mod online;
mod registry;
mod state;
#[cfg(test)]
mod state_tests;
#[cfg(test)]
mod registry_tests;
mod resume;
mod run;
mod send;
mod subscribe;
mod upload;
mod variables;

pub use error::SyncError;
pub use load::{DiscardReason, LoadOptions, LoadOutcome, LoadStatus};
pub use registry::{OperationDef, DEFAULT_PAGE_SIZE};
pub use load_machine::{LoadError, LoadPhase, RelationState};
pub use resume::{ResumeReport, Watch};
pub use send::{FlushReport, SendResult, SentOperation, SkipReason};
pub use subscribe::{EventEffect, EventSubscription, SubscriptionDef, SubscriptionStatus};

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::db::Db;
use crate::pacing::{Backoff, Clock, Counters, FetchCoalescing, ResumeDrain, RetryPolicy};
use crate::transport::{Connectivity, Transport};
use crate::upload::UploadConfig;

pub const LIVE_LINGER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub resume: ResumeDrain,
    pub live_linger: Duration,
    pub upload: UploadConfig,
    pub send_backoff: Backoff,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { resume: ResumeDrain::default(), live_linger: LIVE_LINGER, upload: UploadConfig::default(), send_backoff: RetryPolicy::Outbox.backoff().randomized() }
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
    pub waiting: HashMap<String, watch::Receiver<Option<FlightEnd>>>,
}

pub(crate) struct WatchEntry {
    pub model: String,
    pub relation: String,
    pub params: Map<String, Value>,
    pub key: String,
}

pub(crate) struct Inner {
    stopped: watch::Receiver<()>,
    pub db: Db,
    pub transport: Arc<dyn Transport>,
    pub clock: Arc<dyn Clock>,
    pub config: SyncConfig,
    pub operations: Mutex<HashMap<String, Arc<OperationDef>>>,
    pub flights: Mutex<Flights>,
    pub counters: Mutex<Counters>,
    pub landing: Mutex<()>,
    pub issued: AtomicI64,
    pub watches: Mutex<HashMap<u64, WatchEntry>>,
    pub next_watch: AtomicI64,
    pub lives: Mutex<HashMap<String, live::Live>>,
    pub subscription_failures: tokio::sync::broadcast::Sender<SubscriptionFailureNotice>,
    pub owner: Arc<load_owner::LoadOwner>,
    pub outbox_gate: tokio::sync::Mutex<()>,
    pub uploads: Mutex<upload::UploadRuns>,
    pub connectivity: Arc<Connectivity>,
    pub wake: Mutex<Option<tokio::task::AbortHandle>>,
    pub replaced: Mutex<HashMap<String, String>>,
    pub outcomes: Mutex<send::Outcomes>,
    pub final_attempts: Mutex<send::FinalAttempts>,
}

impl Inner {
    pub(crate) fn until_closed(&self, work: impl std::future::Future<Output = ()> + Send + 'static) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut stopped = self.stopped.clone();
        async move {
            tokio::select! {
                biased;
                _ = stopped.changed() => {}
                () = work => {}
            }
        }
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
    _stop: watch::Sender<()>,
    owner: Arc<load_owner::LoadOwner>,
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.owner.submit(load_machine::Event::Close);
    }
}

impl Sync {
    pub fn new(db: Db, transport: Arc<dyn Transport>, clock: Arc<dyn Clock>, config: SyncConfig) -> Self {
        db.set_clock(clock.clone());
        let max_files = config.upload.max_files;
        let (stop, stopped) = watch::channel(());
        let owner = Arc::new(load_owner::LoadOwner::default());
        let resets = Arc::downgrade(&owner);
        db.on_reset(Arc::new(move |models| {
            if let Some(owner) = resets.upgrade() {
                owner.reset(models);
            }
        }));
        let lifetime = Lifetime { _stop: stop, owner: owner.clone() };
        let connectivity = db.connectivity();
        let inner = Inner {
            stopped,
            db,
            transport,
            clock,
            config,
            operations: Mutex::new(HashMap::new()),
            flights: Mutex::new(Flights::default()),
            counters: Mutex::new(Counters::default()),
            landing: Mutex::new(()),
            issued: AtomicI64::new(0),
            watches: Mutex::new(HashMap::new()),
            next_watch: AtomicI64::new(0),
            lives: Mutex::new(HashMap::new()),
            subscription_failures: tokio::sync::broadcast::channel(32).0,
            owner,
            outbox_gate: tokio::sync::Mutex::new(()),
            uploads: Mutex::new(upload::UploadRuns::new(max_files)),
            connectivity,
            wake: Mutex::new(None),
            replaced: Mutex::new(HashMap::new()),
            outcomes: Mutex::new(send::Outcomes::default()),
            final_attempts: Mutex::new(send::FinalAttempts::default()),
        };
        Self { inner: Arc::new(inner), _lifetime: Arc::new(lifetime) }
    }

    pub fn register(&self, def: OperationDef) {
        locked(&self.inner.operations).insert(def.name.clone(), Arc::new(def));
    }

    #[cfg(test)]
    pub fn counters(&self) -> Counters {
        let mut counters = locked(&self.inner.counters).clone();
        counters.lost_outcomes = locked(&self.inner.outcomes).lost();
        counters
    }
}

#[cfg(any(test, feature = "stress"))]
pub mod rig;
#[cfg(feature = "stress")]
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
pub(crate) mod stable_order;
#[cfg(test)]
mod load_more_tests;
#[cfg(test)]
mod scalar_params_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod merge_tests;
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
mod subscribe_tests;
#[cfg(test)]
mod variables_tests;
#[cfg(test)]
mod response_paths_tests;
#[cfg(test)]
mod write_consistency_tests;
