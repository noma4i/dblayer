mod error;
mod extract;
mod freshness;
mod invalidate;
mod land;
mod live;
mod load;
mod normalize;
mod registry;
mod resume;
mod send;
mod subscribe;
mod upload;
mod variables;

pub use error::SyncError;
pub use invalidate::ReconnectReport;
pub use load::{DiscardReason, LoadOptions, LoadOutcome, LoadStatus};
pub use registry::{OperationDef, DEFAULT_PAGE_SIZE};
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
use crate::pacing::{Clock, Counters, FetchCoalescing, FreshnessTable, ResumeDrain};
use crate::transport::{Connectivity, Transport};
use crate::upload::UploadConfig;

pub const LIVE_LINGER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub freshness: FreshnessTable,
    pub resume: ResumeDrain,
    pub live_linger: Duration,
    pub upload: UploadConfig,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { freshness: FreshnessTable::default(), resume: ResumeDrain::default(), live_linger: LIVE_LINGER, upload: UploadConfig::default() }
    }
}

pub(crate) type FlightResult = Result<LoadOutcome, SyncError>;

#[derive(Default)]
pub(crate) struct Flights {
    pub coalescing: FetchCoalescing,
    pub waiting: HashMap<String, watch::Receiver<Option<FlightResult>>>,
}

pub(crate) struct WatchEntry {
    pub model: String,
    pub relation: String,
    pub params: Map<String, Value>,
    pub key: String,
}

pub(crate) struct Inner {
    pub db: Db,
    pub transport: Arc<dyn Transport>,
    pub clock: Arc<dyn Clock>,
    pub config: SyncConfig,
    pub operations: Mutex<HashMap<String, OperationDef>>,
    pub flights: Mutex<Flights>,
    pub counters: Mutex<Counters>,
    pub landing: Mutex<()>,
    pub issued: AtomicI64,
    pub watches: Mutex<HashMap<u64, WatchEntry>>,
    pub next_watch: AtomicI64,
    pub lives: Mutex<HashMap<String, live::Live>>,
    pub outbox_gate: tokio::sync::Mutex<()>,
    pub uploads: Mutex<upload::UploadRuns>,
    pub connectivity: Connectivity,
}

impl Inner {
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
}

impl Sync {
    pub fn new(db: Db, transport: Arc<dyn Transport>, clock: Arc<dyn Clock>, config: SyncConfig) -> Self {
        db.set_clock(clock.clone());
        let max_files = config.upload.max_files;
        let inner = Inner {
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
            outbox_gate: tokio::sync::Mutex::new(()),
            uploads: Mutex::new(upload::UploadRuns::new(max_files)),
            connectivity: Connectivity::new(),
        };
        Self { inner: Arc::new(inner) }
    }

    pub fn register(&self, def: OperationDef) {
        locked(&self.inner.operations).insert(def.name.clone(), def);
    }

    pub fn counters(&self) -> Counters {
        locked(&self.inner.counters).clone()
    }
}

#[cfg(test)]
mod rig;

#[cfg(test)]
mod extract_tests;
#[cfg(test)]
mod freshness_tests;
#[cfg(test)]
mod land_tests;
#[cfg(test)]
mod load_more_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod normalize_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod subscribe_tests;
#[cfg(test)]
mod variables_tests;
