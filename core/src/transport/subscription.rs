use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;

use super::request::Response;

#[derive(Clone)]
pub struct SubscriptionHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    cancelled: AtomicBool,
    on_cancel: Box<dyn Fn() + Send + Sync>,
}

impl SubscriptionHandle {
    pub fn new(on_cancel: impl Fn() + Send + Sync + 'static) -> Self {
        Self { inner: Arc::new(HandleInner { cancelled: AtomicBool::new(false), on_cancel: Box::new(on_cancel) }) }
    }

    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            (self.inner.on_cancel)();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionFailureKind {
    Transport,
    Graphql,
    Internal,
}

impl SubscriptionFailureKind {
    pub fn fault(self) -> SubscriptionFault {
        match self {
            SubscriptionFailureKind::Transport => SubscriptionFault::Transient,
            SubscriptionFailureKind::Graphql => SubscriptionFault::Graphql,
            SubscriptionFailureKind::Internal => SubscriptionFault::Internal,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionFailure {
    pub kind: SubscriptionFailureKind,
    pub message: String,
    pub details: Option<String>,
}

impl SubscriptionFailure {
    pub fn new(kind: SubscriptionFailureKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), details: None }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionEvent {
    Ready,
    Next(Response),
    Failed(SubscriptionFailure),
}

pub struct Subscription {
    events: UnboundedReceiver<SubscriptionEvent>,
    handle: SubscriptionHandle,
}

impl Subscription {
    pub fn new(events: UnboundedReceiver<SubscriptionEvent>, handle: SubscriptionHandle) -> Self {
        Self { events, handle }
    }

    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        self.events.recv().await
    }

    pub fn cancel(&self) {
        self.handle.cancel();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionFault {
    Transient,
    Graphql,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRetry {
    pub transient_limit: u32,
    pub attempts: u32,
    pub reported_faults: HashSet<SubscriptionFault>,
}

impl Default for SubscriptionRetry {
    fn default() -> Self {
        Self { transient_limit: 4, attempts: 0, reported_faults: HashSet::new() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOutcome {
    pub state: SubscriptionRetry,
    pub reports_degraded: bool,
}

impl SubscriptionRetry {
    pub fn record_failure(&self, fault: SubscriptionFault) -> SubscriptionOutcome {
        let already_reported = self.reported_faults.contains(&fault);
        let mut state = self.clone();
        let reports_degraded = if fault == SubscriptionFault::Transient {
            state.attempts += 1;
            state.attempts == state.transient_limit && !already_reported
        } else {
            !already_reported
        };
        if reports_degraded {
            state.reported_faults.insert(fault);
        }
        SubscriptionOutcome { state, reports_degraded }
    }

    pub fn confirmed(&self) -> SubscriptionRetry {
        SubscriptionRetry { transient_limit: self.transient_limit, ..Default::default() }
    }
}
