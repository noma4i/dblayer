use super::error::TransportError;
use crate::runtime::{Clock, RetryPolicy};
use super::Connectivity;

// GraphQL list cursor errors require restarting pagination from the first page.
pub(crate) const INVALID_CURSOR: &str = "INVALID_CURSOR";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Network,
    Server,
    Retriable,
    Fatal,
}

impl RetryClass {
    pub fn classify(is_network: bool, http_status: Option<u16>, graphql_codes: &[&str]) -> RetryClass {
        if is_network {
            return RetryClass::Network;
        }
        let outcome = match http_status {
            Some(status) if !(200..300).contains(&status) => super::WireOutcome::Status(status),
            _ => super::WireOutcome::Answer { sequence: None, body: serde_json::json!({ "errors": graphql_codes.iter().map(|code| serde_json::json!({ "extensions": { "code": code } })).collect::<Vec<_>>() }) },
        };
        match super::classify_answer(&outcome, false, true) {
            super::AnswerKind::Transient { .. } if http_status.is_some_and(|status| (500..=599).contains(&status)) => RetryClass::Server,
            super::AnswerKind::Transient { .. } => RetryClass::Retriable,
            _ => RetryClass::Fatal,
        }
    }

    pub fn of_error(error: &TransportError) -> RetryClass {
        match error {
            TransportError::Security { .. } | TransportError::Config { .. } => RetryClass::Fatal,
            TransportError::Network(_) => RetryClass::Network,
            TransportError::Http { status, .. } => RetryClass::classify(false, Some(*status), &[]),
            TransportError::Graphql { messages, extensions } => {
                let codes: Vec<_> = extensions.iter().filter_map(|value| value.code.as_deref()).chain(messages.iter().map(String::as_str)).collect();
                RetryClass::classify(false, Some(200), &codes)
            }
            TransportError::GraphqlAnswer { response } => if matches!(response.kind, Some(super::AnswerKind::Transient { .. })) { RetryClass::Retriable } else { RetryClass::Fatal },
            TransportError::RequestTooLarge { .. } | TransportError::Session { .. } | TransportError::Interrupted(_) | TransportError::Decode(_) | TransportError::Unscripted(_) | TransportError::Storage(_) | TransportError::Internal(_) => RetryClass::Fatal,
        }
    }

}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RetryOwner { Bootstrap, Session, Window, Calls, Outbox, Transfer, Feed }

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RetryEntry {
    pub owner: RetryOwner,
    pub kind: crate::runtime::RetryPolicy,
    pub id: u64,
    pub attempt: u32,
    pub delay_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct RetrySnapshot {
    pub waiting: Vec<RetryEntry>,
}

/// The retries currently waiting for their timer.
pub struct RetryCalls(tokio::sync::watch::Sender<RetrySnapshot>);

impl Default for RetryCalls {
    fn default() -> Self { Self(tokio::sync::watch::channel(RetrySnapshot::default()).0) }
}

impl RetryCalls {
    pub fn current(&self) -> RetrySnapshot { self.0.borrow().clone() }

    pub fn watch(&self) -> tokio::sync::watch::Receiver<RetrySnapshot> { self.0.subscribe() }

    pub(crate) fn record(&self, entry: RetryEntry) {
        crate::lock::effect(|| self.0.send_modify(|snapshot| {
            snapshot.waiting.retain(|old| (old.owner, old.id) != (entry.owner, entry.id));
            snapshot.waiting.push(entry);
        }));
    }

    pub(crate) fn leave(&self, owner: RetryOwner, id: u64) {
        crate::lock::effect(|| self.0.send_if_modified(|snapshot| {
            let before = snapshot.waiting.len();
            snapshot.waiting.retain(|entry| (entry.owner, entry.id) != (owner, id));
            snapshot.waiting.len() != before
        }));
    }
}

pub(crate) fn retry_progress() -> &'static std::sync::Arc<RetryCalls> {
    static PROGRESS: std::sync::OnceLock<std::sync::Arc<RetryCalls>> = std::sync::OnceLock::new();
    PROGRESS.get_or_init(|| std::sync::Arc::new(RetryCalls::default()))
}

pub(crate) struct Retrying<'a> {
    clock: &'a dyn crate::runtime::Clock,
    progress: &'a RetryCalls,
    policy: RetryPolicy,
    attempt: u64,
    id: u64,
    online: &'a Connectivity,
}

impl Drop for Retrying<'_> {
    fn drop(&mut self) { self.progress.leave(RetryOwner::Bootstrap, self.id); }
}

impl<'a> Retrying<'a> {
    pub(crate) fn new(clock: &'a dyn Clock, progress: &'a RetryCalls, online: &'a Connectivity, policy: RetryPolicy) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self { clock, progress, online, policy, attempt: 0, id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) }
    }

    pub(crate) async fn failed(&mut self, error: TransportError) -> Result<(), TransportError> {
        if RetryClass::of_error(&error) == RetryClass::Fatal || !self.policy.allows(u32::try_from(self.attempt.saturating_add(2)).unwrap_or(u32::MAX)) { return Err(self.refused(error)); }
        let delay = self.policy.delay(u32::try_from(self.attempt.saturating_add(1)).unwrap_or(u32::MAX));
        self.attempt = self.attempt.saturating_add(1);
        self.progress.record(RetryEntry { owner: RetryOwner::Bootstrap, kind: self.policy, id: self.id, attempt: u32::try_from(self.attempt).unwrap_or(u32::MAX), delay_ms: delay.as_millis() as u64 });
        self.online.online().await;
        self.online.wait(self.clock, delay).await;
        self.progress.leave(RetryOwner::Bootstrap, self.id);
        Ok(())
    }

    fn refused(&mut self, error: TransportError) -> TransportError {
        self.progress.leave(RetryOwner::Bootstrap, self.id);
        error
    }
}

