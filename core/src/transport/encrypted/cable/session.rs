use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use crate::transport::{GraphQLRequest, SessionCode, Subscription, SubscriptionClose, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind, SubscriptionHandle};
use super::CableError;
use crate::pacing::{Backoff, Clock, RetryPolicy};
use crate::transport::{SubscriptionRetry, SubscriptionFault};

struct Entry {
    request: GraphQLRequest,
    events: mpsc::UnboundedSender<SubscriptionEvent>,
}

pub struct CableSession {
    entries: BTreeMap<String, Entry>,
    next_id: u64,
    cancellations: mpsc::UnboundedReceiver<String>,
    cancel: mpsc::UnboundedSender<String>,
    clock: Arc<dyn Clock>,
    last_traffic: Duration,
    pong_due: Option<Duration>,
    ping_ts: u64,
    opened: bool,
    retry: SubscriptionRetry,
    backoff: Backoff,
    terminal: Option<SubscriptionFailure>,
}

impl CableSession {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let (cancel, cancellations) = mpsc::unbounded_channel();
        let backoff = RetryPolicy::Subscription.backoff().randomized();
        Self { entries: BTreeMap::new(), next_id: 0, cancellations, cancel, last_traffic: clock.now(), clock, pong_due: None, ping_ts: 0, opened: false, retry: SubscriptionRetry::default(), backoff, terminal: None }
    }

    pub fn subscribe(&mut self, request: GraphQLRequest) -> (String, Subscription) {
        self.next_id += 1;
        let id = self.next_id.to_string();
        let (events, receiver) = mpsc::unbounded_channel();
        if let Some(failure) = &self.terminal {
            let _ = events.send(SubscriptionEvent::Failed(failure.clone()));
        } else {
            self.entries.insert(id.clone(), Entry { request, events });
        }
        let cancel = self.cancel.clone();
        let token = id.clone();
        let subscription = Subscription::new(receiver, SubscriptionHandle::new(move || { let _ = cancel.send(token.clone()); }));
        (id, subscription)
    }

    pub fn ready(&mut self) -> Vec<Value> {
        self.opened = true;
        self.pong_due = None;
        self.traffic();
        self.entries.keys().filter_map(|id| self.frame(id)).collect()
    }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn resume(&mut self) { self.terminal = None; }

    pub fn refuse(&mut self, failure: SubscriptionFailure) {
        self.terminal = Some(failure.clone());
        for (_, entry) in std::mem::take(&mut self.entries) {
            let _ = entry.events.send(SubscriptionEvent::Failed(failure.clone()));
        }
    }

    pub fn frame(&self, id: &str) -> Option<Value> {
        self.entries.get(id).map(|entry| json!({
            "t":"subscribe", "id":id, "query":entry.request.document,
            "variables":entry.request.variables, "operationName":entry.request.operation_name,
        }))
    }

    pub fn unsubscribe(&mut self, id: &str) -> Option<Value> {
        self.entries.remove(id).map(|_| json!({"t":"unsubscribe", "id":id}))
    }

    pub async fn cancelled(&mut self) -> Option<Value> {
        let id = self.cancellations.recv().await?;
        self.unsubscribe(&id)
    }

    pub fn traffic(&mut self) { self.last_traffic = self.clock.now(); }

    pub(super) fn now(&self) -> Duration { self.clock.now() }

    pub fn failed_open(&mut self, online: bool) -> Option<Duration> {
        if !online || self.is_empty() { return None; }
        self.retry = self.retry.record_failure(SubscriptionFault::Transient).state;
        Some(self.backoff.delay(self.retry.attempts.saturating_sub(1)))
    }

    pub fn closed(&mut self, code: u16, reason: &str) -> SubscriptionClose {
        self.opened = false;
        self.pong_due = None;
        let session = match (code, reason) {
            (_, "LOGGED_OUT") => Some(SessionCode::SessionEnded),
            (4401, _) => Some(SessionCode::Unauthenticated),
            (4403, _) | (_, "ACCOUNT_LOCKED") => Some(SessionCode::AccountUnavailable),
            _ => None,
        };
        let close = SubscriptionClose { code, terminal: session.is_some(), session };
        if let Some(code) = &close.session {
            let mut failure = SubscriptionFailure::from_error(crate::transport::TransportError::Session { code: *code, detail: reason.into() });
            failure.close = Some(close.clone());
            self.refuse(failure);
        }
        close
    }

    pub fn deadline(&self) -> Option<Duration> {
        (self.opened && !self.entries.is_empty()).then(|| self.pong_due.unwrap_or(self.last_traffic + Duration::from_secs(25)))
    }

    pub fn tick(&mut self) -> Result<Option<Value>, CableError> {
        let Some(deadline) = self.deadline() else { return Ok(None) };
        if self.clock.now() < deadline { return Ok(None); }
        if self.pong_due.is_some() {
            self.opened = false;
            return Err(CableError::Closed);
        }
        self.pong_due = Some(self.clock.now() + Duration::from_secs(20));
        self.ping_ts = self.ping_ts.wrapping_add(1);
        Ok(Some(json!({"t":"ping", "ts":self.ping_ts})))
    }

    pub fn receive(&mut self, body: Value) -> Result<(), CableError> {
        if body["t"] == "pong" {
            if self.pong_due.is_none() || body["ts"].as_u64() != Some(self.ping_ts) { return Ok(()); }
            self.pong_due = None;
            self.retry = self.retry.confirmed();
            self.traffic();
            return Ok(());
        }
        let id = body["id"].as_str().ok_or(CableError::Frame)?;
        let Some(entry) = self.entries.get(id) else { return Ok(()) };
        match body["t"].as_str() {
            Some("subscribed") => {
                let _ = entry.events.send(SubscriptionEvent::Ready);
                self.retry = self.retry.confirmed();
            }
            Some("next") => {
                let response: crate::transport::Response = serde_json::from_value(body["result"].clone()).map_err(|_| CableError::Frame)?;
                if response.shape() == crate::transport::ResponseShape::Empty { return Err(CableError::Frame); }
                let _ = entry.events.send(SubscriptionEvent::response(response));
                self.pong_due = None;
                self.retry = self.retry.confirmed();
            }
            Some("error") => {
                let code = body["code"].as_str().ok_or(CableError::Frame)?.to_string();
                let error = crate::transport::TransportError::Graphql { messages: vec![code.clone()], extensions: Some(crate::transport::GraphqlExtensions { code: Some(code), transaction_disposition: None, fields: Default::default() }) };
                let mut failure = SubscriptionFailure::from_error(error);
                if body["retryable"] == true { failure.kind = SubscriptionFailureKind::Transport; }
                let _ = entry.events.send(SubscriptionEvent::Failed(failure));
                self.entries.remove(id);
            }
            Some("complete") => { self.entries.remove(id); }
            _ => return Err(CableError::Frame),
        }
        self.traffic();
        Ok(())
    }
}
