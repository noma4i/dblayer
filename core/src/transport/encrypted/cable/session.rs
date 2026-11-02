use std::sync::Arc;
use std::time::Duration;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use crate::transport::{Connectivity, GraphQLRequest, ServerSubscription, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind, ServerSubscriptionHandle};
use super::CableError;
use super::lifecycle::{Entry, Event, Heard, Lifecycle};
use crate::pacing::Clock;

pub struct CableSession {
    lifecycle: Lifecycle,
    next_id: u64,
    cancellations: mpsc::UnboundedReceiver<String>,
    cancel: mpsc::UnboundedSender<String>,
}

impl CableSession {
    pub fn new(clock: Arc<dyn Clock>, online: Arc<Connectivity>) -> Self {
        let (cancel, cancellations) = mpsc::unbounded_channel();
        Self { lifecycle: Lifecycle::new(clock, online), next_id: 0, cancellations, cancel }
    }

    pub fn subscribe(&mut self, request: GraphQLRequest) -> (String, ServerSubscription) {
        self.next_id += 1;
        let id = self.next_id.to_string();
        let (events, receiver) = mpsc::unbounded_channel();
        if let Some(failure) = self.lifecycle.refusal() {
            crate::db::lock::effect(|| { let _ = events.send(SubscriptionEvent::Failed(failure.clone())); });
        } else {
            self.lifecycle.core.entries.insert(id.clone(), Entry { request, events });
            self.lifecycle.handle(Event::Demand);
            if self.lifecycle.is_open() {
                let frame = self.lifecycle.core.frame(&id);
                self.lifecycle.core.frames.extend(frame);
            }
        }
        let cancel = self.cancel.clone();
        let token = id.clone();
        let subscription = ServerSubscription::new(receiver, ServerSubscriptionHandle::new(move || { crate::db::lock::effect(|| { let _ = cancel.send(token.clone()); }); }));
        (id, subscription)
    }

    pub fn has_demand(&self) -> bool { !self.lifecycle.core.entries.is_empty() }

    pub fn is_connecting(&self) -> bool { self.lifecycle.is_connecting() }

    pub fn is_open(&self) -> bool { self.lifecycle.is_open() }

    pub fn take_frames(&mut self) -> Vec<Value> { std::mem::take(&mut self.lifecycle.core.frames) }

    pub fn resume(&mut self) { self.lifecycle.handle(Event::Resume); }

    pub fn refuse(&mut self, failure: SubscriptionFailure) { self.lifecycle.handle(Event::Refuse(failure)); }

    pub fn connectivity(&mut self) { self.lifecycle.handle(Event::Connectivity); }

    pub fn opened(&mut self) { self.lifecycle.handle(Event::Opened); }

    pub fn open_failed(&mut self) { self.lifecycle.handle(Event::OpenFailed); }

    pub fn closed(&mut self, code: u16, reason: &str) { self.lifecycle.handle(Event::Closed { code, reason: reason.into() }); }

    pub fn lost(&mut self) { self.lifecycle.handle(Event::Lost); }

    pub fn sent(&mut self) { self.lifecycle.handle(Event::Sent); }

    pub fn tick(&mut self) { self.lifecycle.handle(Event::Tick); }

    pub fn deadline(&self) -> Option<Duration> { self.lifecycle.deadline() }

    #[cfg(test)]
    pub(super) fn now(&self) -> Duration { self.lifecycle.now() }

    pub fn unsubscribe(&mut self, id: &str) {
        if self.lifecycle.core.entries.remove(id).is_none() { return; }
        if self.lifecycle.is_open() { self.lifecycle.core.frames.push(json!({"t":"unsubscribe", "id":id})); }
        self.lifecycle.handle(Event::Demand);
    }

    pub async fn cancelled(&mut self) {
        let Some(id) = self.cancellations.recv().await else { return };
        self.unsubscribe(&id);
    }

    pub fn receive(&mut self, body: Value) -> Result<(), CableError> {
        if body["t"] == "pong" {
            if let Some(ts) = body["ts"].as_u64() { self.lifecycle.handle(Event::Heard(Heard::Pong(ts))); }
            return Ok(());
        }
        let id = body["id"].as_str().ok_or(CableError::Frame)?;
        let Some(events) = self.lifecycle.core.entries.get(id).map(|entry| entry.events.clone()) else { return Ok(()) };
        match body["t"].as_str() {
            Some("subscribed") => {
                crate::db::lock::effect(|| { let _ = events.send(SubscriptionEvent::Ready); });
                self.lifecycle.handle(Event::Heard(Heard::Subscribed));
            }
            Some("next") => {
                let response: crate::transport::Response = serde_json::from_value(body["result"].clone()).map_err(|_| CableError::Frame)?;
                if response.shape() == crate::transport::ResponseShape::Empty { return Err(CableError::Frame); }
                crate::db::lock::effect(|| { let _ = events.send(SubscriptionEvent::response(response)); });
                self.lifecycle.handle(Event::Heard(Heard::Next));
            }
            Some("error") => {
                let code = body["code"].as_str().ok_or(CableError::Frame)?.to_string();
                let error = crate::transport::TransportError::Graphql { messages: vec![code.clone()], extensions: Some(crate::transport::GraphqlExtensions { code: Some(code), transaction_disposition: None, fields: Default::default() }) };
                let mut failure = SubscriptionFailure::from_error(error);
                if body["retryable"] == true { failure.kind = SubscriptionFailureKind::Transport; }
                crate::db::lock::effect(|| { let _ = events.send(SubscriptionEvent::Failed(failure)); });
                self.finish(id);
            }
            Some("complete") => self.finish(id),
            _ => return Err(CableError::Frame),
        }
        Ok(())
    }

    fn finish(&mut self, id: &str) {
        self.lifecycle.core.entries.remove(id);
        self.lifecycle.handle(Event::Heard(Heard::Other));
        self.lifecycle.handle(Event::Demand);
    }
}
