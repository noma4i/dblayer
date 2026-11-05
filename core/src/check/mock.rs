use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::oneshot;

use super::value::Observation;
use crate::ffi::{ByteProgress, CableLease, DbError, EncryptedSocketEvent, PutReply, SubscriptionSink, Transport, TransportRequest};
use crate::transport::{NetworkPurpose, PutFile, TransferIdentity};

type Reply = Result<String, DbError>;

struct Seen {
    id: String,
    kind: &'static str,
    purpose: &'static str,
    announced: bool,
}

#[derive(Default)]
struct Inner {
    next: u64,
    seen: Vec<Seen>,
    waiting: HashMap<String, oneshot::Sender<Reply>>,
    waiting_puts: HashMap<String, oneshot::Sender<Result<PutReply, DbError>>>,
    sinks: HashMap<String, Arc<SubscriptionSink>>,
}

/// The transport mock of the shared vocabulary. It answers no request by itself: a request waits until an input of the trace answers, fails or drops it.
#[derive(Default)]
pub struct Mock {
    inner: Mutex<Inner>,
}

fn deliver<T>(sender: oneshot::Sender<T>, value: T) -> bool {
    sender.send(value).is_ok() // lock-ok: the mock holds no library lock
}

fn locked(mock: &Mock) -> std::sync::MutexGuard<'_, Inner> {
    mock.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn purpose_name(purpose: NetworkPurpose) -> &'static str {
    match purpose {
        NetworkPurpose::Standard => "standard",
        NetworkPurpose::Transfer => "transfer",
    }
}

fn error_of(kind: &str) -> Option<DbError> {
    match kind {
        "temporary" => Some(DbError::Network { detail: "temporary".into() }),
        "permanent" => Some(DbError::Graphql { messages: vec!["permanent".into()], extensions: None }),
        "auth" => Some(DbError::Http { status: 401, detail: "auth".into() }),
        _ => None,
    }
}

impl Mock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: &'static str, purpose: &'static str) -> String {
        let mut inner = locked(self);
        inner.next += 1;
        let id = format!("r{}", inner.next);
        inner.seen.push(Seen { id: id.clone(), kind, purpose, announced: false });
        id
    }

    /// How many requests arrived. The runner waits until this number and the other counters stop moving.
    pub fn count(&self) -> usize {
        locked(self).seen.len()
    }

    /// The requests that arrived since the last call, in arrival order, as `request` observations.
    pub fn announce(&self) -> Vec<Observation> {
        let mut inner = locked(self);
        let mut out = Vec::new();
        for seen in inner.seen.iter_mut().filter(|seen| !seen.announced) {
            seen.announced = true;
            out.push(Observation::new("request", json!({ "id": seen.id, "kind": seen.kind, "purpose": seen.purpose })));
        }
        out
    }

    /// Answers a waiting query, mutation or put with the JSON body. False when no such request waits.
    pub fn answer(&self, id: &str, body: &str) -> bool {
        let mut inner = locked(self);
        if let Some(sender) = inner.waiting.remove(id) {
            return deliver(sender, Ok(body.to_string()));
        }
        if let Some(sender) = inner.waiting_puts.remove(id) {
            let reply = serde_json::from_str::<serde_json::Value>(body).ok();
            let status = reply.as_ref().and_then(|value| value["status"].as_u64()).unwrap_or(200) as u16;
            return deliver(sender, Ok(PutReply { status, etag: None }));
        }
        false
    }

    /// Fails a waiting request or a subscription with `temporary`, `permanent` or `auth`. False for an unknown id or kind.
    pub fn fail(&self, id: &str, kind: &str) -> bool {
        let Some(error) = error_of(kind) else { return false };
        self.finish(id, error)
    }

    /// Breaks the connection of a waiting request or a subscription.
    pub fn drop_connection(&self, id: &str) -> bool {
        self.finish(id, DbError::Network { detail: "dropped".into() })
    }

    fn finish(&self, id: &str, error: DbError) -> bool {
        let mut inner = locked(self);
        if let Some(sender) = inner.waiting.remove(id) {
            return deliver(sender, Err(error));
        }
        if let Some(sender) = inner.waiting_puts.remove(id) {
            return deliver(sender, Err(error));
        }
        if let Some(sink) = inner.sinks.remove(id) {
            drop(inner);
            sink.failed(error);
            return true;
        }
        false
    }

    /// Fails every waiting request with `Interrupted`. The runner calls it before a close: the app's transport cancels its calls when the database closes.
    pub fn release_all(&self) {
        let mut inner = locked(self);
        for (_, sender) in inner.waiting.drain() { deliver(sender, Err(DbError::Interrupted { detail: "closed".into() })); }
        for (_, sender) in inner.waiting_puts.drain() { deliver(sender, Err(DbError::Interrupted { detail: "closed".into() })); }
    }

    pub fn ready(&self, id: &str) -> bool {
        let sink = locked(self).sinks.get(id).cloned();
        sink.map(|sink| sink.ready()).is_some()
    }

    pub fn frame(&self, id: &str, event: &str) -> Result<bool, DbError> {
        let sink = locked(self).sinks.get(id).cloned();
        match sink {
            Some(sink) => sink.event(event.to_string()).map(|()| true),
            None => Ok(false),
        }
    }

    async fn wait(&self, kind: &'static str, request: &TransportRequest) -> Reply {
        let id = self.record(kind, purpose_name(request.purpose));
        let (sender, receiver) = oneshot::channel();
        locked(self).waiting.insert(id, sender);
        receiver.await.unwrap_or_else(|_| Err(DbError::Interrupted { detail: "the mock was dropped".into() }))
    }
}

#[async_trait]
impl Transport for Mock {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> { self.wait("query", &request).await }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> { self.wait("mutation", &request).await }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> {
        let id = self.record("subscription", purpose_name(request.purpose));
        sink.opened(id.clone())?;
        locked(self).sinks.insert(id, sink);
        Ok(())
    }
    fn unsubscribe(&self, token: String) { locked(self).sinks.remove(&token); }
    async fn put(&self, _: String, _: HashMap<String, String>, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let id = self.record("put", purpose_name(NetworkPurpose::Transfer));
        let (sender, receiver) = oneshot::channel();
        locked(self).waiting_puts.insert(id, sender);
        receiver.await.unwrap_or_else(|_| Err(DbError::Interrupted { detail: "the mock was dropped".into() }))
    }
}
