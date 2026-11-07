use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::oneshot;

use super::value::Observation;
use crate::ffi::{ByteProgress, CredentialStore, DbError, EncryptedReply, FakeHttp, FakeTransport, PutReply, TransportRequest};
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::cable::peer_tests::CablePeer;
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
    waiting_http: HashMap<String, (serde_json::Value, oneshot::Sender<Result<EncryptedReply, DbError>>)>,
    /// The socket of each cable request: the token of the lease.
    cables: HashMap<String, String>,
}

/// The transport mock of the shared vocabulary. It answers no request by itself: a request waits until an input of the trace answers, fails or drops it.
pub struct Mock {
    inner: Mutex<Inner>,
    peer: CablePeer,
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
        Arc::new(Self { inner: Mutex::new(Inner::default()), peer: CablePeer::new(crate::transport::encrypted::protocol_tests::config()) })
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
        if let Some((request, sender)) = inner.waiting_http.remove(id) {
            let Ok(body) = serde_json::from_str::<serde_json::Value>(body) else { return false };
            let sequence = request["sequence"].as_i64();
            return deliver(sender, Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, body) }));
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
        if kind == "permanent" && locked(self).waiting_http.contains_key(id) {
            return self.answer(id, r#"{"errors":[{"message":"permanent","extensions":{"code":"permanent"}}]}"#);
        }
        if kind == "auth" {
            if let Some((_, sender)) = locked(self).waiting_http.remove(id) {
                return deliver(sender, Ok(EncryptedReply { status: 401, body: String::new() }));
            }
        }
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
        if let Some((_, sender)) = inner.waiting_http.remove(id) {
            return deliver(sender, Err(error));
        }
        if let Some(token) = inner.cables.remove(id) {
            drop(inner);
            return self.peer.close(&token);
        }
        false
    }

    /// Fails every waiting request with `Interrupted`. The runner calls it before a close: the app's transport cancels its calls when the database closes.
    pub fn release_all(&self) {
        let mut inner = locked(self);
        for (_, sender) in inner.waiting.drain() { deliver(sender, Err(DbError::Interrupted { detail: "closed".into() })); }
        for (_, sender) in inner.waiting_puts.drain() { deliver(sender, Err(DbError::Interrupted { detail: "closed".into() })); }
        for (_, (_, sender)) in inner.waiting_http.drain() { deliver(sender, Err(DbError::Interrupted { detail: "closed".into() })); }
    }

    pub fn ready(&self, id: &str) -> bool {
        locked(self).cables.contains_key(id)
    }

    pub fn frame(&self, id: &str, event: &str) -> Result<bool, DbError> {
        let token = locked(self).cables.get(id).cloned();
        match token {
            Some(token) => Ok(self.peer.event(&token, serde_json::from_str(event).map_err(|error| DbError::Invalid { detail: error.to_string() })?)),
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
impl FakeTransport for Mock {
    fn cable(&self) -> Option<&CablePeer> { Some(&self.peer) }
    fn cable_opened(&self, token: &str) {
        let id = self.record("subscription", purpose_name(NetworkPurpose::Standard));
        locked(self).cables.insert(id, token.to_string());
    }
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> { self.wait("query", &request).await }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> { self.wait("mutation", &request).await }
    async fn put(&self, _: String, _: HashMap<String, String>, _: PutFile, _: TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let id = self.record("put", purpose_name(NetworkPurpose::Transfer));
        let (sender, receiver) = oneshot::channel();
        locked(self).waiting_puts.insert(id, sender);
        receiver.await.unwrap_or_else(|_| Err(DbError::Interrupted { detail: "the mock was dropped".into() }))
    }
}

/// The encrypted HTTP exchange of the shared vocabulary: the request waits for an input of the trace, the answer body is sealed with the fixture server key.
#[async_trait]
impl FakeHttp for Mock {
    async fn post(&self, _path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).map_err(|detail| DbError::Invalid { detail })?;
        let id = self.record("http", purpose_name(purpose));
        let (sender, receiver) = oneshot::channel();
        locked(self).waiting_http.insert(id, (request, sender));
        receiver.await.unwrap_or_else(|_| Err(DbError::Interrupted { detail: "the mock was dropped".into() }))
    }
}

/// The credential slot of the app side: it lives as long as the runner.
#[derive(Default)]
pub struct Credentials(Mutex<Option<String>>);

impl CredentialStore for Credentials {
    fn get(&self) -> Result<Option<String>, DbError> { Ok(self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()) }
    fn set(&self, credential: String) -> Result<(), DbError> { *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(credential); Ok(()) }
    fn clear(&self) -> Result<(), DbError> { *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None; Ok(()) }
}
