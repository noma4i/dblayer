use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::transport as core;
use super::DbError;

#[derive(Clone, uniffi::Record)]
pub struct TransportRequest {
    pub document: String,
    pub operation_name: String,
    pub variables_json: String,
}

impl From<core::GraphQLRequest> for TransportRequest {
    fn from(request: core::GraphQLRequest) -> Self {
        Self { document: request.document, operation_name: request.operation_name, variables_json: request.variables.to_string() }
    }
}

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait Transport: Send + Sync {
    async fn open_cable(&self) -> Result<String, DbError>;
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError>;
    async fn cable_next(&self, token: String) -> Option<EncryptedSocketEvent>;
    async fn cable_close(&self, token: String);
    async fn query(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<String, DbError>;
    fn unsubscribe(&self, token: String);
    async fn put(&self, url: String, headers: HashMap<String, String>, bytes: Vec<u8>, progress: Arc<ByteProgress>) -> Result<PutReply, DbError>;
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum EncryptedSocketEvent {
    Text { text: String },
    Closed { code: u16, reason: String },
}

struct ForeignSocket { transport: Arc<dyn Transport>, token: String }

#[async_trait]
impl core::CableSocket for ForeignSocket {
    async fn send(&self, text: String) -> Result<(), core::TransportError> { self.transport.cable_send(self.token.clone(), text).await.map_err(transport_error) }
    async fn next(&self) -> Option<core::SocketEvent> {
        self.transport.cable_next(self.token.clone()).await.map(|event| match event {
            EncryptedSocketEvent::Text { text } => core::SocketEvent::Text(text),
            EncryptedSocketEvent::Closed { code, reason } => core::SocketEvent::Closed { code, reason },
        })
    }
    async fn close(&self) { self.transport.cable_close(self.token.clone()).await; }
}

#[derive(uniffi::Object)]
pub struct SubscriptionSink { sender: Mutex<Option<mpsc::UnboundedSender<core::SubscriptionEvent>>> }

#[uniffi::export]
impl SubscriptionSink {
    pub fn ready(&self) { self.send(core::SubscriptionEvent::Ready); }
    pub fn event(&self, json: String) -> Result<(), DbError> {
        let response: core::Response = serde_json::from_str(&json)?;
        if response.data.is_none() && response.errors.is_empty() {
            let detail = format!("subscription frame without data: {}", json.chars().take(120).collect::<String>());
            self.send(core::SubscriptionEvent::Failed(core::SubscriptionFailure::new(core::SubscriptionFailureKind::Internal, detail.clone())));
            return Err(DbError::Invalid { detail });
        }
        self.send(core::SubscriptionEvent::response(response));
        Ok(())
    }
    pub fn failed(&self, error: DbError) {
        self.send(core::SubscriptionEvent::Failed(core::SubscriptionFailure::from_error(transport_error(error))));
    }
    pub fn closed(&self) { crate::db::locked(&self.sender).take(); }
}

impl SubscriptionSink {
    fn send(&self, event: core::SubscriptionEvent) {
        if let Some(sender) = crate::db::locked(&self.sender).as_ref() { let _ = sender.send(event); }
    }
}

#[derive(uniffi::Object)]
pub struct ByteProgress { cancel: core::CancelToken, sent: core::ByteSink }

#[uniffi::export]
impl ByteProgress {
    pub fn sent(&self, bytes: u64) { (self.sent)(bytes); }
    pub fn is_cancelled(&self) -> bool { self.cancel.is_cancelled() }
}

#[derive(uniffi::Record)]
pub struct PutReply { pub status: u16, pub etag: Option<String> }

#[derive(uniffi::Object)]
pub struct OnlineSink { pub(crate) sync: Option<crate::sync::Sync> }

#[uniffi::export]
impl OnlineSink {
    pub fn set_online(&self, online: bool) {
        let Some(sync) = &self.sync else { return };
        let Ok(executor) = super::database::executor() else { return };
        let _entered = executor.enter();
        sync.set_online(online);
    }
}

pub(super) struct ForeignTransport(pub Arc<dyn Transport>);

pub(super) fn transport_error(error: DbError) -> core::TransportError {
    match error {
        DbError::Security { code, detail } => core::TransportError::Security { code, detail },
        DbError::Config { code, detail } => core::TransportError::Config { code, detail },
        DbError::Network { detail } => core::TransportError::Network(detail),
        DbError::Interrupted { detail } => core::TransportError::Interrupted(detail),
        DbError::Http { status, detail } => core::TransportError::Http { status, body: detail },
        DbError::RequestTooLarge { size, limit } => core::TransportError::RequestTooLarge { size, limit },
        DbError::Graphql { messages, extensions } => core::TransportError::Graphql { messages, extensions },
        DbError::Session { code, detail } => core::TransportError::Session { code, detail },
        other => core::TransportError::Decode(other.to_string()),
    }
}

#[async_trait]
impl core::Transport for ForeignTransport {
    async fn open_cable(&self) -> Result<Arc<dyn core::CableSocket>, core::TransportError> {
        Ok(Arc::new(ForeignSocket { token: self.0.open_cable().await.map_err(transport_error)?, transport: self.0.clone() }))
    }
    async fn query(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        let json = self.0.query(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn mutate(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        let json = self.0.mutate(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn subscribe(&self, request: core::GraphQLRequest) -> Result<core::Subscription, core::TransportError> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let sink = Arc::new(SubscriptionSink { sender: Mutex::new(Some(sender)) });
        let token = self.0.subscribe(request.into(), sink.clone()).await.map_err(transport_error)?;
        let transport = self.0.clone();
        Ok(core::Subscription::new(receiver, core::SubscriptionHandle::new(move || { transport.unsubscribe(token.clone()); sink.closed(); })))
    }
    async fn put(&self, request: core::PutRequest) -> Result<core::PutResponse, core::TransportError> {
        let progress = Arc::new(ByteProgress { cancel: request.cancel, sent: request.sent });
        let reply = self.0.put(request.url, request.headers.into_iter().collect(), request.body, progress).await.map_err(transport_error)?;
        Ok(core::PutResponse { status: reply.status, etag: reply.etag })
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod transport_tests;
