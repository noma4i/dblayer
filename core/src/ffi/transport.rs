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
    async fn query(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<String, DbError>;
    fn unsubscribe(&self, token: String);
    async fn put(&self, url: String, headers: HashMap<String, String>, bytes: Vec<u8>, progress: Arc<ByteProgress>) -> Result<PutReply, DbError>;
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
        self.send(core::SubscriptionEvent::Next(response));
        Ok(())
    }
    pub fn failed(&self, kind: String, detail: String) {
        let kind = match kind.as_str() {
            "graphql" => core::SubscriptionFailureKind::Graphql,
            "internal" => core::SubscriptionFailureKind::Internal,
            _ => core::SubscriptionFailureKind::Transport,
        };
        self.send(core::SubscriptionEvent::Failed(core::SubscriptionFailure::new(kind, detail)));
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
pub struct OnlineSink { pub(crate) sync: crate::sync::Sync }

#[uniffi::export]
impl OnlineSink {
    pub fn set_online(&self, online: bool) {
        let Ok(executor) = super::database::executor() else { return };
        let _entered = executor.enter();
        self.sync.set_online(online);
    }
}

pub(super) struct ForeignTransport(pub Arc<dyn Transport>);

fn transport_error(error: DbError) -> core::TransportError {
    match error {
        DbError::Network { detail } => core::TransportError::Network(detail),
        DbError::Interrupted { detail } => core::TransportError::Interrupted(detail),
        DbError::Http { status, detail } => core::TransportError::Http { status, body: detail },
        DbError::Graphql { messages } => core::TransportError::Graphql { messages },
        other => core::TransportError::Decode(other.to_string()),
    }
}

#[async_trait]
impl core::Transport for ForeignTransport {
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
