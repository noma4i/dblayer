use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use async_trait::async_trait;
use crate::transport as core;
use super::lease::TokenLease;
use super::DbError;

#[derive(Clone, uniffi::Record)]
pub struct TransportRequest {
    pub document: String,
    pub operation_name: String,
    pub variables_json: String,
    pub purpose: core::NetworkPurpose,
}

impl From<core::GraphQLRequest> for TransportRequest {
    fn from(request: core::GraphQLRequest) -> Self {
        Self { document: request.document, operation_name: request.operation_name, variables_json: request.variables.to_string(), purpose: request.purpose }
    }
}

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait Transport: Send + Sync {
    /// Opens the WebSocket at `url` (built by the core) and reports its token to `lease`. The platform sends and receives raw text frames only.
    async fn open_cable(&self, lease: Arc<CableLease>, url: String) -> Result<(), DbError>;
    async fn cable_send(&self, token: String, text: String) -> Result<(), DbError>;
    async fn cable_next(&self, token: String) -> Option<EncryptedSocketEvent>;
    async fn cable_close(&self, token: String);
    async fn query(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError>;
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError>;
    fn unsubscribe(&self, token: String);
    async fn put(&self, url: String, headers: HashMap<String, String>, file: core::PutFile, identity: core::TransferIdentity, progress: Arc<ByteProgress>) -> Result<PutReply, DbError>;
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum EncryptedSocketEvent {
    Text { text: String },
    Closed { code: u16, reason: String },
}

#[derive(uniffi::Object)]
pub struct CableLease { lease: TokenLease }

#[uniffi::export]
impl CableLease {
    pub fn opened(&self, token: String) -> Result<(), DbError> { self.lease.opened(token) }
}

struct Closing<'a> { lease: &'a TokenLease, done: bool }

impl Drop for Closing<'_> {
    fn drop(&mut self) {
        if self.done { self.lease.finish_close(); } else { self.lease.restore_close(); }
    }
}

struct ForeignSocket { transport: Arc<dyn Transport>, lease: Arc<CableLease>, token: String }

impl Drop for ForeignSocket {
    fn drop(&mut self) {
        crate::db::lock::assert_no_library_lock("ForeignSocket::drop");
        self.lease.lease.abandon();
    }
}

#[async_trait]
impl core::CableSocket for ForeignSocket {
    async fn send(&self, text: String) -> Result<(), core::TransportError> { crate::db::lock::assert_no_library_lock("Transport::cable_send"); self.transport.cable_send(self.token.clone(), text).await.map_err(transport_error) }
    async fn next(&self) -> Option<core::SocketEvent> {
        crate::db::lock::assert_no_library_lock("Transport::cable_next");
        self.transport.cable_next(self.token.clone()).await.map(|event| match event {
            EncryptedSocketEvent::Text { text } => core::SocketEvent::Text(text),
            EncryptedSocketEvent::Closed { code, reason } => core::SocketEvent::Closed { code, reason },
        })
    }
    async fn close(&self) {
        let Some(token) = self.lease.lease.begin_close() else { return };
        let mut closing = Closing { lease: &self.lease.lease, done: false };
        crate::db::lock::assert_no_library_lock("Transport::cable_close");
        self.transport.cable_close(token).await;
        closing.done = true;
    }
}

#[derive(uniffi::Object)]
pub struct SubscriptionSink { sender: Mutex<Option<core::SubscriptionSender>>, lease: TokenLease }

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
    pub fn closed(&self) { self.close(); }
    pub fn opened(&self, token: String) -> Result<(), DbError> { self.lease.opened(token) }
}

impl SubscriptionSink {
    fn new(handle: tokio::runtime::Handle, sender: core::SubscriptionSender, release: impl Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static) -> Self {
        Self { sender: Mutex::new(Some(sender)), lease: TokenLease::new(handle, release) }
    }
    fn abandon(&self) {
        self.lease.abandon();
        self.close();
    }
    fn close(&self) {
        let sender = crate::db::locked(&self.sender).take();
        drop(sender);
    }
    fn send(&self, event: core::SubscriptionEvent) {
        let sender = crate::db::locked(&self.sender).clone();
        if let Some(sender) = sender { crate::db::lock::effect(|| { let _ = sender.send(event); }); }
    }
}

#[derive(uniffi::Object)]
pub struct ByteProgress { cancel: core::CancelToken, sent: core::ByteSink }

#[uniffi::export]
impl ByteProgress {
    pub fn sent(&self, bytes: u64) { crate::db::lock::assert_no_library_lock("ByteProgress::sent"); (self.sent)(bytes); }
    pub fn is_cancelled(&self) -> bool { self.cancel.is_cancelled() }
}

#[derive(uniffi::Record)]
pub struct PutReply { pub status: u16, pub etag: Option<String> }

#[derive(uniffi::Object)]
pub struct OnlineSink { pub(crate) sync: Option<crate::sync::WeakSync>, pub(crate) online: Arc<core::Connectivity>, pub(crate) life: std::sync::Weak<crate::owners::life::Life> }

impl OnlineSink {
    fn live_sync(&self) -> Option<Option<crate::sync::Sync>> {
        match &self.sync {
            Some(weak) => weak.upgrade().map(Some),
            None => Some(None),
        }
    }
}

#[uniffi::export]
impl OnlineSink {
    pub fn set_online(&self, online: bool) {
        let Some(sync) = self.live_sync() else { return };
        let Some(life) = self.life.upgrade() else { return };
        let was_online = self.online.is_online();
        let waiter = life.set_online(online);
        let Ok(executor) = super::database::executor() else { return };
        let (sender, receiver) = std::sync::mpsc::channel();
        crate::task::spawn_on(crate::task::Carry::Detached, executor.handle(), async move { let outcome = waiter.wait().await; crate::db::lock::effect(|| sender.send(outcome)); });
        if crate::blocking::blocking_unit(|| receiver.recv_timeout(std::time::Duration::from_secs(10))).is_err() { return; }
        if let Some(sync) = sync {
            let _entered = executor.enter();
            sync.online_changed(was_online, online);
        }
    }

    pub fn set_power_saving(&self, saving: bool) {
        let Some(Some(sync)) = self.live_sync() else { return };
        let Ok(executor) = super::database::executor() else { return };
        let _entered = executor.enter();
        sync.set_power_saving(saving);
    }

    pub fn enter_foreground(&self) {
        let Some(Some(sync)) = self.live_sync() else { return };
        let Ok(executor) = super::database::executor() else { return };
        let _entered = executor.enter();
        sync.enter_foreground();
    }
}

pub(super) struct ForeignTransport(pub Arc<dyn Transport>);

impl Drop for ForeignTransport {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("ForeignTransport::drop"); }
}

struct Calling<'a> {
    call: Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>,
    abandon: Option<Box<dyn FnOnce() + Send + 'a>>,
}

impl<'a> Calling<'a> {
    fn new(call: Pin<Box<dyn Future<Output = Result<(), DbError>> + Send + 'a>>, abandon: impl FnOnce() + Send + 'a) -> Self {
        Self { call, abandon: Some(Box::new(abandon)) }
    }
}

impl Future for Calling<'_> {
    type Output = Result<(), DbError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = self.call.as_mut().poll(context);
        if let Poll::Ready(result) = &polled {
            let abandon = self.abandon.take();
            if let (Err(_), Some(abandon)) = (result, abandon) { abandon(); }
        }
        polled
    }
}

impl Drop for Calling<'_> {
    fn drop(&mut self) {
        crate::db::lock::assert_no_library_lock("Calling::drop");
        if let Some(abandon) = self.abandon.take() { abandon(); }
    }
}

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
    async fn open_cable(&self, url: &str) -> Result<Arc<dyn core::CableSocket>, core::TransportError> {
        crate::db::lock::assert_no_library_lock("Transport::open_cable");
        let transport = self.0.clone();
        let handle = super::database::executor().map_err(transport_error)?.handle().clone();
        let lease = Arc::new(CableLease { lease: TokenLease::new(handle, move |token| {
            let transport = transport.clone();
            Box::pin(async move { crate::db::lock::assert_no_library_lock("Transport::cable_close"); transport.cable_close(token).await })
        }) });
        let abandoned = lease.clone();
        Calling::new(self.0.open_cable(lease.clone(), url.to_string()), move || abandoned.lease.abandon()).await.map_err(transport_error)?;
        let Some(token) = lease.lease.token() else {
            return Err(transport_error(DbError::Invalid { detail: "open_cable finished without opened".into() }));
        };
        Ok(Arc::new(ForeignSocket { transport: self.0.clone(), lease, token }))
    }
    async fn query(&self, _admitted: &crate::ffi::legacy::Admitted, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::db::lock::assert_no_library_lock("Transport::query");
        let json = self.0.query(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn mutate(&self, _admitted: &crate::ffi::legacy::Admitted, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::db::lock::assert_no_library_lock("Transport::mutate");
        let json = self.0.mutate(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn subscribe(&self, _admitted: &crate::ffi::legacy::Admitted, request: core::GraphQLRequest) -> Result<core::ServerSubscription, core::TransportError> {
        crate::db::lock::assert_no_library_lock("Transport::subscribe");
        let (sender, receiver) = core::subscription_channel();
        let transport = self.0.clone();
        let handle = super::database::executor().map_err(transport_error)?.handle().clone();
        let sink = Arc::new(SubscriptionSink::new(handle, sender, move |token| {
            let transport = transport.clone();
            Box::pin(async move { crate::db::lock::assert_no_library_lock("Transport::unsubscribe"); transport.unsubscribe(token) })
        }));
        let abandoned = sink.clone();
        Calling::new(self.0.subscribe(request.into(), sink.clone()), move || abandoned.abandon()).await.map_err(transport_error)?;
        if sink.lease.token().is_none() {
            sink.abandon();
            return Err(transport_error(DbError::Invalid { detail: "subscribe finished without opened".into() }));
        }
        Ok(core::ServerSubscription::new(receiver, core::ServerSubscriptionHandle::new(move || sink.abandon())))
    }
    async fn put(&self, _admitted: &crate::ffi::legacy::Admitted, request: core::PutRequest) -> Result<core::PutResponse, core::TransportError> {
        crate::db::lock::assert_no_library_lock("Transport::put");
        let progress = Arc::new(ByteProgress { cancel: request.cancel, sent: request.sent });
        let reply = self.0.put(request.url, request.headers.into_iter().collect(), request.file, request.identity, progress).await.map_err(transport_error)?;
        Ok(core::PutResponse { status: reply.status, etag: reply.etag })
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod transport_tests;
