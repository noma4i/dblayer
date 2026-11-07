use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
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
    async fn put(&self, url: String, headers: HashMap<String, String>, file: core::PutFile, identity: core::TransferIdentity, progress: Arc<ByteProgress>) -> Result<core::PutReply, DbError>;
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
        crate::lock::assert_no_library_lock("ForeignSocket::drop");
        self.lease.lease.abandon();
    }
}

#[async_trait]
impl core::CableSocket for ForeignSocket {
    async fn send(&self, text: String) -> Result<(), core::TransportError> { crate::lock::assert_no_library_lock("Transport::cable_send"); self.transport.cable_send(self.token.clone(), text).await.map_err(transport_error) }
    async fn next(&self) -> Option<core::SocketEvent> {
        crate::lock::assert_no_library_lock("Transport::cable_next");
        self.transport.cable_next(self.token.clone()).await.map(|event| match event {
            EncryptedSocketEvent::Text { text } => core::SocketEvent::Text(text),
            EncryptedSocketEvent::Closed { code, reason } => core::SocketEvent::Closed { code, reason },
        })
    }
    async fn close(&self) {
        let Some(token) = self.lease.lease.begin_close() else { return };
        let mut closing = Closing { lease: &self.lease.lease, done: false };
        crate::lock::assert_no_library_lock("Transport::cable_close");
        self.transport.cable_close(token).await;
        closing.done = true;
    }
}

#[derive(uniffi::Object)]
pub struct ByteProgress { cancel: core::CancelToken, sent: core::ByteSink }

#[uniffi::export]
impl ByteProgress {
    pub fn sent(&self, bytes: u64) { crate::lock::assert_no_library_lock("ByteProgress::sent"); (self.sent)(bytes); }
    pub fn is_cancelled(&self) -> bool { self.cancel.is_cancelled() }
}

enum SignalTarget {
    Life(std::sync::Weak<crate::owners::life::Life>),
    Connectivity(Arc<core::Connectivity>),
}

/// The inputs of the platform signals: each OS value reaches the core as one input, without dedup.
#[derive(uniffi::Object)]
pub struct OnlineSink { target: SignalTarget }

impl OnlineSink {
    pub(crate) fn of_life(life: &Arc<crate::owners::life::Life>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Life(Arc::downgrade(life)) }) }

    pub(crate) fn of_connectivity(connectivity: Arc<core::Connectivity>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Connectivity(connectivity) }) }

    fn life(&self) -> Option<Arc<crate::owners::life::Life>> {
        match &self.target {
            SignalTarget::Life(life) => life.upgrade(),
            SignalTarget::Connectivity(_) => None,
        }
    }
}

#[uniffi::export]
impl OnlineSink {
    /// Returns once the owners heard the change.
    pub fn set_online(&self, online: bool) {
        if let SignalTarget::Connectivity(connectivity) = &self.target {
            connectivity.change(online);
            return;
        }
        let Some(life) = self.life() else { return };
        let waiter = life.set_online(online);
        let Ok(executor) = super::database::executor() else { return };
        let (sender, receiver) = std::sync::mpsc::channel();
        crate::task::spawn_on(crate::task::Carry::Detached, executor.handle(), async move { let outcome = waiter.wait().await; crate::lock::effect(|| { let _ = sender.send(outcome); }); });
        let _ = crate::blocking::blocking_unit(|| receiver.recv_timeout(std::time::Duration::from_secs(10)));
    }

    pub fn set_power_saving(&self, saving: bool) {
        if let Some(life) = self.life() { drop(life.set_power_saving(saving)); }
    }

    pub fn enter_foreground(&self) {
        if let Some(life) = self.life() { drop(life.foreground()); }
    }
}

/// The OS observers of the platform: the core starts them after `open` and stops them in `Closing`, and `close` returns after `stop`.
#[uniffi::export(with_foreign)]
pub trait PlatformObserver: Send + Sync {
    /// Starts the OS observers. Each OS value (online, foreground, power saving) goes to `sink` as one input, the current value first.
    fn start(&self, sink: Arc<OnlineSink>);
    /// Ends the OS observers: no callback reaches the sink after it returns.
    fn stop(&self);
}

pub(super) struct ObserverStop(pub Arc<dyn PlatformObserver>);

impl Drop for ObserverStop {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ObserverStop::drop"); }
}

impl crate::owners::life::ObserverPort for ObserverStop {
    fn stop(&self, done: crate::owners::life::StopDone) {
        self.0.stop();
        done.finish();
    }
}

pub(super) struct ForeignTransport(pub Arc<dyn Transport>);

impl Drop for ForeignTransport {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ForeignTransport::drop"); }
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
        crate::lock::assert_no_library_lock("Calling::drop");
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
impl core::Upstream for ForeignTransport {
    async fn open_cable(&self, url: &str) -> Result<Arc<dyn core::CableSocket>, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::open_cable");
        let transport = self.0.clone();
        let handle = super::database::executor().map_err(transport_error)?.handle().clone();
        let lease = Arc::new(CableLease { lease: TokenLease::new(handle, move |token| {
            let transport = transport.clone();
            Box::pin(async move { crate::lock::assert_no_library_lock("Transport::cable_close"); transport.cable_close(token).await })
        }) });
        let abandoned = lease.clone();
        Calling::new(self.0.open_cable(lease.clone(), url.to_string()), move || abandoned.lease.abandon()).await.map_err(transport_error)?;
        let Some(token) = lease.lease.token() else {
            return Err(transport_error(DbError::Invalid { detail: "open_cable finished without opened".into() }));
        };
        Ok(Arc::new(ForeignSocket { transport: self.0.clone(), lease, token }))
    }
    async fn query(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::query");
        let json = self.0.query(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn mutate(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::mutate");
        let json = self.0.mutate(request.into()).await.map_err(transport_error)?;
        serde_json::from_str(&json).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
    async fn put(&self, request: core::PutRequest) -> Result<core::PutReply, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::put");
        let progress = Arc::new(ByteProgress { cancel: request.cancel, sent: request.sent });
        self.0.put(request.url, request.headers.into_iter().collect(), request.file, request.identity, progress).await.map_err(transport_error)
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod transport_tests;
