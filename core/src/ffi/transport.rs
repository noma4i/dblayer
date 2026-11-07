use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use crate::transport as core;
use super::cable::{CableOutcome, ForeignSocket, Mailbox, CableEvents};
use super::DbError;

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait Transport: Send + Sync {
    /// The GraphQL address of a database without an encrypted session.
    fn endpoint(&self) -> String;
    /// Runs one HTTP exchange and returns the raw outcome: the executor never classifies, follows a redirect or retries.
    async fn send(&self, request: core::HttpRequest) -> core::RawOutcome;
    /// Opens the WebSocket at `url` (built by the core), reports its frames to `events` and returns the connection after the handshake.
    async fn open_cable(&self, url: String, connect_ms: u64, events: Arc<dyn CableEvents>) -> CableOutcome;
    async fn put(&self, url: String, headers: HashMap<String, String>, file: core::PutFile, identity: core::TransferIdentity, progress: Arc<ByteProgress>) -> core::RawOutcome;
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

pub(super) struct ForeignTransport { port: Arc<dyn Transport>, endpoint: Option<String> }

impl ForeignTransport {
    pub(super) fn plain(port: Arc<dyn Transport>) -> Result<Self, DbError> {
        let endpoint = core::graphql_endpoint(&port.endpoint()).map_err(DbError::from)?;
        Ok(Self { port, endpoint: Some(endpoint) })
    }

    pub(super) fn beside_session(port: Arc<dyn Transport>) -> Self { Self { port, endpoint: None } }

    async fn graphql(&self, request: core::GraphQLRequest, kind: core::ExchangeKind) -> Result<core::Response, core::TransportError> {
        let Some(endpoint) = &self.endpoint else { return Err(core::TransportError::Config { code: core::ConfigCode::InvalidRequest, detail: "the database has no GraphQL endpoint".into() }) };
        let outcome = crate::lock::effect(|| self.port.send(core::graphql_call(endpoint, &request))).await;
        serde_json::from_str(&core::graphql_text(outcome, kind)?).map_err(|error| core::TransportError::Decode(error.to_string()))
    }
}

impl Drop for ForeignTransport {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ForeignTransport::drop"); }
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
        let mailbox = Arc::new(Mailbox::default());
        match self.port.open_cable(url.to_string(), core::CONNECT_TIMEOUT_MS, mailbox.clone()).await {
            CableOutcome::Open { connection } => Ok(Arc::new(ForeignSocket::new(connection, mailbox))),
            CableOutcome::Failed { failure } => Err(core::classify(&failure, core::ExchangeKind::Post)),
        }
    }
    async fn query(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::query");
        self.graphql(request, core::ExchangeKind::Query).await
    }
    async fn mutate(&self, request: core::GraphQLRequest) -> Result<core::Response, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::mutate");
        self.graphql(request, core::ExchangeKind::Mutation).await
    }
    async fn put(&self, request: core::PutRequest) -> Result<core::PutReply, core::TransportError> {
        crate::lock::assert_no_library_lock("Transport::put");
        let cancel = request.cancel.clone();
        let progress = Arc::new(ByteProgress { cancel: request.cancel, sent: request.sent });
        match self.port.put(request.url, request.headers.into_iter().collect(), request.file, request.identity, progress).await {
            core::RawOutcome::Response { status, etag, .. } => Ok(core::PutReply { status, etag }),
            core::RawOutcome::Failed { .. } if cancel.is_cancelled() => Err(core::TransportError::Interrupted("the upload was cancelled".into())),
            core::RawOutcome::Failed { failure } => Err(core::classify(&failure, core::ExchangeKind::Post)),
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod transport_tests;
