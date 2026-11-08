use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use crate::transport as core;
use super::cable::{CableOutcome, ForeignSocket, Mailbox, CableEvents};
use super::DbError;

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait Transport: Send + Sync {
    /// The headers of the app, read again for every request and every cable.
    fn app_headers(&self) -> HashMap<String, String>;
    /// Runs one HTTP exchange and returns the raw outcome: the executor never classifies, follows a redirect or retries.
    async fn send(&self, request: core::HttpRequest) -> core::RawOutcome;
    /// Opens the WebSocket at `url` (built by the core), reports its frames to `events` and returns the connection after the handshake.
    async fn open_cable(&self, url: String, headers: HashMap<String, String>, connect_ms: u64, events: Arc<dyn CableEvents>) -> CableOutcome;
    /// Uploads the file range to the presigned `url` with the core timeouts, reports the sent bytes to `progress`.
    async fn put(&self, target: PutTarget, file: core::PutFile, identity: core::TransferIdentity, progress: Arc<ByteProgress>) -> core::RawOutcome;
}

/// Where one PUT goes: the presigned `url`, its headers as the executor sets them and the core timeouts.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PutTarget {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub connect_ms: u64,
    pub request_ms: u64,
}

#[derive(uniffi::Object)]
pub struct ByteProgress { cancel: core::CancelToken, sent: core::ByteSink }

#[uniffi::export]
impl ByteProgress {
    pub fn sent(&self, bytes: u64) { crate::lock::assert_no_library_lock("ByteProgress::sent"); (self.sent)(bytes); }
    pub fn is_cancelled(&self) -> bool { self.cancel.is_cancelled() }
}

struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) { self.0.unpark(); }
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut context) { return value; }
        std::thread::park();
    }
}

enum SignalTarget {
    Life(std::sync::Weak<crate::owners::life::Life>),
    Connectivity(Arc<core::Connectivity>),
}

/// The foreground registrations of one sink: a delivery counts only while its token is live, and revoking waits for the deliveries in flight.
#[derive(Default)]
pub(crate) struct Registrations { live: std::sync::RwLock<std::collections::BTreeSet<u64>>, next: std::sync::atomic::AtomicU64 }

impl Registrations {
    fn issue(&self) -> u64 {
        let token = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(token);
        token
    }

    fn revoke(&self, token: u64) { self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&token); }

    pub(crate) fn revoke_all(&self) { self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner).clear(); }

    fn deliver(&self, token: u64, send: impl FnOnce()) {
        let live = self.live.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if live.contains(&token) { send(); }
    }
}

/// The inputs of the platform signals: each OS value reaches the core as one input, without dedup.
#[derive(uniffi::Object)]
pub struct OnlineSink { target: SignalTarget, registrations: Arc<Registrations> }

impl OnlineSink {
    pub(crate) fn of_life(life: &Arc<crate::owners::life::Life>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Life(Arc::downgrade(life)), registrations: Arc::default() }) }

    pub(crate) fn of_connectivity(connectivity: Arc<core::Connectivity>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Connectivity(connectivity), registrations: Arc::default() }) }

    /// Waits for the outcome of the owner: the answer of `Network`, `Closed` once the database ended.
    pub(crate) fn apply_online(&self, online: bool) -> Result<(), DbError> {
        if let SignalTarget::Connectivity(connectivity) = &self.target {
            connectivity.change(online);
            return Ok(());
        }
        let Some(life) = self.life() else { return Err(super::command::closed()) };
        let waiter = life.set_online(online);
        match crate::blocking::blocking_unit(|| block_on(waiter.wait())) {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Online { closed: false }) => Ok(()),
            crate::runtime::Outcome::Value(_) | crate::runtime::Outcome::Closing => Err(super::command::closed()),
            crate::runtime::Outcome::EpochEnded(_) => Err(super::command::ended()),
            crate::runtime::Outcome::Internal => Err(DbError::Internal { detail: "the answer of the owner was lost".into() }),
        }
    }

    pub(crate) fn registrations(&self) -> Arc<Registrations> { self.registrations.clone() }

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
    pub fn set_online(&self, online: bool) { let _ = self.apply_online(online); }

    pub fn set_power_saving(&self, saving: bool) {
        if let Some(life) = self.life() { drop(life.set_power_saving(saving)); }
    }

    /// Registers one foreground observer and returns its token: every `enter_foreground` of the observer carries it.
    pub fn register_foreground(&self) -> u64 { self.registrations.issue() }

    /// Ends the registration of `token`: a delivery with it is dropped from now on.
    pub fn unregister_foreground(&self, token: u64) { self.registrations.revoke(token); }

    /// One return to the foreground. Dropped when the registration of `token` ended, also after `stop` of the observers was asked.
    pub fn enter_foreground(&self, token: u64) {
        if let Some(life) = self.life() { self.registrations.deliver(token, || drop(life.foreground())); }
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

pub(super) struct ObserverStop(pub Arc<dyn PlatformObserver>, pub Arc<Registrations>);

impl Drop for ObserverStop {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ObserverStop::drop"); }
}

impl crate::owners::life::ObserverPort for ObserverStop {
    fn stop(&self, done: crate::owners::life::StopDone) {
        self.1.revoke_all();
        self.0.stop();
        done.finish();
    }
}

pub(super) struct ForeignTransport { port: Arc<dyn Transport>, endpoint: Option<String> }

impl ForeignTransport {
    pub(super) fn plain(port: Arc<dyn Transport>, endpoint: Option<String>) -> Result<Self, DbError> {
        let endpoint = core::graphql_endpoint(&endpoint.unwrap_or_default()).map_err(DbError::from)?;
        Ok(Self { port, endpoint: Some(endpoint) })
    }

    pub(super) fn beside_session(port: Arc<dyn Transport>) -> Self { Self { port, endpoint: None } }

    async fn graphql(&self, request: core::GraphQLRequest, kind: core::ExchangeKind) -> Result<core::Response, core::TransportError> {
        let Some(endpoint) = &self.endpoint else { return Err(core::TransportError::Config { code: core::ConfigCode::InvalidRequest, detail: "the database has no GraphQL endpoint".into() }) };
        let wire = request.authored()?;
        let app_headers = crate::lock::effect(|| self.port.app_headers());
        let outcome = crate::lock::effect(|| self.port.send(core::graphql_call(endpoint, &request, wire, app_headers))).await;
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
        let mailbox = Arc::new(Mailbox::new());
        let headers = core::merge_headers(crate::lock::effect(|| self.port.app_headers()), HashMap::new());
        let (port, url, events) = (self.port.clone(), url.to_string(), mailbox.clone());
        match port.open_cable(url, headers, core::CONNECT_TIMEOUT_MS, events.clone()).await {
            CableOutcome::Open { connection } => Ok(Arc::new(ForeignSocket::new(connection, events)) as Arc<dyn core::CableSocket>),
            CableOutcome::Failed { failure, handshake_status } => Err(core::classify_upgrade(&failure, handshake_status)),
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
        let headers = core::merge_headers(HashMap::new(), request.headers.into_iter().collect());
        let target = PutTarget { url: request.url, headers, connect_ms: core::CONNECT_TIMEOUT_MS, request_ms: core::REQUEST_TIMEOUT_MS };
        match self.port.put(target, request.file, request.identity, progress).await {
            core::RawOutcome::Response { status, etag, .. } => Ok(core::PutReply { status, etag }),
            core::RawOutcome::Failed { .. } if cancel.is_cancelled() => Err(core::TransportError::Interrupted("the upload was cancelled".into())),
            core::RawOutcome::Failed { failure } => Err(core::classify(&failure, core::ExchangeKind::Post)),
        }
    }
}

#[cfg(test)]
mod tests;

