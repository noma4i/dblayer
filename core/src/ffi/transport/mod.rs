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
    /// Makes the next request and the next cable open use a new connection (`URLSession.flush`, `connectionPool.evictAll()`). Requests in flight finish on their own.
    fn rotate_connections(&self);
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
    /// The most bytes of the answer the platform reads, counted while reading.
    pub response_limit: u64,
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
    Status,
}

/// The foreground registrations of one sink: a delivery counts only while its token is live, and revoking waits for the deliveries in flight.
#[derive(Default)]
pub(crate) struct Registrations { live: std::sync::RwLock<Registered> }

#[derive(Default)]
struct Registered { tokens: std::collections::BTreeSet<u64>, next: u64 }

impl Registrations {
    fn issue(&self) -> u64 {
        let mut state = self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next += 1;
        let token = state.next;
        state.tokens.insert(token);
        token
    }

    fn revoke(&self, token: u64) { self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner).tokens.remove(&token); }

    pub(crate) fn revoke_all(&self) { self.live.write().unwrap_or_else(std::sync::PoisonError::into_inner).tokens.clear(); }

    fn deliver(&self, token: u64, send: impl FnOnce()) {
        let state = self.live.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.tokens.contains(&token) { send(); }
    }
}

/// The inputs of the platform signals: each OS value reaches the core as one input, without dedup.
#[derive(uniffi::Object)]
pub struct OnlineSink { target: SignalTarget, registrations: Arc<Registrations> }

impl OnlineSink {
    pub(crate) fn of_life(life: &Arc<crate::owners::life::Life>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Life(Arc::downgrade(life)), registrations: Arc::default() }) }

    pub(crate) fn of_status() -> Arc<Self> { Arc::new(Self { target: SignalTarget::Status, registrations: Arc::default() }) }

    pub(crate) fn of_connectivity(connectivity: Arc<core::Connectivity>) -> Arc<Self> { Arc::new(Self { target: SignalTarget::Connectivity(connectivity), registrations: Arc::default() }) }

    pub(crate) fn apply_online(&self, online: bool) {
        match &self.target {
            SignalTarget::Status => self.apply_path(super::network_status::path_of_online(online)),
            SignalTarget::Connectivity(connectivity) => connectivity.change(online, super::network_status::next_stamp()),
            SignalTarget::Life(_) => { if let Some(life) = self.life() { post_life(&life, online, super::network_status::next_stamp()); } }
        }
    }

    fn apply_path(&self, path: super::NetworkPath) {
        let stamp = super::network_status::record_network(path);
        feed_followers(path.online, stamp);
    }

    pub(crate) fn registrations(&self) -> Arc<Registrations> { self.registrations.clone() }

    fn life(&self) -> Option<Arc<crate::owners::life::Life>> {
        match &self.target {
            SignalTarget::Life(life) => life.upgrade(),
            SignalTarget::Connectivity(_) | SignalTarget::Status => None,
        }
    }
}

#[uniffi::export]
impl OnlineSink {
    /// Sends the change to the owners and returns.
    pub fn set_online(&self, online: bool) { self.apply_online(online); }

    /// The status input of the process: the default network of the phone, its online flag feeds the owners as `set_online` does.
    pub fn set_path(&self, path: super::NetworkPath) {
        match &self.target {
            SignalTarget::Status => self.apply_path(path),
            SignalTarget::Connectivity(_) | SignalTarget::Life(_) => self.apply_online(path.online),
        }
    }

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

    /// One move to the background. Dropped when the registration of `token` ended.
    pub fn enter_background(&self, token: u64) {
        if let Some(life) = self.life() { self.registrations.deliver(token, || drop(life.background())); }
    }
}

enum Follower {
    Life(std::sync::Weak<crate::owners::life::Life>),
    Connectivity(std::sync::Weak<crate::transport::Connectivity>),
}

fn followers() -> &'static std::sync::Mutex<Vec<Follower>> {
    static FOLLOWERS: std::sync::OnceLock<std::sync::Mutex<Vec<Follower>>> = std::sync::OnceLock::new();
    FOLLOWERS.get_or_init(Default::default)
}

fn alive(follower: &Follower) -> bool {
    match follower {
        Follower::Life(life) => life.strong_count() > 0,
        Follower::Connectivity(connectivity) => connectivity.strong_count() > 0,
    }
}

pub(crate) fn follow_life(life: &Arc<crate::owners::life::Life>) {
    let mut all = crate::lock::lock(followers());
    all.retain(alive);
    all.push(Follower::Life(Arc::downgrade(life)));
    drop(all);
    let processed = *super::network_status::network_value().borrow();
    if let Some(path) = processed.path { post_life(life, path.online, processed.stamp); }
}

pub(crate) fn follow_connectivity(connectivity: &Arc<crate::transport::Connectivity>) {
    let mut all = crate::lock::lock(followers());
    all.retain(alive);
    all.push(Follower::Connectivity(Arc::downgrade(connectivity)));
    drop(all);
    let processed = *super::network_status::network_value().borrow();
    if let Some(path) = processed.path { connectivity.change(path.online, processed.stamp); }
}

pub(crate) fn feed_followers(online: bool, stamp: u64) {
    let live: Vec<Follower> = {
        let mut all = crate::lock::lock(followers());
        all.retain(alive);
        all.iter().map(|follower| match follower {
            Follower::Life(life) => Follower::Life(life.clone()),
            Follower::Connectivity(connectivity) => Follower::Connectivity(connectivity.clone()),
        }).collect()
    };
    for follower in live {
        match follower {
            Follower::Life(life) => { if let Some(life) = life.upgrade() { post_life(&life, online, stamp); } }
            Follower::Connectivity(connectivity) => { if let Some(connectivity) = connectivity.upgrade() { connectivity.change(online, stamp); } }
        }
    }
}

pub(super) fn post_life(life: &Arc<crate::owners::life::Life>, online: bool, stamp: u64) { drop(life.set_online(online, stamp)); }

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

pub(super) struct ForeignTransport { port: Arc<dyn Transport>, endpoint: Option<String>, clock: Arc<dyn crate::runtime::Clock> }

/// D-493: one port call of an HTTP exchange ends within `REQUEST_TIMEOUT_MS` of the core clock. The port future is dropped at the bound (the platform task is cancelled) and a late answer is lost.
pub(super) async fn bounded(clock: &dyn crate::runtime::Clock, call: impl std::future::Future<Output = core::RawOutcome>) -> core::RawOutcome {
    let deadline = clock.now().saturating_add(std::time::Duration::from_millis(core::REQUEST_TIMEOUT_MS));
    tokio::select! {
        outcome = call => outcome,
        () = clock.sleep_until(deadline) => core::RawOutcome::Failed { failure: core::RawFailure::deadline() },
    }
}

/// D-492: a failure of the network class rotates the connections before the outcome goes to the author.
pub(super) fn rotated(outcome: core::RawOutcome, rotate: impl FnOnce()) -> core::RawOutcome {
    if matches!(&outcome, core::RawOutcome::Failed { failure } if failure.is_network()) { rotate(); }
    outcome
}

impl ForeignTransport {
    pub(super) fn plain(port: Arc<dyn Transport>, endpoint: Option<String>, clock: Arc<dyn crate::runtime::Clock>) -> Result<Self, DbError> {
        let endpoint = core::graphql_endpoint(&endpoint.unwrap_or_default()).map_err(DbError::from)?;
        Ok(Self { port, endpoint: Some(endpoint), clock })
    }

    pub(super) fn beside_session(port: Arc<dyn Transport>, clock: Arc<dyn crate::runtime::Clock>) -> Self { Self { port, endpoint: None, clock } }

    fn rotate(&self) { crate::lock::effect(|| self.port.rotate_connections()); }

    async fn graphql(&self, request: core::GraphQLRequest, kind: core::ExchangeKind) -> Result<core::Response, core::TransportError> {
        let Some(endpoint) = &self.endpoint else { return Err(core::TransportError::Config { code: core::ConfigCode::InvalidRequest, detail: "the database has no GraphQL endpoint".into() }) };
        let wire = request.authored()?;
        let app_headers = crate::lock::effect(|| self.port.app_headers());
        let outcome = rotated(bounded(&*self.clock, crate::lock::effect(|| self.port.send(core::graphql_call(endpoint, &request, wire, app_headers)))).await, || self.rotate());
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
        DbError::NoSession { .. } => core::TransportError::NoSession,
        DbError::Http { status, detail } => core::TransportError::Http { status, body: detail },
        DbError::RequestTooLarge { size, limit } => core::TransportError::RequestTooLarge { size, limit },
        DbError::ResponseTooLarge { limit } => core::TransportError::ResponseTooLarge { limit },
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
            CableOutcome::Failed { failure, handshake_status } => {
                if handshake_status.is_none() && failure.is_network() { self.rotate(); }
                Err(core::classify_upgrade(&failure, handshake_status))
            }
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
        let target = PutTarget { url: request.url, headers, connect_ms: core::CONNECT_TIMEOUT_MS, request_ms: core::REQUEST_TIMEOUT_MS, response_limit: core::MAX_PUT_RESPONSE_BYTES };
        let outcome = self.port.put(target, request.file, request.identity, progress).await;
        let outcome = if cancel.is_cancelled() { outcome } else { rotated(outcome, || self.rotate()) };
        match outcome {
            core::RawOutcome::Response { status, etag, .. } => Ok(core::PutReply { status, etag }),
            core::RawOutcome::Failed { .. } if cancel.is_cancelled() => Err(core::TransportError::Interrupted("the upload was cancelled".into())),
            core::RawOutcome::Failed { failure } => Err(core::classify(&failure, core::ExchangeKind::Put)),
        }
    }
}

#[cfg(test)]
mod tests;

