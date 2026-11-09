use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::transport::{self as core, EncryptedConfig, EncryptedTransport, Exchange, ServerConfig, SessionCode};
use super::database::{blocking, cancellable, parse_json, Database, Encrypted, Wiring};
use crate::owners::life::LifeReply;
use super::transport::{transport_error, ForeignTransport};
use super::{DbError, Transport};
use crate::store::Store;

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait EncryptedHttp: Send + Sync {
    /// The headers of the app, read again for every request.
    fn app_headers(&self) -> std::collections::HashMap<String, String>;
    /// Runs one POST built by the core and returns the raw outcome: the executor never classifies, follows a redirect or retries.
    async fn send(&self, request: core::HttpRequest) -> core::RawOutcome;
}

#[uniffi::export(rust, foreign)]
pub trait CredentialStore: Send + Sync {
    fn get(&self) -> Result<Option<String>, DbError>;
    fn set(&self, credential: String) -> Result<(), DbError>;
    fn clear(&self) -> Result<(), DbError>;
}

struct ForeignHttp { port: Arc<dyn EncryptedHttp>, origin: String }

impl Drop for ForeignHttp {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ForeignHttp::drop"); }
}

#[async_trait]
impl core::HttpExchange for ForeignHttp {
    async fn post(&self, path: &str, body: String, purpose: core::NetworkPurpose, wire: core::WireIds) -> Result<core::HttpReply, core::TransportError> {
        crate::lock::assert_no_library_lock("EncryptedHttp::post");
        let app_headers = crate::lock::effect(|| self.port.app_headers());
        let request = core::encrypted_call(&self.origin, path, body, (purpose, wire), app_headers)?;
        match crate::lock::effect(|| self.port.send(request)).await {
            core::RawOutcome::Response { status, body, .. } => Ok(core::HttpReply { status, body }),
            core::RawOutcome::Failed { failure } => Err(core::classify(&failure, core::ExchangeKind::Post)),
        }
    }

}

struct ForeignCredentials(Arc<dyn CredentialStore>);

impl Drop for ForeignCredentials {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("ForeignCredentials::drop"); }
}

impl core::CredentialSlot for ForeignCredentials {
    fn get(&self) -> Result<Option<String>, core::TransportError> { crate::lock::assert_no_library_lock("CredentialStore::get"); self.0.get().map_err(transport_error) }
    fn set(&self, credential: &str) -> Result<(), core::TransportError> { crate::lock::assert_no_library_lock("CredentialStore::set"); self.0.set(credential.to_string()).map_err(transport_error) }
    fn clear(&self) -> Result<(), core::TransportError> { crate::lock::assert_no_library_lock("CredentialStore::clear"); self.0.clear().map_err(transport_error) }
}

fn typed(error: core::TransportError) -> DbError {
    match error {
        core::TransportError::Graphql { messages, .. } if messages.len() == 1 => DbError::Session { code: SessionCode::of(&messages[0]), detail: messages[0].clone() },
        other => DbError::from(other),
    }
}

fn refused(answer: core::Answer) -> Result<core::Answer, DbError> {
    match crate::transport::refusal_name(&answer.body) {
        Some(name) => Err(DbError::Session { code: SessionCode::of(&name), detail: name }),
        None => Ok(answer),
    }
}

pub(super) fn wire(
    db: &Arc<Store>,
    clock: &Arc<dyn crate::runtime::Clock>,
    encrypted: EncryptedConfig,
    http: Arc<dyn EncryptedHttp>,
    credentials: Arc<dyn CredentialStore>,
    transport: Arc<dyn Transport>,
) -> Result<Wiring, DbError> {
    let seal = crate::owners::session_seal::SealConfig::new(&encrypted, clock.clone()).map_err(|error| DbError::Invalid { detail: format!("{error:?}") })?;
    let exchange = Arc::new(Exchange::new(&encrypted, Arc::new(ForeignHttp { port: http, origin: encrypted.origin.clone() }), clock.clone()).map_err(|error| DbError::Invalid { detail: error.to_string() })?);
    let secret = Arc::new(crate::transport::encrypted::credential::Secret::new(Arc::new(ForeignCredentials(credentials))));
    crate::transport::encrypted::credential::finish_session_end(db, &secret).map_err(DbError::from)?;
    let rest: Arc<dyn core::Upstream> = Arc::new(ForeignTransport::beside_session(transport));
    let encrypted_transport = Arc::new(EncryptedTransport::new(exchange.clone(), secret.clone(), db.clone(), rest));
    let cable = encrypted_transport.cable_lines();
    Ok(Wiring { transport: Some(encrypted_transport.clone()), encrypted: Some(Encrypted { exchange, secret, transport: encrypted_transport }), cable: Some(cable), seal: Some(Arc::new(seal)) })
}

#[derive(uniffi::Object)]
pub struct RetryProgress { pub(super) inner: Arc<core::RetryCalls> }

#[uniffi::export]
impl RetryProgress {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { inner: Arc::default() }) }

    pub fn current(&self) -> core::RetrySnapshot { self.inner.current() }

    /// A reader of the snapshots: its first `next` returns the current snapshot, each later one the newest snapshot that is newer than the last it returned. A slow reader skips the snapshots between, never the last one.
    pub fn subscribe(&self) -> Arc<RetrySubscription> {
        let (stopped, _) = tokio::sync::watch::channel(false);
        Arc::new(RetrySubscription { changes: tokio::sync::Mutex::new(Reading { changes: self.inner.watch(), first: true }), stopped })
    }
}

struct Reading { changes: tokio::sync::watch::Receiver<core::RetrySnapshot>, first: bool }

#[derive(uniffi::Object)]
pub struct RetrySubscription { changes: tokio::sync::Mutex<Reading>, stopped: tokio::sync::watch::Sender<bool> }

#[uniffi::export]
impl RetrySubscription {
    /// The next snapshot, `None` once the reader was cancelled or the progress is gone.
    pub async fn next(&self) -> Option<core::RetrySnapshot> {
        let mut stopped = self.stopped.subscribe();
        let mut reading = self.changes.lock().await;
        if *stopped.borrow() { return None; }
        if std::mem::take(&mut reading.first) { return Some(reading.changes.borrow_and_update().clone()); }
        tokio::select! {
            biased;
            _ = async { let _ = stopped.wait_for(|stopped| *stopped).await; } => None,
            changed = reading.changes.changed() => changed.ok().filter(|_| !*self.stopped.borrow()).map(|()| reading.changes.borrow_and_update().clone()),
        }
    }

    /// Ends this reader: a waiting `next` and every later one return `None`.
    pub fn cancel(&self) { crate::lock::effect(|| self.stopped.send_replace(true)); }
}

fn session_body(body: &str) -> Result<String, DbError> {
    let answer = core::Answer { sequence: None, body: parse_json(body)? };
    Ok(refused(answer)?.body.to_string())
}

#[uniffi::export]
impl Database {
    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> {
        self.not_closed()?;
        let body_json = self.offload(move || { parse_json::<Value>(&body_json)?; Ok(body_json) }).await?;
        match self.await_call(self.life.session_call(operation, body_json)).await? {
            LifeReply::Session(Ok(body)) => self.offload(move || session_body(&body)).await,
            LifeReply::Session(Err(error)) => Err(session_failure(error)),
            _ => Err(DbError::Interrupted { detail: "the call ended".into() }),
        }
    }

    pub async fn interrupted_session_answer(&self) -> Result<Option<String>, DbError> {
        match self.await_life(self.life.interrupted_session_answer()).await? {
            LifeReply::SessionResume(Ok(Some(body))) => self.offload(move || session_body(&body)).await.map(Some),
            LifeReply::SessionResume(Ok(None)) => Ok(None),
            LifeReply::SessionResume(Err(error)) => Err(session_failure(error)),
            _ => Err(DbError::Interrupted { detail: "the call ended".into() }),
        }
    }

    /// Ends the session: the old work ends, the credential and the data of the account go. Returns after the clearing. The same call ends a plain or local database too.
    pub async fn session_reset(&self) -> Result<(), DbError> {
        match self.await_life(self.life.session_reset()).await? {
            LifeReply::Life(Ok(())) => Ok(()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Storage)) => Err(DbError::Storage { detail: "the account could not be cleared".into() }),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Closed)) => Err(super::command::closed()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Runtime)) => Err(DbError::Runtime { detail: "the owners failed".into() }),
            _ => Err(super::cause_error(&super::EndCause::SessionEnded)),
        }
    }
}

pub(crate) fn session_failure(error: crate::owners::session::SessionError) -> DbError {
    use crate::owners::session::SessionError as Failed;
    match error {
        Failed::Invalid => DbError::Invalid { detail: "the answer does not confirm the sequence".into() },
        Failed::NoCredential => DbError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
        Failed::StateLost => DbError::Session { code: SessionCode::StateLost, detail: "the stored credential does not match the stored session state".into() },
        Failed::Revoked { body } => DbError::Session { code: SessionCode::SessionEnded, detail: crate::owners::session::revoked_detail(&body) },
        Failed::SequenceConflict => DbError::Session { code: SessionCode::SequenceConflict, detail: "the sequence of the request conflicts with the server".into() },
        Failed::Unauthorized => DbError::Unauthorized { detail: "the server refused the credential".into() },
        Failed::Security { code, detail } => DbError::Security { code, detail },
        Failed::Http { status } => DbError::Http { status, detail: "the request was refused".into() },
        Failed::TooLarge { size, limit } => DbError::RequestTooLarge { size, limit },
        Failed::Rejected(crate::owners::contract::RejectReason::Storage(detail)) => DbError::Storage { detail },
        Failed::Rejected(crate::owners::contract::RejectReason::NoSession) => DbError::NoSession { detail: "the session of this request ended".into() },
        Failed::Rejected(crate::owners::contract::RejectReason::Closed) => super::command::closed(),
        Failed::Rejected(reason @ (crate::owners::contract::RejectReason::StaleGeneration { .. } | crate::owners::contract::RejectReason::Invalid)) => DbError::Internal { detail: format!("the request was refused by the store: {reason:?}") },
        Failed::Session { code, body } => DbError::from(core::TransportError::Session { code, detail: body }),
        Failed::SessionExists => DbError::Session { code: SessionCode::AlreadyAuthenticated, detail: "the database has a session".into() },
        Failed::Storage(detail) => DbError::Storage { detail },
        Failed::NoSession => DbError::NoSession { detail: "the database has no session".into() },
        Failed::Interrupted => DbError::Interrupted { detail: "the call ended".into() },
        Failed::Closed => super::command::closed(),
    }
}

/// The server config before the database exists: `observers` report the network state to the fetch and stop when it ends.
#[uniffi::export]
pub async fn fetch_server_config(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, observers: Option<Arc<dyn super::PlatformObserver>>) -> Result<ServerConfig, DbError> {
    fetch_observed(encrypted, platform, version, build, FetchPorts { http, retries, observers }, Arc::new(crate::runtime::SystemClock)).await
}

#[derive(uniffi::Record)]
pub struct FetchPorts {
    pub http: Arc<dyn EncryptedHttp>,
    pub retries: Option<Arc<RetryProgress>>,
    pub observers: Option<Arc<dyn super::PlatformObserver>>,
}

#[uniffi::export]
pub async fn fetch_server_config_with_clock(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, ports: FetchPorts, clock: Arc<super::TestClock>) -> Result<ServerConfig, DbError> {
    fetch_observed(encrypted, platform, version, build, ports, clock.manual()).await
}

async fn fetch_observed(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, ports: FetchPorts, clock: Arc<dyn crate::runtime::Clock>) -> Result<ServerConfig, DbError> {
    struct StopOnDrop(Option<Arc<dyn super::PlatformObserver>>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) { if let Some(observers) = self.0.take() { crate::lock::effect(|| observers.stop()); } }
    }
    let FetchPorts { http, retries, observers } = ports;
    let connectivity = core::Connectivity::new(false);
    let _stop = StopOnDrop(observers.clone());
    if let Some(observers) = observers {
        let sink = super::OnlineSink::of_connectivity(connectivity.clone());
        blocking(move || { observers.start(sink); Ok(()) }).await?;
    }
    fetch_server_config_with(encrypted, platform, version, build, http, retries, Waits { connectivity, clock }).await
}

pub(super) struct Waits { pub connectivity: Arc<core::Connectivity>, pub clock: Arc<dyn crate::runtime::Clock> }

pub(super) async fn fetch_server_config_with(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, waits: Waits) -> Result<ServerConfig, DbError> {
    cancellable(async move {
        let progress = retries.map(|retries| retries.inner.clone()).unwrap_or_default();
        core::encrypted::bootstrap::fetch(&encrypted, &core::encrypted::bootstrap::AppRelease { platform, version: &version, build: build.as_deref() }, Arc::new(ForeignHttp { port: http, origin: encrypted.origin.clone() }), &*waits.clock, &progress, &waits.connectivity).await.map_err(typed)
    }).await?
}
