use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::transport::{self as core, EncryptedConfig, EncryptedTransport, Exchange, ServerConfig, SessionCode};
use super::database::{parse_json, Database, Encrypted, Wiring};
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
    pub fn new() -> Arc<Self> { Arc::new(Self { inner: core::retry::retry_progress().clone() }) }

    pub fn current(&self) -> core::RetrySnapshot { self.inner.current() }

    /// A reader of the snapshots: its first `next` returns the current snapshot, each later one the newest snapshot that is newer than the last it returned. A slow reader skips the snapshots between, never the last one.
    pub fn subscribe(&self) -> Arc<RetrySubscription> { Arc::new(RetrySubscription(super::reading::Reader::of(self.inner.watch()))) }
}

#[derive(uniffi::Object)]
pub struct RetrySubscription(super::reading::Reader<core::RetrySnapshot>);

#[uniffi::export]
impl RetrySubscription {
    /// The next snapshot, `None` once the reader was cancelled or the progress is gone.
    pub async fn next(&self) -> Option<core::RetrySnapshot> { self.0.next().await }

    /// Ends this reader: a waiting `next` and every later one return `None`.
    pub fn cancel(&self) { self.0.cancel(); }
}

#[uniffi::export]
impl Database {
    pub fn retry_progress(&self) -> Arc<RetryProgress> { RetryProgress::new() }

    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        this_db.not_closed()?;
        let body_json = this_db.offload(move || { parse_json::<Value>(&body_json)?; Ok(body_json) }).await?;
        match this_db.await_life(this_db.life.session_call(operation, body_json)).await? {
            LifeReply::Session(Ok(body)) => Ok(body),
            LifeReply::Session(Err(error)) => Err(session_failure(error)),
            _ => Err(super::database::foreign_reply()),
        }
    }).await? }

    pub async fn interrupted_session_answer(&self) -> Result<Option<String>, DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        match this_db.await_life(this_db.life.interrupted_session_answer()).await? {
            LifeReply::SessionResume(Ok(Some(body))) => Ok(Some(body)),
            LifeReply::SessionResume(Ok(None)) => Ok(None),
            LifeReply::SessionResume(Err(error)) => Err(session_failure(error)),
            _ => Err(super::database::foreign_reply()),
        }
    }).await? }

    /// Ends the session: the old work ends, the credential and the data of the account go. Returns after the clearing. The same call ends a plain or local database too.
    pub async fn session_reset(&self) -> Result<(), DbError> { let this_db = self.clone(); super::database::on_core(self.scope(), async move {
        match this_db.await_life(this_db.life.session_reset()).await? {
            LifeReply::Life(Ok(())) => Ok(()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Storage)) => Err(DbError::Storage { detail: "the account could not be cleared".into() }),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Closed)) => Err(super::command::closed()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Runtime)) => Err(DbError::Runtime { detail: "the owners failed".into() }),
            _ => Err(super::cause_error(&super::EndCause::SessionEnded)),
        }
    }).await? }
}

pub(crate) fn session_failure(error: crate::owners::session::SessionError) -> DbError {
    use crate::owners::session::SessionError as Failed;
    match error {
        Failed::Invalid => DbError::Invalid { detail: "the answer does not confirm the sequence".into() },
        Failed::NoCredential => DbError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
        Failed::StateLost => DbError::Session { code: SessionCode::StateLost, detail: "the stored credential does not match the stored session state".into() },
        Failed::Revoked { body } => DbError::Session { code: SessionCode::SessionEnded, detail: body },
        Failed::SequenceConflict => DbError::Session { code: SessionCode::SequenceConflict, detail: "the sequence of the request conflicts with the server".into() },
        Failed::Unauthorized => DbError::Unauthorized { detail: "the server refused the credential".into() },
        Failed::Security { code, detail } => DbError::Security { code, detail },
        Failed::Http { status } => DbError::Http { status, detail: "the request was refused".into() },
        Failed::Network => DbError::Network { detail: "the request did not reach the server".into() },
        Failed::TooLarge { size, limit } => DbError::RequestTooLarge { size, limit },
        Failed::ResponseTooLarge { limit } => DbError::ResponseTooLarge { limit },
        Failed::Rejected(crate::owners::contract::RejectReason::Storage(detail)) => DbError::Storage { detail },
        Failed::Rejected(crate::owners::contract::RejectReason::NoSession) => super::cause_error(&super::EndCause::SessionEnded),
        Failed::Rejected(crate::owners::contract::RejectReason::Closed) => super::command::closed(),
        Failed::Rejected(crate::owners::contract::RejectReason::Invalid(detail)) => DbError::Invalid { detail },
        Failed::Session { code, body } => DbError::from(core::TransportError::Session { code, detail: body }),
        Failed::Graphql { code, body } => DbError::from(core::TransportError::graphql_answer(code, body)),
        Failed::SessionExists => DbError::Session { code: SessionCode::AlreadyAuthenticated, detail: "the database has a session".into() },
        Failed::Storage(detail) => DbError::Storage { detail },
        Failed::NoSession => super::cause_error(&super::EndCause::SessionEnded),
        Failed::Interrupted => DbError::Interrupted { detail: "the call ended".into() },
        Failed::NotSent => DbError::Network { detail: "the request was not sent".into() },
        Failed::Closed => super::command::closed(),
    }
}

/// The server config before the database exists: `observers` report the network state to the fetch and stop when it ends.
#[uniffi::export]
pub async fn fetch_server_config(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, observers: Option<Arc<dyn super::PlatformObserver>>) -> Result<ServerConfig, DbError> {
    super::database::on_core(super::database::Scope::root(), fetch_observed(encrypted, platform, version, build, FetchPorts { http, retries, observers }, Arc::new(crate::runtime::SystemClock))).await?
}

#[derive(uniffi::Record)]
pub struct FetchPorts {
    pub http: Arc<dyn EncryptedHttp>,
    pub retries: Option<Arc<RetryProgress>>,
    pub observers: Option<Arc<dyn super::PlatformObserver>>,
}

#[uniffi::export]
pub async fn fetch_server_config_with_clock(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, ports: FetchPorts, clock: Arc<super::TestClock>) -> Result<ServerConfig, DbError> {
    super::database::on_core(super::database::Scope::root(), fetch_observed(encrypted, platform, version, build, ports, clock.manual())).await?
}

async fn fetch_observed(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, ports: FetchPorts, clock: Arc<dyn crate::runtime::Clock>) -> Result<ServerConfig, DbError> {
    struct StopOnDrop(Option<Arc<dyn super::PlatformObserver>>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) { if let Some(observers) = self.0.take() { crate::lock::effect(|| observers.stop()); } }
    }
    let FetchPorts { http, retries, observers } = ports;
    let connectivity = core::Connectivity::new(false);
    super::transport::follow_connectivity(&connectivity);
    let _stop = StopOnDrop(observers.clone());
    if let Some(observers) = observers {
        let sink = super::OnlineSink::of_connectivity(connectivity.clone());
        super::database::start_observer(observers, sink)?.await.map_err(super::database::joined)?;
    }
    fetch_server_config_with(encrypted, platform, version, build, http, retries, Waits { connectivity, clock }).await
}

pub(super) struct Waits { pub connectivity: Arc<core::Connectivity>, pub clock: Arc<dyn crate::runtime::Clock> }

pub(super) async fn fetch_server_config_with(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, waits: Waits) -> Result<ServerConfig, DbError> {
    let progress = retries.map(|retries| retries.inner.clone()).unwrap_or_else(|| core::retry::retry_progress().clone());
    core::encrypted::bootstrap::fetch(&encrypted, &core::encrypted::bootstrap::AppRelease { platform, version: &version, build: build.as_deref() }, Arc::new(ForeignHttp { port: http, origin: encrypted.origin.clone() }), &*waits.clock, &progress, &waits.connectivity).await.map_err(DbError::from)
}
