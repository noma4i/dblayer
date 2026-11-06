use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::transport::{self as core, EncryptedClient, EncryptedConfig, EncryptedTransport, ServerConfig, SessionCode};
use super::database::{cancellable, Database, Wiring};
use crate::owners::life::LifeReply;
use super::transport::{transport_error, ForeignTransport};
use super::{DbError, Transport};

#[derive(uniffi::Record)]
pub struct EncryptedReply { pub status: u16, pub body: String }

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait EncryptedHttp: Send + Sync {
    async fn post(&self, path: String, body: String, purpose: core::NetworkPurpose) -> Result<EncryptedReply, DbError>;
}

#[uniffi::export(rust, foreign)]
pub trait CredentialStore: Send + Sync {
    fn get(&self) -> Result<Option<String>, DbError>;
    fn set(&self, credential: String) -> Result<(), DbError>;
    fn clear(&self) -> Result<(), DbError>;
}

struct ForeignHttp(Arc<dyn EncryptedHttp>);

impl Drop for ForeignHttp {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("ForeignHttp::drop"); }
}

#[async_trait]
impl core::HttpExchange for ForeignHttp {
    async fn post(&self, path: &str, body: String, purpose: core::NetworkPurpose) -> Result<core::HttpReply, core::TransportError> {
        crate::db::lock::assert_no_library_lock("EncryptedHttp::post");
        let reply = self.0.post(path.to_string(), body, purpose).await.map_err(transport_error)?;
        Ok(core::HttpReply { status: reply.status, body: reply.body })
    }

}

struct ForeignCredentials(Arc<dyn CredentialStore>);

impl Drop for ForeignCredentials {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("ForeignCredentials::drop"); }
}

impl core::CredentialSlot for ForeignCredentials {
    fn get(&self) -> Result<Option<String>, core::TransportError> { crate::db::lock::assert_no_library_lock("CredentialStore::get"); self.0.get().map_err(transport_error) }
    fn set(&self, credential: &str) -> Result<(), core::TransportError> { crate::db::lock::assert_no_library_lock("CredentialStore::set"); self.0.set(credential.to_string()).map_err(transport_error) }
    fn clear(&self) -> Result<(), core::TransportError> { crate::db::lock::assert_no_library_lock("CredentialStore::clear"); self.0.clear().map_err(transport_error) }
}

fn typed(error: core::TransportError) -> DbError {
    match error {
        core::TransportError::Graphql { messages, .. } if messages.len() == 1 => DbError::Session { code: SessionCode::of(&messages[0]), detail: messages[0].clone() },
        other => DbError::from(other),
    }
}

fn refused(answer: core::Answer) -> Result<core::Answer, DbError> {
    match answer.body.get("error").and_then(Value::as_str) {
        Some(code) => Err(DbError::Session { code: SessionCode::of(code), detail: code.to_string() }),
        None => Ok(answer),
    }
}

pub(super) fn wire(
    db: &crate::db::Db,
    clock: &Arc<dyn crate::pacing::Clock>,
    encrypted: EncryptedConfig,
    http: Arc<dyn EncryptedHttp>,
    credentials: Arc<dyn CredentialStore>,
    transport: Arc<dyn Transport>,
    retries: Option<Arc<RetryProgress>>,
) -> Result<Wiring, DbError> {
    let seal = crate::owners::session_seal::SealConfig::new(&encrypted, clock.clone()).map_err(|error| DbError::Invalid { detail: format!("{error:?}") })?;
    let client = EncryptedClient::new(&encrypted, Arc::new(ForeignHttp(http)), Arc::new(ForeignCredentials(credentials)), db.clone(), clock.clone())
        .map_err(|error| DbError::Invalid { detail: error.to_string() })?;
    let client = match retries {
        Some(retries) => client.observed_by(retries.inner.clone()),
        None => client,
    };
    let client = Arc::new(client);
    let rest: Arc<dyn core::Transport> = Arc::new(ForeignTransport(transport));
    let cable = client.cable_lines(rest.clone());
    Ok((Some(Arc::new(EncryptedTransport::new(client.clone(), rest))), Some(client), Some(cable), Some(Arc::new(seal))))
}

#[uniffi::export(rust, foreign)]
pub trait RetryProgressListener: Send + Sync {
    fn changed(&self, snapshot: core::RetrySnapshot);
}

#[derive(uniffi::Object)]
pub struct RetryProgress { pub(super) inner: Arc<core::RetryCalls> }

#[uniffi::export]
impl RetryProgress {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { inner: Arc::default() }) }

    pub fn current(&self) -> core::RetrySnapshot { self.inner.current() }

    /// Delivers the current snapshot at once, then each newer one. A slow listener sees the newest snapshot and delays no request.
    pub fn subscribe(&self, listener: Arc<dyn RetryProgressListener>) -> Result<Arc<RetrySubscription>, DbError> {
        let mut changes = self.inner.watch();
        let runtime = super::database::executor()?;
        let calls = Arc::new(crate::ffi::CallState::new());
        let task = crate::task::spawn_on(crate::task::Carry::Detached, runtime.handle(), {
            let calls = calls.clone();
            async move {
                loop {
                    let snapshot = changes.borrow_and_update().clone();
                    let (listener, calls) = (listener.clone(), calls.clone());
                    let _ = crate::task::spawn_blocking(crate::task::Carry::Detached, move || {
                        crate::db::lock::assert_no_library_lock("RetryProgressListener::changed");
                        calls.call(|| listener.changed(snapshot))
                    }).await;
                    if changes.changed().await.is_err() { break; }
                }
            }
        });
        Ok(Arc::new(RetrySubscription { task: task.abort_handle(), calls }))
    }
}

#[derive(uniffi::Object)]
pub struct RetrySubscription { task: tokio::task::AbortHandle, pub(crate) calls: Arc<crate::ffi::CallState> }

#[uniffi::export]
impl RetrySubscription {
    pub fn cancel(&self) {
        self.calls.cancel();
        crate::db::lock::effect(|| self.task.abort());
    }
}

impl Drop for RetrySubscription {
    fn drop(&mut self) { self.cancel(); }
}

#[uniffi::export]
impl Database {
    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> {
        serde_json::from_str::<Value>(&body_json)?;
        match self.await_life(self.life.session_call(operation, body_json)).await? {
            LifeReply::Session(Ok(body)) => {
                let answer = core::Answer { sequence: None, body: serde_json::from_str(&body)? };
                Ok(refused(answer)?.body.to_string())
            }
            LifeReply::Session(Err(error)) => Err(session_failure(error)),
            _ => Err(super::command::ended()),
        }
    }

    pub async fn interrupted_session_answer(&self) -> Result<Option<String>, DbError> {
        match self.await_life(self.life.interrupted_session_answer()).await? {
            LifeReply::SessionResume(Ok(Some(body))) => {
                let answer = core::Answer { sequence: None, body: serde_json::from_str(&body)? };
                Ok(Some(refused(answer)?.body.to_string()))
            }
            LifeReply::SessionResume(Ok(None)) => Ok(None),
            LifeReply::SessionResume(Err(error)) => Err(session_failure(error)),
            _ => Err(super::command::ended()),
        }
    }

    /// Ends the session: the old work ends, the credential and the data of the account go. Returns after the clearing. The same call ends a plain or local database too.
    pub async fn session_reset(&self) -> Result<(), DbError> {
        match self.await_life(self.life.session_reset()).await? {
            LifeReply::Life(Ok(())) => Ok(()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Storage)) => Err(DbError::Database { detail: "the account could not be cleared".into() }),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Closed)) => Err(super::command::closed()),
            LifeReply::Life(Err(crate::owners::lifecycle::LifecycleError::Runtime)) => Err(DbError::Database { detail: "the owners failed".into() }),
            _ => Err(super::command::ended()),
        }
    }
}

pub(super) fn session_failure(error: crate::owners::session::SessionError) -> DbError {
    use crate::owners::session::SessionError as Failed;
    match error {
        Failed::Invalid | Failed::Protocol => DbError::Invalid { detail: "the answer does not confirm the sequence".into() },
        Failed::NoCredential => DbError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
        Failed::StateLost => DbError::Session { code: SessionCode::StateLost, detail: "the stored credential does not match the stored session state".into() },
        Failed::Revoked => DbError::Session { code: SessionCode::SessionEnded, detail: "the session was ended by the server".into() },
        Failed::SequenceConflict => DbError::Session { code: SessionCode::SequenceConflict, detail: "the sequence of the request conflicts with the server".into() },
        Failed::Unauthorized => DbError::Http { status: 401, detail: String::new() },
        Failed::Http { status } => DbError::Http { status, detail: "the request was refused".into() },
        Failed::TooLarge { size, limit } => DbError::RequestTooLarge { size, limit },
        Failed::Rejected(reason) => DbError::Database { detail: format!("the request was not stored: {reason:?}") },
        Failed::Session(code) => DbError::Session { code, detail: code.name().to_string() },
        Failed::SessionExists => DbError::Session { code: SessionCode::AlreadyAuthenticated, detail: "the database has a session".into() },
        Failed::Storage(detail) => DbError::Database { detail },
        Failed::NoSession => DbError::NoSession { detail: "the database has no session".into() },
        Failed::Interrupted => super::command::ended(),
        Failed::Closed => super::command::closed(),
    }
}

#[uniffi::export]
pub async fn fetch_server_config(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, connectivity: Arc<core::Connectivity>) -> Result<ServerConfig, DbError> {
    cancellable(async move {
        let progress = retries.map(|retries| retries.inner.clone()).unwrap_or_default();
        core::encrypted::bootstrap::fetch(&encrypted, &core::encrypted::bootstrap::AppRelease { platform, version: &version, build: build.as_deref() }, Arc::new(ForeignHttp(http)), &crate::pacing::SystemClock, &progress, &connectivity).await.map_err(typed)
    }).await?
}
