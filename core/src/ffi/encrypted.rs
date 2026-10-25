use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::sync::SyncError;
use crate::transport::{self as core, EncryptedClient, EncryptedConfig, EncryptedTransport, ServerConfig, SessionCode};
use super::database::{cancellable, Database, Wiring};
use super::transport::{transport_error, ForeignTransport};
use super::{DbError, Transport};

#[derive(uniffi::Record)]
pub struct EncryptedReply { pub status: u16, pub body: String }

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait EncryptedHttp: Send + Sync {
    async fn post(&self, path: String, body: String) -> Result<EncryptedReply, DbError>;
}

#[uniffi::export(rust, foreign)]
pub trait CredentialStore: Send + Sync {
    fn get(&self) -> Result<Option<String>, DbError>;
    fn set(&self, credential: String) -> Result<(), DbError>;
    fn clear(&self) -> Result<(), DbError>;
}

struct ForeignHttp(Arc<dyn EncryptedHttp>);

#[async_trait]
impl core::HttpExchange for ForeignHttp {
    async fn post(&self, path: &str, body: String) -> Result<core::HttpReply, core::TransportError> {
        let reply = self.0.post(path.to_string(), body).await.map_err(transport_error)?;
        Ok(core::HttpReply { status: reply.status, body: reply.body })
    }

}

struct ForeignCredentials(Arc<dyn CredentialStore>);

impl core::CredentialSlot for ForeignCredentials {
    fn get(&self) -> Result<Option<String>, core::TransportError> { self.0.get().map_err(transport_error) }
    fn set(&self, credential: &str) -> Result<(), core::TransportError> { self.0.set(credential.to_string()).map_err(transport_error) }
    fn clear(&self) -> Result<(), core::TransportError> { self.0.clear().map_err(transport_error) }
}

fn typed(error: core::TransportError) -> DbError {
    match error {
        core::TransportError::Graphql { messages, .. } if messages.len() == 1 => DbError::Session { code: SessionCode::of(&messages[0]), detail: messages[0].clone() },
        other => DbError::from(SyncError::Transport(other)),
    }
}

fn refused(answer: core::Answer) -> Result<core::Answer, DbError> {
    match answer.body.get("error").and_then(Value::as_str) {
        Some(code) => Err(DbError::Session { code: SessionCode::of(code), detail: code.to_string() }),
        None => Ok(answer),
    }
}

pub(super) async fn resume(client: Arc<EncryptedClient>, sync: Option<crate::sync::Sync>) -> Result<Option<String>, DbError> {
    let serial = match &sync {
        Some(sync) => Some(sync.inner.outbox_gate.lock().await),
        None => None,
    };
    let stored = client.pending_call().map_err(typed)?;
    let owner = client.pending_owner().map_err(typed)?;
    let upload = match (&sync, &owner) {
        (Some(sync), Some(owner)) => sync.inner.claim_upload_resume(owner).await?,
        _ => None,
    };
    if client.pending_call().map_err(typed)? != stored { return Ok(None); }
    let (answer, result) = match client.resume_pending().await {
        Ok(Some(answer)) => (Some(answer.clone()), core::response_of(answer)),
        Ok(None) => (None, Err(core::TransportError::Graphql { messages: vec!["the stored request belongs to a sessionless operation".into()], extensions: None })),
        Err(error) if core::encrypted::permanently_refused(&error) => (None, Err(error)),
        Err(error) => return Err(typed(error)),
    };
    if let (Some(sync), Some(serial), Some(owner)) = (&sync, &serial, owner) {
        if !matches!(&result, Ok(response) if core::missing_from_registry(response)) {
            sync.inner.settle_resumed(serial, upload, owner, result).await?;
        }
    }
    answer.map(refused).transpose().map(|answer| answer.map(|answer| answer.body.to_string()))
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
    let client = EncryptedClient::new(&encrypted, Arc::new(ForeignHttp(http)), Arc::new(ForeignCredentials(credentials)), db.clone(), clock.clone())
        .map_err(|error| DbError::Invalid { detail: error.to_string() })?;
    let client = match retries {
        Some(retries) => client.observed_by(retries.inner.clone()),
        None => client,
    };
    let client = Arc::new(client);
    Ok((Some(Arc::new(EncryptedTransport::new(client.clone(), Arc::new(ForeignTransport(transport))))), Some(client)))
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
        let task = runtime.spawn(async move {
            loop {
                let snapshot = changes.borrow_and_update().clone();
                let listener = listener.clone();
                let _ = tokio::task::spawn_blocking(move || std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener.changed(snapshot)))).await;
                if changes.changed().await.is_err() { break; }
            }
        });
        Ok(Arc::new(RetrySubscription(task.abort_handle())))
    }
}

#[derive(uniffi::Object)]
pub struct RetrySubscription(tokio::task::AbortHandle);

#[uniffi::export]
impl RetrySubscription {
    pub fn cancel(&self) { self.0.abort(); }
}

impl Drop for RetrySubscription {
    fn drop(&mut self) { self.0.abort(); }
}

#[uniffi::export]
impl Database {
    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> {
        let client = self.session()?.clone();
        if !client.has_session_operation(&operation) { return Err(DbError::Invalid { detail: format!("unknown session operation {operation}") }); }
        let body = serde_json::from_str::<Value>(&body_json)?;
        let answer = cancellable(async move { client.session_call(&operation, body).await }).await?.map_err(typed)?;
        let body = refused(answer)?.body.to_string();
        if let Some(sync) = &self.sync { sync.preload_on_session(); }
        Ok(body)
    }

    pub async fn session_resume(&self) -> Result<Option<String>, DbError> {
        let client = self.session()?.clone();
        let sync = self.sync.clone();
        cancellable(resume(client, sync)).await?
    }

    pub fn session_reset(&self) -> Result<(), DbError> {
        self.session()?.reset().map_err(typed)
    }
}

impl Database {
    fn session(&self) -> Result<&Arc<EncryptedClient>, DbError> {
        self.session.as_ref().ok_or_else(|| DbError::Invalid { detail: "the database was not opened with an encrypted session".into() })
    }
}

#[uniffi::export]
pub async fn fetch_server_config(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>, retries: Option<Arc<RetryProgress>>, connectivity: Arc<core::Connectivity>) -> Result<ServerConfig, DbError> {
    cancellable(async move {
        let progress = retries.map(|retries| retries.inner.clone()).unwrap_or_default();
        core::encrypted::bootstrap::fetch(&encrypted, platform, &version, build.as_deref(), Arc::new(ForeignHttp(http)), &crate::pacing::SystemClock, &progress, &connectivity).await.map_err(typed)
    }).await?
}
