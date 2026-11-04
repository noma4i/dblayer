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
        other => DbError::from(SyncError::Transport(other)),
    }
}

fn refused(answer: core::Answer) -> Result<core::Answer, DbError> {
    match answer.body.get("error").and_then(Value::as_str) {
        Some(code) => Err(DbError::Session { code: SessionCode::of(code), detail: code.to_string() }),
        None => Ok(answer),
    }
}

pub(super) async fn resume(admitted: &crate::lifecycle::Admitted, client: Arc<EncryptedClient>, sync: Option<crate::sync::Sync>) -> Result<(Option<String>, core::encrypted::After), DbError> {
    let effects = Arc::new(crate::sync::DeliveryEffects::new(sync.as_ref().map(|sync| &sync.inner)));
    let serial = match &sync {
        Some(sync) => Some(sync.inner.outbox_gate.lock().await),
        None => None,
    };
    let stored = client.pending_call().map_err(typed)?;
    let owner = client.pending_owner().map_err(typed)?;
    let upload = match (&sync, &owner) {
        (Some(sync), Some(owner)) => sync.inner.claim_upload_resume(admitted, owner).await?,
        _ => None,
    };
    if client.pending_call().map_err(typed)? != stored { return Ok((None, core::encrypted::After::Nothing)); }
    let (answer, result, after) = match client.resume_pending(admitted).await {
        Ok((Some(answer), after)) => (Some(answer.clone()), core::response_of(answer), after),
        Ok((None, after)) => (None, Err(core::TransportError::Graphql { messages: vec!["the stored request belongs to a sessionless operation".into()], extensions: None }), after),
        Err(error) if core::encrypted::permanently_refused(&error) => (None, Err(error), core::encrypted::After::Nothing),
        Err(error) => return Err(typed(error)),
    };
    drop(serial);
    let settled = if let (Some(sync), Some(owner)) = (&sync, owner) {
        if !matches!(&result, Ok(response) if core::missing_from_registry(response)) {
            sync.inner.settle_resumed(admitted, effects.clone(), upload, owner, result).await.map_err(DbError::from)
        } else {
            Ok(())
        }
    } else { Ok(()) };
    effects.run();
    settled?;
    Ok((answer.map(refused).transpose()?.map(|answer| answer.body.to_string()), after))
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
        let calls = Arc::new(crate::db::CallState::new());
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
pub struct RetrySubscription { task: tokio::task::AbortHandle, pub(crate) calls: Arc<crate::db::CallState> }

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
        let client = self.session()?.clone();
        if !client.has_session_operation(&operation) { return Err(DbError::Invalid { detail: format!("unknown session operation {operation}") }); }
        let body = serde_json::from_str::<Value>(&body_json)?;
        let epoch = self.streams.snapshot().session_epoch;
        let call_client = client.clone();
        let (answer, after) = self.in_epoch_as(crate::lifecycle::Origin::Explicit, epoch, "session call", operation != "logout", |admitted| async move {
            let (answer, after) = cancellable(async move { call_client.call_admitted(&admitted, operation, body).await }).await?.map_err(typed)?;
            Ok((refused(answer)?, after))
        }).await?;
        cancellable(async move { client.conclude(epoch, after).await }).await?.map_err(typed)?;
        Ok(answer.body.to_string())
    }

    pub async fn session_resume(&self) -> Result<Option<String>, DbError> {
        let client = self.session()?.clone();
        let sync = self.sync.clone();
        let epoch = self.streams.snapshot().session_epoch;
        let resume_client = client.clone();
        let restore = !client.logout_pending().map_err(typed)?;
        let (answer, after) = self.in_epoch_as(crate::lifecycle::Origin::Explicit, epoch, "session resume", restore, |admitted| async move {
            cancellable(async move { resume(&admitted, resume_client, sync).await }).await?
        }).await?;
        cancellable(async move { client.conclude(epoch, after).await }).await?.map_err(typed)?;
        Ok(answer)
    }

    /// Ends the session: the old work ends, the credential and the data of the account go. Returns after the clearing. The same call ends a plain or local database too.
    pub async fn session_reset(&self) -> Result<(), DbError> {
        match self.session.clone() {
            Some(client) => cancellable(async move { client.reset().await }).await?.map_err(typed),
            None => {
                let db = self.db.clone();
                cancellable(async move { db.end_session::<DbError>(crate::lifecycle::End::Local, None, |_| Ok(())).await }).await?
            }
        }
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
        core::encrypted::bootstrap::fetch(&encrypted, &core::encrypted::bootstrap::AppRelease { platform, version: &version, build: build.as_deref() }, Arc::new(ForeignHttp(http)), &crate::pacing::SystemClock, &progress, &connectivity).await.map_err(typed)
    }).await?
}
