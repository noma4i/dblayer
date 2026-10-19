use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::sync::SyncError;
use crate::transport::{self as core, EncryptedClient, EncryptedConfig, EncryptedTransport, ServerConfig, SessionCode};
use super::database::{executor, Database, Wiring};
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

fn path_of(operation: &str) -> Result<String, DbError> {
    match operation {
        "otp" | "confirm" => Ok(format!("/api/v3/auth/{operation}")),
        "social" => Ok("/api/v3/auth/provider".to_string()),
        "logout" => Ok(core::LOGOUT_PATH.to_string()),
        other => Err(DbError::Invalid { detail: format!("unknown session operation {other}") }),
    }
}

pub(super) async fn resume(client: Arc<EncryptedClient>, sync: Option<crate::sync::Sync>) -> Result<Option<String>, DbError> {
    let serial = match &sync {
        Some(sync) => Some(sync.inner.outbox_gate.lock().await),
        None => None,
    };
    let stored = client.pending_call().map_err(typed)?;
    let upload = match (&sync, &stored) {
        (Some(sync), Some((path, body))) => sync.inner.claim_upload_resume(&|request| core::repeats(request, path, body)).await?,
        _ => None,
    };
    if client.pending_call().map_err(typed)? != stored { return Ok(None); }
    let Some(answer) = client.resume().await.map_err(typed)? else { return Ok(None) };
    if let (Some(sync), Some(serial), Some((path, body))) = (&sync, &serial, stored) {
        let result = core::response_of(answer.clone());
        if !matches!(&result, Ok(response) if core::missing_from_registry(response)) {
            sync.inner.settle_resumed(serial, upload, &move |request| core::repeats(request, &path, &body), result).await?;
        }
    }
    refused(answer).map(|answer| Some(answer.body.to_string()))
}

pub(super) fn wire(
    db: &crate::db::Db,
    clock: &Arc<dyn crate::pacing::Clock>,
    encrypted: EncryptedConfig,
    http: Arc<dyn EncryptedHttp>,
    credentials: Arc<dyn CredentialStore>,
    transport: Arc<dyn Transport>,
) -> Result<Wiring, DbError> {
    let client = EncryptedClient::new(&encrypted, Arc::new(ForeignHttp(http)), Arc::new(ForeignCredentials(credentials)), db.clone(), clock.clone())
        .map_err(|error| DbError::Invalid { detail: error.to_string() })?;
    let client = Arc::new(client);
    Ok((Some(Arc::new(EncryptedTransport::new(client.clone(), Arc::new(ForeignTransport(transport))))), Some(client)))
}

#[uniffi::export]
impl Database {
    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> {
        let client = self.session()?.clone();
        let (path, body) = (path_of(&operation)?, serde_json::from_str::<Value>(&body_json)?);
        let answer = executor()?.spawn(async move { client.mutate(&path, body).await }).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(typed)?;
        Ok(refused(answer)?.body.to_string())
    }

    pub async fn session_resume(&self) -> Result<Option<String>, DbError> {
        let client = self.session()?.clone();
        let sync = self.sync.clone();
        executor()?.spawn(resume(client, sync)).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
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
pub async fn fetch_server_config(encrypted: EncryptedConfig, platform: core::AppPlatform, version: String, build: Option<String>, http: Arc<dyn EncryptedHttp>) -> Result<ServerConfig, DbError> {
    executor()?.spawn(async move {
        core::encrypted::bootstrap::fetch_with(&encrypted, platform, &version, build.as_deref(), Arc::new(ForeignHttp(http)), &crate::pacing::SystemClock).await.map_err(typed)
    }).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?
}
