use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::sync::SyncError;
use crate::transport::{self as core, EncryptedClient, EncryptedConfig, EncryptedTransport, ServerConfig};
use super::database::{executor, Config, Database};
use super::transport::{transport_error, ForeignTransport};
use super::{DbError, Transport};

#[derive(uniffi::Record)]
pub struct EncryptedReply { pub status: u16, pub body: String }

#[uniffi::export(rust, foreign)]
#[async_trait]
pub trait EncryptedHttp: Send + Sync {
    async fn post(&self, path: String, body: String) -> Result<EncryptedReply, DbError>;
    async fn get(&self, path: String) -> Result<EncryptedReply, DbError>;
}

#[uniffi::export(rust, foreign)]
pub trait CredentialStore: Send + Sync {
    fn get(&self) -> Result<Option<String>, DbError>;
    fn set(&self, credential: String) -> Result<(), DbError>;
    fn clear(&self) -> Result<(), DbError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SessionCode {
    SequenceConflict,
    ReplayedRequest,
    RateLimited,
    AccountUnavailable,
    Unauthenticated,
    SessionEnded,
    InvalidCode,
    InvalidPhone,
    InvalidProvider,
    ProviderRejected,
    AlreadyAuthenticated,
    ServerKeyMismatch,
    Other,
}

impl SessionCode {
    fn of(code: &str) -> Self {
        match code {
            "SEQUENCE_CONFLICT" => Self::SequenceConflict,
            "REPLAYED_REQUEST" => Self::ReplayedRequest,
            "RATE_LIMITED" => Self::RateLimited,
            "ACCOUNT_UNAVAILABLE" => Self::AccountUnavailable,
            "UNAUTHENTICATED" => Self::Unauthenticated,
            "SESSION_ENDED" => Self::SessionEnded,
            "INVALID_CODE" => Self::InvalidCode,
            "INVALID_PHONE" => Self::InvalidPhone,
            "INVALID_PROVIDER" => Self::InvalidProvider,
            "PROVIDER_REJECTED" => Self::ProviderRejected,
            "ALREADY_AUTHENTICATED" => Self::AlreadyAuthenticated,
            _ => Self::Other,
        }
    }
}


struct ForeignHttp(Arc<dyn EncryptedHttp>);

#[async_trait]
impl core::HttpExchange for ForeignHttp {
    async fn post(&self, path: &str, body: String) -> Result<core::HttpReply, core::TransportError> {
        let reply = self.0.post(path.to_string(), body).await.map_err(transport_error)?;
        Ok(core::HttpReply { status: reply.status, body: reply.body })
    }

    async fn get(&self, path: &str) -> Result<core::HttpReply, core::TransportError> {
        let reply = self.0.get(path.to_string()).await.map_err(transport_error)?;
        Ok(core::HttpReply { status: reply.status, body: reply.body })
    }
}

struct ForeignCredentials(Arc<dyn CredentialStore>);

impl core::CredentialStore for ForeignCredentials {
    fn get(&self) -> Result<Option<String>, core::TransportError> { self.0.get().map_err(transport_error) }
    fn set(&self, credential: &str) -> Result<(), core::TransportError> { self.0.set(credential.to_string()).map_err(transport_error) }
    fn clear(&self) -> Result<(), core::TransportError> { self.0.clear().map_err(transport_error) }
}

fn typed(error: core::TransportError) -> DbError {
    match error {
        core::TransportError::Graphql { messages } if messages.len() == 1 => DbError::Session { code: SessionCode::of(&messages[0]), detail: messages[0].clone() },
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

#[uniffi::export]
impl Database {
    #[uniffi::constructor]
    pub fn open_encrypted(
        config: Config,
        models_json: String,
        encrypted: EncryptedConfig,
        http: Arc<dyn EncryptedHttp>,
        credentials: Arc<dyn CredentialStore>,
        transport: Arc<dyn Transport>,
    ) -> Result<Arc<Self>, DbError> {
        Self::start_with(config, models_json, Arc::new(crate::pacing::SystemClock), |db, clock| {
            let client = EncryptedClient::new(&encrypted, Arc::new(ForeignHttp(http)), Arc::new(ForeignCredentials(credentials)), db.clone(), clock.clone())
                .map_err(|error| DbError::Invalid { detail: error.to_string() })?;
            let client = Arc::new(client);
            Ok((Some(Arc::new(EncryptedTransport::new(client.clone(), Arc::new(ForeignTransport(transport))))), Some(client)))
        })
    }

    pub async fn session_call(&self, operation: String, body_json: String) -> Result<String, DbError> {
        let client = self.session()?.clone();
        let (path, body) = (path_of(&operation)?, serde_json::from_str::<Value>(&body_json)?);
        let answer = executor()?.spawn(async move { client.mutate(&path, body).await }).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(typed)?;
        Ok(refused(answer)?.body.to_string())
    }

    pub async fn session_resume(&self) -> Result<Option<String>, DbError> {
        let client = self.session()?.clone();
        let answer = executor()?.spawn(async move { client.resume().await }).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?.map_err(typed)?;
        answer.map(|answer| refused(answer).map(|answer| answer.body.to_string())).transpose()
    }

    pub async fn server_config(&self) -> Result<ServerConfig, DbError> {
        let client = self.session()?.clone();
        let config = executor()?.spawn(async move { client.server_config().await }).await.map_err(|error| DbError::Invalid { detail: error.to_string() })?;
        match config {
            Ok(config) => Ok(config),
            Err(core::ConfigError::KeyMismatch(detail)) => Err(DbError::Session { code: SessionCode::ServerKeyMismatch, detail }),
            Err(core::ConfigError::Transport(error)) => Err(typed(error)),
        }
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
