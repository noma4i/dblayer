use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as Gate;

use super::config::{ConfigError, ServerConfig};
use super::jwe::{open_response, random_token, seal_request, RequestPlain};
use super::key::ServerKey;
use super::state::{self, Pending};
use super::{CredentialSlot, HttpExchange};
use crate::db::Db;
use crate::pacing::Clock;
use crate::transport::TransportError;

pub const LOGOUT_PATH: &str = "/api/v3/logout";
const CONFIG_PATH: &str = "/api/v3/config";
const MAX_REQUEST_BYTES: usize = 256 * 1024;
const REPLAYED: &str = "REPLAYED_REQUEST";

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedConfig {
    pub origin: String,
    pub server_key: String,
    pub pin: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub sequence: Option<i64>,
    pub body: Value,
}

pub struct EncryptedClient {
    pub(super) config: EncryptedConfig,
    audience: String,
    key: ServerKey,
    http: Arc<dyn HttpExchange>,
    credentials: Arc<dyn CredentialSlot>,
    db: Db,
    clock: Arc<dyn Clock>,
    credential: Mutex<Option<String>>,
    mutations: Gate<()>,
}

pub(super) fn digest_of(credential: &str) -> String {
    Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn config_path(platform: Option<&str>, version: Option<&str>) -> String {
    let pairs = [("platform", platform), ("version", version)];
    let query: Vec<String> = pairs.iter().filter_map(|(name, value)| value.map(|value| format!("{name}={}", percent_encoded(value)))).collect();
    if query.is_empty() { CONFIG_PATH.to_string() } else { format!("{CONFIG_PATH}?{}", query.join("&")) }
}

fn percent_encoded(value: &str) -> String {
    value.bytes().map(|byte| if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') { (byte as char).to_string() } else { format!("%{byte:02X}") }).collect()
}

fn check_path(path: &str) -> Result<(), TransportError> {
    match path.contains(['?', '#']) {
        true => Err(TransportError::Decode(format!("an encrypted path carries no query or fragment: {path}"))),
        false => Ok(()),
    }
}

fn storage(error: rusqlite::Error) -> TransportError {
    TransportError::Network(format!("session storage: {error}"))
}

fn confirmed_error(body: &Value) -> Option<&str> {
    body.get("error").and_then(Value::as_str)
}

impl EncryptedClient {
    pub fn new(
        config: &EncryptedConfig,
        http: Arc<dyn HttpExchange>,
        credentials: Arc<dyn CredentialSlot>,
        db: Db,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, TransportError> {
        let audience = config.origin.trim_end_matches('/').to_string();
        if !super::origin::allowed(&audience) {
            return Err(TransportError::Decode(format!("the origin must be https, or http on a development host, without a path: {}", config.origin)));
        }
        let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| TransportError::Decode(error.to_string()))?;
        Ok(Self { config: config.clone(), audience, key, http, credentials, db, clock, credential: Mutex::new(None), mutations: Gate::new(()) })
    }

    pub async fn mutate(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        check_path(path)?;
        let _turn = self.mutations.lock().await;
        let credential = self.credential()?;
        let (pending, token) = match self.with_connection(state::pending)? {
            Some(pending) if pending.path == path && serde_json::from_str::<Value>(&pending.body).ok().as_ref() == Some(&body) => {
                let token = self.pending_token(&credential, &pending)?;
                (pending, token)
            }
            Some(_) => return Err(TransportError::Network("another encrypted request is pending, resolve it first".into())),
            None => {
                let sequence = self.with_connection(state::session)?.map_or(1, |session| session.sequence + 1);
                let pending = Pending {
                    credential_digest: digest_of(&credential),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    response_key: random_token(),
                    sequence,
                    path: path.to_string(),
                    body: body.to_string(),
                };
                let token = self.pending_token(&credential, &pending)?;
                self.with_connection(|connection| state::store_pending(connection, &pending))?;
                (pending, token)
            }
        };
        self.settle(pending, token).await
    }

    pub fn pending_call(&self) -> Result<Option<(String, Value)>, TransportError> {
        Ok(self.with_connection(state::pending)?.and_then(|pending| Some((pending.path, serde_json::from_str(&pending.body).ok()?))))
    }

    pub async fn resume(&self) -> Result<Option<Answer>, TransportError> {
        let _turn = self.mutations.lock().await;
        let credential = self.credential()?;
        let Some(pending) = self.with_connection(state::pending)? else { return Ok(None) };
        let token = self.pending_token(&credential, &pending)?;
        self.settle(pending, token).await.map(Some)
    }

    pub async fn read(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        check_path(path)?;
        let credential = self.credential()?;
        let mut answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        if confirmed_error(&answer.body) == Some(REPLAYED) {
            answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        }
        Ok(answer)
    }

    pub async fn server_config(&self, platform: Option<&str>, version: Option<&str>) -> Result<ServerConfig, ConfigError> {
        let reply = self.http.get(&config_path(platform, version)).await.map_err(ConfigError::Transport)?;
        if reply.status != 200 {
            return Err(ConfigError::Transport(TransportError::Http { status: reply.status, body: reply.body }));
        }
        ServerConfig::parse(&reply.body, &self.key)
    }

    pub fn reset(&self) -> Result<(), TransportError> {
        self.credentials.clear()?;
        *crate::db::locked(&self.credential) = None;
        self.with_connection(state::clear)
    }

    fn pending_token(&self, credential: &str, pending: &Pending) -> Result<String, TransportError> {
        let body: Value = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
        self.request_token(credential, &pending.request_id, &pending.response_key, Some(pending.sequence), &pending.path, &body)
    }

    async fn settle(&self, pending: Pending, token: String) -> Result<Answer, TransportError> {
        let answer = self.post(&pending.path, token, &pending.request_id, &pending.response_key).await?;
        if answer.sequence != Some(pending.sequence) {
            return Err(match confirmed_error(&answer.body) {
                Some(code) => TransportError::refusal(code),
                None => TransportError::Decode("the answer does not confirm the sequence".into()),
            });
        }
        match pending.path == LOGOUT_PATH && answer.body.get("revoked") == Some(&Value::Bool(true)) {
            true => self.reset()?,
            false => self.with_connection(|connection| state::commit(connection, pending.sequence))?,
        }
        Ok(answer)
    }

    async fn exchange(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<Answer, TransportError> {
        let token = self.request_token(credential, request_id, response_key, sequence, path, body)?;
        self.post(path, token, request_id, response_key).await
    }

    fn request_token(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<String, TransportError> {
        let plain = RequestPlain {
            audience: self.audience.clone(),
            kid: self.key.pin().to_string(),
            path: path.to_string(),
            request_id: request_id.to_string(),
            issued_at: self.clock.now().as_secs() as i64,
            response_key: response_key.to_string(),
            credential: credential.to_string(),
            sequence,
            body: body.clone(),
        };
        let token = seal_request(&self.key, &plain).map_err(|error| TransportError::Decode(error.to_string()))?;
        if token.len() > MAX_REQUEST_BYTES {
            return Err(TransportError::RequestTooLarge { size: token.len() as u64, limit: MAX_REQUEST_BYTES as u64 });
        }
        Ok(token)
    }

    async fn post(&self, path: &str, token: String, request_id: &str, response_key: &str) -> Result<Answer, TransportError> {
        let reply = self.http.post(path, token).await?;
        if reply.status != 200 {
            return Err(TransportError::Http { status: reply.status, body: reply.body });
        }
        let opened = open_response(&reply.body, response_key).map_err(|error| TransportError::Decode(error.to_string()))?;
        if opened.request_id != request_id {
            return Err(TransportError::Decode("the response belongs to another request".into()));
        }
        Ok(Answer { sequence: opened.sequence, body: opened.body })
    }

    pub(super) fn credential(&self) -> Result<String, TransportError> {
        let mut cached = crate::db::locked(&self.credential);
        if let Some(credential) = cached.as_ref() {
            return Ok(credential.clone());
        }
        let stored = self.credentials.get()?;
        let session = self.with_connection(state::session)?;
        let credential = match stored {
            Some(credential) if session.as_ref().is_some_and(|session| session.credential_digest == digest_of(&credential)) => credential,
            _ => {
                let credential = random_token();
                self.credentials.set(&credential)?;
                self.with_connection(|connection| state::start(connection, &digest_of(&credential)))?;
                credential
            }
        };
        *cached = Some(credential.clone());
        Ok(credential)
    }

    fn with_connection<T>(&self, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        let inner = self.db.shared.store.lock();
        run(&inner.connection).map_err(storage)
    }
}
