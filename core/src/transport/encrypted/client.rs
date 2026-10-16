use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as Gate;

use super::config::{ConfigError, ServerConfig};
use super::jwe::{open_response, random_token, seal_request, RequestPlain};
use super::key::ServerKey;
use super::state::{self, Pending};
use super::{CredentialStore, HttpExchange};
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
    audience: String,
    key: ServerKey,
    http: Arc<dyn HttpExchange>,
    credentials: Arc<dyn CredentialStore>,
    db: Db,
    clock: Arc<dyn Clock>,
    credential: Mutex<Option<String>>,
    mutations: Gate<()>,
}

pub(super) fn digest_of(credential: &str) -> String {
    Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
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
        credentials: Arc<dyn CredentialStore>,
        db: Db,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, TransportError> {
        let audience = config.origin.trim_end_matches('/').to_string();
        let origin_ok = audience.strip_prefix("https://").is_some_and(|host| !host.is_empty() && !host.contains(['/', '?', '#']));
        if !origin_ok {
            return Err(TransportError::Decode(format!("the origin must be https without a path: {}", config.origin)));
        }
        let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| TransportError::Decode(error.to_string()))?;
        Ok(Self { audience, key, http, credentials, db, clock, credential: Mutex::new(None), mutations: Gate::new(()) })
    }

    pub async fn mutate(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        let _turn = self.mutations.lock().await;
        let credential = self.credential()?;
        let pending = match self.with_connection(state::pending)? {
            Some(pending) if pending.path == path && serde_json::from_str::<Value>(&pending.body).ok().as_ref() == Some(&body) => pending,
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
                self.with_connection(|connection| state::store_pending(connection, &pending))?;
                pending
            }
        };
        self.settle(&credential, pending).await
    }

    pub async fn resume(&self) -> Result<Option<Answer>, TransportError> {
        let _turn = self.mutations.lock().await;
        let credential = self.credential()?;
        let Some(pending) = self.with_connection(state::pending)? else { return Ok(None) };
        self.settle(&credential, pending).await.map(Some)
    }

    pub async fn read(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        let credential = self.credential()?;
        let mut answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        if confirmed_error(&answer.body) == Some(REPLAYED) {
            answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        }
        Ok(answer)
    }

    pub async fn server_config(&self) -> Result<ServerConfig, ConfigError> {
        let reply = self.http.get(CONFIG_PATH).await.map_err(ConfigError::Transport)?;
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

    async fn settle(&self, credential: &str, pending: Pending) -> Result<Answer, TransportError> {
        let body: Value = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
        let answer = self.exchange(credential, &pending.request_id, &pending.response_key, Some(pending.sequence), &pending.path, &body).await?;
        if answer.sequence != Some(pending.sequence) {
            return Err(match confirmed_error(&answer.body) {
                Some(code) => TransportError::Graphql { messages: vec![code.to_string()] },
                None => TransportError::Decode("the answer does not confirm the sequence".into()),
            });
        }
        self.with_connection(|connection| state::commit(connection, pending.sequence))?;
        if pending.path == LOGOUT_PATH && answer.body.get("revoked") == Some(&Value::Bool(true)) {
            self.reset()?;
        }
        Ok(answer)
    }

    async fn exchange(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<Answer, TransportError> {
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
            return Err(TransportError::Http { status: 413, body: String::new() });
        }
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

    fn credential(&self) -> Result<String, TransportError> {
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
