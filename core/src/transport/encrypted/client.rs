use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as Gate;

use super::jwe::{open_response, random_token, seal_request, RequestPlain, RequestSession};
use super::key::ServerKey;
use super::state::{self, Pending};
use super::{CredentialSlot, HttpExchange};
use crate::db::Db;
use crate::pacing::Clock;
use crate::transport::TransportError;
use crate::transport::retry::{RetryClass, RetryEvents, Retrying};

const MAX_REQUEST_BYTES: usize = 256 * 1024;
const REPLAYED: &str = "REPLAYED_REQUEST";
const PERMANENT_HTTP_REFUSALS: [u16; 6] = [400, 404, 405, 413, 415, 422];

pub(crate) fn permanently_refused(error: &TransportError) -> bool {
    matches!(error, TransportError::Http { status, .. } if PERMANENT_HTTP_REFUSALS.contains(status))
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedConfig {
    pub origin: String,
    pub server_key: String,
    pub pin: String,
    pub endpoints: EncryptedEndpoints,
    pub protocol_label: String,
    pub cable_purpose: String,
    pub cable_channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedEndpoints {
    pub graphql: String,
    pub cable: String,
    pub logout: String,
    pub bootstrap: String,
    pub session_operations: std::collections::HashMap<String, EncryptedSessionOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedSessionOperation {
    pub path: String,
    pub sessionless: bool,
}

impl EncryptedConfig {
    pub fn validate(&self) -> Result<(), TransportError> {
        for path in [&self.endpoints.graphql, &self.endpoints.cable, &self.endpoints.logout, &self.endpoints.bootstrap].into_iter().chain(self.endpoints.session_operations.values().map(|operation| &operation.path)) {
            check_path(path)?;
        }
        if [&self.protocol_label, &self.cable_purpose, &self.cable_channel].into_iter().any(|value| value.trim().is_empty() || value.chars().any(char::is_control)) {
            return Err(TransportError::Decode("encrypted protocol labels must be nonempty and contain no control characters".into()));
        }
        if self.endpoints.session_operations.keys().any(|name| name.trim().is_empty() || name == "logout") {
            return Err(TransportError::Decode("session operation names must be nonempty and exclude logout".into()));
        }
        Ok(())
    }
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
    retry_events: RetryEvents,
}

pub(super) fn digest_of(credential: &str) -> String {
    Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn check_path(path: &str) -> Result<(), TransportError> {
    if !path.starts_with('/') || path.starts_with("//") || !path.is_ascii() || path.contains(['?', '#', '\\']) || path.bytes().any(|byte| byte <= 32 || byte == 127) {
        return Err(TransportError::Decode("encrypted endpoints require encoded absolute paths without query or fragment".into()));
    }
    let bytes = path.as_bytes();
    if bytes.iter().enumerate().any(|(index, byte)| *byte == b'%' && !bytes.get(index + 1..index + 3).is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))) {
        return Err(TransportError::Decode("encrypted endpoints require valid percent encoding".into()));
    }
    if path.to_ascii_lowercase().replace("%2e", ".").split('/').any(|part| matches!(part, "." | "..")) {
        return Err(TransportError::Decode("encrypted endpoints cannot contain dot segments".into()));
    }
    Ok(())
}

fn storage(error: rusqlite::Error) -> TransportError {
    TransportError::Network(format!("session storage: {error}"))
}

fn confirmed_error(body: &Value) -> Option<&str> {
    body.get("error").and_then(Value::as_str)
}

pub(super) fn temporary_error(body: &Value) -> Option<TransportError> {
    let codes: Vec<_> = body.get("error").and_then(Value::as_str).into_iter().chain(
        body.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|error| error.get("extensions")?.get("code")?.as_str())
    ).collect();
    if codes.iter().any(|code| crate::transport::SessionCode::from_wire(code).is_some()) || RetryClass::classify(false, None, &codes) == RetryClass::Fatal { return None; }
    Some(TransportError::Graphql { messages: codes.into_iter().map(str::to_string).collect(), extensions: None })
}

impl EncryptedClient {
    pub async fn session_call(&self, operation: &str, body: Value) -> Result<Answer, TransportError> {
        if operation == "logout" { return self.mutate(&self.config.endpoints.logout, body).await; }
        let operation = self.config.endpoints.session_operations.get(operation).ok_or_else(|| TransportError::Decode(format!("unknown session operation {operation}")))?;
        if !operation.sessionless { return self.mutate(&operation.path, body).await; }
        let mut retry = Retrying::new(self.clock.as_ref(), &self.retry_events);
        loop {
            let request_id = uuid::Uuid::new_v4().to_string();
            let response_key = random_token();
            let token = self.request_token(None, &request_id, &response_key, &operation.path, &body)?;
            let result = self.post(&operation.path, token, &request_id, &response_key).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
            match result { Ok(answer) => return Ok(answer), Err(error) => retry.failed(error).await? }
        }
    }

    pub fn set_retry_listener(&self, listener: Option<Arc<dyn crate::transport::RetryListener>>) { self.retry_events.set(listener); }

    pub fn has_session_operation(&self, operation: &str) -> bool {
        operation == "logout" || self.config.endpoints.session_operations.contains_key(operation)
    }

    pub fn new(
        config: &EncryptedConfig,
        http: Arc<dyn HttpExchange>,
        credentials: Arc<dyn CredentialSlot>,
        db: Db,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, TransportError> {
        config.validate()?;
        let audience = config.origin.trim_end_matches('/').to_string();
        if !super::origin::allowed(&audience) {
            return Err(TransportError::Decode(format!("the origin must be https, or http on a development host, without a path: {}", config.origin)));
        }
        let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| TransportError::Decode(error.to_string()))?;
        Ok(Self { config: config.clone(), audience, key, http, credentials, db, clock, credential: Mutex::new(None), mutations: Gate::new(()), retry_events: RetryEvents::default() })
    }

    pub async fn mutate(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        self.mutate_owned(path, body, None).await
    }

    pub(crate) async fn mutate_owned(&self, path: &str, body: Value, owner: Option<crate::transport::RequestOwner>) -> Result<Answer, TransportError> {
        check_path(path)?;
        let _turn = self.mutations.lock().await;
        let credential = self.credential()?;
        let (pending, token) = match self.with_connection(state::pending)? {
            Some(pending) if pending.owner == owner && pending.path == path && serde_json::from_str::<Value>(&pending.body).ok().as_ref() == Some(&body) => {
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
                    owner,
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

    pub(crate) fn pending_owner(&self) -> Result<Option<crate::transport::RequestOwner>, TransportError> {
        Ok(self.with_connection(state::pending)?.and_then(|pending| pending.owner))
    }

    pub async fn resume(&self) -> Result<Option<Answer>, TransportError> {
        match self.resume_pending().await {
            Err(error) if permanently_refused(&error) => Ok(None),
            result => result,
        }
    }

    pub(crate) async fn resume_pending(&self) -> Result<Option<Answer>, TransportError> {
        let _turn = self.mutations.lock().await;
        let Some(pending) = self.with_connection(state::pending)? else { return Ok(None) };
        if self.config.endpoints.session_operations.values().any(|operation| operation.sessionless && operation.path == pending.path) {
            self.with_connection(state::drop_pending)?;
            return Ok(None);
        }
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

    pub fn reset(&self) -> Result<(), TransportError> {
        self.credentials.clear()?;
        *crate::db::locked(&self.credential) = None;
        self.with_connection(state::clear)
    }

    fn pending_token(&self, credential: &str, pending: &Pending) -> Result<String, TransportError> {
        let body: Value = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
        self.request_token(Some(RequestSession { credential: credential.into(), sequence: Some(pending.sequence) }), &pending.request_id, &pending.response_key, &pending.path, &body)
    }

    async fn settle(&self, mut pending: Pending, mut token: String) -> Result<Answer, TransportError> {
      let mut retry = Retrying::new(self.clock.as_ref(), &self.retry_events);
      loop {
        let answer = match self.post(&pending.path, token, &pending.request_id, &pending.response_key).await {
            Err(error) if permanently_refused(&error) => {
                self.with_connection(state::drop_pending)?;
                return Err(error);
            }
            Err(error) => {
                retry.failed(error).await?;
                token = self.pending_token(&self.credential()?, &pending)?;
                continue;
            }
            Ok(answer) => answer,
        };
        if let Some(error) = temporary_error(&answer.body) {
            if answer.sequence == Some(pending.sequence) {
                let confirmed = pending.sequence;
                pending.sequence += 1;
                pending.request_id = uuid::Uuid::new_v4().to_string();
                pending.response_key = random_token();
                if let Err(error) = self.pending_token(&self.credential()?, &pending) {
                    self.with_connection(|connection| state::commit(connection, confirmed))?;
                    return Err(error);
                }
                self.with_connection(|connection| state::retry_confirmed(connection, &pending))?;
            }
            retry.failed(error).await?;
            token = self.pending_token(&self.credential()?, &pending)?;
            continue;
        }
        if answer.sequence != Some(pending.sequence) {
            return Err(match confirmed_error(&answer.body) {
                Some(code) => TransportError::refusal(code),
                None => TransportError::Decode("the answer does not confirm the sequence".into()),
            });
        }
        match pending.path == self.config.endpoints.logout && answer.body.get("revoked") == Some(&Value::Bool(true)) {
            true => self.reset()?,
            false => self.with_connection(|connection| state::commit(connection, pending.sequence))?,
        }
        return Ok(answer);
      }
    }

    async fn exchange(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<Answer, TransportError> {
        let mut retry = Retrying::new(self.clock.as_ref(), &self.retry_events);
        loop {
            let token = self.request_token(Some(RequestSession { credential: credential.into(), sequence }), request_id, response_key, path, body)?;
            let result = self.post(path, token, request_id, response_key).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
            match result { Ok(answer) => return Ok(answer), Err(error) => retry.failed(error).await? }
        }
    }

    fn request_token(&self, session: Option<RequestSession>, request_id: &str, response_key: &str, path: &str, body: &Value) -> Result<String, TransportError> {
        let plain = RequestPlain {
            audience: self.audience.clone(),
            kid: self.key.pin().to_string(),
            path: path.to_string(),
            request_id: request_id.to_string(),
            issued_at: self.clock.now().as_secs() as i64,
            response_key: response_key.to_string(),
            session,
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
