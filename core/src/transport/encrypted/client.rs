use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};

use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::jwe::{open_response, random_token, seal_request, RequestPlain, RequestSession};
use super::key::ServerKey;
use super::state::{self, Pending};
use super::{CredentialSlot, HttpExchange};
use crate::db::Db;
use crate::ffi::legacy::{Admitted, Origin};
use crate::lifecycle::authority::{Authority, Internal};
use crate::pacing::{Clock, RetryPolicy};
use crate::transport::{SessionCode, TransportError};
use crate::transport::retry::{RetryClass, RetryCalls, Retrying};

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
    pub(super) credentials: Arc<dyn CredentialSlot>,
    db: Db,
    pub(super) clock: Arc<dyn Clock>,
    pub(crate) online: Arc<crate::transport::Connectivity>,
    credential: Arc<CredentialCell>,
    pub(super) life: std::sync::OnceLock<std::sync::Weak<crate::owners::life::Life>>,
    #[cfg(test)]
    pub(super) kept: Mutex<Vec<Arc<crate::owners::life::Life>>>,
    progress: Arc<RetryCalls>,
}

#[derive(Default)]
struct CredentialState {
    cached: Option<String>,
    generation: u64,
    loading: Option<ThreadId>,
}

#[derive(Default)]
struct CredentialCell {
    state: Mutex<CredentialState>,
    settled: Condvar,
}

struct Loading<'a>(&'a CredentialCell);

impl Drop for Loading<'_> {
    fn drop(&mut self) {
        crate::db::locked(&self.0.state).loading = None;
        self.0.settled.notify_all();
    }
}

pub(crate) fn digest_of(credential: &str) -> String {
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
    match error {
        rusqlite::Error::FromSqlConversionFailure(..) => TransportError::Decode(format!("session storage: {error}")),
        _ => TransportError::Network(format!("session storage: {error}")),
    }
}

pub(super) fn session_failure(error: crate::owners::session::SessionError) -> TransportError {
    use crate::owners::session::SessionError as Failed;
    match error {
        Failed::Revoked => TransportError::Session { code: SessionCode::SessionEnded, detail: "the session was ended by the server".into() },
        Failed::NoCredential => TransportError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
        Failed::SequenceConflict => TransportError::Session { code: SessionCode::SequenceConflict, detail: "the sequence of the request conflicts with the server".into() },
        Failed::Protocol | Failed::Invalid => TransportError::Decode("the answer does not confirm the sequence".into()),
        Failed::Unauthorized => TransportError::Http { status: 401, body: String::new() },
        Failed::Http => TransportError::Network("the request was refused".into()),
        Failed::Rejected(reason) => TransportError::Network(format!("the request was not stored: {reason:?}")),
        Failed::Interrupted | Failed::NoSession | Failed::Closed => TransportError::Interrupted("the session ended while its answer was pending".into()),
    }
}

fn confirmed_error(body: &Value) -> Option<&str> {
    body.get("error").and_then(Value::as_str)
}

pub(crate) fn temporary_error(body: &Value) -> Option<TransportError> {
    let codes: Vec<_> = body.get("error").and_then(Value::as_str).into_iter().chain(
        body.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|error| error.get("extensions")?.get("code")?.as_str())
    ).collect();
    if codes.iter().any(|code| crate::transport::SessionCode::from_wire(code).is_some()) || RetryClass::classify(false, None, &codes) == RetryClass::Fatal { return None; }
    Some(TransportError::Graphql { messages: codes.into_iter().map(str::to_string).collect(), extensions: None })
}

impl EncryptedClient {
    pub(super) fn admit(&self, target: &'static str) -> Result<Admitted, TransportError> {
        Ok(self.db.shared.store.lifecycle.admit(Origin::Explicit, target)?)
    }

    pub(super) fn validate(&self, admitted: &Admitted) -> Result<(), TransportError> {
        if Arc::ptr_eq(admitted.life(), &self.db.shared.store.lifecycle) && admitted.is_current() { Ok(()) } else { Err(crate::ffi::legacy::Ended.into()) }
    }

    fn write_in<T>(&self, admitted: &Admitted, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.write_as(&admitted.authority(), run)
    }

    fn write_as<T>(&self, authority: &Authority, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.db.shared.store.write(authority, || TransportError::Interrupted("the session ended while its answer was pending".into()), |inner| run(&inner.connection).map_err(storage))
    }

    /// One request of the executor of Session: seals `body` for `path` and posts it once. No retry and no state: the owner Session keeps both.
    pub(crate) async fn send_once(&self, credential: Option<&str>, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value, purpose: crate::transport::NetworkPurpose) -> Result<Answer, TransportError> {
        check_path(path)?;
        let session = credential.map(|credential| RequestSession { credential: credential.into(), sequence });
        let token = self.request_token(session, request_id, response_key, path, body)?;
        self.post(path, token, request_id, response_key, purpose).await
    }

    pub(crate) fn logout_path(&self) -> &str { &self.config.endpoints.logout }

    /// The declared session operations and `logout`, as the owner Session takes them.
    pub(crate) fn session_operations(&self) -> Vec<crate::owners::session::SessionOperation> {
        let declared = self.config.endpoints.session_operations.iter().map(|(name, operation)| crate::owners::session::SessionOperation { name: name.clone(), path: operation.path.clone(), sessionless: operation.sessionless });
        declared.chain(std::iter::once(crate::owners::session::SessionOperation { name: "logout".into(), path: self.config.endpoints.logout.clone(), sessionless: false })).collect()
    }

    pub(crate) fn stored_session(&self) -> Result<Option<state::Session>, TransportError> { self.read_connection(state::session) }

    pub(crate) fn stored_pending(&self) -> Result<Option<Pending>, TransportError> { self.read_connection(state::pending) }

    pub fn observed_by(mut self, progress: Arc<RetryCalls>) -> Self {
        self.progress = progress;
        self
    }

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
        let online = db.connectivity();
        let client = Self { config: config.clone(), audience, key, http, credentials, db, clock, online, credential: Arc::default(), life: std::sync::OnceLock::new(), #[cfg(test)] kept: Mutex::new(Vec::new()), progress: Arc::default() };
        if client.db.shared.store.take_session_end_replay() {
            client.credentials.clear()?;
            let admitted = client.admit("open encrypted state")?;
            client.write_in(&admitted, state::clear)?;
        }
        Ok(client)
    }

    /// Joins the owners: from now on a request with a `sequence` goes through the owner Session.
    pub(crate) fn attach(&self, life: &Arc<crate::owners::life::Life>) { let _ = self.life.set(Arc::downgrade(life)); }

    pub(crate) fn credential_slot(&self) -> Arc<dyn CredentialSlot> { self.credentials.clone() }

    /// The cached credential is gone with the account.
    pub(crate) fn forget_credential(&self) {
        let mut state = crate::db::locked(&self.credential.state);
        state.cached = None;
        state.generation += 1;
    }

    /// A request with a `sequence` for the old stack: the owner Session sends it, in order with every other request of the session.
    pub(crate) async fn mutate_owned(&self, admitted: &Admitted, path: &str, body: Value, owner: Option<crate::transport::RequestOwner>, purpose: crate::transport::NetworkPurpose) -> Result<Answer, TransportError> {
        self.validate(admitted)?;
        check_path(path)?;
        let life = self.life.get().and_then(std::sync::Weak::upgrade).ok_or_else(|| TransportError::Interrupted("the database closed".into()))?;
        let waiter = life.mutate_wait(owner, path.to_string(), body.to_string(), u8::from(purpose == crate::transport::NetworkPurpose::Transfer));
        match admitted.run(async { waiter.wait().await }).await? {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Mutated(outcome)) => match outcome {
                crate::owners::session::AuthorOutcome::Response(text) => Ok(Answer { sequence: None, body: serde_json::from_str(&text).map_err(|error| TransportError::Decode(error.to_string()))? }),
                crate::owners::session::AuthorOutcome::Error(error) => Err(session_failure(error)),
            },
            _ => Err(TransportError::Interrupted("the session ended while its answer was pending".into())),
        }
    }

    pub fn pending_call(&self) -> Result<Option<(String, Value)>, TransportError> {
        self.read_connection(state::pending)?.map(|pending| {
            let body = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
            Ok((pending.path, body))
        }).transpose()
    }

    pub async fn read(&self, admitted: &Admitted, path: &str, body: Value) -> Result<Answer, TransportError> {
        self.validate(admitted)?;
        admitted.run(self.read_in(admitted, path, body)).await?
    }

    async fn read_in(&self, admitted: &Admitted, path: &str, body: Value) -> Result<Answer, TransportError> {
        check_path(path)?;
        let credential = self.credential(admitted)?;
        let mut answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        if confirmed_error(&answer.body) == Some(REPLAYED) {
            answer = self.exchange(&credential, &uuid::Uuid::new_v4().to_string(), &random_token(), None, path, &body).await?;
        }
        Ok(answer)
    }

    pub fn signed_in(&self) -> Result<Option<String>, TransportError> {
        Ok(self.read_connection(state::session)?.filter(|session| session.sequence > 0).map(|session| session.credential_digest))
    }

    async fn exchange(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<Answer, TransportError> {
        let mut retry = Retrying::new(self.clock.as_ref(), &self.progress, &self.online, RetryPolicy::Http);
        loop {
            self.online.online().await;
            let token = self.request_token(Some(RequestSession { credential: credential.into(), sequence }), request_id, response_key, path, body)?;
            let result = self.post(path, token, request_id, response_key, crate::transport::NetworkPurpose::Standard).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
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

    async fn post(&self, path: &str, token: String, request_id: &str, response_key: &str, purpose: crate::transport::NetworkPurpose) -> Result<Answer, TransportError> {
        crate::db::lock::assert_no_library_lock("HttpExchange::post");
        let reply = self.http.post(path, token, purpose).await?;
        if reply.status != 200 {
            return Err(TransportError::Http { status: reply.status, body: reply.body });
        }
        let opened = open_response(&reply.body, response_key).map_err(|error| TransportError::Decode(error.to_string()))?;
        if opened.request_id != request_id {
            return Err(TransportError::Decode("the response belongs to another request".into()));
        }
        Ok(Answer { sequence: opened.sequence, body: opened.body })
    }

    pub(super) fn credential(&self, admitted: &Admitted) -> Result<String, TransportError> {
        self.validate(admitted)?;
        let cell = &self.credential;
        let (cached, generation, _loading) = {
            let mut state = crate::db::locked(&cell.state);
            loop {
                if let Some(credential) = &state.cached { break (Some(credential.clone()), state.generation, None); }
                match state.loading {
                    None => {
                        state.loading = Some(thread::current().id());
                        break (None, state.generation, Some(Loading(cell)));
                    }
                    Some(owner) if owner == thread::current().id() => return Err(TransportError::Network("the credential callback re-entered the credential load".into())),
                    Some(_) => state = crate::db::lock::wait(&cell.settled, state),
                }
            }
        };
        match cached {
            Some(credential) => self.verified(credential),
            None => self.load_credential(admitted, generation),
        }
    }

    pub(super) fn cable_credential(&self, admitted: &Admitted) -> Result<String, TransportError> {
        self.credential(admitted)
    }

    fn verified(&self, credential: String) -> Result<String, TransportError> {
        match self.read_connection(state::session)? {
            Some(session) if session.credential_digest == digest_of(&credential) => Ok(credential),
            _ => Err(TransportError::Session { code: SessionCode::StateLost, detail: "lost session sequence state for the stored credential".into() }),
        }
    }

    fn load_credential(&self, admitted: &Admitted, generation: u64) -> Result<String, TransportError> {
        crate::db::lock::assert_no_library_lock("CredentialSlot::get");
        let (credential, minted) = match crate::blocking::blocking_unit(|| self.credentials.get())? {
            Some(stored) => (self.verified(stored), false),
            None => {
                let credential = random_token();
                crate::db::lock::assert_no_library_lock("CredentialSlot::set");
                crate::blocking::blocking_unit(|| self.credentials.set(&credential))?;
                (Ok(credential), true)
            }
        };
        let mut state = crate::db::locked(&self.credential.state);
        if state.generation != generation {
            drop(state);
            if minted { crate::db::lock::assert_no_library_lock("CredentialSlot::clear"); crate::blocking::blocking_unit(|| self.credentials.clear())?; }
            return Err(TransportError::Network("the session was reset while its credential was loading".into()));
        }
        let credential = credential?;
        if minted {
            self.write_in(admitted, |connection| state::start(connection, &digest_of(&credential)))?;
        }
        state.cached = Some(credential.clone());
        Ok(credential)
    }

    #[cfg(test)]
    pub(crate) fn sequence_for_test(&self) -> i64 {
        self.read_connection(state::session).ok().flatten().map_or(0, |session| session.sequence)
    }

    #[cfg(test)]
    pub(crate) fn sign_in_for_test(&self) -> Result<(), TransportError> {
        let admitted = self.admit("test sign in")?;
        self.credential(&admitted)?;
        self.write_as(&Authority::Internal(Internal::for_tests()), |connection| state::commit(connection, 1))
    }

    fn read_connection<T>(&self, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.db.shared.store.read_locked(run).map_err(storage)
    }
}
