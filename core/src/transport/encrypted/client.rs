use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};

use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as Gate;

use super::jwe::{open_response, random_token, seal_request, RequestPlain, RequestSession};
use super::key::ServerKey;
use super::state::{self, Pending};
use super::{CredentialSlot, HttpExchange};
use crate::db::Db;
use crate::lifecycle::{Admitted, Authority, End, Origin};
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
    credentials: Arc<dyn CredentialSlot>,
    db: Db,
    pub(super) clock: Arc<dyn Clock>,
    pub(crate) online: Arc<crate::transport::Connectivity>,
    credential: Arc<CredentialCell>,
    mutations: Gate<()>,
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
    #[cfg(test)]
    parked: Mutex<Option<Arc<ResetPark>>>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ResetPark {
    pub(crate) entered: std::sync::atomic::AtomicBool,
    pub(crate) release: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl CredentialCell {
    fn park(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        let Some(park) = crate::db::locked(&self.parked).take() else { return };
        park.entered.store(true, SeqCst);
        for _ in 0..10_000 {
            if park.release.load(SeqCst) { break; }
            thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

struct Loading<'a>(&'a CredentialCell);

impl Drop for Loading<'_> {
    fn drop(&mut self) {
        crate::db::locked(&self.0.state).loading = None;
        self.0.settled.notify_all();
    }
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
    match error {
        rusqlite::Error::FromSqlConversionFailure(..) => TransportError::Decode(format!("session storage: {error}")),
        _ => TransportError::Network(format!("session storage: {error}")),
    }
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

/// What the caller does once its lease has ended: the answer was committed under the lease, and a change of the session needs the whole database.
pub(crate) enum After {
    Nothing,
    Revoked,
}

impl EncryptedClient {
    pub(super) fn admit(&self, target: &'static str) -> Result<Admitted, TransportError> {
        Ok(self.db.shared.store.lifecycle.admit_for_session(Origin::Explicit, target)?)
    }

    pub(super) fn validate(&self, admitted: &Admitted) -> Result<(), TransportError> {
        if Arc::ptr_eq(admitted.life(), &self.db.shared.store.lifecycle) && admitted.is_current() { Ok(()) } else { Err(crate::lifecycle::Ended.into()) }
    }

    fn write_in<T>(&self, admitted: &Admitted, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.write_as(&admitted.authority(), run)
    }

    fn write_as<T>(&self, authority: &Authority, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.db.shared.store.write(authority, || TransportError::Interrupted("the session ended while its answer was pending".into()), |inner| run(&inner.connection).map_err(storage))
    }

    pub(crate) async fn conclude(&self, epoch: u64, after: After) -> Result<(), TransportError> {
        match after {
            After::Nothing => Ok(()),
            After::Revoked => self.end_session(End::Server, Some(epoch)).await,
        }
    }

    pub async fn session_call(&self, admitted: &Admitted, operation: &str, body: Value) -> Result<Answer, TransportError> {
        self.validate(admitted)?;
        let epoch = admitted.value();
        let (answer, after) = self.call_admitted(admitted, operation.to_string(), body).await?;
        self.conclude(epoch, after).await?;
        Ok(answer)
    }

    pub(crate) async fn call_admitted(&self, admitted: &Admitted, operation: String, body: Value) -> Result<(Answer, After), TransportError> {
        self.validate(admitted)?;
        admitted.run(self.session_call_in(admitted, operation, body)).await?
    }

    async fn session_call_in(&self, admitted: &Admitted, operation: String, body: Value) -> Result<(Answer, After), TransportError> {
        if operation == "logout" { return self.mutate_in(admitted, &self.config.endpoints.logout, body, None, false).await; }
        let operation = self.config.endpoints.session_operations.get(&operation).ok_or_else(|| TransportError::Decode(format!("unknown session operation {operation}")))?;
        if !operation.sessionless {
            return self.mutate_in(admitted, &operation.path, body, None, false).await;
        }
        let mut retry = Retrying::new(self.clock.as_ref(), &self.progress, &self.online, RetryPolicy::Http);
        loop {
            self.online.online().await;
            let request_id = uuid::Uuid::new_v4().to_string();
            let response_key = random_token();
            let token = self.request_token(None, &request_id, &response_key, &operation.path, &body)?;
            let result = self.post(&operation.path, token, &request_id, &response_key, false).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
            match result {
                Ok(answer) => {
                    return Ok((answer, After::Nothing));
                }
                Err(error) => retry.failed(error).await?,
            }
        }
    }

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
        let client = Self { config: config.clone(), audience, key, http, credentials, db, clock, online, credential: Arc::default(), mutations: Gate::new(()), progress: Arc::default() };
        if client.db.shared.store.take_session_end_replay() {
            client.credentials.clear()?;
            let admitted = client.admit("open encrypted state")?;
            client.write_in(&admitted, state::clear)?;
        }
        Ok(client)
    }

    pub async fn mutate(&self, admitted: &Admitted, path: &str, body: Value) -> Result<Answer, TransportError> {
        self.validate(admitted)?;
        let epoch = admitted.value();
        let (answer, after) = admitted.run(self.mutate_in(admitted, path, body, None, false)).await??;
        self.conclude(epoch, after).await?;
        Ok(answer)
    }

    pub(crate) async fn mutate_owned(&self, admitted: &Admitted, path: &str, body: Value, owner: Option<crate::transport::RequestOwner>, upload: bool) -> Result<Answer, TransportError> {
        self.validate(admitted)?;
        let epoch = admitted.value();
        let (answer, after) = admitted.run(self.mutate_in(admitted, path, body, owner, upload)).await??;
        if matches!(after, After::Revoked) { self.conclude(epoch, after).await?; }
        Ok(answer)
    }

    async fn mutate_in(&self, admitted: &Admitted, path: &str, body: Value, owner: Option<crate::transport::RequestOwner>, upload: bool) -> Result<(Answer, After), TransportError> {
        check_path(path)?;
        let _turn = self.mutations.lock().await;
        let credential = self.credential(admitted)?;
        let pending = match self.read_connection(state::pending)? {
            Some(pending) if pending.owner == owner && pending.path == path && serde_json::from_str::<Value>(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))? == body => {
                self.pending_token(&credential, &pending)?;
                pending
            }
            Some(_) => return Err(TransportError::Network("another encrypted request is pending, resolve it first".into())),
            None => {
                let sequence = self.read_connection(state::session)?.map_or(1, |session| session.sequence + 1);
                let pending = Pending {
                    credential_digest: digest_of(&credential),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    response_key: random_token(),
                    sequence,
                    path: path.to_string(),
                    body: body.to_string(),
                    owner,
                    upload,
                };
                self.pending_token(&credential, &pending)?;
                self.write_in(admitted, |connection| state::store_pending(connection, &pending))?;
                pending
            }
        };
        self.await_answer(admitted, pending).await
    }

    pub fn pending_call(&self) -> Result<Option<(String, Value)>, TransportError> {
        self.read_connection(state::pending)?.map(|pending| {
            let body = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
            Ok((pending.path, body))
        }).transpose()
    }

    pub(crate) fn pending_owner(&self) -> Result<Option<crate::transport::RequestOwner>, TransportError> {
        Ok(self.read_connection(state::pending)?.and_then(|pending| pending.owner))
    }

    pub async fn resume(&self, admitted: &Admitted) -> Result<Option<Answer>, TransportError> {
        self.validate(admitted)?;
        let epoch = admitted.value();
        let result = admitted.run(self.resume_in(admitted)).await?;
        match result {
            Err(error) if permanently_refused(&error) => Ok(None),
            Err(error) => Err(error),
            Ok(None) => Ok(None),
            Ok(Some((answer, after))) => { self.conclude(epoch, after).await?; Ok(Some(answer)) }
        }
    }

    pub(crate) async fn resume_pending(&self, admitted: &Admitted) -> Result<Option<Answer>, TransportError> {
        self.validate(admitted)?;
        let epoch = admitted.value();
        let resumed = admitted.run(self.resume_in(admitted)).await??;
        let Some((answer, after)) = resumed else { return Ok(None) };
        if matches!(after, After::Revoked) { self.conclude(epoch, after).await?; }
        Ok(Some(answer))
    }

    async fn resume_in(&self, admitted: &Admitted) -> Result<Option<(Answer, After)>, TransportError> {
        let _turn = self.mutations.lock().await;
        let Some(pending) = self.read_connection(state::pending)? else { return Ok(None) };
        if self.config.endpoints.session_operations.values().any(|operation| operation.sessionless && operation.path == pending.path) {
            self.write_in(admitted, state::drop_pending)?;
            return Ok(None);
        }
        let credential = self.credential(admitted)?;
        let Some(pending) = self.read_connection(state::pending)? else { return Ok(None) };
        self.pending_token(&credential, &pending)?;
        self.await_answer(admitted, pending).await.map(Some)
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

    /// Ends the session: the old epoch's work ends, the credential and its tables clear, and the data of the account goes. A caller that carries the epoch it was admitted in is refused once that epoch has gone.
    pub(crate) async fn end_session(&self, end: End, expected: Option<u64>) -> Result<(), TransportError> {
        let (credentials, cell, db) = (self.credentials.clone(), self.credential.clone(), self.db.clone());
        let clear = move |authority: &Authority| -> Result<(), TransportError> {
            crate::db::lock::assert_no_library_lock("CredentialSlot::clear");
            credentials.clear()?;
            let mut state = crate::db::locked(&cell.state);
            state.cached = None;
            state.generation += 1;
            #[cfg(test)]
            cell.park();
            let cleared = db.shared.store.write(authority, || TransportError::Interrupted("the session ended while its credential cleared".into()), |inner| state::clear(&inner.connection).map_err(storage));
            drop(state);
            cleared
        };
        self.db.end_session(end, expected, end != End::Server, clear).await
    }

    pub async fn reset(&self) -> Result<(), TransportError> {
        self.end_session(End::Local, None).await
    }

    fn pending_token(&self, credential: &str, pending: &Pending) -> Result<String, TransportError> {
        let body: Value = serde_json::from_str(&pending.body).map_err(|error| TransportError::Decode(error.to_string()))?;
        self.request_token(Some(RequestSession { credential: credential.into(), sequence: Some(pending.sequence) }), &pending.request_id, &pending.response_key, &pending.path, &body)
    }

    async fn await_answer(&self, admitted: &Admitted, mut pending: Pending) -> Result<(Answer, After), TransportError> {
      let mut retry = Retrying::new(self.clock.as_ref(), &self.progress, &self.online, RetryPolicy::Http);
      loop {
        self.online.online().await;
        let token = self.pending_token(&self.credential(admitted)?, &pending)?;
        let answer = match self.post(&pending.path, token, &pending.request_id, &pending.response_key, pending.upload).await {
            Err(error) if permanently_refused(&error) => {
                self.write_in(admitted, state::drop_pending)?;
                return Err(error);
            }
            Err(error) => {
                if pending.owner.is_some() { return Err(error); }
                retry.failed(error).await?;
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
                if let Err(error) = self.pending_token(&self.credential(admitted)?, &pending) {
                    self.write_in(admitted, |connection| state::commit(connection, confirmed))?;
                    return Err(error);
                }
                self.write_in(admitted, |connection| state::retry_confirmed(connection, &pending))?;
            }
            if pending.owner.is_some() { return Err(error); }
            retry.failed(error).await?;
            continue;
        }
        if answer.sequence != Some(pending.sequence) {
            return Err(match confirmed_error(&answer.body) {
                Some(code) => TransportError::refusal(code),
                None => TransportError::Decode("the answer does not confirm the sequence".into()),
            });
        }
        let after = match pending.path == self.config.endpoints.logout && answer.body.get("revoked") == Some(&Value::Bool(true)) {
            true => After::Revoked,
            false => {
                self.write_in(admitted, |connection| state::commit(connection, pending.sequence))?;
                After::Nothing
            }
        };
        return Ok((answer, after));
      }
    }

    async fn exchange(&self, credential: &str, request_id: &str, response_key: &str, sequence: Option<i64>, path: &str, body: &Value) -> Result<Answer, TransportError> {
        let mut retry = Retrying::new(self.clock.as_ref(), &self.progress, &self.online, RetryPolicy::Http);
        loop {
            self.online.online().await;
            let token = self.request_token(Some(RequestSession { credential: credential.into(), sequence }), request_id, response_key, path, body)?;
            let result = self.post(path, token, request_id, response_key, false).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
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

    async fn post(&self, path: &str, token: String, request_id: &str, response_key: &str, upload: bool) -> Result<Answer, TransportError> {
        crate::db::lock::assert_no_library_lock("HttpExchange::post");
        let reply = self.http.post(path, token, upload).await?;
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
    pub(crate) fn park_next_reset_for_test(&self) -> Arc<ResetPark> {
        let park = Arc::new(ResetPark::default());
        *crate::db::locked(&self.credential.parked) = Some(park.clone());
        park
    }

    /// Clears the credential and the session state without ending the session of the database.
    #[cfg(test)]
    pub(crate) fn clear_credential_for_test(&self) -> Result<(), TransportError> {
        self.credentials.clear()?;
        crate::db::locked(&self.credential.state).cached = None;
        self.write_as(&Authority::Internal(crate::lifecycle::Internal::for_tests()), state::clear)
    }

    #[cfg(test)]
    pub(crate) fn sequence_for_test(&self) -> i64 {
        self.read_connection(state::session).ok().flatten().map_or(0, |session| session.sequence)
    }

    #[cfg(test)]
    pub(crate) fn sign_in_for_test(&self) -> Result<(), TransportError> {
        let admitted = self.admit("test sign in")?;
        self.credential(&admitted)?;
        self.write_as(&Authority::Internal(crate::lifecycle::Internal::for_tests()), |connection| state::commit(connection, 1))
    }

    fn read_connection<T>(&self, run: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, TransportError> {
        self.db.shared.store.read_locked(run).map_err(storage)
    }
}
