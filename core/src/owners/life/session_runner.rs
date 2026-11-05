//! Executor of the effects of Session (`design.md` F3): the HTTP exchange of `EncryptedClient` (encoding only), the random credential,
//! the `CredentialStore` calls and the writes of `encrypted_*` that go to Store as batches by value. It does IO and returns the result as an input; it takes no transition.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Wiring};
use crate::owners::contract::{BatchAuthor, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::lifecycle::{LifecycleInput, Peer};
use crate::owners::session::{stored_answer, AttemptId, Credential, OperationClass, PendingCall, SecretState, SessionCalls, SessionEffect, SessionInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::encrypted::client::{digest_of, permanently_refused, temporary_error};
use crate::transport::encrypted::jwe::random_token;
use crate::transport::encrypted::{CredentialSlot, EncryptedClient};
use crate::transport::{NetworkPurpose, SessionCode, TransportError};

const PERSISTED_QUERY_NOT_FOUND: &str = "PERSISTED_QUERY_NOT_FOUND";

struct Wire {
    id: String,
    key: String,
}

enum SecretFailure {
    Lost,
    Store(String),
}

pub(super) struct SessionRunner {
    wiring: Arc<Wiring>,
    client: Arc<EncryptedClient>,
    credentials: Arc<dyn CredentialSlot>,
    executor: tokio::runtime::Handle,
    running: HashMap<AttemptId, AbortHandle>,
    /// The identity of a credential to its full digest.
    digests: HashMap<u64, String>,
    /// The generated secrets that `StoreSecret` has not written yet, by identity.
    generated: HashMap<u64, String>,
    wires: HashMap<RequestId, Wire>,
    /// The epoch that the last step ended on the server: the `Reply` of the same step waits for the next epoch.
    ending: Option<u64>,
}

/// What the stored rows and the secret give to the owner and its executor at open.
pub(crate) struct Restored {
    pub credential: Option<Credential>,
    pub pending: Option<PendingCall>,
    pub dropped_pending: bool,
    pub answers: std::collections::BTreeMap<RequestId, crate::owners::session::StoredAnswer>,
    pub secret: SecretState,
    wire: Option<(RequestId, Wire)>,
    digest: Option<(u64, String)>,
}

impl SessionRunner {
    pub fn new(wiring: Arc<Wiring>, client: Arc<EncryptedClient>, credentials: Arc<dyn CredentialSlot>, executor: tokio::runtime::Handle, restored: &Restored) -> Self {
        let wires = restored.wire.iter().map(|(request_id, wire)| (*request_id, Wire { id: wire.id.clone(), key: wire.key.clone() })).collect();
        let digests = restored.digest.iter().cloned().collect();
        Self { wiring, client, credentials, executor, running: HashMap::new(), digests, generated: HashMap::new(), wires, ending: None }
    }

    /// What `Open` of the owner takes: the stored `encrypted_session`, the stored `encrypted_pending` and what `CredentialStore.get` found.
    pub fn stored(client: &EncryptedClient) -> Result<Restored, TransportError> {
        let session = client.stored_session()?;
        let credential = session.as_ref().and_then(|session| crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence: u64::try_from(session.sequence).unwrap_or(0) }));
        let secret = match crate::blocking::blocking_unit(|| client.credential_slot().get()) {
            Ok(Some(secret)) => crate::store::identity_of(&digest_of(&secret)).map_or(SecretState::Missing, SecretState::Present),
            Ok(None) => SecretState::Missing,
            Err(error) => SecretState::Error(error.to_string()),
        };
        let answers = client.stored_answers()?;
        let Some(row) = client.stored_pending()? else {
            return Ok(Restored { credential, pending: None, dropped_pending: false, answers, secret, wire: None, digest: None });
        };
        let sequence = u64::try_from(row.sequence).map_err(|_| TransportError::Decode(format!("the stored pending has the sequence {}", row.sequence)))?;
        let sessionless = row.credential_digest.is_empty();
        let first_sign_in = !sessionless && sequence == 1 && credential.as_ref().is_some_and(|credential| credential.sequence == 0);
        let identity = first_sign_in.then(|| crate::store::identity_of(&row.credential_digest)).flatten();
        let operations = client.session_operations();
        let operation = operations.iter().find(|def| def.path == row.path && row.owner.is_none()).map_or_else(String::new, |def| def.name.clone());
        let class = if sessionless { OperationClass::Sessionless } else if row.path == client.logout_path() { OperationClass::Logout } else if first_sign_in { OperationClass::Login } else { OperationClass::Authenticated };
        let request_id = match &row.owner {
            Some(crate::transport::RequestOwner::Operation { seq }) => u64::try_from(*seq).unwrap_or(RESTORED_REQUEST),
            _ => RESTORED_REQUEST,
        };
        let pending = PendingCall {
            request_id,
            sequence,
            class,
            operation,
            path: row.path,
            body: row.body,
            credential: identity,
            purpose: u8::from(row.purpose == NetworkPurpose::Transfer),
            author: row.owner.as_ref().map(crate::owners::session::author_of),
            owner: row.owner,
            epoch: 0,
            reply: None,
        };
        let digest = identity.map(|identity| (identity, row.credential_digest));
        Ok(Restored { credential, pending: Some(pending), dropped_pending: false, answers, secret, wire: Some((request_id, Wire { id: row.request_id, key: row.response_key })), digest })
    }

    fn commit(&self, batch: u64, epoch: u64, ops: Vec<StoreOp>) {
        self.wiring.to_store(StoreInput::Commit(StoreBatch { author: BatchAuthor::Session, id: batch, epoch, basis: Default::default(), rows: Vec::new(), intents: 1, credential: None, ops }));
    }

    fn digest_for(&self, pending: &PendingCall) -> Result<String, TransportError> {
        if pending.class == OperationClass::Sessionless {
            return Ok(String::new());
        }
        match pending.credential {
            Some(identity) => self.digests.get(&identity).cloned().ok_or_else(|| TransportError::Decode(format!("no digest for the identity {identity}"))),
            None => Ok(self.client.stored_session()?.map_or_else(String::new, |session| session.credential_digest)),
        }
    }

    fn secret_for(&self, pending: &PendingCall) -> Result<Option<String>, SecretFailure> {
        if pending.class == OperationClass::Sessionless {
            return Ok(None);
        }
        let expected = self.digest_for(pending).map_err(|error| SecretFailure::Store(error.to_string()))?;
        match crate::blocking::blocking_unit(|| self.credentials.get()).map_err(|error| SecretFailure::Store(error.to_string()))? {
            Some(secret) if digest_of(&secret) == expected => Ok(Some(secret)),
            _ => Err(SecretFailure::Lost),
        }
    }
}

pub(crate) const RESTORED_REQUEST: RequestId = 1 << 62;

fn refusal_of(body: &Value) -> Option<SessionCode> {
    body.get("error").and_then(Value::as_str).into_iter()
        .chain(body.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|error| error.get("extensions")?.get("code")?.as_str()))
        .find_map(SessionCode::from_wire)
}

fn persisted_query_missing(body: &Value) -> bool {
    body.get("errors").and_then(Value::as_array).into_iter().flatten().any(|error| error.get("extensions").and_then(|extensions| extensions.get("code")).and_then(Value::as_str) == Some(PERSISTED_QUERY_NOT_FOUND))
}

/// The body of a persisted GraphQL request goes with its document only after the server missed the hash.
fn body_of(text: &str, full_document: bool) -> Value {
    let mut body: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    if !full_document && body.pointer("/extensions/persistedQuery").is_some() {
        if let Some(object) = body.as_object_mut() { object.remove("query"); }
    }
    body
}

pub(super) fn input_of(request_id: RequestId, attempt_id: AttemptId, logout: bool, result: Result<crate::transport::encrypted::Answer, TransportError>) -> SessionInput {
    match result {
        Ok(answer) => {
            if persisted_query_missing(&answer.body) {
                return SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: false, persisted_missing: true };
            }
            if let Some(code) = refusal_of(&answer.body) {
                return SessionInput::Response { request_id, attempt_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), refusal: Some(code) };
            }
            if temporary_error(&answer.body).is_some() {
                return SessionInput::HttpError { request_id, attempt_id, temporary: true, auth: false, persisted_missing: false };
            }
            if logout && answer.body.get("revoked") == Some(&Value::Bool(true)) {
                return SessionInput::Revoked { request_id, attempt_id, body: answer.body.to_string() };
            }
            SessionInput::Response { request_id, attempt_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), refusal: None }
        }
        Err(TransportError::Http { status: 401 | 403, .. }) => SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: true, persisted_missing: false },
        Err(error) if permanently_refused(&error) || matches!(error, TransportError::RequestTooLarge { .. } | TransportError::Decode(_)) => SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: false, persisted_missing: false },
        Err(TransportError::Interrupted(_)) => SessionInput::Dropped { request_id, attempt_id },
        Err(_) => SessionInput::HttpError { request_id, attempt_id, temporary: true, auth: false, persisted_missing: false },
    }
}

impl EffectRunner<SessionCalls> for SessionRunner {
    fn step_done(&mut self) {
        self.ending = None;
    }

    fn run(&mut self, _stamp: EpochStamp, effect: SessionEffect, back: &Back<SessionCalls>) {
        let control = EpochStamp::Control;
        match effect {
            SessionEffect::GenerateCredential { secret_op_id } => {
                let secret = random_token();
                let digest = digest_of(&secret);
                let Some(identity) = crate::store::identity_of(&digest) else { return back.reply(control, SessionInput::SecretStored { secret_op_id, ok: false }) };
                self.digests.insert(identity, digest);
                self.generated.insert(identity, secret);
                back.reply(control, SessionInput::CredentialGenerated { secret_op_id, credential: identity });
            }
            SessionEffect::StoreSecret { credential, secret_op_id } => {
                let ok = self.generated.get(&credential).is_some_and(|secret| crate::blocking::blocking_unit(|| self.credentials.set(secret)).is_ok());
                self.generated.remove(&credential);
                back.reply(control, SessionInput::SecretStored { secret_op_id, ok });
            }
            SessionEffect::ClearSecret { secret_op_id } => {
                let ok = self.client.erase_credential().is_ok();
                back.reply(control, SessionInput::SecretCleared { secret_op_id, ok });
            }
            SessionEffect::ReadSecret { secret_op_id } => {
                let read = match crate::blocking::blocking_unit(|| self.credentials.get()) {
                    Ok(Some(secret)) => crate::store::identity_of(&digest_of(&secret)).map_or(SecretState::Missing, SecretState::Present),
                    Ok(None) => SecretState::Missing,
                    Err(error) => SecretState::Error(error.to_string()),
                };
                back.reply(control, SessionInput::SecretRead { secret_op_id, read });
            }
            SessionEffect::CommitPending { batch, epoch, pending, session } => {
                let credential_digest = match self.digest_for(&pending) {
                    Ok(digest) => digest,
                    Err(error) => return back.reply(control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(error.to_string()) })),
                };
                let wire = Wire { id: uuid::Uuid::new_v4().to_string(), key: random_token() };
                let mut ops = vec![StoreOp::Pending { pending: pending.clone(), credential_digest: credential_digest.clone(), wire_id: wire.id.clone(), response_key: wire.key.clone() }];
                if session.is_some() { ops.push(StoreOp::Session { digest: Some(credential_digest), sequence: Some(0) }); }
                self.wires.clear();
                self.wires.insert(pending.request_id, wire);
                self.commit(batch, epoch, ops);
            }
            SessionEffect::CommitLanding { batch, epoch, digest, sequence, answer, erase_session } => {
                let digest = match digest.map(|identity| self.digests.get(&identity).cloned().ok_or(identity)).transpose() {
                    Ok(digest) => digest,
                    Err(identity) => return back.reply(control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(format!("no digest for the identity {identity}")) })),
                };
                let mut ops = if erase_session { vec![StoreOp::EraseSession, StoreOp::RemovePending] } else if digest.is_none() && sequence.is_none() { vec![StoreOp::RemovePending] } else { vec![StoreOp::Session { digest, sequence }, StoreOp::RemovePending] };
                ops.extend(answer.and_then(|(request_id, answer)| Some(StoreOp::Answer { request_id, result: stored_answer(&answer)? })));
                self.commit(batch, epoch, ops);
            }
            SessionEffect::HttpRequest { pending, attempt_id, full_document } => {
                let request_id = pending.request_id;
                let Some(wire) = self.wires.get(&request_id) else {
                    return back.reply(control, SessionInput::SecretMissing { request_id, attempt_id, lost: true });
                };
                let (wire, key) = (wire.id.clone(), wire.key.clone());
                let credential = match self.secret_for(&pending) {
                    Ok(secret) => secret,
                    Err(failure) => return back.reply(control, SessionInput::SecretMissing { request_id, attempt_id, lost: matches!(failure, SecretFailure::Lost) }),
                };
                let (client, back) = (self.client.clone(), back.clone());
                let logout = pending.class == OperationClass::Logout;
                let body = body_of(&pending.body, full_document);
                let purpose = if pending.purpose == 0 { NetworkPurpose::Standard } else { NetworkPurpose::Transfer };
                let sequence = (pending.class != OperationClass::Sessionless).then_some(i64::try_from(pending.sequence).unwrap_or(i64::MAX));
                let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, async move {
                    crate::db::lock::assert_no_library_lock("HttpExchange::post");
                    let result = client.send_once(credential.as_deref(), &wire, &key, sequence, &pending.path, &body, purpose).await;
                    back.reply(EpochStamp::Control, input_of(request_id, attempt_id, logout, result));
                });
                if let Some(previous) = self.running.insert(attempt_id, task.abort_handle()) {
                    crate::db::lock::effect(|| previous.abort());
                }
            }
            SessionEffect::HttpCancel { attempt_id } => {
                if let Some(task) = self.running.remove(&attempt_id) {
                    crate::db::lock::effect(|| task.abort());
                }
            }
            SessionEffect::ScheduleTick { .. } | SessionEffect::RetryState { .. } => {}
            SessionEffect::CredentialCommitted(committed) => self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(committed)),
            SessionEffect::ServerEnded(ended) => {
                self.ending = Some(ended.epoch);
                self.wiring.to_lifecycle(LifecycleInput::ServerEnded(ended));
            }
            SessionEffect::SessionReset => self.wiring.to_lifecycle(LifecycleInput::SessionReset { reply: None }),
            SessionEffect::RestoreClassified { epoch } => self.wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Session, epoch }),
            SessionEffect::Reply { id, answer } => {
                let (id, reply) = (OutcomeId::from_raw(id as ReplyId), LifeReply::Session(answer));
                match self.ending.take() {
                    Some(epoch) => self.wiring.settle_after_epoch(epoch, id, reply),
                    None => { self.wiring.outcomes.settle(id, reply); }
                }
            }
            SessionEffect::Answer { author, req_id, outcome } => self.wiring.answer_author(author, req_id, outcome),
            SessionEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}
