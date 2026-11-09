//! Executor of the effects of Session: the HTTP exchange of `Exchange` (encoding only), the random credential,
//! the `CredentialStore` calls and the writes of `encrypted_*` that go to Store as batches by value. It does IO and returns the result as an input; it takes no transition.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Wiring};
use crate::owners::contract::{BatchAuthor, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::lifecycle::{LifecycleInput, Peer};
use crate::owners::network::NetworkInput;
use crate::owners::session::{secret_id, stored_answer, AttemptId, Credential, OperationClass, PendingCall, SecretState, SessionCalls, SessionEffect, SessionInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::encrypted::credential::{dropped_pending, stored_answers, stored_pending, stored_session, Secret};
use crate::transport::encrypted::exchange::{digest_of, permanently_refused, Exchange};
use crate::transport::retry::temporary_error;
use crate::store::Store;
use crate::transport::encrypted::jwe::random_token;
use crate::transport::encrypted::CredentialSlot;
use crate::transport::{NetworkPurpose, SessionCode, TransportError};


struct Wire {
    id: String,
    key: String,
    cut: u64,
}

enum SecretFailure {
    Lost,
    Store,
}

pub(super) struct SessionRunner {
    wiring: Arc<Wiring>,
    exchange: Arc<Exchange>,
    secret: Arc<Secret>,
    store: Arc<Store>,
    seal: Arc<crate::owners::session_seal::SealConfig>,
    executor: tokio::runtime::Handle,
    running: HashMap<AttemptId, AbortHandle>,
    /// The identity of a credential to its full digest.
    digests: HashMap<u64, String>,
    /// The generated secrets that `StoreSecret` has not written yet, by identity.
    generated: HashMap<u64, String>,
    /// The credentials generated off the owner threads reach the maps above through this channel before the owner hears of them.
    minted: Receiver<(u64, String, String)>,
    mint: Sender<(u64, String, String)>,
    wires: HashMap<RequestId, Wire>,
    operations: HashMap<RequestId, i64>,
}

/// What the stored rows and the secret give to the owner and its executor at open.
pub(crate) struct Restored {
    pub credential: Option<Credential>,
    pub pending: Option<PendingCall>,
    pub dropped_pending: bool,
    pub answers: std::collections::BTreeMap<RequestId, crate::owners::session::StoredAnswer>,
    pub secret: SecretState,
    pub restore_error: Option<String>,
    pub unowned_pending: bool,
    wire: Option<(RequestId, Wire)>,
    digest: Option<(u64, String)>,
}

impl SessionRunner {
    pub fn new(wiring: Arc<Wiring>, (exchange, secret, store): (Arc<Exchange>, Arc<Secret>, Arc<Store>), seal: Arc<crate::owners::session_seal::SealConfig>, executor: tokio::runtime::Handle, restored: &Restored) -> Self {
        let wires = restored.wire.iter().map(|(request_id, wire)| (*request_id, Wire { id: wire.id.clone(), key: wire.key.clone(), cut: wiring.gate.cut() })).collect();
        let digests = restored.digest.iter().cloned().collect();
        let (mint, minted) = std::sync::mpsc::channel();
        if let Some(pending) = &restored.pending {
            if let Some(owner @ crate::transport::RequestOwner::Transfer { .. }) = &pending.owner { wiring.hold_request(owner.clone(), pending.request_id); }
        }
        let operations = restored.pending.iter().filter_map(|pending| match pending.owner { Some(crate::transport::RequestOwner::Operation { seq }) => Some((pending.request_id, seq)), _ => None }).collect();
        Self { wiring, exchange, secret, store, seal, executor, running: HashMap::new(), digests, generated: HashMap::new(), minted, mint, wires, operations }
    }

    /// What `Open` of the owner takes: the rows and what `CredentialStore.get` found.
    pub fn stored(exchange: &Exchange, secret: &Secret, store: &Store) -> Result<Restored, TransportError> {
        let secret = match crate::blocking::blocking_unit(|| secret.slot().get()) {
            Ok(Some(secret)) => crate::store::identity_of(&digest_of(&secret)).map_or(SecretState::Missing, SecretState::Present),
            Ok(None) => SecretState::Missing,
            Err(error) => SecretState::Error(error.to_string()),
        };
        match Self::rows(exchange, store) {
            Ok(rows) => Ok(Restored { secret, ..rows }),
            Err(error) => {
                let session = stored_session(store)?;
                let credential = session.as_ref().map(|session| {
                    let sequence = u64::try_from(session.sequence).map_err(|_| TransportError::Decode(format!("the stored session has the sequence {}", session.sequence)))?;
                    Ok::<_, TransportError>(crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence }))
                }).transpose()?.flatten();
                let unowned_pending = store.read_locked(crate::store::session_state::unowned_pending).map_err(|error| TransportError::Network(format!("session storage: {error}")))?;
                Ok(Restored { credential, pending: None, dropped_pending: false, answers: Default::default(), secret, restore_error: Some(error.to_string()), unowned_pending, wire: None, digest: None })
            }
        }
    }

    /// The stored `encrypted_session`, `encrypted_pending` and `encrypted_answer`: the snapshot of `ReadIntents` reads no secret.
    pub(super) fn rows(exchange: &Exchange, store: &Store) -> Result<Restored, TransportError> {
        let session = stored_session(store)?;
        let credential = match &session {
            Some(session) => {
                let sequence = u64::try_from(session.sequence).map_err(|_| TransportError::Decode(format!("the stored session has the sequence {}", session.sequence)))?;
                crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence })
            }
            None => None,
        };
        let secret = SecretState::Missing;
        let answers = stored_answers(store)?;
        let dropped_pending = dropped_pending(store)?;
        let row = if dropped_pending { None } else { stored_pending(store)? };
        let Some(row) = row else {
            return Ok(Restored { credential, pending: None, dropped_pending, answers, secret, restore_error: None, unowned_pending: false, wire: None, digest: None });
        };
        let sequence = u64::try_from(row.sequence).map_err(|_| TransportError::Decode(format!("the stored pending has the sequence {}", row.sequence)))?;
        let sessionless = row.credential_digest.is_empty();
        let first_sign_in = !sessionless && sequence == 1 && credential.as_ref().is_some_and(|credential| credential.sequence == 0);
        let identity = first_sign_in.then(|| crate::store::identity_of(&row.credential_digest)).flatten();
        let operations = exchange.session_operations();
        let operation = operations.iter().find(|def| def.path == row.path && row.owner.is_none()).map_or_else(String::new, |def| def.name.clone());
        let class = if sessionless { OperationClass::Sessionless } else if row.path == exchange.logout_path() { OperationClass::Logout } else if first_sign_in { OperationClass::Login } else { OperationClass::Authenticated };
        let request_id = row.req_id;
        let pending = PendingCall {
            request_id,
            sequence,
            class,
            operation,
            path: row.path,
            body: row.body,
            credential: identity,
            purpose: u8::from(row.purpose == NetworkPurpose::Transfer),
            author: row.owner.as_ref().and_then(crate::owners::session::author_of),
            owner: row.owner,
            epoch: 0,
            reply: None,
        };
        let digest = identity.map(|identity| (identity, row.credential_digest));
        let unowned_pending = pending.author.is_none();
        Ok(Restored { credential, pending: Some(pending), dropped_pending: false, answers, secret, restore_error: None, unowned_pending, wire: Some((request_id, Wire { id: row.request_id, key: row.response_key, cut: 0 })), digest })
    }

    fn take_minted(&mut self) {
        while let Ok((identity, digest, secret)) = self.minted.try_recv() {
            self.digests.insert(identity, digest);
            self.generated.insert(identity, secret);
        }
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
            None => Ok(stored_session(&self.store)?.map_or_else(String::new, |session| session.credential_digest)),
        }
    }

    /// The IO of the secret runs off the thread of the owners: the result comes back as an input with the same `secret_op_id`.
    fn off_thread(&self, back: &Back<SessionCalls>, id: crate::runtime::EffectId, work: impl FnOnce() -> SessionInput + Send + 'static) {
        let back = back.clone();
        let watch = back.watch(id);
        let launch = self.wiring.handle.launch();
        drop(crate::task::spawn_blocking_on(&self.executor, Some(launch), move || {
            let _watch = watch;
            back.reply(EpochStamp::Control, work());
        }));
    }

}

/// The secret the request is signed with: the stored one when its digest is the expected one. The read runs off the owner threads.
fn secret_for(credentials: &dyn CredentialSlot, expected: Option<&str>) -> Result<Option<String>, SecretFailure> {
    let Some(expected) = expected else { return Ok(None) };
    match crate::blocking::blocking_unit(|| credentials.get()).map_err(|_| SecretFailure::Store)? {
        Some(secret) if digest_of(&secret) == expected => Ok(Some(secret)),
        _ => Err(SecretFailure::Lost),
    }
}

fn refusal_of(body: &Value) -> Option<SessionCode> {
    body.get("error").and_then(Value::as_str).into_iter()
        .chain(body.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|error| error.get("extensions")?.get("code")?.as_str()))
        .find_map(SessionCode::from_wire)
}

pub(super) fn input_of(request_id: RequestId, attempt_id: AttemptId, logout: bool, result: Result<crate::transport::encrypted::Answer, TransportError>) -> SessionInput {
    match result {
        Ok(answer) => {
            if let Some(code) = refusal_of(&answer.body) {
                return SessionInput::Response { request_id, attempt_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), refusal: Some(code) };
            }
            if temporary_error(&answer.body).is_some() {
                return SessionInput::HttpError { request_id, attempt_id, temporary: true, auth: false, status: 0, too_large: None, security: None, body: Some(answer.body.to_string()) };
            }
            if logout && answer.body.get("revoked") == Some(&Value::Bool(true)) {
                return SessionInput::Revoked { request_id, attempt_id, body: answer.body.to_string() };
            }
            SessionInput::Response { request_id, attempt_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), refusal: None }
        }
        Err(TransportError::Security { code, detail }) => SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: false, status: 0, too_large: None, security: Some((code, detail)), body: None },
        Err(TransportError::Http { status: 401 | 403, .. }) => SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: true, status: 0, too_large: None, security: None, body: None },
        Err(error) if permanently_refused(&error) || matches!(error, TransportError::RequestTooLarge { .. } | TransportError::Decode(_) | TransportError::Graphql { .. } | TransportError::Storage(_) | TransportError::Internal(_)) => {
            let (status, too_large) = match &error {
                TransportError::Http { status, .. } => (*status, None),
                TransportError::RequestTooLarge { size, limit } => (0, Some((*size, *limit))),
                _ => (0, None),
            };
            SessionInput::HttpError { request_id, attempt_id, temporary: false, auth: false, status, too_large, security: None, body: None }
        }
        Err(TransportError::Interrupted(_)) => SessionInput::Dropped { request_id, attempt_id },
        Err(_) => SessionInput::HttpError { request_id, attempt_id, temporary: true, auth: false, status: 0, too_large: None, security: None, body: None },
    }
}

impl EffectRunner<SessionCalls> for SessionRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: SessionEffect, back: &Back<SessionCalls>) {
        let control = EpochStamp::Control;
        self.take_minted();
        match effect {
            SessionEffect::SealProbe { seal_id, pending, credential } => {
                let (seal, credentials) = (self.seal.clone(), self.secret.slot());
                self.off_thread(back, crate::owners::session::seal_id(seal_id), move || match seal.probe(credentials.as_ref(), credential, &pending) {
                    Ok(size) => SessionInput::Sealed { seal_id, size },
                    Err(error) => SessionInput::SealFailed { seal_id, error },
                });
            }
            SessionEffect::GenerateCredential { secret_op_id } => {
                let mint = self.mint.clone();
                self.off_thread(back, secret_id(0, secret_op_id), move || {
                    let secret = random_token();
                    let digest = digest_of(&secret);
                    let Some(identity) = crate::store::identity_of(&digest) else { return SessionInput::SecretStored { secret_op_id, ok: false } };
                    let _ = crate::lock::effect(|| mint.send((identity, digest, secret)));
                    SessionInput::CredentialGenerated { secret_op_id, credential: identity }
                });
            }
            SessionEffect::StoreSecret { credential, secret_op_id } => {
                let (secret, credentials) = (self.generated.remove(&credential), self.secret.slot());
                self.off_thread(back, secret_id(1, secret_op_id), move || {
                    let ok = secret.is_some_and(|secret| credentials.set(&secret).is_ok());
                    SessionInput::SecretStored { secret_op_id, ok }
                });
            }
            SessionEffect::ClearSecret { secret_op_id } => {
                let secret = self.secret.clone();
                self.off_thread(back, secret_id(2, secret_op_id), move || SessionInput::SecretCleared { secret_op_id, ok: secret.erase().is_ok() });
            }
            SessionEffect::ReadSecret { secret_op_id } => {
                let credentials = self.secret.slot();
                self.off_thread(back, secret_id(3, secret_op_id), move || {
                    let read = match credentials.get() {
                        Ok(Some(secret)) => crate::store::identity_of(&digest_of(&secret)).map_or(SecretState::Missing, SecretState::Present),
                        Ok(None) => SecretState::Missing,
                        Err(error) => SecretState::Error(error.to_string()),
                    };
                    SessionInput::SecretRead { secret_op_id, read }
                });
            }
            SessionEffect::CommitPending { batch, epoch, pending, session } => {
                let credential_digest = match self.digest_for(&pending) {
                    Ok(digest) => digest,
                    Err(error) => return back.reply(control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(error.to_string()) })),
                };
                let wire = Wire { id: uuid::Uuid::new_v4().to_string(), key: random_token(), cut: self.wiring.gate.cut() };
                let mut ops = vec![StoreOp::Pending { pending: pending.clone(), credential_digest: credential_digest.clone(), wire_id: wire.id.clone(), response_key: wire.key.clone() }];
                if session.is_some() { ops.push(StoreOp::Session { digest: Some(credential_digest), sequence: Some(0) }); }
                self.wires.clear();
                self.wires.insert(pending.request_id, wire);
                self.operations.clear();
                if let Some(crate::transport::RequestOwner::Operation { seq }) = pending.owner { self.operations.insert(pending.request_id, seq); }
                self.commit(batch, epoch, ops);
            }
            SessionEffect::CommitLanding { batch, epoch, digest, sequence, answer, erase_session } => {
                let digest = match digest.map(|identity| self.digests.get(&identity).cloned().ok_or(identity)).transpose() {
                    Ok(digest) => digest,
                    Err(identity) => return back.reply(control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(format!("no digest for the identity {identity}")) })),
                };
                let mut ops = if erase_session { vec![StoreOp::EraseSession, StoreOp::RemovePending] } else if digest.is_none() && sequence.is_none() { vec![StoreOp::RemovePending] } else { vec![StoreOp::Session { digest, sequence }, StoreOp::RemovePending] };
                ops.extend(answer.and_then(|(request_id, answer)| Some(StoreOp::Answer { request_id, operation: self.operations.get(&request_id).copied(), result: stored_answer(&answer)? })));
                self.commit(batch, epoch, ops);
            }
            SessionEffect::HttpRequest { pending, attempt_id } => {
                let request_id = pending.request_id;
                let Some(wire) = self.wires.get(&request_id) else {
                    return back.reply(control, SessionInput::SecretMissing { request_id, attempt_id, lost: true });
                };
                let (wire, key, cut) = (wire.id.clone(), wire.key.clone(), wire.cut);
                let expected = if pending.class == OperationClass::Sessionless { None } else {
                    match self.digest_for(&pending) {
                        Ok(digest) => Some(digest),
                        Err(_) => return back.reply(control, SessionInput::SecretMissing { request_id, attempt_id, lost: false }),
                    }
                };
                let (exchange, back, credentials) = (self.exchange.clone(), back.clone(), self.secret.slot());
                let logout = pending.class == OperationClass::Logout;
                let body: Value = serde_json::from_str(&pending.body).unwrap_or(Value::Null);
                let purpose = if pending.purpose == 0 { NetworkPurpose::Standard } else { NetworkPurpose::Transfer };
                let sequence = (pending.class != OperationClass::Sessionless).then_some(i64::try_from(pending.sequence).unwrap_or(i64::MAX));
                let watch = back.watch(crate::owners::session::http_id(attempt_id));
                let launch = self.wiring.handle.launch();
                let gate = self.wiring.gate.clone();
                let task = crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
                    let _watch = watch;
                    crate::lock::assert_no_library_lock("HttpExchange::post");
                    let credential = match secret_for(credentials.as_ref(), expected.as_deref()) {
                        Ok(secret) => secret,
                        Err(failure) => return back.reply(EpochStamp::Control, SessionInput::SecretMissing { request_id, attempt_id, lost: matches!(failure, SecretFailure::Lost) }),
                    };
                    let sent = crate::runtime::send_in_cut(&gate, cut, || exchange.send_once(crate::transport::encrypted::SendOnce { credential: credential.as_deref(), request_id: &wire, response_key: &key, sequence, path: &pending.path, body: &body, purpose, wire: crate::transport::WireIds::new(author_kind(&pending), request_id, attempt_id) })).await;
                    back.reply(EpochStamp::Control, match sent {
                        Ok(result) => input_of(request_id, attempt_id, logout, result),
                        Err(_) => SessionInput::GateClosed { request_id, attempt_id },
                    });
                }));
                if let Some(previous) = self.running.insert(attempt_id, task.abort_handle()) {
                    crate::lock::effect(|| previous.abort());
                }
            }
            SessionEffect::HttpCancel { attempt_id } => {
                if let Some(task) = self.running.remove(&attempt_id) {
                    crate::lock::effect(|| task.abort());
                }
            }
            SessionEffect::ScheduleTick { .. } | SessionEffect::RetryState { .. } => {}
            SessionEffect::CredentialCommitted(committed) => self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(committed)),
            SessionEffect::ServerEnded(ended) => self.wiring.to_lifecycle(LifecycleInput::ServerEnded(ended)),
            SessionEffect::SessionReset => self.wiring.to_lifecycle(LifecycleInput::SessionReset { reply: None }),
            SessionEffect::RestoreClassified { epoch } => self.wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Session, epoch }),
            SessionEffect::SnapshotRequest { epoch } => self.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Session, epoch }),
            SessionEffect::Reply { id, answer } => {
                let (id, reply) = (OutcomeId::from_raw(id as ReplyId), LifeReply::Session(answer));
                self.wiring.outcomes.settle(id, reply);
            }
            SessionEffect::ResumeReply { id, answer } => {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::SessionResume(answer));
            }
            SessionEffect::ExecutorFailed => self.wiring.to_lifecycle(LifecycleInput::ExecutorFailed { executor: "session".into() }),
            SessionEffect::Answer { author, req_id, outcome } => {
                self.wiring.answer_author(author, req_id, outcome);
            }
            SessionEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}

fn author_kind(pending: &crate::owners::session::PendingCall) -> crate::transport::Author {
    use crate::owners::session::{CALLS_AUTHOR, OUTBOX_AUTHOR, TRANSFER_AUTHOR};
    use crate::transport::{Author, RequestOwner};
    match (&pending.owner, pending.author) {
        (Some(RequestOwner::Transfer { .. }), _) | (_, Some(TRANSFER_AUTHOR)) => Author::Transfer,
        (Some(RequestOwner::Operation { .. } | RequestOwner::Unmatched { .. }), _) | (_, Some(OUTBOX_AUTHOR)) => Author::Outbox,
        (_, Some(CALLS_AUTHOR)) => Author::Calls,
        _ => Author::Session,
    }
}
