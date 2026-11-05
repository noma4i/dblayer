//! Executor of the effects of Session (`design.md` F3): the HTTP exchange of `EncryptedClient` (encoding only), the random credential
//! and the writes of `encrypted_*` that go to Store as batches by value. It does IO and returns the result as an input; it takes no transition.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Wiring};
use crate::owners::contract::{BatchAuthor, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::session::{Credential, PendingCall, SessionCalls, SessionEffect, SessionInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::encrypted::client::{digest_of, permanently_refused, temporary_error};
use crate::transport::encrypted::jwe::random_token;
use crate::transport::encrypted::{CredentialSlot, EncryptedClient};
use crate::transport::{NetworkPurpose, TransportError};

pub(crate) const RESTORED_REQUEST: RequestId = 1 << 62;

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
    running: HashMap<RequestId, AbortHandle>,
    digests: HashMap<u64, String>,
    wires: HashMap<RequestId, Wire>,
    /// The identity that the last step committed: the `Reply` of the same step waits for its publication.
    committing: Option<u64>,
    /// The epoch that the last step ended on the server: the `Reply` of the same step waits for the next epoch.
    ending: Option<u64>,
}

/// What the stored rows give to the owner and its executor at open.
pub(crate) struct Restored {
    pub credential: Option<Credential>,
    pub pending: Option<PendingCall>,
    wire: Option<Wire>,
    digest: Option<(u64, String)>,
}

impl SessionRunner {
    pub fn new(wiring: Arc<Wiring>, client: Arc<EncryptedClient>, credentials: Arc<dyn CredentialSlot>, executor: tokio::runtime::Handle, restored: &Restored) -> Self {
        let wires = restored.wire.iter().map(|wire| (RESTORED_REQUEST, Wire { id: wire.id.clone(), key: wire.key.clone() })).collect();
        let digests = restored.digest.iter().cloned().collect();
        Self { wiring, client, credentials, executor, running: HashMap::new(), digests, wires, committing: None, ending: None }
    }

    /// What `Open` of the owner takes: the stored `encrypted_session` (signed in when its sequence is above 0) and the stored `encrypted_pending`.
    pub fn stored(client: &EncryptedClient) -> Result<Restored, TransportError> {
        let session = client.stored_session()?;
        let credential = session.as_ref().filter(|session| session.sequence > 0).and_then(|session| crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence: u64::try_from(session.sequence).unwrap_or(0) }));
        let operations = client.session_operations();
        let Some(row) = client.stored_pending()? else {
            if session.is_none() && matches!(client.credential_slot().get(), Ok(Some(_))) {
                client.erase_credential()?;
            }
            return Ok(Restored { credential, pending: None, wire: None, digest: None });
        };
        let sequence = u64::try_from(row.sequence).map_err(|_| TransportError::Decode(format!("the stored pending has the sequence {}", row.sequence)))?;
        let sessionless = row.credential_digest.is_empty();
        let first_sign_in = credential.is_none() && !sessionless;
        let identity = first_sign_in.then(|| crate::store::identity_of(&row.credential_digest)).flatten();
        let operation = operations.iter().find(|def| def.path == row.path && row.owner.is_none()).map_or_else(String::new, |def| def.name.clone());
        let pending = PendingCall {
            request_id: RESTORED_REQUEST,
            sequence,
            operation,
            path: row.path,
            body: row.body,
            sessionless,
            credential: identity,
            purpose: u8::from(row.purpose == NetworkPurpose::Transfer),
            author: row.owner.as_ref().map(crate::owners::session::author_of),
            owner: row.owner,
            reply: None,
        };
        let digest = identity.map(|identity| (identity, row.credential_digest));
        Ok(Restored { credential, pending: Some(pending), wire: Some(Wire { id: row.request_id, key: row.response_key }), digest })
    }

    fn commit(&self, back: &Back<SessionCalls>, batch: u64, epoch: u64, ops: Vec<StoreOp>) {
        let _ = back;
        self.wiring.to_store(StoreInput::Commit(StoreBatch { author: BatchAuthor::Session, id: batch, epoch, basis: Default::default(), rows: Vec::new(), intents: 1, credential: None, ops }));
    }

    fn digest_for(&self, pending: &PendingCall) -> Result<String, TransportError> {
        if pending.sessionless {
            return Ok(String::new());
        }
        match pending.credential {
            Some(identity) => self.digests.get(&identity).cloned().ok_or_else(|| TransportError::Decode(format!("no digest for the identity {identity}"))),
            None => Ok(self.client.stored_session()?.map_or_else(String::new, |session| session.credential_digest)),
        }
    }

    fn secret_for(&self, pending: &PendingCall) -> Result<Option<String>, SecretFailure> {
        if pending.sessionless {
            return Ok(None);
        }
        let expected = self.digest_for(pending).map_err(|error| SecretFailure::Store(error.to_string()))?;
        match crate::blocking::blocking_unit(|| self.credentials.get()).map_err(|error| SecretFailure::Store(error.to_string()))? {
            Some(secret) if digest_of(&secret) == expected => Ok(Some(secret)),
            _ => Err(SecretFailure::Lost),
        }
    }
}

pub(super) fn input_of(request_id: RequestId, logout: bool, result: Result<crate::transport::encrypted::Answer, TransportError>) -> SessionInput {
    match result {
        Ok(answer) => {
            if let Some(error) = temporary_error(&answer.body) {
                let _ = error;
                return SessionInput::HttpError { request_id, temporary: true, auth: false };
            }
            if logout && answer.body.get("revoked") == Some(&Value::Bool(true)) {
                return SessionInput::Revoked { request_id, body: answer.body.to_string() };
            }
            SessionInput::Response { request_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), credential: None }
        }
        Err(TransportError::Http { status: 401 | 403, .. }) => SessionInput::HttpError { request_id, temporary: false, auth: true },
        Err(error) if permanently_refused(&error) || matches!(error, TransportError::RequestTooLarge { .. } | TransportError::Decode(_)) => SessionInput::HttpError { request_id, temporary: false, auth: false },
        Err(TransportError::Interrupted(_)) => SessionInput::Dropped { request_id },
        Err(_) => SessionInput::HttpError { request_id, temporary: true, auth: false },
    }
}

impl EffectRunner<SessionCalls> for SessionRunner {
    fn step_done(&mut self) {
        self.committing = None;
        self.ending = None;
    }

    fn run(&mut self, _stamp: EpochStamp, effect: SessionEffect, back: &Back<SessionCalls>) {
        match effect {
            SessionEffect::GenerateCredential => {
                let credential = random_token();
                if let Err(error) = crate::blocking::blocking_unit(|| self.credentials.set(&credential)) {
                    return back.reply(EpochStamp::Control, SessionInput::CredentialFailed { detail: error.to_string() });
                }
                let digest = digest_of(&credential);
                let Some(identity) = crate::store::identity_of(&digest) else {
                    return back.reply(EpochStamp::Control, SessionInput::CredentialFailed { detail: "the digest of the credential has no identity".into() });
                };
                self.digests.insert(identity, digest);
                back.reply(EpochStamp::Control, SessionInput::CredentialGenerated { credential: identity });
            }
            SessionEffect::CommitPending { batch, epoch, pending } => {
                let credential_digest = match self.digest_for(&pending) {
                    Ok(digest) => digest,
                    Err(error) => return back.reply(EpochStamp::Control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(error.to_string()) })),
                };
                let wire = Wire { id: uuid::Uuid::new_v4().to_string(), key: random_token() };
                let op = StoreOp::Pending { pending: pending.clone(), credential_digest, wire_id: wire.id.clone(), response_key: wire.key.clone() };
                self.wires.clear();
                self.wires.insert(pending.request_id, wire);
                self.commit(back, batch, epoch, vec![op]);
            }
            SessionEffect::CommitLanding { batch, epoch, digest, sequence } => {
                let digest = match digest.map(|identity| self.digests.get(&identity).cloned().ok_or(identity)).transpose() {
                    Ok(digest) => digest,
                    Err(identity) => return back.reply(EpochStamp::Control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(format!("no digest for the identity {identity}")) })),
                };
                let ops = if digest.is_none() && sequence.is_none() { vec![StoreOp::RemovePending] } else { vec![StoreOp::Session { digest, sequence }, StoreOp::RemovePending] };
                self.commit(back, batch, epoch, ops);
            }
            SessionEffect::RemovePending { batch, epoch } => {
                self.commit(back, batch, epoch, vec![StoreOp::RemovePending]);
            }
            SessionEffect::HttpRequest { pending } => {
                let request_id = pending.request_id;
                let Some(wire) = self.wires.get(&request_id) else {
                    return back.reply(EpochStamp::Control, SessionInput::CredentialUnavailable { request_id, lost: true, detail: format!("no wire id for the request {request_id}") });
                };
                let (wire, key) = (wire.id.clone(), wire.key.clone());
                let credential = match self.secret_for(&pending) {
                    Ok(secret) => secret,
                    Err(failure) => {
                        let (lost, detail) = match failure {
                            SecretFailure::Lost => (true, String::new()),
                            SecretFailure::Store(detail) => (false, detail),
                        };
                        return back.reply(EpochStamp::Control, SessionInput::CredentialUnavailable { request_id, lost, detail });
                    }
                };
                let (client, back) = (self.client.clone(), back.clone());
                let logout = pending.path == client.logout_path();
                let body: Value = serde_json::from_str(&pending.body).unwrap_or(Value::Null);
                let purpose = if pending.purpose == 0 { NetworkPurpose::Standard } else { NetworkPurpose::Transfer };
                let sequence = (!pending.sessionless).then_some(i64::try_from(pending.sequence).unwrap_or(i64::MAX));
                let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, async move {
                    crate::db::lock::assert_no_library_lock("HttpExchange::post");
                    let result = client.send_once(credential.as_deref(), &wire, &key, sequence, &pending.path, &body, purpose).await;
                    back.reply(EpochStamp::Control, input_of(request_id, logout, result));
                });
                if let Some(previous) = self.running.insert(request_id, task.abort_handle()) {
                    crate::db::lock::effect(|| previous.abort());
                }
            }
            SessionEffect::HttpCancel { request_id } => {
                if let Some(task) = self.running.remove(&request_id) {
                    crate::db::lock::effect(|| task.abort());
                }
            }
            SessionEffect::ScheduleTick { .. } => {}
            SessionEffect::CredentialCommitted(committed) => {
                self.committing = Some(committed.identity);
                self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(committed));
            }
            SessionEffect::ServerEnded(ended) => {
                self.ending = Some(ended.epoch);
                self.wiring.to_lifecycle(LifecycleInput::ServerEnded(ended));
            }
            SessionEffect::Reply { id, answer } => {
                let (id, reply) = (OutcomeId::from_raw(id as ReplyId), LifeReply::Session(answer));
                match (self.committing.take(), self.ending.take()) {
                    (Some(identity), _) => self.wiring.settle_after_publish(identity, id, reply),
                    (None, Some(epoch)) => self.wiring.settle_after_epoch(epoch, id, reply),
                    (None, None) => { self.wiring.outcomes.settle(id, reply); }
                }
            }
            SessionEffect::Answer { author, req_id, outcome } => self.wiring.answer_author(author, req_id, outcome),
            SessionEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}
