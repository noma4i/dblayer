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

/// The request id of the owner as the wire carries it: a UUID of version 4 that holds the 64 bits of the id.
pub(crate) fn wire_request_id(id: RequestId) -> String {
    format!("{:08x}-0000-4000-8000-{:012x}", id >> 48, id & 0xffff_ffff_ffff)
}

/// The request id of the owner that a wire id holds.
pub(crate) fn request_id_of_wire(wire: &str) -> Option<RequestId> {
    let (high, rest) = wire.split_once('-')?;
    let node = rest.rsplit('-').next()?;
    Some((u64::from_str_radix(high, 16).ok()? << 48) | u64::from_str_radix(node, 16).ok()?)
}

/// The request id of the owner for a stored wire id: the id it holds, `None` when `wire_request_id` did not make the wire id.
pub(crate) fn own_request_id(wire: &str) -> Option<RequestId> { request_id_of_wire(wire).filter(|id| wire_request_id(*id) == wire) }

/// The owner id of a stored pending whose wire id was not made by `wire_request_id`: the wire id stays as stored.
pub(crate) const RESTORED_REQUEST: RequestId = 1 << 62;

/// A credential the executor generated: the secret, its full digest and the 8 bytes the owners compare.
struct Minted {
    credential: String,
    digest: String,
}

pub(super) struct SessionRunner {
    wiring: Arc<Wiring>,
    client: Arc<EncryptedClient>,
    credentials: Arc<dyn CredentialSlot>,
    executor: tokio::runtime::Handle,
    running: HashMap<RequestId, AbortHandle>,
    minted: HashMap<u64, Minted>,
    keys: HashMap<RequestId, String>,
    /// The identity that the last step committed: the `Reply` of the same step waits for its publication.
    committing: Option<u64>,
    /// The epoch that the last step ended on the server: the `Reply` of the same step waits for the next epoch.
    ending: Option<u64>,
    /// The wire id of a restored pending that this executor did not make.
    restored_wire: Option<String>,
}

/// What the stored rows give to the owner and its executor at open.
pub(crate) struct Restored {
    pub credential: Option<Credential>,
    pub pending: Option<PendingCall>,
    /// The wire id of a pending that keeps the id it was stored with.
    wire: Option<String>,
    /// The response key stored with the pending.
    key: Option<String>,
}

impl SessionRunner {
    pub fn new(wiring: Arc<Wiring>, client: Arc<EncryptedClient>, credentials: Arc<dyn CredentialSlot>, executor: tokio::runtime::Handle, restored: &Restored) -> Self {
        let keys = restored.pending.iter().zip(&restored.key).map(|(pending, key)| (pending.request_id, key.clone())).collect();
        Self { wiring, client, credentials, executor, running: HashMap::new(), minted: HashMap::new(), keys, committing: None, ending: None, restored_wire: restored.wire.clone() }
    }

    fn wire(&self, id: RequestId) -> String {
        match (&self.restored_wire, id == RESTORED_REQUEST) {
            (Some(wire), true) => wire.clone(),
            _ => wire_request_id(id),
        }
    }

    /// What `Open` of the owner takes: the stored `encrypted_session` (signed in when its sequence is above 0) and the stored `encrypted_pending`.
    pub fn stored(client: &EncryptedClient) -> Result<Restored, TransportError> {
        let credential = client.stored_session()?.filter(|session| session.sequence > 0).and_then(|session| crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence: u64::try_from(session.sequence).unwrap_or(0) }));
        let operations = client.session_operations();
        let Some(row) = client.stored_pending()? else { return Ok(Restored { credential, pending: None, wire: None, key: None }) };
        let sequence = u64::try_from(row.sequence).map_err(|_| TransportError::Decode(format!("the stored pending has the sequence {}", row.sequence)))?;
        let own = own_request_id(&row.request_id);
        let request_id = own.unwrap_or(RESTORED_REQUEST);
        let restored_wire = own.is_none().then(|| row.request_id.clone());
        let pending = {
            let sessionless = row.credential_digest.is_empty();
            let operation = operations.iter().find(|def| def.path == row.path && row.owner.is_none()).map_or_else(String::new, |def| def.name.clone());
            PendingCall {
                request_id,
                sequence,
                operation,
                path: row.path,
                body: row.body,
                sessionless,
                credential: None,
                purpose: u8::from(row.purpose == NetworkPurpose::Transfer),
                author: row.owner.as_ref().map(crate::owners::session::author_of),
                owner: row.owner,
                reply: None,
            }
        };
        Ok(Restored { credential, pending: Some(pending), wire: restored_wire, key: Some(row.response_key) })
    }

    fn commit(&self, back: &Back<SessionCalls>, batch: u64, epoch: u64, ops: Vec<StoreOp>) {
        let _ = back;
        self.wiring.to_store(StoreInput::Commit(StoreBatch { author: BatchAuthor::Session, id: batch, epoch, basis: Default::default(), rows: Vec::new(), intents: 1, credential: None, ops }));
    }

    fn digest_for(&self, pending: &PendingCall) -> Result<String, TransportError> {
        if pending.sessionless {
            return Ok(String::new());
        }
        match pending.credential.and_then(|digest| self.minted.get(&digest)) {
            Some(minted) => Ok(minted.digest.clone()),
            None => Ok(self.client.stored_session()?.map_or_else(String::new, |session| session.credential_digest)),
        }
    }

    fn secret_for(&self, pending: &PendingCall) -> Result<(Option<String>, String), TransportError> {
        let key = self.keys.get(&pending.request_id).cloned().ok_or_else(|| TransportError::Decode(format!("no response key for the request {}", pending.request_id)))?;
        if pending.sessionless {
            return Ok((None, key));
        }
        let credential = match pending.credential.and_then(|digest| self.minted.get(&digest)) {
            Some(minted) => minted.credential.clone(),
            None => crate::blocking::blocking_unit(|| self.credentials.get())?.ok_or_else(|| TransportError::Network("the credential is not stored".into()))?,
        };
        Ok((Some(credential), key))
    }
}

fn input_of(request_id: RequestId, result: Result<crate::transport::encrypted::Answer, TransportError>) -> SessionInput {
    match result {
        Ok(answer) => {
            if let Some(error) = temporary_error(&answer.body) {
                let _ = error;
                return SessionInput::HttpError { request_id, temporary: true, auth: false };
            }
            let revoked = answer.body.get("revoked") == Some(&Value::Bool(true));
            SessionInput::Response { request_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), credential: None, revoked }
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
                    self.wiring.fault(format!("credential set: {error}"));
                    return;
                }
                let digest = digest_of(&credential);
                let Some(identity) = crate::store::identity_of(&digest) else { return };
                self.minted.insert(identity, Minted { credential, digest });
                back.reply(EpochStamp::Control, SessionInput::CredentialGenerated { credential: identity });
            }
            SessionEffect::CommitPending { batch, epoch, pending } => {
                let credential_digest = match self.digest_for(&pending) {
                    Ok(digest) => digest,
                    Err(error) => return back.reply(EpochStamp::Control, SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(error.to_string()) })),
                };
                self.keys.clear();
                let response_key = random_token();
                self.keys.insert(pending.request_id, response_key.clone());
                self.commit(back, batch, epoch, vec![StoreOp::Pending { pending, credential_digest, response_key }]);
            }
            SessionEffect::CommitLanding { batch, epoch, digest, sequence } => {
                self.keys.clear();
                let digest = digest.and_then(|identity| self.minted.get(&identity)).map(|minted| minted.digest.clone());
                self.commit(back, batch, epoch, vec![StoreOp::Session { digest, sequence }, StoreOp::RemovePending]);
            }
            SessionEffect::RemovePending { batch, epoch } => {
                self.keys.clear();
                self.commit(back, batch, epoch, vec![StoreOp::RemovePending]);
            }
            SessionEffect::HttpRequest { pending } => {
                let wire = self.wire(pending.request_id);
                let (credential, key) = match self.secret_for(&pending) {
                    Ok(secret) => secret,
                    Err(error) => {
                        self.wiring.fault(format!("request {}: {error}", pending.request_id));
                        return back.reply(EpochStamp::Control, SessionInput::HttpError { request_id: pending.request_id, temporary: true, auth: false });
                    }
                };
                let (client, back) = (self.client.clone(), back.clone());
                let request_id = pending.request_id;
                let wire = wire.clone();
                let body: Value = serde_json::from_str(&pending.body).unwrap_or(Value::Null);
                let purpose = if pending.purpose == 0 { NetworkPurpose::Standard } else { NetworkPurpose::Transfer };
                let sequence = (!pending.sessionless).then_some(i64::try_from(pending.sequence).unwrap_or(i64::MAX));
                let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, async move {
                    crate::db::lock::assert_no_library_lock("HttpExchange::post");
                    let result = client.send_once(credential.as_deref(), &wire, &key, sequence, &pending.path, &body, purpose).await;
                    let input = input_of(request_id, result);
                    back.reply(EpochStamp::Control, input);
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
