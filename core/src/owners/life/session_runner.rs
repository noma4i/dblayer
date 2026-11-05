//! Executor of the effects of Session (`design.md` F3): the HTTP exchange of `EncryptedClient` (encoding only), the random credential
//! and the writes of `encrypted_*` that go to Store as batches by value. It does IO and returns the result as an input; it takes no transition.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Wiring};
use crate::owners::contract::{BatchAuthor, ReplyId, RequestId};
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::session::{Credential, PendingCall, SessionCalls, SessionEffect, SessionInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::encrypted::client::{digest_of, permanently_refused, temporary_error};
use crate::transport::encrypted::jwe::random_token;
use crate::transport::encrypted::{CredentialSlot, EncryptedClient};
use crate::transport::{NetworkPurpose, TransportError};

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
}

impl SessionRunner {
    pub fn new(wiring: Arc<Wiring>, client: Arc<EncryptedClient>, credentials: Arc<dyn CredentialSlot>, executor: tokio::runtime::Handle) -> Self {
        Self { wiring, client, credentials, executor, running: HashMap::new(), minted: HashMap::new(), keys: HashMap::new() }
    }

    /// What `Open` of the owner takes: the stored `encrypted_session` (signed in when its sequence is above 0) and the stored `encrypted_pending`.
    pub fn stored(client: &EncryptedClient) -> Result<(Option<Credential>, Option<PendingCall>), TransportError> {
        let credential = client.stored_session()?.filter(|session| session.sequence > 0).and_then(|session| crate::store::identity_of(&session.credential_digest).map(|digest| Credential { digest, sequence: u64::try_from(session.sequence).unwrap_or(0) }));
        let pending = client.stored_pending()?.and_then(|row| {
            let request_id = row.request_id.parse().ok()?;
            Some(PendingCall {
                request_id,
                sequence: u64::try_from(row.sequence).ok()?,
                operation: String::new(),
                path: row.path,
                body: row.body,
                sessionless: row.credential_digest.is_empty(),
                credential: None,
                purpose: u8::from(row.purpose == NetworkPurpose::Transfer),
                author: None,
                reply: None,
            })
        });
        Ok((credential, pending))
    }

    fn commit(&self, back: &Back<SessionCalls>, batch: u64, epoch: u64, ops: Vec<StoreOp>) {
        let _ = back;
        self.wiring.to_store(StoreInput::Commit(StoreBatch { author: BatchAuthor::Session, id: batch, epoch, basis: Default::default(), rows: Vec::new(), intents: 1, credential: None, ops }));
    }

    fn digest_for(&self, pending: &PendingCall) -> String {
        if pending.sessionless {
            return String::new();
        }
        match pending.credential.and_then(|digest| self.minted.get(&digest)) {
            Some(minted) => minted.digest.clone(),
            None => self.client.stored_session().ok().flatten().map(|session| session.credential_digest).unwrap_or_default(),
        }
    }

    fn secret_for(&self, pending: &PendingCall, stored_key: Option<String>) -> Result<(Option<String>, String), TransportError> {
        let key = self.keys.get(&pending.request_id).cloned().or(stored_key).unwrap_or_else(random_token);
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

fn input_of(request_id: RequestId, logout: &str, path: &str, result: Result<crate::transport::encrypted::Answer, TransportError>) -> SessionInput {
    match result {
        Ok(answer) => {
            if let Some(error) = temporary_error(&answer.body) {
                let _ = error;
                return SessionInput::HttpError { request_id, temporary: true, auth: false };
            }
            let revoked = path == logout && answer.body.get("revoked") == Some(&Value::Bool(true));
            SessionInput::Response { request_id, body: answer.body.to_string(), sequence: answer.sequence.and_then(|sequence| u64::try_from(sequence).ok()), credential: None, revoked }
        }
        Err(TransportError::Http { status: 401 | 403, .. }) => SessionInput::HttpError { request_id, temporary: false, auth: true },
        Err(error) if permanently_refused(&error) => SessionInput::HttpError { request_id, temporary: false, auth: false },
        Err(TransportError::Interrupted(_)) => SessionInput::Dropped { request_id },
        Err(_) => SessionInput::HttpError { request_id, temporary: true, auth: false },
    }
}

impl EffectRunner<SessionCalls> for SessionRunner {
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
                let response_key = self.keys.entry(pending.request_id).or_insert_with(random_token).clone();
                let credential_digest = self.digest_for(&pending);
                self.commit(back, batch, epoch, vec![StoreOp::PutPending { pending, credential_digest, response_key }]);
            }
            SessionEffect::CommitLanding { batch, epoch, digest, sequence } => {
                let digest = digest.and_then(|identity| self.minted.get(&identity)).map(|minted| minted.digest.clone());
                self.commit(back, batch, epoch, vec![StoreOp::PutSession { digest, sequence }, StoreOp::RemovePending]);
            }
            SessionEffect::RemovePending { batch, epoch } => self.commit(back, batch, epoch, vec![StoreOp::RemovePending]),
            SessionEffect::HttpRequest { pending } => {
                let stored_key = self.client.stored_pending().ok().flatten().filter(|row| row.request_id == pending.request_id.to_string()).map(|row| row.response_key);
                let (credential, key) = match self.secret_for(&pending, stored_key) {
                    Ok(secret) => secret,
                    Err(_) => return back.reply(EpochStamp::Control, SessionInput::HttpError { request_id: pending.request_id, temporary: true, auth: false }),
                };
                let (client, back) = (self.client.clone(), back.clone());
                let request_id = pending.request_id;
                let body: Value = serde_json::from_str(&pending.body).unwrap_or(Value::Null);
                let purpose = if pending.purpose == 0 { NetworkPurpose::Standard } else { NetworkPurpose::Transfer };
                let sequence = (!pending.sessionless).then_some(i64::try_from(pending.sequence).unwrap_or(i64::MAX));
                let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, async move {
                    crate::db::lock::assert_no_library_lock("HttpExchange::post");
                    let result = client.send_once(credential.as_deref(), &request_id.to_string(), &key, sequence, &pending.path, &body, purpose).await;
                    let input = input_of(request_id, client.logout_path(), &pending.path, result);
                    back.reply(EpochStamp::Control, input);
                });
                if let Some(previous) = self.running.insert(request_id, task.abort_handle()) {
                    crate::db::lock::effect(|| previous.abort());
                }
            }
            SessionEffect::HttpCancel { request_id } => {
                self.keys.remove(&request_id);
                if let Some(task) = self.running.remove(&request_id) {
                    crate::db::lock::effect(|| task.abort());
                }
            }
            SessionEffect::ScheduleTick { .. } => {}
            SessionEffect::CredentialCommitted(committed) => self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(committed)),
            SessionEffect::ServerEnded(ended) => self.wiring.to_lifecycle(LifecycleInput::ServerEnded(ended)),
            SessionEffect::Reply { id, answer } => {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id as ReplyId), LifeReply::Session(answer));
            }
            SessionEffect::Answer { author, req_id, outcome } => self.wiring.answer_author(author, req_id, outcome),
            SessionEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}
