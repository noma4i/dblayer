//! Owner `SessionCalls` (`specs/machines/lifecycle.md`, section "Session owner"): the credential, `sequence`, `encrypted_session` and `encrypted_pending`.

use std::collections::VecDeque;

use super::contract::{BatchId, Committed, CredentialCommitted, Drained, EpochEnded, Online, Published, RejectReason, Rejected, RequestId, ServerEnded};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

use super::lifecycle::ReplyId;
pub type Digest = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    Invalid,
    Rejected(RejectReason),
    Revoked,
    SequenceConflict,
    Unauthorized,
    Http,
    Interrupted,
    NoSession,
    Closed,
}

pub type SessionAnswer = Result<String, SessionError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCall {
    pub request_id: RequestId,
    pub sequence: u64,
    pub operation: String,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Persisting { pending: PendingCall, batch: BatchId },
    Calling { pending: PendingCall, attempts: u64, sent: bool },
    Landing { pending: PendingCall, batch: BatchId, answer: String, identity: Option<Digest> },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEffect {
    /// Store `Commit{encrypted_pending}`.
    CommitPending { batch: BatchId, request_id: RequestId, sequence: u64 },
    /// Store `Commit{encrypted_session{digest or the previous one, sequence}, remove pending}`.
    CommitLanding { batch: BatchId, digest: Option<Digest>, sequence: u64 },
    /// Store `Commit{remove pending}`.
    RemovePending { batch: BatchId },
    HttpRequest { request_id: RequestId },
    HttpCancel { request_id: RequestId },
    /// `Schedule Tick` by the Backoff of Http for the attempt number `attempts`.
    ScheduleTick { attempts: u64 },
    CredentialCommitted(CredentialCommitted),
    ServerEnded(ServerEnded),
    Reply { id: ReplyId, answer: SessionAnswer },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum SessionInput {
    Open { credential: Option<Credential>, pending: Option<PendingCall> },
    SessionCall { operation: String, reply: ReplyId },
    Response { request_id: RequestId, body: String, sequence: Option<u64>, credential: Option<Digest>, revoked: bool },
    HttpError { request_id: RequestId, temporary: bool, auth: bool },
    Committed(Committed),
    Rejected(Rejected),
    Tick(Time),
    Online(Online),
    Published(Published),
    EpochEnded(EpochEnded),
    Closing,
}

pub struct SessionState {
    phase: SessionPhase,
    credential: Option<Credential>,
    queue: VecDeque<(String, ReplyId)>,
    online: bool,
    epoch: Epoch,
    call_epoch: Epoch,
    declared: Vec<String>,
    next_request: RequestId,
    next_batch: BatchId,
}

impl SessionState {
    pub fn new(declared: &[&str], online: bool) -> Self {
        Self { phase: SessionPhase::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, call_epoch: 0, declared: declared.iter().map(|name| name.to_string()).collect(), next_request: 1, next_batch: 1 }
    }

    pub fn phase(&self) -> &SessionPhase { &self.phase }

    pub fn credential(&self) -> Option<&Credential> { self.credential.as_ref() }

    pub fn queued(&self) -> usize { self.queue.len() }

    fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    /// `Idle`, then the next call of the queue as `Idle x SessionCall`.
    fn idle(&mut self, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if let Some((operation, reply)) = self.queue.pop_front() {
            self.session_call(operation, reply, fx);
        }
    }

    fn session_call(&mut self, operation: String, reply: ReplyId, fx: &mut Vec<SessionEffect>) {
        if !self.declared.contains(&operation) {
            fx.push(SessionEffect::Reply { id: reply, answer: Err(SessionError::Invalid) });
            self.idle(fx);
            return;
        }
        let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
        let request_id = self.next_request;
        self.next_request += 1;
        let batch = self.batch();
        self.call_epoch = self.epoch;
        fx.push(SessionEffect::CommitPending { batch, request_id, sequence });
        self.phase = SessionPhase::Persisting { pending: PendingCall { request_id, sequence, operation, reply: Some(reply) }, batch };
    }
}

fn reply(fx: &mut Vec<SessionEffect>, id: Option<ReplyId>, answer: SessionAnswer) { fx.extend(id.map(|id| SessionEffect::Reply { id, answer })); }

fn pending_of(phase: &SessionPhase) -> Option<PendingCall> {
    match phase {
        SessionPhase::Persisting { pending, .. } | SessionPhase::Calling { pending, .. } | SessionPhase::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}

pub struct SessionCalls;

impl StateOwner for SessionCalls {
    const EPOCHED: bool = false;

    type State = SessionState;
    type Input = SessionInput;
    type Effect = SessionEffect;

    fn step(state: &mut SessionState, input: SessionInput) -> Effects<Self> {
        let mut fx = Vec::new();
        state.run(input, &mut fx);
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(now: Time) -> SessionInput { SessionInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> SessionInput { SessionInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> SessionInput { SessionInput::Closing }
}

impl SessionState {
    /// One row of the table per match arm, in table order.
    fn run(&mut self, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let phase = std::mem::replace(&mut self.phase, SessionPhase::Closed);
        match (phase, input) {
            (SessionPhase::Closed, SessionInput::SessionCall { reply: id, .. }) => fx.push(SessionEffect::Reply { id, answer: Err(SessionError::Closed) }),
            (SessionPhase::Closed, _) => {}
            (phase, SessionInput::Closing) => {
                if let Some(pending) = pending_of(&phase) {
                    reply(fx, pending.reply, Err(SessionError::Closed));
                    fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                }
                fx.extend(self.queue.drain(..).map(|(_, id)| SessionEffect::Reply { id, answer: Err(SessionError::Closed) }));
                fx.push(SessionEffect::Drained(Drained { owner: "session" }));
            }
            (phase, SessionInput::Online(Online(online))) => {
                self.online = online;
                self.phase = phase;
                if online {
                    if let SessionPhase::Calling { pending, sent: sent @ false, .. } = &mut self.phase {
                        *sent = true;
                        fx.push(SessionEffect::HttpRequest { request_id: pending.request_id });
                    }
                }
            }
            (phase, SessionInput::Published(published)) => {
                self.epoch = published.epoch;
                self.phase = phase;
            }
            (SessionPhase::Idle, SessionInput::Open { credential, pending }) => {
                self.credential = credential;
                self.phase = SessionPhase::Idle;
                if let Some(pending) = pending {
                    self.next_request = self.next_request.max(pending.request_id + 1);
                    if self.online {
                        fx.push(SessionEffect::HttpRequest { request_id: pending.request_id });
                    }
                    self.phase = SessionPhase::Calling { pending: PendingCall { reply: None, ..pending }, attempts: 0, sent: self.online };
                }
            }
            (phase, SessionInput::Open { .. }) => self.phase = phase,
            (SessionPhase::Idle, SessionInput::SessionCall { operation, reply }) => self.session_call(operation, reply, fx),
            (phase, SessionInput::SessionCall { operation, reply }) => {
                self.phase = phase;
                self.queue.push_back((operation, reply));
            }
            (SessionPhase::Idle, SessionInput::EpochEnded(EpochEnded { cause, .. })) => {
                if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                    self.credential = None;
                }
                self.phase = SessionPhase::Idle;
            }
            (phase, SessionInput::EpochEnded(EpochEnded { epoch, cause })) => {
                if epoch == self.call_epoch && matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                    let Some(pending) = pending_of(&phase) else {
                        self.phase = phase;
                        return;
                    };
                    reply(fx, pending.reply, Err(SessionError::Interrupted));
                    fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                    fx.extend(self.queue.drain(..).map(|(_, id)| SessionEffect::Reply { id, answer: Err(SessionError::NoSession) }));
                    self.credential = None;
                    self.phase = SessionPhase::Idle;
                } else {
                    self.phase = phase;
                }
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                let sent = self.online;
                if sent {
                    fx.push(SessionEffect::HttpRequest { request_id: pending.request_id });
                }
                self.phase = SessionPhase::Calling { pending, attempts: 0, sent };
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                reply(fx, pending.reply, Err(SessionError::Rejected(reason)));
                self.idle(fx);
            }
            (SessionPhase::Calling { pending, .. }, SessionInput::Response { request_id, body, sequence, credential, revoked }) if request_id == pending.request_id => {
                if revoked {
                    reply(fx, pending.reply, Err(SessionError::Revoked));
                    let batch = self.batch();
                    fx.push(SessionEffect::RemovePending { batch });
                    fx.push(SessionEffect::ServerEnded(ServerEnded { epoch: self.call_epoch }));
                    self.idle(fx);
                } else if sequence == Some(pending.sequence) {
                    let batch = self.batch();
                    fx.push(SessionEffect::CommitLanding { batch, digest: credential, sequence: pending.sequence });
                    self.phase = SessionPhase::Landing { pending, batch, answer: body, identity: credential };
                } else {
                    reply(fx, pending.reply, Err(SessionError::SequenceConflict));
                    self.idle(fx);
                }
            }
            (SessionPhase::Calling { pending, attempts, .. }, SessionInput::HttpError { request_id, temporary, auth }) if request_id == pending.request_id => {
                if temporary && self.online {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, sent: false };
                } else if temporary {
                    self.phase = SessionPhase::Calling { pending, attempts, sent: false };
                } else {
                    let batch = self.batch();
                    fx.push(SessionEffect::RemovePending { batch });
                    reply(fx, pending.reply, Err(if auth { SessionError::Unauthorized } else { SessionError::Http }));
                    self.idle(fx);
                }
            }
            (SessionPhase::Calling { pending, attempts, .. }, SessionInput::Tick(_)) => {
                let sent = self.online;
                if sent {
                    fx.push(SessionEffect::HttpRequest { request_id: pending.request_id });
                }
                self.phase = SessionPhase::Calling { pending, attempts, sent };
            }
            (SessionPhase::Landing { pending, batch, answer, identity }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                match identity {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        fx.push(SessionEffect::CredentialCommitted(CredentialCommitted { identity: digest }));
                    }
                    None => {
                        if let Some(credential) = &mut self.credential {
                            credential.sequence = pending.sequence;
                        }
                    }
                }
                reply(fx, pending.reply, Ok(answer));
                self.idle(fx);
            }
            (SessionPhase::Landing { pending, batch, .. }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                reply(fx, pending.reply, Err(SessionError::Rejected(reason)));
                self.idle(fx);
            }
            (phase, _) => self.phase = phase,
        }
    }
}
