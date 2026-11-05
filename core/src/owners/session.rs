//! Owner `SessionCalls` (`specs/machines/lifecycle.md`, section "Session owner"): the credential, `sequence`, `encrypted_session` and `encrypted_pending`.

use std::collections::VecDeque;

use super::contract::{BatchId, Committed, CredentialCommitted, Drained, EpochEnded, Online, Published, RejectReason, Rejected, RequestId, ServerEnded};
use std::time::Duration;

use crate::runtime::{Cause, Effect, Effects, Epoch, ScheduleId, StateOwner, Time};

use super::contract::ReplyId;
pub type Digest = u64;

/// The author of a `Mutate`: the address the answer `Transport{req_id, ..}` goes to.
pub type AuthorId = u64;

/// The address of Outbox: the author of a `Mutate` whose owner is an operation.
pub const OUTBOX_AUTHOR: AuthorId = 1;
/// The address of Transfer: the author of a `Mutate` whose owner is a transfer step.
pub const TRANSFER_AUTHOR: AuthorId = 2;

/// The owner of a `Mutate`: `encrypted_pending` keeps it, and a pending restored at open answers the author this owner names.
pub type MutateOwner = crate::transport::RequestOwner;

/// The author an owner names: Outbox for an operation, Transfer for a transfer step.
pub fn author_of(owner: &MutateOwner) -> AuthorId {
    match owner {
        MutateOwner::Operation { .. } | MutateOwner::Unmatched { .. } => OUTBOX_AUTHOR,
        MutateOwner::Transfer { .. } => TRANSFER_AUTHOR,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    Invalid,
    Rejected(RejectReason),
    Revoked,
    SequenceConflict,
    Protocol,
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

/// A session operation declared by the config: its path and whether it carries no session. A call that carries the session while `credential = None` is the sign-in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionOperation {
    pub name: String,
    pub path: String,
    pub sessionless: bool,
}

/// `pending` of the table: the row `encrypted_pending`, by value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCall {
    pub request_id: RequestId,
    pub sequence: u64,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub sessionless: bool,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<MutateOwner>,
    pub reply: Option<ReplyId>,
}

/// An entry of `queue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queued {
    Call { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, path: String, body: String, purpose: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Generating { call: Queued },
    Persisting { pending: PendingCall, batch: BatchId },
    Calling { pending: PendingCall, attempts: u64, sent: bool },
    Landing { pending: PendingCall, batch: BatchId, answer: String, identity: Option<Digest> },
    Closed,
}

/// What the author of a `Mutate` hears: `Transport{req_id, Response(body)}` or `Transport{req_id, Error{Permanent, X}}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorOutcome {
    Response(String),
    Error(SessionError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEffect {
    /// Store `Commit{encrypted_pending = pending}`.
    CommitPending { batch: BatchId, epoch: Epoch, pending: PendingCall },
    /// Store `Commit{encrypted_session{digest or the previous one, sequence}, remove pending}`; no `sequence` for a call without a session.
    CommitLanding { batch: BatchId, epoch: Epoch, digest: Option<Digest>, sequence: Option<u64> },
    /// Store `Commit{remove pending}`.
    RemovePending { batch: BatchId, epoch: Epoch },
    HttpRequest { pending: PendingCall },
    HttpCancel { request_id: RequestId },
    /// `Schedule Tick` by the Backoff of Http for the attempt number `attempts`. `step` turns it into the Runtime effect `Schedule` (R5).
    ScheduleTick { attempts: u64 },
    GenerateCredential,
    CredentialCommitted(CredentialCommitted),
    ServerEnded(ServerEnded),
    Reply { id: ReplyId, answer: SessionAnswer },
    Answer { author: AuthorId, req_id: RequestId, outcome: AuthorOutcome },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum SessionInput {
    Open { credential: Option<Credential>, pending: Option<PendingCall> },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, path: String, body: String, purpose: u8 },
    CredentialGenerated { credential: Digest },
    Response { request_id: RequestId, body: String, sequence: Option<u64>, credential: Option<Digest>, revoked: bool },
    HttpError { request_id: RequestId, temporary: bool, auth: bool },
    Dropped { request_id: RequestId },
    /// Outbox and Transfer take their request back: the one in the network stops and is repeated with the same `request_id`, the one in the queue is removed.
    Cancel { req_id: RequestId },
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
    queue: VecDeque<Queued>,
    online: bool,
    epoch: Epoch,
    call_epoch: Epoch,
    operations: Vec<SessionOperation>,
    next_request: RequestId,
    next_batch: BatchId,
    /// A pending restored at open waits for the first `Published`: it belongs to that epoch.
    restored: bool,
}

impl SessionState {
    pub fn new(declared: &[&str], online: bool) -> Self {
        let operations = declared.iter().map(|name| SessionOperation { name: name.to_string(), path: format!("/{name}"), sessionless: false }).collect();
        Self::with_operations(operations, online)
    }

    pub fn with_operations(operations: Vec<SessionOperation>, online: bool) -> Self {
        Self { phase: SessionPhase::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, call_epoch: 0, operations, next_request: 1, next_batch: 1, restored: false }
    }

    pub fn phase(&self) -> &SessionPhase { &self.phase }

    pub fn credential(&self) -> Option<&Credential> { self.credential.as_ref() }

    pub fn queued(&self) -> usize { self.queue.len() }

    fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    fn request(&mut self) -> RequestId {
        self.next_request += 1;
        self.next_request - 1
    }

    /// `Idle`, then the next queued entry as `Idle x` its input.
    fn idle(&mut self, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if let Some(queued) = self.queue.pop_front() {
            self.begin(queued, fx);
        }
    }

    fn begin(&mut self, queued: Queued, fx: &mut Vec<SessionEffect>) {
        match queued {
            Queued::Call { operation, body, reply } => {
                let Some(def) = self.operations.iter().find(|def| def.name == operation).cloned() else {
                    fx.push(SessionEffect::Reply { id: reply, answer: Err(SessionError::Invalid) });
                    self.idle(fx);
                    return;
                };
                self.call_epoch = self.epoch;
                if !def.sessionless && self.credential.is_none() {
                    fx.push(SessionEffect::GenerateCredential);
                    self.phase = SessionPhase::Generating { call: Queued::Call { operation, body, reply } };
                    return;
                }
                let sequence = if def.sessionless { 0 } else { self.credential.as_ref().map_or(1, |credential| credential.sequence + 1) };
                let request_id = self.request();
                let pending = PendingCall { request_id, sequence, operation, path: def.path, body, sessionless: def.sessionless, credential: None, purpose: 0, author: None, owner: None, reply: Some(reply) };
                self.persist(pending, fx);
            }
            Queued::Mutate { author, owner, req_id, path, body, purpose } => {
                let Some(credential) = self.credential.clone() else {
                    fx.push(SessionEffect::Answer { author, req_id, outcome: AuthorOutcome::Error(SessionError::NoSession) });
                    self.idle(fx);
                    return;
                };
                self.call_epoch = self.epoch;
                let pending = PendingCall { request_id: req_id, sequence: credential.sequence + 1, operation: String::new(), path, body, sessionless: false, credential: None, purpose, author: Some(author), owner, reply: None };
                self.persist(pending, fx);
            }
        }
    }

    fn persist(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        fx.push(SessionEffect::CommitPending { batch, epoch: self.call_epoch, pending: pending.clone() });
        self.phase = SessionPhase::Persisting { pending, batch };
    }
}

/// The result of the pending request goes to its call (`reply`) and to its `Mutate` author.
fn finish(fx: &mut Vec<SessionEffect>, pending: &PendingCall, result: SessionAnswer) {
    if let Some(id) = pending.reply {
        fx.push(SessionEffect::Reply { id, answer: result.clone() });
    }
    if let Some(author) = pending.author {
        let outcome = match result {
            Ok(body) => AuthorOutcome::Response(body),
            Err(error) => AuthorOutcome::Error(error),
        };
        fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome });
    }
}

fn refuse(fx: &mut Vec<SessionEffect>, queued: Queued, error: SessionError) {
    match queued {
        Queued::Call { reply, .. } => fx.push(SessionEffect::Reply { id: reply, answer: Err(error) }),
        Queued::Mutate { author, req_id, .. } => fx.push(SessionEffect::Answer { author, req_id, outcome: AuthorOutcome::Error(error) }),
    }
}

fn pending_of(phase: &SessionPhase) -> Option<PendingCall> {
    match phase {
        SessionPhase::Persisting { pending, .. } | SessionPhase::Calling { pending, .. } | SessionPhase::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}

/// The only retry timer of Session: a new `Schedule` replaces the previous one.
const RETRY_TICK: ScheduleId = ScheduleId(1);

/// Backoff of Http for the attempt number `attempts` (>= 1).
fn http_delay(attempts: u64) -> Duration {
    crate::pacing::RetryPolicy::Http.backoff().delay(u32::try_from(attempts.saturating_sub(1)).unwrap_or(u32::MAX))
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
        fx.into_iter().map(|effect| match effect {
            SessionEffect::ScheduleTick { attempts } => Effect::Schedule { id: RETRY_TICK, after: http_delay(attempts), input: SessionInput::Tick(Time(0)) },
            other => Effect::Run(other),
        }).collect()
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
            (SessionPhase::Closed, SessionInput::Mutate { author, req_id, .. }) => fx.push(SessionEffect::Answer { author, req_id, outcome: AuthorOutcome::Error(SessionError::Closed) }),
            (SessionPhase::Closed, _) => {}
            (phase, SessionInput::Closing) => {
                if let Some(pending) = pending_of(&phase) {
                    finish(fx, &pending, Err(SessionError::Closed));
                    fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                }
                if let SessionPhase::Generating { call } = phase {
                    refuse(fx, call, SessionError::Closed);
                }
                for queued in self.queue.drain(..) {
                    refuse(fx, queued, SessionError::Closed);
                }
                fx.push(SessionEffect::Drained(Drained { owner: "session" }));
            }
            (phase, SessionInput::Online(Online(online))) => {
                self.online = online;
                self.phase = phase;
                if let SessionPhase::Calling { pending, sent, .. } = &mut self.phase {
                    if online && !*sent {
                        *sent = true;
                        fx.push(SessionEffect::HttpRequest { pending: pending.clone() });
                    } else if !online && *sent {
                        *sent = false;
                        fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                    }
                }
            }
            (phase, SessionInput::Published(published)) => {
                self.epoch = published.epoch;
                if std::mem::take(&mut self.restored) { self.call_epoch = published.epoch; }
                self.phase = phase;
            }
            (SessionPhase::Idle, SessionInput::Open { credential, pending }) => {
                self.credential = credential;
                self.phase = SessionPhase::Idle;
                if let Some(pending) = pending {
                    self.next_request = self.next_request.max(pending.request_id + 1);
                    let author = pending.owner.as_ref().map(author_of).or(pending.author);
                    let pending = PendingCall { reply: None, author, ..pending };
                    if self.online {
                        fx.push(SessionEffect::HttpRequest { pending: pending.clone() });
                    }
                    self.phase = SessionPhase::Calling { pending, attempts: 0, sent: self.online };
                    if self.epoch == 0 { self.restored = true; } else { self.call_epoch = self.epoch; }
                }
            }
            (phase, SessionInput::Open { .. }) => self.phase = phase,
            (SessionPhase::Idle, SessionInput::SessionCall { operation, body, reply }) => self.begin(Queued::Call { operation, body, reply }, fx),
            (SessionPhase::Idle, SessionInput::Mutate { author, owner, req_id, path, body, purpose }) => self.begin(Queued::Mutate { author, owner, req_id, path, body, purpose }, fx),
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::SessionCall { operation, body, reply }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::Mutate { author, owner, req_id, path, body, purpose }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, path, body, purpose });
            }
            (SessionPhase::Generating { call: Queued::Call { operation, body, reply } }, SessionInput::CredentialGenerated { credential }) => {
                let Some(def) = self.operations.iter().find(|def| def.name == operation).cloned() else {
                    fx.push(SessionEffect::Reply { id: reply, answer: Err(SessionError::Invalid) });
                    self.idle(fx);
                    return;
                };
                let request_id = self.request();
                let pending = PendingCall { request_id, sequence: 1, operation, path: def.path, body, sessionless: false, credential: Some(credential), purpose: 0, author: None, owner: None, reply: Some(reply) };
                self.persist(pending, fx);
            }
            (SessionPhase::Idle, SessionInput::EpochEnded(EpochEnded { cause, .. })) => {
                if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                    self.credential = None;
                }
                self.phase = SessionPhase::Idle;
            }
            (SessionPhase::Generating { call }, SessionInput::EpochEnded(EpochEnded { epoch, cause })) if epoch == self.call_epoch && matches!(cause, Cause::SessionReset | Cause::ServerEnded) => {
                refuse(fx, call, SessionError::Interrupted);
                for queued in self.queue.drain(..) {
                    refuse(fx, queued, SessionError::NoSession);
                }
                self.credential = None;
                self.phase = SessionPhase::Idle;
            }
            (phase, SessionInput::EpochEnded(EpochEnded { epoch, cause })) => {
                let pending = pending_of(&phase).filter(|_| epoch == self.call_epoch && matches!(cause, Cause::SessionReset | Cause::ServerEnded));
                let Some(pending) = pending else {
                    self.phase = phase;
                    return;
                };
                finish(fx, &pending, Err(SessionError::Interrupted));
                fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                for queued in self.queue.drain(..) {
                    refuse(fx, queued, SessionError::NoSession);
                }
                self.credential = None;
                self.phase = SessionPhase::Idle;
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                let sent = self.online;
                if sent {
                    fx.push(SessionEffect::HttpRequest { pending: pending.clone() });
                }
                self.phase = SessionPhase::Calling { pending, attempts: 0, sent };
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                finish(fx, &pending, Err(SessionError::Rejected(reason)));
                self.idle(fx);
            }
            (SessionPhase::Calling { pending, .. }, SessionInput::Response { request_id, body, sequence, credential, revoked }) if request_id == pending.request_id => {
                if revoked {
                    finish(fx, &pending, Err(SessionError::Revoked));
                    let batch = self.batch();
                    fx.push(SessionEffect::RemovePending { batch, epoch: self.call_epoch });
                    fx.push(SessionEffect::ServerEnded(ServerEnded { epoch: self.call_epoch }));
                    self.idle(fx);
                } else if pending.sessionless || sequence == Some(pending.sequence) {
                    let batch = self.batch();
                    let identity = pending.credential.or(credential);
                    fx.push(SessionEffect::CommitLanding { batch, epoch: self.call_epoch, digest: identity, sequence: (!pending.sessionless).then_some(pending.sequence) });
                    self.phase = SessionPhase::Landing { pending, batch, answer: body, identity };
                } else {
                    let error = if sequence.is_none() { SessionError::Protocol } else { SessionError::SequenceConflict };
                    finish(fx, &pending, Err(error));
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
                    fx.push(SessionEffect::RemovePending { batch, epoch: self.call_epoch });
                    finish(fx, &pending, Err(if auth { SessionError::Unauthorized } else { SessionError::Http }));
                    self.idle(fx);
                }
            }
            (SessionPhase::Calling { pending, attempts, .. }, SessionInput::Dropped { request_id }) if request_id == pending.request_id => {
                if self.online {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, sent: false };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, sent: false };
                }
            }
            (SessionPhase::Calling { pending, attempts, sent }, SessionInput::Cancel { req_id }) if req_id == pending.request_id => {
                if sent {
                    fx.push(SessionEffect::HttpCancel { request_id: pending.request_id });
                }
                self.phase = SessionPhase::Calling { pending, attempts, sent: false };
            }
            (phase, SessionInput::Cancel { req_id }) => {
                self.queue.retain(|queued| !matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id));
                self.phase = phase;
            }
            (SessionPhase::Calling { pending, attempts, .. }, SessionInput::Tick(_)) => {
                let sent = self.online;
                if sent {
                    fx.push(SessionEffect::HttpRequest { pending: pending.clone() });
                }
                self.phase = SessionPhase::Calling { pending, attempts, sent };
            }
            (SessionPhase::Landing { pending, batch, answer, identity }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                match identity {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        fx.push(SessionEffect::CredentialCommitted(CredentialCommitted { identity: digest }));
                    }
                    None if !pending.sessionless => {
                        if let Some(credential) = &mut self.credential {
                            credential.sequence = pending.sequence;
                        }
                    }
                    None => {}
                }
                finish(fx, &pending, Ok(answer));
                self.idle(fx);
            }
            (SessionPhase::Landing { pending, batch, .. }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                finish(fx, &pending, Err(SessionError::Rejected(reason)));
                self.idle(fx);
            }
            (phase, _) => self.phase = phase,
        }
    }
}
