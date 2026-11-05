//! Owner `SessionCalls`: the credential, `sequence`, `encrypted_session` and `encrypted_pending`.

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use super::contract::{BatchId, Committed, CredentialCommitted, Drained, EpochEnded, ErrorClass, Online, Published, PublishedState, RejectReason, Rejected, ReplyId, RequestId, ServerEnded};
pub use crate::transport::encrypted::state::StoredAnswer;
use crate::runtime::{Cause, Effect, Effects, Epoch, ScheduleId, StateOwner, Time};

pub type Digest = u64;
pub type AuthorId = u64;
pub type AttemptId = u64;
pub type SecretOpId = u64;
pub type StreamId = u64;

pub const OUTBOX_AUTHOR: AuthorId = 1;
pub const TRANSFER_AUTHOR: AuthorId = 2;

/// The owner of a `Mutate`: `encrypted_pending` keeps it, and a pending restored at open answers the author this owner names.
pub type MutateOwner = crate::transport::RequestOwner;

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
    Session(crate::transport::SessionCode),
    Storage(String),
    SessionExists,
    NoCredential,
    Revoked,
    StateLost,
    SequenceConflict,
    Protocol,
    Unauthorized,
    Http,
    Interrupted,
    NoSession,
    Closed,
}

pub type SessionAnswer = Result<String, SessionError>;

/// The row of `encrypted_answer` for a terminal outcome of a `Mutate`; the other errors of a call never land.
pub fn stored_answer(answer: &SessionAnswer) -> Option<StoredAnswer> {
    match answer {
        Ok(body) => Some(StoredAnswer::Ok(body.clone())),
        Err(SessionError::Http) => Some(StoredAnswer::Http),
        Err(SessionError::Unauthorized) => Some(StoredAnswer::Unauthorized),
        Err(SessionError::Revoked) => Some(StoredAnswer::Revoked),
        Err(SessionError::Session(code)) => Some(StoredAnswer::Session(*code)),
        Err(_) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationClass {
    Login,
    Authenticated,
    Sessionless,
    Logout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionOperation {
    pub name: String,
    pub path: String,
    pub class: OperationClass,
}

/// `pending` of the table: the row `encrypted_pending`, by value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCall {
    pub request_id: RequestId,
    pub sequence: u64,
    pub class: OperationClass,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<MutateOwner>,
    pub epoch: Epoch,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queued {
    Call { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, epoch: Epoch, path: String, body: String, purpose: u8 },
}

/// What `CredentialStore.get` found: the digest of the secret, nothing, or an access error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretState {
    Present(Digest),
    Missing,
    Error(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallPhase {
    InFlight(AttemptId),
    WaitingRetry,
    WaitingNetwork,
    WaitingSecret,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Block {
    Storage,
    SequenceConflict,
    Protocol,
    StateLost,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AfterStore {
    Persist,
    Clear(SessionError),
    Drain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterClear {
    Idle,
    Drain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Generating { call: Queued, op: SecretOpId },
    Storing { call: Queued, credential_new: Digest, op: SecretOpId, after: AfterStore },
    ClearingSecret { call: Option<Queued>, reason: Option<SessionError>, op: SecretOpId, after: AfterClear },
    PendingHeld { pending: PendingCall },
    SecretMismatch,
    SecretUnavailable { error: String, read: Option<SecretOpId> },
    Persisting { pending: PendingCall, batch: BatchId },
    Calling { pending: PendingCall, attempts: u64, phase: CallPhase, document_sent: bool },
    Landing { pending: PendingCall, batch: BatchId, answer: SessionAnswer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool },
    Blocked { pending: Option<PendingCall>, reason: Block },
    Closed,
}

/// What the author of a `Mutate` hears: `Transport{req_id, Response(body) / Error{class, X} / Dropped}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorOutcome {
    Response(String),
    Error { class: ErrorClass, error: SessionError },
    Dropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryPhase {
    Idle,
    InFlight,
    WaitingRetry,
    WaitingNetwork,
    WaitingSecret,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEffect {
    /// Store `Commit{encrypted_pending = pending}`, with `Session{digest, 0}` at the first sign-in.
    CommitPending { batch: BatchId, epoch: Epoch, pending: PendingCall, session: Option<Digest> },
    /// Store `Commit{Session{digest or the previous one, sequence}, RemovePending, Answer}`; no `sequence` for a call without a session.
    CommitLanding { batch: BatchId, epoch: Epoch, digest: Option<Digest>, sequence: Option<u64>, answer: Option<(RequestId, SessionAnswer)> },
    HttpRequest { pending: PendingCall, attempt_id: AttemptId, full_document: bool },
    HttpCancel { attempt_id: AttemptId },
    /// `Schedule Tick` by the Backoff of Http for the attempt number `attempts`. `step` turns it into the Runtime effect `Schedule` (R5).
    ScheduleTick { attempts: u64 },
    GenerateCredential { secret_op_id: SecretOpId },
    StoreSecret { credential: Digest, secret_op_id: SecretOpId },
    ClearSecret { secret_op_id: SecretOpId },
    ReadSecret { secret_op_id: SecretOpId },
    CredentialCommitted(CredentialCommitted),
    ServerEnded(ServerEnded),
    /// Lifecycle `SessionReset{reply = None}`.
    SessionReset,
    /// Lifecycle `RestoreClassified{Session, epoch}`.
    RestoreClassified { epoch: Epoch },
    Reply { id: ReplyId, answer: SessionAnswer },
    Answer { author: AuthorId, req_id: RequestId, outcome: AuthorOutcome },
    RetryState { stream_id: StreamId, attempts: u64, phase: RetryPhase },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum SessionInput {
    Open { credential: Option<Credential>, pending: Option<PendingCall>, dropped_pending: bool, secret: SecretState },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, epoch: Epoch, path: String, body: String, purpose: u8 },
    /// Outbox, Transfer and Calls take their request back: the one in the network goes on, the one in the queue is removed.
    Cancel { req_id: RequestId },
    CredentialGenerated { secret_op_id: SecretOpId, credential: Digest },
    SecretStored { secret_op_id: SecretOpId, ok: bool },
    SecretCleared { secret_op_id: SecretOpId, ok: bool },
    SecretRead { secret_op_id: SecretOpId, read: SecretState },
    SecretMissing { request_id: RequestId, attempt_id: AttemptId, lost: bool },
    Foreground { seq: u64 },
    InterestPlus { stream_id: StreamId },
    InterestMinus { stream_id: StreamId },
    RestoreReady { epoch: Epoch },
    /// The snapshot of `ReadIntents`: Session classifies `pending`; `answers` go to the authors, `pending_lost` is the dropped `encrypted_pending` (also `Open.dropped_pending`).
    Restored { epoch: Epoch, pending: Option<PendingCall>, answers: std::collections::BTreeMap<RequestId, StoredAnswer>, pending_lost: bool },
    Response { request_id: RequestId, attempt_id: AttemptId, body: String, sequence: Option<u64>, refusal: Option<crate::transport::SessionCode> },
    Revoked { request_id: RequestId, attempt_id: AttemptId, body: String },
    HttpError { request_id: RequestId, attempt_id: AttemptId, temporary: bool, auth: bool, persisted_missing: bool },
    Dropped { request_id: RequestId, attempt_id: AttemptId },
    Committed(Committed),
    Rejected(Rejected),
    Tick(Time),
    Online(Online),
    Published(Published),
    EpochEnded(EpochEnded),
    Idle,
    Closing,
}

pub struct SessionState {
    phase: SessionPhase,
    credential: Option<Credential>,
    queue: VecDeque<Queued>,
    online: Option<bool>,
    epoch: Epoch,
    published: PublishedState,
    after_publish: Option<(ReplyId, String)>,
    subscribers: BTreeSet<StreamId>,
    restored_direct: Option<PendingCall>,
    operations: Vec<SessionOperation>,
    next_request: RequestId,
    next_batch: BatchId,
    next_attempt: AttemptId,
    next_secret: SecretOpId,
    restore_ready: bool,
    adopt: bool,
    held: Option<SessionPhase>,
    reported: (u64, RetryPhase),
}

fn is_ended(cause: Cause) -> bool { matches!(cause, Cause::SessionReset | Cause::ServerEnded) }

fn author_error(fx: &mut Vec<SessionEffect>, author: AuthorId, req_id: RequestId, class: ErrorClass, error: SessionError) {
    fx.push(SessionEffect::Answer { author, req_id, outcome: AuthorOutcome::Error { class, error } });
}

fn reply(fx: &mut Vec<SessionEffect>, id: ReplyId, answer: SessionAnswer) { fx.push(SessionEffect::Reply { id, answer }); }

fn blocked_error(reason: Block) -> SessionError {
    match reason {
        Block::Storage => SessionError::Storage("the stored request is not confirmed".into()),
        Block::SequenceConflict => SessionError::SequenceConflict,
        Block::Protocol => SessionError::Protocol,
        Block::StateLost => SessionError::StateLost,
    }
}

fn pending_of(phase: &SessionPhase) -> Option<PendingCall> {
    match phase {
        SessionPhase::Persisting { pending, .. } | SessionPhase::Calling { pending, .. } | SessionPhase::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}

/// The result of the pending request goes to its call (`reply`) and to its `Mutate` author.
fn finish(fx: &mut Vec<SessionEffect>, pending: &PendingCall, result: SessionAnswer) {
    if let Some(id) = pending.reply { reply(fx, id, result.clone()); }
    if let Some(author) = pending.author {
        let outcome = match result {
            Ok(body) => AuthorOutcome::Response(body),
            Err(error) => AuthorOutcome::Error { class: ErrorClass::Permanent, error },
        };
        fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome });
    }
}

fn refuse(fx: &mut Vec<SessionEffect>, queued: Queued, error: SessionError) {
    match queued {
        Queued::Call { reply: id, .. } => reply(fx, id, Err(error)),
        Queued::Mutate { author, req_id, .. } => author_error(fx, author, req_id, ErrorClass::Permanent, error),
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
        let interest = if let SessionInput::InterestPlus { stream_id } = &input { Some(*stream_id) } else { None };
        state.run(input, &mut fx);
        let view = state.view();
        if view != state.reported {
            state.reported = view;
            for stream_id in &state.subscribers { fx.push(SessionEffect::RetryState { stream_id: *stream_id, attempts: view.0, phase: view.1 }); }
        }
        if let Some(stream_id) = interest {
            if !matches!(state.phase, SessionPhase::Closed) { fx.push(SessionEffect::RetryState { stream_id, attempts: view.0, phase: view.1 }); }
        }
        fx.into_iter().map(|effect| match effect {
            SessionEffect::ScheduleTick { attempts } => Effect::Schedule { id: RETRY_TICK, after: http_delay(attempts), input: SessionInput::Tick(Time(0)) },
            other => Effect::Run(other),
        }).collect()
    }

    fn tick(_now: Time) -> SessionInput { SessionInput::Idle }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> SessionInput { SessionInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> SessionInput { SessionInput::Closing }
}

impl SessionState {
    pub fn new(declared: &[&str], online: bool) -> Self {
        let operations = declared.iter().map(|name| SessionOperation { name: name.to_string(), path: format!("/{name}"), class: OperationClass::Authenticated }).collect();
        Self::with_operations(operations, Some(online))
    }

    pub fn with_operations(operations: Vec<SessionOperation>, online: Option<bool>) -> Self {
        Self { phase: SessionPhase::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, published: PublishedState::Session, after_publish: None, subscribers: BTreeSet::new(), restored_direct: None, operations, next_request: 1, next_batch: 1, next_attempt: 1, next_secret: 1, restore_ready: false, adopt: false, held: None, reported: (0, RetryPhase::Idle) }
    }

    pub fn phase(&self) -> &SessionPhase { &self.phase }

    pub fn credential(&self) -> Option<&Credential> { self.credential.as_ref() }

    pub fn queued(&self) -> usize { self.queue.len() }

    pub fn subscribers(&self) -> usize { self.subscribers.len() }

    pub fn awaiting_publication(&self) -> Option<&(ReplyId, String)> { self.after_publish.as_ref() }

    pub fn restored_direct(&self) -> Option<&PendingCall> { self.restored_direct.as_ref() }

    fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    fn request(&mut self) -> RequestId {
        self.next_request += 1;
        self.next_request - 1
    }

    fn attempt(&mut self) -> AttemptId {
        self.next_attempt += 1;
        self.next_attempt - 1
    }

    fn secret_op(&mut self) -> SecretOpId {
        self.next_secret += 1;
        self.next_secret - 1
    }

    fn is_online(&self) -> bool { self.online == Some(true) }

    fn def(&self, operation: &str) -> Option<SessionOperation> { self.operations.iter().find(|def| def.name == operation).cloned() }

    fn refuse_queue(&mut self, fx: &mut Vec<SessionEffect>, error: SessionError) {
        for queued in self.queue.drain(..) { refuse(fx, queued, error.clone()); }
    }

    fn view(&self) -> (u64, RetryPhase) {
        match &self.phase {
            SessionPhase::Calling { attempts, phase, .. } => (*attempts, match phase {
                CallPhase::InFlight(_) => RetryPhase::InFlight,
                CallPhase::WaitingRetry => RetryPhase::WaitingRetry,
                CallPhase::WaitingNetwork => RetryPhase::WaitingNetwork,
                CallPhase::WaitingSecret => RetryPhase::WaitingSecret,
            }),
            _ => (0, RetryPhase::Idle),
        }
    }

    fn idle(&mut self, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if let Some(held) = self.held.take() {
            let error = if matches!(held, SessionPhase::SecretMismatch) { SessionError::StateLost } else { SessionError::Storage("the secret is unavailable".into()) };
            self.refuse_queue(fx, error);
            self.phase = held;
            return;
        }
        if self.restore_ready {
            if let Some(pending) = self.restored_direct.take() {
                self.start_calling(pending, fx);
                return;
            }
        }
        self.drain(fx);
    }

    fn drain(&mut self, fx: &mut Vec<SessionEffect>) {
        while matches!(self.phase, SessionPhase::Idle) {
            match self.published {
                PublishedState::EndingFailed | PublishedState::StorageFailed => {
                    self.refuse_queue(fx, SessionError::Storage("the session could not be saved".into()));
                    return;
                }
                PublishedState::Ending | PublishedState::Closing => return,
                _ => {}
            }
            let Some(queued) = self.queue.pop_front() else { return };
            self.begin(queued, fx);
        }
    }

    fn begin(&mut self, queued: Queued, fx: &mut Vec<SessionEffect>) {
        match queued {
            Queued::Call { operation, body, reply: id } => {
                let Some(def) = self.def(&operation) else {
                    reply(fx, id, Err(SessionError::Invalid));
                    return;
                };
                let call = Queued::Call { operation: operation.clone(), body: body.clone(), reply: id };
                let epoch = self.epoch;
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                match def.class {
                    OperationClass::Login if self.credential.is_some() => reply(fx, id, Err(SessionError::SessionExists)),
                    OperationClass::Login => self.generate(call, fx),
                    OperationClass::Authenticated if self.credential.is_none() => self.generate(call, fx),
                    OperationClass::Logout if self.credential.is_none() => reply(fx, id, Err(SessionError::NoCredential)),
                    OperationClass::Sessionless => {
                        let request_id = self.request();
                        self.persist(PendingCall { request_id, sequence: 0, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, None, fx);
                    }
                    OperationClass::Authenticated | OperationClass::Logout => {
                        let request_id = self.request();
                        self.persist(PendingCall { request_id, sequence, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, None, fx);
                    }
                }
            }
            Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => {
                if self.published == PublishedState::Closing || epoch != self.epoch || self.credential.is_none() {
                    author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::NoSession);
                    return;
                }
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                self.persist(PendingCall { request_id: req_id, sequence, class: OperationClass::Authenticated, operation: String::new(), path, body, credential: None, purpose, author: Some(author), owner, epoch, reply: None }, None, fx);
            }
        }
    }

    fn generate(&mut self, call: Queued, fx: &mut Vec<SessionEffect>) {
        let op = self.secret_op();
        fx.push(SessionEffect::GenerateCredential { secret_op_id: op });
        self.phase = SessionPhase::Generating { call, op };
    }

    fn persist(&mut self, pending: PendingCall, session: Option<Digest>, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        fx.push(SessionEffect::CommitPending { batch, epoch: pending.epoch, pending: pending.clone(), session });
        self.phase = SessionPhase::Persisting { pending, batch };
    }

    fn send(&mut self, pending: PendingCall, attempts: u64, document_sent: bool, fx: &mut Vec<SessionEffect>) {
        let attempt_id = self.attempt();
        fx.push(SessionEffect::HttpRequest { pending: pending.clone(), attempt_id, full_document: document_sent });
        self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::InFlight(attempt_id), document_sent };
    }

    fn start_calling(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        if self.is_online() {
            self.send(pending, 0, false, fx);
        } else {
            self.phase = SessionPhase::Calling { pending, attempts: 0, phase: CallPhase::WaitingNetwork, document_sent: false };
        }
    }

    fn land(&mut self, pending: PendingCall, answer: SessionAnswer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        let record = pending.author.map(|_| (pending.request_id, answer.clone()));
        fx.push(SessionEffect::CommitLanding { batch, epoch: pending.epoch, digest: identity, sequence, answer: record });
        self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended };
    }

    fn clearing(&mut self, call: Option<Queued>, reason: Option<SessionError>, after: AfterClear, fx: &mut Vec<SessionEffect>) {
        let op = self.secret_op();
        fx.push(SessionEffect::ClearSecret { secret_op_id: op });
        self.phase = SessionPhase::ClearingSecret { call, reason, op, after };
    }

    fn drained(&mut self, fx: &mut Vec<SessionEffect>) {
        self.refuse_queue(fx, SessionError::Closed);
        self.held = None;
        self.restored_direct = None;
        self.phase = SessionPhase::Closed;
        fx.push(SessionEffect::Drained(Drained { owner: "session" }));
    }

    fn answer_publication(&mut self, fx: &mut Vec<SessionEffect>, state: PublishedState, identity: Option<Digest>) {
        let Some((id, body)) = self.after_publish.take() else { return };
        let digest = self.credential.as_ref().map(|credential| credential.digest);
        if state == PublishedState::Session && identity.is_some() && identity == digest {
            reply(fx, id, Ok(body));
        } else if matches!(state, PublishedState::StorageFailed | PublishedState::EndingFailed) {
            reply(fx, id, Err(SessionError::Storage("the session could not be saved".into())));
        } else if state == PublishedState::Ending {
            reply(fx, id, Err(SessionError::Interrupted));
        } else {
            self.after_publish = Some((id, body));
        }
    }

    fn adopt_epoch(&mut self, epoch: Epoch) {
        if let Some(pending) = &mut self.restored_direct { pending.epoch = epoch; }
        if let SessionPhase::PendingHeld { pending } | SessionPhase::Calling { pending, .. } = &mut self.phase { pending.epoch = epoch; }
    }

    fn queue_mutate(&mut self, queued: Queued) {
        let Queued::Mutate { req_id, .. } = &queued else { return };
        if !self.queue.iter().any(|entry| matches!(entry, Queued::Mutate { req_id: seen, .. } if seen == req_id)) { self.queue.push_back(queued); }
    }

    /// One row of the table per match arm, in table order.
    fn run(&mut self, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let phase = std::mem::replace(&mut self.phase, SessionPhase::Closed);
        if matches!(phase, SessionPhase::Closed) {
            match input {
                SessionInput::SessionCall { reply: id, .. } => reply(fx, id, Err(SessionError::Closed)),
                SessionInput::Mutate { author, req_id, .. } => author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::Closed),
                _ => {}
            }
            return;
        }
        match input {
            SessionInput::Closing => self.closing(phase, fx),
            SessionInput::InterestPlus { stream_id } => {
                self.subscribers.insert(stream_id);
                self.phase = phase;
            }
            SessionInput::InterestMinus { stream_id } => {
                self.subscribers.remove(&stream_id);
                self.phase = phase;
            }
            SessionInput::Online(Online(online)) => self.online_changed(phase, online, fx),
            SessionInput::Published(Published { epoch, identity, state, .. }) => {
                self.epoch = epoch;
                self.published = state;
                self.phase = phase;
                if std::mem::take(&mut self.adopt) { self.adopt_epoch(epoch); }
                self.answer_publication(fx, state, identity);
                if matches!(self.phase, SessionPhase::Idle) { self.drain(fx); }
            }
            SessionInput::Restored { epoch, pending, .. } => {
                self.phase = phase;
                fx.push(SessionEffect::RestoreClassified { epoch });
                if let (SessionPhase::Idle, Some(pending)) = (&self.phase, pending) {
                    if pending.owner.is_none() { self.restored_direct = Some(PendingCall { epoch, ..pending }); }
                }
                if self.restore_ready && matches!(self.phase, SessionPhase::Idle) {
                    if let Some(pending) = self.restored_direct.take() { self.start_calling(pending, fx); }
                }
            }
            SessionInput::RestoreReady { .. } => {
                self.phase = phase;
                self.restore_ready = true;
                if matches!(self.phase, SessionPhase::Idle) {
                    if let Some(pending) = self.restored_direct.take() { self.start_calling(pending, fx); }
                }
            }
            SessionInput::Cancel { req_id } => {
                self.phase = phase;
                if let Some(at) = self.queue.iter().position(|queued| matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id)) {
                    if let Some(removed) = self.queue.remove(at) { refuse(fx, removed, SessionError::Interrupted); }
                }
            }
            SessionInput::EpochEnded(EpochEnded { epoch, cause }) => {
                self.restored_direct = None;
                if is_ended(cause) {
                    if let Some((id, _)) = self.after_publish.take() { reply(fx, id, Err(SessionError::Interrupted)); }
                    self.held = None;
                }
                self.on_epoch_ended(phase, epoch, cause, fx);
            }
            input => self.by_phase(phase, input, fx),
        }
    }

    fn on_epoch_ended(&mut self, phase: SessionPhase, epoch: Epoch, cause: Cause, fx: &mut Vec<SessionEffect>) {
        match phase {
            SessionPhase::Idle => {
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Generating { call, .. } if is_ended(cause) => {
                refuse(fx, call, SessionError::Interrupted);
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Storing { call, credential_new, op, after } if is_ended(cause) => {
                self.credential = None;
                let after = if matches!(after, AfterStore::Drain) { AfterStore::Drain } else { AfterStore::Clear(SessionError::Interrupted) };
                self.phase = SessionPhase::Storing { call, credential_new, op, after };
            }
            SessionPhase::ClearingSecret { call, reason, op, after } => {
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::ClearingSecret { call, reason, op, after };
            }
            SessionPhase::PendingHeld { .. } | SessionPhase::Blocked { .. } | SessionPhase::SecretMismatch | SessionPhase::SecretUnavailable { .. } if is_ended(cause) => {
                self.credential = None;
                self.refuse_queue(fx, SessionError::NoSession);
                self.idle(fx);
            }
            phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }) => {
                let Some(pending) = pending_of(&phase).filter(|pending| epoch == pending.epoch && is_ended(cause)) else {
                    self.phase = phase;
                    return;
                };
                finish(fx, &pending, Err(SessionError::Interrupted));
                if let SessionPhase::Calling { phase: CallPhase::InFlight(attempt_id), .. } = phase { fx.push(SessionEffect::HttpCancel { attempt_id }); }
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.idle(fx);
            }
            other => self.phase = other,
        }
    }

    fn closing(&mut self, phase: SessionPhase, fx: &mut Vec<SessionEffect>) {
        if let Some((id, _)) = self.after_publish.take() { reply(fx, id, Err(SessionError::Closed)); }
        match phase {
            SessionPhase::Persisting { pending, .. } | SessionPhase::Landing { pending, .. } => finish(fx, &pending, Err(SessionError::Closed)),
            SessionPhase::Calling { pending, phase, .. } => {
                finish(fx, &pending, Err(SessionError::Closed));
                if let CallPhase::InFlight(attempt_id) = phase { fx.push(SessionEffect::HttpCancel { attempt_id }); }
            }
            SessionPhase::Generating { call, .. } => refuse(fx, call, SessionError::Closed),
            SessionPhase::Storing { call, credential_new, op, .. } => {
                self.refuse_queue(fx, SessionError::Closed);
                self.phase = SessionPhase::Storing { call, credential_new, op, after: AfterStore::Drain };
                return;
            }
            SessionPhase::ClearingSecret { call, reason, op, .. } => {
                self.refuse_queue(fx, SessionError::Closed);
                self.phase = SessionPhase::ClearingSecret { call, reason, op, after: AfterClear::Drain };
                return;
            }
            _ => {}
        }
        self.drained(fx);
    }

    fn online_changed(&mut self, phase: SessionPhase, online: bool, fx: &mut Vec<SessionEffect>) {
        self.online = Some(online);
        match phase {
            SessionPhase::Calling { pending, attempts, phase: CallPhase::InFlight(attempt_id), document_sent } if !online => {
                fx.push(SessionEffect::HttpCancel { attempt_id });
                if let Some(author) = pending.author {
                    fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Dropped });
                    self.phase = SessionPhase::PendingHeld { pending };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent } if online => self.send(pending, attempts, document_sent, fx),
            SessionPhase::Blocked { pending: Some(pending), reason: Block::Storage } if online && pending.author.is_none() => self.send(pending, 0, false, fx),
            other => self.phase = other,
        }
    }

    fn open(&mut self, credential: Option<Credential>, pending: Option<PendingCall>, dropped_pending: bool, secret: SecretState, fx: &mut Vec<SessionEffect>) {
        self.credential = credential.clone();
        if dropped_pending {
            self.phase = SessionPhase::Blocked { pending: None, reason: Block::StateLost };
            return;
        }
        let secret = match secret {
            SecretState::Error(error) => {
                self.phase = SessionPhase::SecretUnavailable { error, read: None };
                return;
            }
            other => other,
        };
        self.phase = SessionPhase::Idle;
        match (credential, secret) {
            (None, SecretState::Missing) => {}
            (None, _) => self.clearing(None, None, AfterClear::Idle, fx),
            (Some(_), SecretState::Missing) => self.missing_secret(fx),
            (Some(credential), SecretState::Present(digest)) if digest != credential.digest => self.phase = SessionPhase::SecretMismatch,
            (Some(_), _) => {
                match pending {
                    Some(pending) if pending.owner.is_some() => {
                        let author = pending.owner.as_ref().map(author_of).or(pending.author);
                        self.adopt = true;
                        self.phase = SessionPhase::PendingHeld { pending: PendingCall { reply: None, author, ..pending } };
                    }
                    Some(pending) if pending.class == OperationClass::Login => {
                        self.adopt = true;
                        self.start_calling(PendingCall { reply: None, ..pending }, fx);
                    }
                    _ => {}
                }
            }
        }
    }

    fn missing_secret(&mut self, fx: &mut Vec<SessionEffect>) {
        self.credential = None;
        fx.push(SessionEffect::SessionReset);
    }

    fn idle_call(&mut self, operation: String, body: String, id: ReplyId, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if self.def(&operation).is_none() {
            reply(fx, id, Err(SessionError::Invalid));
            return;
        }
        match self.published {
            PublishedState::EndingFailed | PublishedState::StorageFailed => reply(fx, id, Err(SessionError::Storage("the session could not be saved".into()))),
            PublishedState::Ending => self.queue.push_back(Queued::Call { operation, body, reply: id }),
            _ => self.begin(Queued::Call { operation, body, reply: id }, fx),
        }
    }

    fn held_call(&mut self, held: SessionPhase, operation: String, body: String, id: ReplyId, error: SessionError, fx: &mut Vec<SessionEffect>) {
        match self.def(&operation) {
            None => {
                reply(fx, id, Err(SessionError::Invalid));
                self.phase = held;
            }
            Some(def) if def.class != OperationClass::Sessionless => {
                reply(fx, id, Err(error));
                self.phase = held;
            }
            Some(_) => {
                self.held = Some(held);
                self.phase = SessionPhase::Idle;
                self.begin(Queued::Call { operation, body, reply: id }, fx);
            }
        }
    }

    fn by_phase(&mut self, phase: SessionPhase, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        match (phase, input) {
            (SessionPhase::Idle, SessionInput::Open { credential, pending, dropped_pending, secret }) => self.open(credential, pending, dropped_pending, secret, fx),
            (phase, SessionInput::Open { .. }) => self.phase = phase,
            (SessionPhase::Idle, SessionInput::SessionCall { operation, body, reply: id }) => self.idle_call(operation, body, id, fx),
            (SessionPhase::Idle, SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = SessionPhase::Idle;
                self.begin(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }, fx);
            }
            (SessionPhase::SecretMismatch, SessionInput::SessionCall { operation, body, reply: id }) => self.held_call(SessionPhase::SecretMismatch, operation, body, id, SessionError::StateLost, fx),
            (SessionPhase::SecretMismatch, SessionInput::Mutate { author, req_id, .. }) => {
                self.phase = SessionPhase::SecretMismatch;
                author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::StateLost);
            }
            (phase @ SessionPhase::SecretUnavailable { .. }, SessionInput::SessionCall { operation, body, reply: id }) => {
                let error = SessionError::Storage("the secret is unavailable".into());
                self.held_call(phase, operation, body, id, error, fx);
            }
            (SessionPhase::SecretUnavailable { error, read }, SessionInput::Mutate { author, req_id, .. }) => {
                self.phase = SessionPhase::SecretUnavailable { error, read };
                author_error(fx, author, req_id, ErrorClass::Network, SessionError::Storage("the secret is unavailable".into()));
            }
            (SessionPhase::SecretUnavailable { error, .. }, SessionInput::Foreground { .. }) => {
                let op = self.secret_op();
                fx.push(SessionEffect::ReadSecret { secret_op_id: op });
                self.phase = SessionPhase::SecretUnavailable { error, read: Some(op) };
            }
            (SessionPhase::SecretUnavailable { error, read }, SessionInput::SecretRead { secret_op_id, read: found }) if read == Some(secret_op_id) => match found {
                SecretState::Present(digest) if self.credential.as_ref().is_some_and(|credential| credential.digest == digest) => self.idle(fx),
                SecretState::Present(_) => self.phase = SessionPhase::SecretMismatch,
                SecretState::Missing => {
                    self.phase = SessionPhase::Idle;
                    self.missing_secret(fx);
                }
                SecretState::Error(_) => self.phase = SessionPhase::SecretUnavailable { error, read: None },
            },
            (SessionPhase::Generating { call, op }, SessionInput::CredentialGenerated { secret_op_id, credential }) if secret_op_id == op => {
                let next = self.secret_op();
                fx.push(SessionEffect::StoreSecret { credential, secret_op_id: next });
                self.phase = SessionPhase::Storing { call, credential_new: credential, op: next, after: AfterStore::Persist };
            }
            (SessionPhase::Storing { call, credential_new, op, after }, SessionInput::SecretStored { secret_op_id, ok }) if secret_op_id == op => match (after, ok) {
                (AfterStore::Persist, true) => {
                    let Queued::Call { operation, body, reply: id } = call else { unreachable!("a generating call is a call") };
                    let def = self.def(&operation).expect("a generating call is declared");
                    let request_id = self.request();
                    let epoch = self.epoch;
                    self.persist(PendingCall { request_id, sequence: 1, class: OperationClass::Login, operation, path: def.path, body, credential: Some(credential_new), purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, Some(credential_new), fx);
                }
                (AfterStore::Persist, false) => self.clearing(Some(call), Some(SessionError::Storage("the secret could not be stored".into())), AfterClear::Idle, fx),
                (AfterStore::Clear(reason), _) => self.clearing(Some(call), Some(reason), AfterClear::Idle, fx),
                (AfterStore::Drain, _) => {
                    refuse(fx, call, SessionError::Closed);
                    self.drained(fx);
                }
            },
            (SessionPhase::ClearingSecret { call, reason, op, after }, SessionInput::SecretCleared { secret_op_id, .. }) if secret_op_id == op => {
                if let (Some(call), Some(reason)) = (call, reason) { refuse(fx, call, reason); }
                match after {
                    AfterClear::Idle => self.idle(fx),
                    AfterClear::Drain => self.drained(fx),
                }
            }
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Storing { .. } | SessionPhase::ClearingSecret { .. }), SessionInput::SessionCall { operation, body, reply: id }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply: id });
            }
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Storing { .. } | SessionPhase::ClearingSecret { .. }), SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (SessionPhase::PendingHeld { pending }, SessionInput::Mutate { req_id, .. }) if req_id == pending.request_id => self.start_calling(pending, fx),
            (SessionPhase::PendingHeld { pending }, SessionInput::SessionCall { operation, body, reply: id }) => {
                self.queue.push_back(Queued::Call { operation, body, reply: id });
                self.phase = SessionPhase::PendingHeld { pending };
            }
            (SessionPhase::PendingHeld { pending }, SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = SessionPhase::PendingHeld { pending };
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (SessionPhase::Blocked { pending, reason }, SessionInput::SessionCall { reply: id, .. }) => {
                self.phase = SessionPhase::Blocked { pending, reason };
                reply(fx, id, Err(SessionError::StateLost));
            }
            (SessionPhase::Blocked { pending, reason }, SessionInput::Mutate { author, req_id, .. }) => {
                let same = pending.as_ref().is_some_and(|pending| pending.request_id == req_id);
                match (same, pending, reason) {
                    (true, Some(pending), Block::Storage) => self.start_calling(pending, fx),
                    (true, pending, reason) => {
                        author_error(fx, author, req_id, ErrorClass::Permanent, blocked_error(reason));
                        self.phase = SessionPhase::Blocked { pending, reason };
                    }
                    (false, pending, reason) => {
                        author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::StateLost);
                        self.phase = SessionPhase::Blocked { pending, reason };
                    }
                }
            }
            (SessionPhase::Blocked { pending: Some(pending), reason: Block::Storage }, SessionInput::Tick(_)) if self.is_online() && pending.author.is_none() => self.send(pending, 0, false, fx),
            (SessionPhase::Persisting { pending, batch }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => self.start_calling(pending, fx),
            (SessionPhase::Persisting { pending, batch }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                finish(fx, &pending, Err(SessionError::Rejected(reason)));
                self.idle(fx);
            }
            (phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                let current = pending_of(&phase).is_some_and(|pending| pending.request_id == req_id);
                self.phase = phase;
                if !current { self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }); }
            }
            (phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::SessionCall { operation, body, reply: id }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply: id });
            }
            (SessionPhase::Calling { pending, attempts, phase, document_sent }, input) => self.calling(pending, attempts, phase, document_sent, input, fx),
            (SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        if let (Some(id), Ok(body)) = (pending.reply, &answer) { self.after_publish = Some((id, body.clone())); }
                        fx.push(SessionEffect::CredentialCommitted(CredentialCommitted { identity: digest }));
                    }
                    None => {
                        if let (Some(sequence), Some(credential)) = (sequence, &mut self.credential) { credential.sequence = sequence; }
                        if server_ended { fx.push(SessionEffect::ServerEnded(ServerEnded { epoch: pending.epoch })); }
                        finish(fx, &pending, answer);
                    }
                }
                self.idle(fx);
            }
            (SessionPhase::Landing { pending, batch, .. }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                finish(fx, &pending, Err(SessionError::Rejected(reason)));
                self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason: Block::Storage };
            }
            (phase, _) => self.phase = phase,
        }
    }

    fn calling(&mut self, pending: PendingCall, attempts: u64, phase: CallPhase, document_sent: bool, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let mine = |request_id: RequestId, attempt_id: AttemptId| pending.request_id == request_id && phase == CallPhase::InFlight(attempt_id);
        match input {
            SessionInput::Response { request_id, attempt_id, body, sequence, refusal } if mine(request_id, attempt_id) => {
                if let Some(code) = refusal {
                    let confirmed = (pending.class != OperationClass::Sessionless && sequence == Some(pending.sequence)).then_some(pending.sequence);
                    self.land(pending, Err(SessionError::Session(code)), None, confirmed, false, fx);
                } else if pending.class == OperationClass::Sessionless {
                    self.land(pending, Ok(body), None, None, false, fx);
                } else if sequence == Some(pending.sequence) {
                    let identity = pending.credential;
                    let sequence = pending.sequence;
                    self.land(pending, Ok(body), identity, Some(sequence), false, fx);
                } else {
                    let (reason, error) = if sequence.is_none() { (Block::Protocol, SessionError::Protocol) } else { (Block::SequenceConflict, SessionError::SequenceConflict) };
                    finish(fx, &pending, Err(error));
                    self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason };
                }
            }
            SessionInput::Revoked { request_id, attempt_id, body } if mine(request_id, attempt_id) => {
                let answer = if pending.class == OperationClass::Logout { Ok(body) } else { Err(SessionError::Revoked) };
                self.land(pending, answer, None, None, true, fx);
            }
            SessionInput::HttpError { request_id, attempt_id, persisted_missing: true, .. } if mine(request_id, attempt_id) && !document_sent => self.send(pending, attempts, true, fx),
            SessionInput::HttpError { request_id, attempt_id, temporary: true, persisted_missing: false, .. } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    author_error(fx, author, pending.request_id, ErrorClass::Network, SessionError::Http);
                    self.phase = SessionPhase::PendingHeld { pending };
                } else if self.is_online() {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry, document_sent };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            SessionInput::HttpError { request_id, attempt_id, auth, .. } if mine(request_id, attempt_id) => {
                let error = if auth { SessionError::Unauthorized } else { SessionError::Http };
                self.land(pending, Err(error), None, None, false, fx);
            }
            SessionInput::Dropped { request_id, attempt_id } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Dropped });
                    self.phase = SessionPhase::PendingHeld { pending };
                } else if self.is_online() {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry, document_sent };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            SessionInput::SecretMissing { request_id, attempt_id, lost } if mine(request_id, attempt_id) => {
                if lost {
                    finish(fx, &pending, Err(SessionError::StateLost));
                    self.phase = SessionPhase::SecretMismatch;
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingSecret, document_sent };
                }
            }
            SessionInput::Tick(_) if phase == CallPhase::WaitingRetry && self.is_online() => self.send(pending, attempts, document_sent, fx),
            SessionInput::Foreground { .. } if phase == CallPhase::WaitingSecret => {
                if self.is_online() {
                    self.send(pending, attempts, document_sent, fx);
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            _ => self.phase = SessionPhase::Calling { pending, attempts, phase, document_sent },
        }
    }
}
