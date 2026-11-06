//! Executable table of the owner `Session` in the `lifecycle` machine.

use std::collections::{BTreeSet, VecDeque};

pub type RequestId = u64;
pub type BatchId = u64;
pub type ReplyId = u64;
pub type AuthorId = u64;
pub type Digest = u64;
pub type AttemptId = u64;
pub type SecretOpId = u64;
pub type StreamId = u64;
pub type Owner = crate::transport::RequestOwner;

pub const OUTBOX_AUTHOR: AuthorId = 1;
pub const TRANSFER_AUTHOR: AuthorId = 2;

pub fn author_of(owner: &Owner) -> AuthorId {
    match owner {
        Owner::Operation { .. } | Owner::Unmatched { .. } => OUTBOX_AUTHOR,
        Owner::Transfer { .. } => TRANSFER_AUTHOR,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause {
    SessionReset,
    ServerEnded,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    NoSession,
    Storage,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fail {
    TooLarge { size: u64, limit: u64 },
    Invalid,
    Reason(Reason),
    Session(crate::transport::SessionCode),
    Storage,
    SessionExists,
    NoCredential,
    Revoked,
    StateLost,
    SequenceConflict,
    Protocol,
    Unauthorized,
    Security { code: crate::transport::SecurityCode, detail: String },
    Http,
    Interrupted,
    NoSession,
    Closed,
}

pub type Answer = Result<String, Fail>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Login,
    Authenticated,
    Sessionless,
    Logout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub name: String,
    pub path: String,
    pub class: Class,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub request_id: RequestId,
    pub sequence: u64,
    pub class: Class,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<Owner>,
    pub epoch: u64,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queued {
    Call { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, epoch: u64, path: String, body: String, purpose: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PubState {
    NoSession,
    Session,
    Ending,
    EndingFailed,
    StorageFailed,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Secret {
    Present(Digest),
    Missing,
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Sealed { seal_id: u64, size: u64 },
    SealFailed { seal_id: u64, error: Fail },
    Open { credential: Option<Credential>, pending: Option<Pending>, dropped_pending: bool, secret: Secret },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, epoch: u64, path: String, body: String, purpose: u8 },
    Cancel { req_id: RequestId },
    Release { req_id: RequestId },
    CredentialGenerated { op: SecretOpId, credential: Digest },
    SecretStored { op: SecretOpId, ok: bool },
    SecretCleared { op: SecretOpId, ok: bool },
    SecretRead { op: SecretOpId, read: Secret },
    SecretMissing { request_id: RequestId, attempt_id: AttemptId, lost: bool },
    GateClosed { request_id: RequestId, attempt_id: AttemptId },
    Foreground,
    InterestPlus(StreamId),
    InterestMinus(StreamId),
    RestoreReady { epoch: u64 },
    RestoreFailed { epoch: u64, error: String },
    Restored { epoch: u64, pending: Option<Pending> },
    Response { request_id: RequestId, attempt_id: AttemptId, body: String, sequence: Option<u64>, refusal: Option<crate::transport::SessionCode> },
    Revoked { request_id: RequestId, attempt_id: AttemptId, body: String },
    HttpError { request_id: RequestId, attempt_id: AttemptId, temporary: bool, auth: bool, persisted_missing: bool, security: Option<(crate::transport::SecurityCode, String)> },
    Dropped { request_id: RequestId, attempt_id: AttemptId },
    Committed(BatchId),
    Rejected(BatchId, Reason),
    Tick,
    Online(bool),
    Snapshot { epoch: u64, online: bool },
    Published { epoch: u64, identity: Option<Digest>, state: PubState },
    EpochEnded { epoch: u64, cause: EndCause },
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Permanent,
    Temporary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Response(String),
    Error(Kind, Fail),
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
pub enum Effect {
    SealProbe { seal_id: u64, pending: Pending, credential: Option<Digest> },
    /// Store `Commit{encrypted_pending = pending}`, with `Session{digest, 0}` at the first sign-in.
    CommitPending { batch: BatchId, epoch: u64, pending: Pending, session: Option<Digest> },
    /// Store `Commit{Session{digest or the previous one, sequence}, RemovePending, Answer}`.
    CommitLanding { batch: BatchId, epoch: u64, digest: Option<Digest>, sequence: Option<u64>, answer: Option<(RequestId, Answer)>, erase_session: bool },
    HttpRequest { pending: Pending, attempt_id: AttemptId, full_document: bool },
    HttpCancel { attempt_id: AttemptId },
    ScheduleTick { attempts: u64 },
    GenerateCredential { op: SecretOpId },
    StoreSecret { credential: Digest, op: SecretOpId },
    ClearSecret { op: SecretOpId },
    ReadSecret { op: SecretOpId },
    CredentialCommitted(Digest),
    ServerEnded { epoch: u64 },
    SessionReset,
    RestoreClassified { epoch: u64 },
    SnapshotRequest { epoch: u64 },
    Answer { author: AuthorId, req_id: RequestId, outcome: Outcome },
    RetryState { stream: StreamId, attempts: u64, phase: RetryPhase },
    Drained,
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
    Clear(Fail),
    Drain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterClear {
    Idle,
    Drain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    Sealing { pending: Pending, seal_id: u64 },
    Generating { call: Queued, op: SecretOpId },
    Storing { call: Queued, credential_new: Digest, op: SecretOpId, after: AfterStore },
    ClearingSecret { call: Option<Queued>, reason: Option<Fail>, op: SecretOpId, after: AfterClear },
    PendingHeld { pending: Pending },
    SecretMismatch,
    SecretUnavailable { error: String, read: Option<SecretOpId> },
    Persisting { pending: Pending, batch: BatchId },
    Calling { pending: Pending, attempts: u64, phase: CallPhase, document_sent: bool },
    Landing { pending: Pending, batch: BatchId, answer: Answer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool },
    Blocked { pending: Option<Pending>, reason: Block, landing: Option<Landed> },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Landed {
    pub answer: Answer,
    pub identity: Option<Digest>,
    pub sequence: Option<u64>,
    pub server_ended: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Out {
    pub effects: Vec<Effect>,
    pub replies: Vec<(ReplyId, Answer)>,
}

pub struct Session {
    pub state: State,
    pub credential: Option<Credential>,
    pub queue: VecDeque<Queued>,
    pub online: Option<bool>,
    pub epoch: u64,
    pub pub_state: PubState,
    /// The reply, its result, and the epoch it waits to pass (`None` waits for the identity of the new credential).
    pub after_publish: Option<(ReplyId, Answer, Option<u64>)>,
    pub subscribers: BTreeSet<StreamId>,
    pub restored_direct: Option<Pending>,
    restore_error: Option<String>,
    operations: Vec<Operation>,
    next_request: RequestId,
    next_batch: BatchId,
    next_attempt: AttemptId,
    next_secret: SecretOpId,
    pub restore_ready: bool,
    restore_floor: u64,
    adopt: bool,
    held: Option<State>,
    reported: (u64, RetryPhase),
}

fn author_error(author: AuthorId, req_id: RequestId, kind: Kind, fail: Fail) -> Effect {
    Effect::Answer { author, req_id, outcome: Outcome::Error(kind, fail) }
}

fn blocked_fail(reason: Block) -> Fail {
    match reason {
        Block::Storage => Fail::Storage,
        Block::SequenceConflict => Fail::SequenceConflict,
        Block::Protocol => Fail::Protocol,
        Block::StateLost => Fail::StateLost,
    }
}

fn pending_of(state: &State) -> Option<Pending> {
    match state {
        State::Persisting { pending, .. } | State::Calling { pending, .. } | State::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}

fn is_ended(cause: EndCause) -> bool { cause != EndCause::Other }

impl Session {
    pub fn new(operations: Vec<Operation>, online: Option<bool>) -> Self {
        Self { state: State::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, pub_state: PubState::Session, after_publish: None, subscribers: BTreeSet::new(), restored_direct: None, restore_error: None, operations, next_request: 1, next_batch: 1, next_attempt: 1, next_secret: 1, restore_ready: false, restore_floor: 0, adopt: false, held: None, reported: (0, RetryPhase::Idle) }
    }

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

    fn finish(out: &mut Out, pending: &Pending, result: Answer) {
        if let Some(reply) = pending.reply { out.replies.push((reply, result.clone())); }
        if let Some(author) = pending.author {
            out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: match result { Ok(body) => Outcome::Response(body), Err(fail) => Outcome::Error(Kind::Permanent, fail) } });
        }
    }

    fn refuse(out: &mut Out, queued: Queued, fail: Fail) {
        match queued {
            Queued::Call { reply, .. } => out.replies.push((reply, Err(fail))),
            Queued::Mutate { author, req_id, .. } => out.effects.push(author_error(author, req_id, Kind::Permanent, fail)),
        }
    }

    fn refuse_queue(&mut self, out: &mut Out, fail: Fail) {
        for queued in self.queue.drain(..) { Self::refuse(out, queued, fail.clone()); }
    }

    fn def(&self, operation: &str) -> Option<Operation> { self.operations.iter().find(|def| def.name == operation).cloned() }

    fn view(&self) -> (u64, RetryPhase) {
        match &self.state {
            State::Calling { attempts, phase, .. } => (*attempts, match phase {
                CallPhase::InFlight(_) => RetryPhase::InFlight,
                CallPhase::WaitingRetry => RetryPhase::WaitingRetry,
                CallPhase::WaitingNetwork => RetryPhase::WaitingNetwork,
                CallPhase::WaitingSecret => RetryPhase::WaitingSecret,
            }),
            _ => (0, RetryPhase::Idle),
        }
    }

    fn idle(&mut self, out: &mut Out) {
        self.state = State::Idle;
        if let Some(held) = self.held.take() {
            let fail = if matches!(held, State::SecretMismatch) { Fail::StateLost } else { Fail::Storage };
            for queued in self.queue.drain(..) { Self::refuse(out, queued, fail.clone()); }
            self.state = held;
            return;
        }
        if self.restore_ready {
            if let Some(pending) = self.restored_direct.take() {
                self.start_calling(pending, out);
                return;
            }
        }
        self.drain(out);
    }

    fn drain(&mut self, out: &mut Out) {
        while matches!(self.state, State::Idle) {
            match self.pub_state {
                PubState::EndingFailed | PubState::StorageFailed => {
                    self.refuse_queue(out, Fail::Storage);
                    return;
                }
                PubState::Ending | PubState::Closing => return,
                _ => {}
            }
            if self.after_publish.is_some() { return; }
            let Some(queued) = self.queue.pop_front() else { return };
            self.begin(queued, out);
        }
    }

    fn begin(&mut self, queued: Queued, out: &mut Out) {
        match queued {
            Queued::Call { operation, body, reply } => {
                let Some(def) = self.def(&operation) else {
                    out.replies.push((reply, Err(Fail::Invalid)));
                    return;
                };
                let call = Queued::Call { operation: operation.clone(), body: body.clone(), reply };
                let epoch = self.epoch;
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                match def.class {
                    Class::Login if self.credential.is_some() => out.replies.push((reply, Err(Fail::SessionExists))),
                    Class::Login => self.generate(call, out),
                    Class::Authenticated if self.credential.is_none() => self.generate(call, out),
                    Class::Logout if self.credential.is_none() => out.replies.push((reply, Err(Fail::NoCredential))),
                    Class::Sessionless => {
                        let request_id = self.request();
                        self.seal(Pending { request_id, sequence: 0, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, out);
                    }
                    Class::Authenticated | Class::Logout => {
                        let request_id = self.request();
                        self.seal(Pending { request_id, sequence, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, out);
                    }
                }
            }
            Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => {
                if self.pub_state == PubState::Closing || epoch != self.epoch || self.credential.is_none() {
                    out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::NoSession));
                    return;
                }
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                self.seal(Pending { request_id: req_id, sequence, class: Class::Authenticated, operation: String::new(), path, body, credential: None, purpose, author: Some(author), owner, epoch, reply: None }, out);
            }
        }
    }

    fn seal(&mut self, pending: Pending, out: &mut Out) {
        let seal_id = self.attempt();
        out.effects.push(Effect::SealProbe { seal_id, pending: pending.clone(), credential: self.credential.as_ref().map(|credential| credential.digest) });
        self.state = State::Sealing { pending, seal_id };
    }

    fn generate(&mut self, call: Queued, out: &mut Out) {
        let op = self.secret_op();
        out.effects.push(Effect::GenerateCredential { op });
        self.state = State::Generating { call, op };
    }

    fn persist(&mut self, pending: Pending, session: Option<Digest>, out: &mut Out) {
        let batch = self.batch();
        out.effects.push(Effect::CommitPending { batch, epoch: pending.epoch, pending: pending.clone(), session });
        self.state = State::Persisting { pending, batch };
    }

    fn send(&mut self, pending: Pending, attempts: u64, document_sent: bool, out: &mut Out) {
        let attempt_id = self.attempt();
        out.effects.push(Effect::HttpRequest { pending: pending.clone(), attempt_id, full_document: document_sent });
        self.state = State::Calling { pending, attempts, phase: CallPhase::InFlight(attempt_id), document_sent };
    }

    fn start_calling(&mut self, pending: Pending, out: &mut Out) {
        if self.is_online() {
            self.send(pending, 0, false, out);
        } else {
            self.state = State::Calling { pending, attempts: 0, phase: CallPhase::WaitingNetwork, document_sent: false };
        }
    }

    fn land(&mut self, pending: Pending, answer: Answer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool, out: &mut Out) {
        let batch = self.batch();
        let record = pending.author.map(|_| (pending.request_id, answer.clone()));
        let erase_session = pending.class == Class::Login && pending.credential.is_some() && answer.is_err() && !server_ended;
        out.effects.push(Effect::CommitLanding { batch, epoch: pending.epoch, digest: identity, sequence, answer: record, erase_session });
        self.state = State::Landing { pending, batch, answer, identity, sequence, server_ended };
    }

    fn clearing(&mut self, call: Option<Queued>, reason: Option<Fail>, after: AfterClear, out: &mut Out) {
        let op = self.secret_op();
        out.effects.push(Effect::ClearSecret { op });
        self.state = State::ClearingSecret { call, reason, op, after };
    }

    fn land_again(&mut self, pending: Pending, landed: Landed, out: &mut Out) {
        self.land(pending, landed.answer, landed.identity, landed.sequence, landed.server_ended, out);
    }

    fn drained(&mut self, out: &mut Out) {
        self.refuse_queue(out, Fail::Closed);
        self.held = None;
        self.restored_direct = None;
        self.state = State::Closed;
        out.effects.push(Effect::Drained);
    }

    pub fn awaiting(&self) -> Option<(ReplyId, Option<u64>)> { self.after_publish.as_ref().map(|(reply, _, wait)| (*reply, *wait)) }

    fn answer_publication(&mut self, out: &mut Out, epoch: u64, state: PubState, identity: Option<Digest>) {
        let Some((reply, result, wait)) = self.after_publish.take() else { return };
        let digest = self.credential.as_ref().map(|credential| credential.digest);
        let failed = matches!(state, PubState::StorageFailed | PubState::EndingFailed);
        match wait {
            _ if failed => out.replies.push((reply, Err(Fail::Storage))),
            None if state == PubState::Session && identity.is_some() && identity == digest => out.replies.push((reply, result)),
            None if state == PubState::Ending => out.replies.push((reply, Err(Fail::Interrupted))),
            Some(ended) if matches!(state, PubState::Session | PubState::NoSession) && epoch > ended => out.replies.push((reply, result)),
            _ => self.after_publish = Some((reply, result, wait)),
        }
    }

    fn adopt_epoch(&mut self, epoch: u64) {
        if let Some(pending) = &mut self.restored_direct { pending.epoch = epoch; }
        if let State::PendingHeld { pending } | State::Calling { pending, .. } = &mut self.state { pending.epoch = epoch; }
    }

    pub fn step(&mut self, input: Input) -> Out {
        let mut out = Out::default();
        let interest = if let Input::InterestPlus(stream) = &input { (!matches!(self.state, State::Sealing { .. })).then_some(*stream) } else { None };
        self.run(input, &mut out);
        let view = self.view();
        if view != self.reported {
            self.reported = view;
            for stream in &self.subscribers { out.effects.push(Effect::RetryState { stream: *stream, attempts: view.0, phase: view.1 }); }
        }
        if let Some(stream) = interest { if !matches!(self.state, State::Closed) { out.effects.push(Effect::RetryState { stream, attempts: view.0, phase: view.1 }); } }
        out
    }

}

#[path = "ref_session_events.rs"]
mod events;
