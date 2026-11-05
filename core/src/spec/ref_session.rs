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
    Resetting,
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
    Open { credential: Option<Credential>, pending: Option<Pending>, dropped_pending: bool, secret: Secret },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, epoch: u64, path: String, body: String, purpose: u8 },
    Cancel { req_id: RequestId },
    CredentialGenerated { op: SecretOpId, credential: Digest },
    SecretStored { op: SecretOpId, ok: bool },
    SecretCleared { op: SecretOpId, ok: bool },
    SecretRead { op: SecretOpId, read: Secret },
    SecretMissing { request_id: RequestId, attempt_id: AttemptId, lost: bool },
    Foreground,
    InterestPlus(StreamId),
    InterestMinus(StreamId),
    RestoreReady { epoch: u64 },
    Restored { epoch: u64, pending: Option<Pending> },
    Response { request_id: RequestId, attempt_id: AttemptId, body: String, sequence: Option<u64>, refusal: Option<crate::transport::SessionCode> },
    Revoked { request_id: RequestId, attempt_id: AttemptId, body: String },
    HttpError { request_id: RequestId, attempt_id: AttemptId, temporary: bool, auth: bool, persisted_missing: bool },
    Dropped { request_id: RequestId, attempt_id: AttemptId },
    Committed(BatchId),
    Rejected(BatchId, Reason),
    Tick,
    Online(bool),
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
    Generating { call: Queued, op: SecretOpId },
    Storing { call: Queued, credential_new: Digest, op: SecretOpId, after: AfterStore },
    ClearingSecret { call: Option<Queued>, reason: Option<Fail>, op: SecretOpId, after: AfterClear },
    PendingHeld { pending: Pending },
    SecretMismatch,
    SecretUnavailable { error: String, read: Option<SecretOpId> },
    Persisting { pending: Pending, batch: BatchId },
    Calling { pending: Pending, attempts: u64, phase: CallPhase, document_sent: bool },
    Landing { pending: Pending, batch: BatchId, answer: Answer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool },
    Blocked { pending: Option<Pending>, reason: Block },
    Closed,
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
    pub after_publish: Option<(ReplyId, String)>,
    pub subscribers: BTreeSet<StreamId>,
    pub restored_direct: Option<Pending>,
    operations: Vec<Operation>,
    next_request: RequestId,
    next_batch: BatchId,
    next_attempt: AttemptId,
    next_secret: SecretOpId,
    restore_ready: bool,
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
        Self { state: State::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, pub_state: PubState::Session, after_publish: None, subscribers: BTreeSet::new(), restored_direct: None, operations, next_request: 1, next_batch: 1, next_attempt: 1, next_secret: 1, restore_ready: false, adopt: false, held: None, reported: (0, RetryPhase::Idle) }
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
                        self.persist(Pending { request_id, sequence: 0, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, None, out);
                    }
                    Class::Authenticated | Class::Logout => {
                        let request_id = self.request();
                        self.persist(Pending { request_id, sequence, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, None, out);
                    }
                }
            }
            Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => {
                if self.pub_state == PubState::Closing || epoch != self.epoch || self.credential.is_none() {
                    out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::NoSession));
                    return;
                }
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                self.persist(Pending { request_id: req_id, sequence, class: Class::Authenticated, operation: String::new(), path, body, credential: None, purpose, author: Some(author), owner, epoch, reply: None }, None, out);
            }
        }
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

    fn drained(&mut self, out: &mut Out) {
        self.refuse_queue(out, Fail::Closed);
        self.held = None;
        self.restored_direct = None;
        self.state = State::Closed;
        out.effects.push(Effect::Drained);
    }

    fn answer_publication(&mut self, out: &mut Out, state: PubState, identity: Option<Digest>) {
        let Some((reply, body)) = self.after_publish.take() else { return };
        let digest = self.credential.as_ref().map(|credential| credential.digest);
        if state == PubState::Session && identity.is_some() && identity == digest {
            out.replies.push((reply, Ok(body)));
        } else if matches!(state, PubState::StorageFailed | PubState::EndingFailed) {
            out.replies.push((reply, Err(Fail::Storage)));
        } else if state == PubState::Ending {
            out.replies.push((reply, Err(Fail::Interrupted)));
        } else {
            self.after_publish = Some((reply, body));
        }
    }

    fn adopt_epoch(&mut self, epoch: u64) {
        if let Some(pending) = &mut self.restored_direct { pending.epoch = epoch; }
        if let State::PendingHeld { pending } | State::Calling { pending, .. } = &mut self.state { pending.epoch = epoch; }
    }

    pub fn step(&mut self, input: Input) -> Out {
        let mut out = Out::default();
        let interest = if let Input::InterestPlus(stream) = &input { Some(*stream) } else { None };
        self.run(input, &mut out);
        let view = self.view();
        if view != self.reported {
            self.reported = view;
            for stream in &self.subscribers { out.effects.push(Effect::RetryState { stream: *stream, attempts: view.0, phase: view.1 }); }
        }
        if let Some(stream) = interest { if !matches!(self.state, State::Closed) { out.effects.push(Effect::RetryState { stream, attempts: view.0, phase: view.1 }); } }
        out
    }

    fn run(&mut self, input: Input, out: &mut Out) {
        let state = std::mem::replace(&mut self.state, State::Closed);
        if matches!(state, State::Closed) {
            match input {
                Input::SessionCall { reply, .. } => out.replies.push((reply, Err(Fail::Closed))),
                Input::Mutate { author, req_id, .. } => out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::Closed)),
                _ => {}
            }
            return;
        }
        match input {
            Input::Closing => self.closing(state, out),
            Input::InterestPlus(stream) => {
                self.subscribers.insert(stream);
                self.state = state;
            }
            Input::InterestMinus(stream) => {
                self.subscribers.remove(&stream);
                self.state = state;
            }
            Input::Online(online) => self.online_changed(state, online, out),
            Input::Published { epoch, identity, state: published } => {
                self.epoch = epoch;
                self.pub_state = published;
                self.state = state;
                if std::mem::take(&mut self.adopt) { self.adopt_epoch(epoch); }
                self.answer_publication(out, published, identity);
                if matches!(self.state, State::Idle) { self.drain(out); }
            }
            Input::Restored { epoch, pending } => {
                self.state = state;
                out.effects.push(Effect::RestoreClassified { epoch });
                if let (State::Idle, Some(pending)) = (&self.state, pending) {
                    if pending.owner.is_none() { self.restored_direct = Some(Pending { epoch, ..pending }); }
                }
                if self.restore_ready && matches!(self.state, State::Idle) {
                    if let Some(pending) = self.restored_direct.take() { self.start_calling(pending, out); }
                }
            }
            Input::RestoreReady { .. } => {
                self.state = state;
                self.restore_ready = true;
                if matches!(self.state, State::Idle) {
                    if let Some(pending) = self.restored_direct.take() { self.start_calling(pending, out); }
                }
            }
            Input::Cancel { req_id } => {
                self.state = state;
                if let Some(at) = self.queue.iter().position(|queued| matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id)) {
                    if let Some(removed) = self.queue.remove(at) { Self::refuse(out, removed, Fail::Interrupted); }
                }
            }
            Input::EpochEnded { epoch, cause } => {
                self.restored_direct = None;
                if is_ended(cause) {
                    if let Some((reply, _)) = self.after_publish.take() { out.replies.push((reply, Err(Fail::Interrupted))); }
                    self.held = None;
                }
                self.on_epoch_ended(state, epoch, cause, out);
            }
            input => self.by_state(state, input, out),
        }
    }

    fn on_epoch_ended(&mut self, state: State, epoch: u64, cause: EndCause, out: &mut Out) {
        match state {
            State::Idle => {
                if is_ended(cause) { self.credential = None; }
                self.state = State::Idle;
            }
            State::Generating { call, .. } if is_ended(cause) => {
                Self::refuse(out, call, Fail::Interrupted);
                self.refuse_queue(out, Fail::NoSession);
                self.credential = None;
                self.state = State::Idle;
            }
            State::Storing { call, credential_new, op, after } if is_ended(cause) => {
                self.credential = None;
                let after = if matches!(after, AfterStore::Drain) { AfterStore::Drain } else { AfterStore::Clear(Fail::Interrupted) };
                self.state = State::Storing { call, credential_new, op, after };
            }
            State::ClearingSecret { call, reason, op, after } => {
                if is_ended(cause) { self.credential = None; }
                self.state = State::ClearingSecret { call, reason, op, after };
            }
            State::PendingHeld { .. } | State::Blocked { .. } | State::SecretMismatch | State::SecretUnavailable { .. } if is_ended(cause) => {
                self.credential = None;
                self.refuse_queue(out, Fail::NoSession);
                self.idle(out);
            }
            state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }) => {
                let Some(pending) = pending_of(&state).filter(|pending| epoch == pending.epoch && is_ended(cause)) else {
                    self.state = state;
                    return;
                };
                Self::finish(out, &pending, Err(Fail::Interrupted));
                if let State::Calling { phase: CallPhase::InFlight(attempt_id), .. } = state { out.effects.push(Effect::HttpCancel { attempt_id }); }
                self.refuse_queue(out, Fail::NoSession);
                self.credential = None;
                self.idle(out);
            }
            other => self.state = other,
        }
    }

    fn closing(&mut self, state: State, out: &mut Out) {
        if let Some((reply, _)) = self.after_publish.take() { out.replies.push((reply, Err(Fail::Closed))); }
        match state {
            State::Persisting { pending, .. } | State::Landing { pending, .. } => Self::finish(out, &pending, Err(Fail::Closed)),
            State::Calling { pending, phase, .. } => {
                Self::finish(out, &pending, Err(Fail::Closed));
                if let CallPhase::InFlight(attempt_id) = phase { out.effects.push(Effect::HttpCancel { attempt_id }); }
            }
            State::Generating { call, .. } => Self::refuse(out, call, Fail::Closed),
            State::Storing { call, credential_new, op, .. } => {
                self.refuse_queue(out, Fail::Closed);
                self.state = State::Storing { call, credential_new, op, after: AfterStore::Drain };
                return;
            }
            State::ClearingSecret { call, reason, op, .. } => {
                self.refuse_queue(out, Fail::Closed);
                self.state = State::ClearingSecret { call, reason, op, after: AfterClear::Drain };
                return;
            }
            _ => {}
        }
        self.drained(out);
    }

    fn online_changed(&mut self, state: State, online: bool, out: &mut Out) {
        self.online = Some(online);
        match state {
            State::Calling { pending, attempts, phase: CallPhase::InFlight(attempt_id), document_sent } if !online => {
                out.effects.push(Effect::HttpCancel { attempt_id });
                if let Some(author) = pending.author {
                    out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: Outcome::Dropped });
                    self.state = State::PendingHeld { pending };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent } if online => self.send(pending, attempts, document_sent, out),
            State::Blocked { pending: Some(pending), reason: Block::Storage } if online && pending.author.is_none() => self.send(pending, 0, false, out),
            other => self.state = other,
        }
    }

    fn open(&mut self, credential: Option<Credential>, pending: Option<Pending>, dropped_pending: bool, secret: Secret, out: &mut Out) {
        self.credential = credential.clone();
        if dropped_pending {
            self.state = State::Blocked { pending: None, reason: Block::StateLost };
            return;
        }
        let secret = match secret {
            Secret::Error(error) => {
                self.state = State::SecretUnavailable { error, read: None };
                return;
            }
            other => other,
        };
        self.state = State::Idle;
        match (credential, secret) {
            (None, Secret::Missing) => {}
            (None, _) => self.clearing(None, None, AfterClear::Idle, out),
            (Some(_), Secret::Missing) => self.missing_secret(out),
            (Some(credential), Secret::Present(digest)) if digest != credential.digest => self.state = State::SecretMismatch,
            (Some(_), _) => {
                match pending {
                    Some(pending) if pending.owner.is_some() => {
                        let author = pending.owner.as_ref().map(author_of).or(pending.author);
                        self.adopt = true;
                        self.state = State::PendingHeld { pending: Pending { reply: None, author, ..pending } };
                    }
                    Some(pending) if pending.class == Class::Login => {
                        self.adopt = true;
                        self.start_calling(Pending { reply: None, ..pending }, out);
                    }
                    _ => {}
                }
            }
        }
    }

    fn missing_secret(&mut self, out: &mut Out) {
        self.credential = None;
        out.effects.push(Effect::SessionReset);
    }

    fn idle_call(&mut self, operation: String, body: String, reply: ReplyId, out: &mut Out) {
        self.state = State::Idle;
        if self.def(&operation).is_none() {
            out.replies.push((reply, Err(Fail::Invalid)));
            return;
        }
        match self.pub_state {
            PubState::EndingFailed | PubState::StorageFailed => out.replies.push((reply, Err(Fail::Storage))),
            PubState::Ending => self.queue.push_back(Queued::Call { operation, body, reply }),
            _ => self.begin(Queued::Call { operation, body, reply }, out),
        }
    }

    fn held_call(&mut self, held: State, operation: String, body: String, reply: ReplyId, fail: Fail, out: &mut Out) {
        match self.def(&operation) {
            None => {
                out.replies.push((reply, Err(Fail::Invalid)));
                self.state = held;
            }
            Some(def) if def.class != Class::Sessionless => {
                out.replies.push((reply, Err(fail)));
                self.state = held;
            }
            Some(_) => {
                self.held = Some(held);
                self.state = State::Idle;
                self.begin(Queued::Call { operation, body, reply }, out);
            }
        }
    }

    fn by_state(&mut self, state: State, input: Input, out: &mut Out) {
        match (state, input) {
            (State::Idle, Input::Open { credential, pending, dropped_pending, secret }) => self.open(credential, pending, dropped_pending, secret, out),
            (state, Input::Open { .. }) => self.state = state,
            (State::Idle, Input::SessionCall { operation, body, reply }) => self.idle_call(operation, body, reply, out),
            (State::Idle, Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = State::Idle;
                self.begin(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }, out);
            }
            (State::SecretMismatch, Input::SessionCall { operation, body, reply }) => self.held_call(State::SecretMismatch, operation, body, reply, Fail::StateLost, out),
            (State::SecretMismatch, Input::Mutate { author, req_id, .. }) => {
                self.state = State::SecretMismatch;
                out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::StateLost));
            }
            (state @ State::SecretUnavailable { .. }, Input::SessionCall { operation, body, reply }) => self.held_call(state, operation, body, reply, Fail::Storage, out),
            (State::SecretUnavailable { error, read }, Input::Mutate { author, req_id, .. }) => {
                self.state = State::SecretUnavailable { error, read };
                out.effects.push(author_error(author, req_id, Kind::Temporary, Fail::Storage));
            }
            (State::SecretUnavailable { error, .. }, Input::Foreground) => {
                let op = self.secret_op();
                out.effects.push(Effect::ReadSecret { op });
                self.state = State::SecretUnavailable { error, read: Some(op) };
            }
            (State::SecretUnavailable { error, read }, Input::SecretRead { op, read: found }) if read == Some(op) => match found {
                Secret::Present(digest) if self.credential.as_ref().is_some_and(|credential| credential.digest == digest) => self.idle(out),
                Secret::Present(_) => self.state = State::SecretMismatch,
                Secret::Missing => {
                    self.state = State::Idle;
                    self.missing_secret(out);
                }
                Secret::Error(_) => self.state = State::SecretUnavailable { error, read: None },
            },
            (State::Generating { call, op }, Input::CredentialGenerated { op: seen, credential }) if seen == op => {
                let next = self.secret_op();
                out.effects.push(Effect::StoreSecret { credential, op: next });
                self.state = State::Storing { call, credential_new: credential, op: next, after: AfterStore::Persist };
            }
            (State::Storing { call, credential_new, op, after }, Input::SecretStored { op: seen, ok }) if seen == op => match (after, ok) {
                (AfterStore::Persist, true) => {
                    let Queued::Call { operation, body, reply } = call else { unreachable!("a generating call is a call") };
                    let def = self.def(&operation).expect("a generating call is declared");
                    let request_id = self.request();
                    let epoch = self.epoch;
                    self.persist(Pending { request_id, sequence: 1, class: Class::Login, operation, path: def.path, body, credential: Some(credential_new), purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, Some(credential_new), out);
                }
                (AfterStore::Persist, false) => self.clearing(Some(call), Some(Fail::Storage), AfterClear::Idle, out),
                (AfterStore::Clear(reason), _) => self.clearing(Some(call), Some(reason), AfterClear::Idle, out),
                (AfterStore::Drain, _) => {
                    Self::refuse(out, call, Fail::Closed);
                    self.drained(out);
                }
            },
            (State::ClearingSecret { call, reason, op, after }, Input::SecretCleared { op: seen, .. }) if seen == op => {
                if let (Some(call), Some(reason)) = (call, reason) { Self::refuse(out, call, reason); }
                match after {
                    AfterClear::Idle => self.idle(out),
                    AfterClear::Drain => self.drained(out),
                }
            }
            (state @ (State::Generating { .. } | State::Storing { .. } | State::ClearingSecret { .. }), Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (state @ (State::Generating { .. } | State::Storing { .. } | State::ClearingSecret { .. }), Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = state;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (State::PendingHeld { pending }, Input::Mutate { req_id, .. }) if req_id == pending.request_id => self.start_calling(pending, out),
            (State::PendingHeld { pending }, Input::SessionCall { operation, body, reply }) => {
                self.queue.push_back(Queued::Call { operation, body, reply });
                self.state = State::PendingHeld { pending };
            }
            (State::PendingHeld { pending }, Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = State::PendingHeld { pending };
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (State::Blocked { pending, reason }, Input::SessionCall { reply, .. }) => {
                self.state = State::Blocked { pending, reason };
                out.replies.push((reply, Err(Fail::StateLost)));
            }
            (State::Blocked { pending, reason }, Input::Mutate { author, req_id, .. }) => {
                let same = pending.as_ref().is_some_and(|pending| pending.request_id == req_id);
                match (same, pending, reason) {
                    (true, Some(pending), Block::Storage) => self.start_calling(pending, out),
                    (true, pending, reason) => {
                        out.effects.push(author_error(author, req_id, Kind::Permanent, blocked_fail(reason)));
                        self.state = State::Blocked { pending, reason };
                    }
                    (false, pending, reason) => {
                        out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::StateLost));
                        self.state = State::Blocked { pending, reason };
                    }
                }
            }
            (State::Blocked { pending: Some(pending), reason: Block::Storage }, Input::Tick) if self.is_online() && pending.author.is_none() => self.send(pending, 0, false, out),
            (State::Persisting { pending, batch }, Input::Committed(committed)) if committed == batch => self.start_calling(pending, out),
            (State::Persisting { pending, batch }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish(out, &pending, Err(Fail::Reason(reason)));
                self.idle(out);
            }
            (state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                let current = pending_of(&state).is_some_and(|pending| pending.request_id == req_id);
                self.state = state;
                if !current { self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }); }
            }
            (state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (State::Calling { pending, attempts, phase, document_sent }, input) => self.calling(pending, attempts, phase, document_sent, input, out),
            (State::Landing { pending, batch, answer, identity, sequence, server_ended }, Input::Committed(committed)) if committed == batch => {
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        if let (Some(reply), Ok(body)) = (pending.reply, &answer) { self.after_publish = Some((reply, body.clone())); }
                        out.effects.push(Effect::CredentialCommitted(digest));
                    }
                    None if pending.class == Class::Login && pending.credential.is_some() && answer.is_err() && !server_ended => {
                        self.credential = None;
                        let call = pending.reply.map(|reply| Queued::Call { operation: pending.operation.clone(), body: pending.body.clone(), reply });
                        let reason = if call.is_some() { answer.err() } else { None };
                        self.clearing(call, reason, AfterClear::Idle, out);
                        return;
                    }
                    None => {
                        if let (Some(sequence), Some(credential)) = (sequence, &mut self.credential) { credential.sequence = sequence; }
                        if server_ended { out.effects.push(Effect::ServerEnded { epoch: pending.epoch }); }
                        Self::finish(out, &pending, answer);
                    }
                }
                self.idle(out);
            }
            (State::Landing { pending, batch, .. }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish(out, &pending, Err(Fail::Reason(reason)));
                self.state = State::Blocked { pending: Some(Pending { reply: None, ..pending }), reason: Block::Storage };
            }
            (state, _) => self.state = state,
        }
    }

    fn queue_mutate(&mut self, queued: Queued) {
        let Queued::Mutate { req_id, .. } = &queued else { return };
        if !self.queue.iter().any(|entry| matches!(entry, Queued::Mutate { req_id: seen, .. } if seen == req_id)) { self.queue.push_back(queued); }
    }

    fn calling(&mut self, pending: Pending, attempts: u64, phase: CallPhase, document_sent: bool, input: Input, out: &mut Out) {
        let mine = |request_id: RequestId, attempt_id: AttemptId| pending.request_id == request_id && phase == CallPhase::InFlight(attempt_id);
        match input {
            Input::Response { request_id, attempt_id, body, sequence, refusal } if mine(request_id, attempt_id) => {
                if let Some(code) = refusal {
                    let confirmed = (pending.class != Class::Sessionless && sequence == Some(pending.sequence)).then_some(pending.sequence);
                    self.land(pending, Err(Fail::Session(code)), None, confirmed, false, out);
                } else if pending.class == Class::Sessionless {
                    self.land(pending, Ok(body), None, None, false, out);
                } else if sequence == Some(pending.sequence) {
                    let identity = pending.credential;
                    let sequence = pending.sequence;
                    self.land(pending, Ok(body), identity, Some(sequence), false, out);
                } else {
                    let (reason, fail) = if sequence.is_none() { (Block::Protocol, Fail::Protocol) } else { (Block::SequenceConflict, Fail::SequenceConflict) };
                    Self::finish(out, &pending, Err(fail));
                    self.state = State::Blocked { pending: Some(Pending { reply: None, ..pending }), reason };
                }
            }
            Input::Revoked { request_id, attempt_id, body } if mine(request_id, attempt_id) => {
                let answer = if pending.class == Class::Logout { Ok(body) } else { Err(Fail::Revoked) };
                self.land(pending, answer, None, None, true, out);
            }
            Input::HttpError { request_id, attempt_id, persisted_missing: true, .. } if mine(request_id, attempt_id) && !document_sent => self.send(pending, attempts, true, out),
            Input::HttpError { request_id, attempt_id, temporary: true, persisted_missing: false, .. } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    out.effects.push(author_error(author, pending.request_id, Kind::Temporary, Fail::Http));
                    self.state = State::PendingHeld { pending };
                } else if self.is_online() {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry, document_sent };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            Input::HttpError { request_id, attempt_id, auth, .. } if mine(request_id, attempt_id) => {
                let fail = if auth { Fail::Unauthorized } else { Fail::Http };
                self.land(pending, Err(fail), None, None, false, out);
            }
            Input::Dropped { request_id, attempt_id } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: Outcome::Dropped });
                    self.state = State::PendingHeld { pending };
                } else if self.is_online() {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry, document_sent };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            Input::SecretMissing { request_id, attempt_id, lost } if mine(request_id, attempt_id) => {
                if lost {
                    Self::finish(out, &pending, Err(Fail::StateLost));
                    self.state = State::SecretMismatch;
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingSecret, document_sent };
                }
            }
            Input::Tick if phase == CallPhase::WaitingRetry && self.is_online() => self.send(pending, attempts, document_sent, out),
            Input::Foreground if phase == CallPhase::WaitingSecret => {
                if self.is_online() {
                    self.send(pending, attempts, document_sent, out);
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork, document_sent };
                }
            }
            _ => self.state = State::Calling { pending, attempts, phase, document_sent },
        }
    }
}
