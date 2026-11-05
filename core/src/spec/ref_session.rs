//! Executable table of the owner `Session` in `specs/machines/lifecycle.md`.

use std::collections::VecDeque;

pub type RequestId = u64;
pub type BatchId = u64;
pub type ReplyId = u64;
pub type AuthorId = u64;
pub type Digest = u64;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub name: String,
    pub path: String,
    pub sessionless: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub request_id: RequestId,
    pub sequence: u64,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub sessionless: bool,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<Owner>,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queued {
    Call { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, path: String, body: String, purpose: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Open { credential: Option<Credential>, pending: Option<Pending> },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, path: String, body: String, purpose: u8 },
    CredentialGenerated(Digest),
    Response { request_id: RequestId, body: String, sequence: Option<u64>, credential: Option<Digest> },
    Revoked { request_id: RequestId, body: String },
    HttpError { request_id: RequestId, temporary: bool, auth: bool },
    Dropped { request_id: RequestId },
    Cancel { req_id: RequestId },
    Committed(BatchId),
    Rejected(BatchId, Reason),
    Tick,
    Online(bool),
    Published { epoch: u64 },
    EpochEnded { epoch: u64, cause: EndCause },
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Response(String),
    Error(Fail),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Store `Commit{encrypted_pending = pending}`.
    CommitPending { batch: BatchId, epoch: u64, pending: Pending },
    /// Store `Commit{encrypted_session{digest or the previous one, sequence}, remove pending}`.
    CommitLanding { batch: BatchId, epoch: u64, digest: Option<Digest>, sequence: Option<u64> },
    /// Store `Commit{remove pending}`.
    RemovePending { batch: BatchId, epoch: u64 },
    HttpRequest { pending: Pending },
    HttpCancel { request_id: RequestId },
    ScheduleTick { attempts: u64 },
    GenerateCredential,
    CredentialCommitted(Digest),
    ServerEnded { epoch: u64 },
    Answer { author: AuthorId, req_id: RequestId, outcome: Outcome },
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    Generating { call: Queued },
    Persisting { pending: Pending, batch: BatchId },
    Calling { pending: Pending, attempts: u64, sent: bool },
    Landing { pending: Pending, batch: BatchId, answer: Answer, identity: Option<Digest>, ended: bool },
    Blocked { pending: Pending, attempts: u64, retry: bool },
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
    pub online: bool,
    pub epoch: u64,
    operations: Vec<Operation>,
    next_request: RequestId,
    next_batch: BatchId,
    /// Epoch of the call in `Generating`, `Persisting`, `Calling`, `Landing`.
    call_epoch: u64,
    restored: bool,
}

impl Session {
    pub fn new(operations: Vec<Operation>, online: bool) -> Self {
        Self { state: State::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, operations, next_request: 1, next_batch: 1, call_epoch: 0, restored: false }
    }

    fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    fn request(&mut self) -> RequestId {
        self.next_request += 1;
        self.next_request - 1
    }

    /// The reply of a call and the answer of a `Mutate` author: the same result for the owner of the pending request.
    fn finish(out: &mut Out, pending: &Pending, result: Answer) {
        if let Some(reply) = pending.reply { out.replies.push((reply, result.clone())); }
        if let Some(author) = pending.author { out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: match result { Ok(body) => Outcome::Response(body), Err(fail) => Outcome::Error(fail) } }); }
    }

    fn refuse(out: &mut Out, queued: Queued, fail: Fail) {
        match queued {
            Queued::Call { reply, .. } => out.replies.push((reply, Err(fail))),
            Queued::Mutate { author, req_id, .. } => out.effects.push(Effect::Answer { author, req_id, outcome: Outcome::Error(fail) }),
        }
    }

    /// `Idle`, then the next queued entry as `Idle x` its input.
    fn idle(&mut self, out: &mut Out) {
        self.state = State::Idle;
        if let Some(queued) = self.queue.pop_front() { self.begin(queued, out); }
    }

    fn begin(&mut self, queued: Queued, out: &mut Out) {
        match queued {
            Queued::Call { operation, body, reply } => {
                let Some(def) = self.operations.iter().find(|def| def.name == operation).cloned() else {
                    out.replies.push((reply, Err(Fail::Invalid)));
                    self.idle(out);
                    return;
                };
                self.call_epoch = self.epoch;
                if !def.sessionless && self.credential.is_none() {
                    out.effects.push(Effect::GenerateCredential);
                    self.state = State::Generating { call: Queued::Call { operation, body, reply } };
                    return;
                }
                let sequence = if def.sessionless { 0 } else { self.credential.as_ref().map_or(1, |credential| credential.sequence + 1) };
                let request_id = self.request();
                let pending = Pending { request_id, sequence, operation, path: def.path, body, sessionless: def.sessionless, credential: None, purpose: 0, author: None, owner: None, reply: Some(reply) };
                self.persist(pending, out);
            }
            Queued::Mutate { author, owner, req_id, path, body, purpose } => {
                let Some(credential) = self.credential.clone() else {
                    out.effects.push(Effect::Answer { author, req_id, outcome: Outcome::Error(Fail::NoSession) });
                    self.idle(out);
                    return;
                };
                self.call_epoch = self.epoch;
                let pending = Pending { request_id: req_id, sequence: credential.sequence + 1, operation: String::new(), path, body, sessionless: false, credential: None, purpose, author: Some(author), owner, reply: None };
                self.persist(pending, out);
            }
        }
    }

    fn persist(&mut self, pending: Pending, out: &mut Out) {
        let batch = self.batch();
        out.effects.push(Effect::CommitPending { batch, epoch: self.call_epoch, pending: pending.clone() });
        self.state = State::Persisting { pending, batch };
    }

    /// One row of the table per match arm, in table order.
    pub fn step(&mut self, input: Input) -> Out {
        let mut out = Out::default();
        let state = std::mem::replace(&mut self.state, State::Closed);
        match (state, input) {
            (State::Closed, Input::SessionCall { reply, .. }) => out.replies.push((reply, Err(Fail::Closed))),
            (State::Closed, Input::Mutate { author, req_id, .. }) => out.effects.push(Effect::Answer { author, req_id, outcome: Outcome::Error(Fail::Closed) }),
            (State::Closed, _) => {}
            (state, Input::Closing) => {
                if let Some(pending) = pending_of(&state) {
                    Self::finish(&mut out, &pending, Err(Fail::Closed));
                    out.effects.push(Effect::HttpCancel { request_id: pending.request_id });
                }
                if let State::Generating { call } = state { Self::refuse(&mut out, call, Fail::Closed); }
                for queued in self.queue.drain(..) { Self::refuse(&mut out, queued, Fail::Closed); }
                out.effects.push(Effect::Drained);
            }
            (state, Input::Online(online)) => {
                self.online = online;
                self.state = state;
                if let State::Calling { pending, sent, .. } = &mut self.state {
                    if online && !*sent {
                        *sent = true;
                        out.effects.push(Effect::HttpRequest { pending: pending.clone() });
                    } else if !online && *sent {
                        *sent = false;
                        out.effects.push(Effect::HttpCancel { request_id: pending.request_id });
                    }
                } else if let State::Blocked { pending, retry: true, .. } = &self.state {
                    if online {
                        let pending = pending.clone();
                        out.effects.push(Effect::HttpRequest { pending: pending.clone() });
                        self.state = State::Calling { pending, attempts: 0, sent: true };
                    }
                }
            }
            (state, Input::Published { epoch }) => {
                self.epoch = epoch;
                if std::mem::take(&mut self.restored) { self.call_epoch = epoch; }
                self.state = state;
            }
            (State::Idle, Input::Open { credential, pending }) => {
                self.credential = credential;
                match pending {
                    None => self.state = State::Idle,
                    Some(pending) => {
                        self.next_request = self.next_request.max(pending.request_id + 1);
                        let author = pending.owner.as_ref().map(author_of).or(pending.author);
                        let pending = Pending { reply: None, author, ..pending };
                        if self.online { out.effects.push(Effect::HttpRequest { pending: pending.clone() }); }
                        self.state = State::Calling { pending, attempts: 0, sent: self.online };
                        if self.epoch == 0 { self.restored = true; } else { self.call_epoch = self.epoch; }
                    }
                }
            }
            (state, Input::Open { .. }) => self.state = state,
            (state @ State::Blocked { .. }, Input::SessionCall { reply, .. }) => {
                self.state = state;
                out.replies.push((reply, Err(Fail::StateLost)));
            }
            (state @ State::Blocked { .. }, Input::Mutate { author, req_id, .. }) => {
                if !matches!(&state, State::Blocked { pending, .. } if pending.request_id == req_id) {
                    out.effects.push(Effect::Answer { author, req_id, outcome: Outcome::Error(Fail::StateLost) });
                }
                self.state = state;
            }
            (State::Idle, Input::SessionCall { operation, body, reply }) => self.begin(Queued::Call { operation, body, reply }, &mut out),
            (State::Idle, Input::Mutate { author, owner, req_id, path, body, purpose }) => self.begin(Queued::Mutate { author, owner, req_id, path, body, purpose }, &mut out),
            (state @ (State::Generating { .. } | State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (state @ (State::Generating { .. } | State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::Mutate { author, owner, req_id, path, body, purpose }) => {
                self.state = state;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, path, body, purpose });
            }
            (State::Generating { call: Queued::Call { operation, body, reply } }, Input::CredentialGenerated(credential)) => {
                let def = self.operations.iter().find(|def| def.name == operation).cloned().expect("a generating call is declared");
                let request_id = self.request();
                let pending = Pending { request_id, sequence: 1, operation, path: def.path, body, sessionless: false, credential: Some(credential), purpose: 0, author: None, owner: None, reply: Some(reply) };
                self.persist(pending, &mut out);
            }
            (State::Idle, Input::EpochEnded { cause, .. }) => {
                if cause != EndCause::Other { self.credential = None; }
                self.state = State::Idle;
            }
            (State::Generating { call }, Input::EpochEnded { epoch, cause }) if epoch == self.call_epoch && cause != EndCause::Other => {
                Self::refuse(&mut out, call, Fail::Interrupted);
                for queued in self.queue.drain(..) { Self::refuse(&mut out, queued, Fail::NoSession); }
                self.credential = None;
                self.state = State::Idle;
            }
            (State::Blocked { .. }, Input::EpochEnded { epoch, cause }) if epoch == self.call_epoch && cause != EndCause::Other => {
                for queued in self.queue.drain(..) { Self::refuse(&mut out, queued, Fail::NoSession); }
                self.credential = None;
                self.state = State::Idle;
            }
            (state, Input::EpochEnded { epoch, cause }) => {
                if epoch == self.call_epoch && cause != EndCause::Other {
                    if let Some(pending) = pending_of(&state) {
                        Self::finish(&mut out, &pending, Err(Fail::Interrupted));
                        out.effects.push(Effect::HttpCancel { request_id: pending.request_id });
                        for queued in self.queue.drain(..) { Self::refuse(&mut out, queued, Fail::NoSession); }
                        self.credential = None;
                        self.state = State::Idle;
                        return out;
                    }
                }
                self.state = state;
            }
            (State::Persisting { pending, batch }, Input::Committed(committed)) if committed == batch => {
                let sent = self.online;
                if sent { out.effects.push(Effect::HttpRequest { pending: pending.clone() }); }
                self.state = State::Calling { pending, attempts: 0, sent };
            }
            (State::Persisting { pending, batch }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish(&mut out, &pending, Err(Fail::Reason(reason)));
                self.idle(&mut out);
            }
            (State::Calling { pending, .. }, Input::Revoked { request_id, .. }) if request_id == pending.request_id => {
                let batch = self.batch();
                out.effects.push(Effect::RemovePending { batch, epoch: self.call_epoch });
                self.state = State::Landing { pending, batch, answer: Err(Fail::Revoked), identity: None, ended: true };
            }
            (State::Calling { pending, .. }, Input::Response { request_id, body, sequence, credential }) if request_id == pending.request_id => {
                if pending.sessionless || sequence == Some(pending.sequence) {
                    let batch = self.batch();
                    let identity = pending.credential.or(credential);
                    out.effects.push(Effect::CommitLanding { batch, epoch: self.call_epoch, digest: identity, sequence: (!pending.sessionless).then_some(pending.sequence) });
                    self.state = State::Landing { pending, batch, answer: Ok(body), identity, ended: false };
                } else if sequence.is_none() {
                    Self::finish(&mut out, &pending, Err(Fail::Protocol));
                    self.idle(&mut out);
                } else {
                    Self::finish(&mut out, &pending, Err(Fail::SequenceConflict));
                    self.idle(&mut out);
                }
            }
            (State::Calling { pending, attempts, .. }, Input::HttpError { request_id, temporary, auth }) if request_id == pending.request_id => {
                if temporary && self.online {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, sent: false };
                } else if temporary {
                    self.state = State::Calling { pending, attempts, sent: false };
                } else {
                    let batch = self.batch();
                    out.effects.push(Effect::RemovePending { batch, epoch: self.call_epoch });
                    let answer = Err(if auth { Fail::Unauthorized } else { Fail::Http });
                    self.state = State::Landing { pending, batch, answer, identity: None, ended: false };
                }
            }
            (State::Calling { pending, attempts, .. }, Input::Dropped { request_id }) if request_id == pending.request_id => {
                if self.online {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, sent: false };
                } else {
                    self.state = State::Calling { pending, attempts, sent: false };
                }
            }
            (state, Input::Cancel { req_id }) => {
                self.queue.retain(|queued| !matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id));
                self.state = state;
            }
            (State::Calling { pending, attempts, sent: false }, Input::Tick) if self.online => {
                out.effects.push(Effect::HttpRequest { pending: pending.clone() });
                self.state = State::Calling { pending, attempts, sent: true };
            }
            (State::Blocked { pending, retry: true, .. }, Input::Tick) if self.online => {
                out.effects.push(Effect::HttpRequest { pending: pending.clone() });
                self.state = State::Calling { pending, attempts: 0, sent: true };
            }
            (State::Landing { pending, batch, answer, identity, ended }, Input::Committed(committed)) if committed == batch => {
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        out.effects.push(Effect::CredentialCommitted(digest));
                    }
                    None if !pending.sessionless && answer.is_ok() => {
                        if let Some(credential) = &mut self.credential { credential.sequence = pending.sequence; }
                    }
                    None => {}
                }
                if ended { out.effects.push(Effect::ServerEnded { epoch: self.call_epoch }); }
                Self::finish(&mut out, &pending, answer);
                self.idle(&mut out);
            }
            (State::Landing { pending, batch, .. }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish(&mut out, &pending, Err(Fail::Reason(reason)));
                if self.online { out.effects.push(Effect::ScheduleTick { attempts: 1 }); }
                self.state = State::Blocked { pending: Pending { reply: None, author: None, ..pending }, attempts: 1, retry: true };
            }
            (state, _) => self.state = state,
        }
        out
    }
}

fn pending_of(state: &State) -> Option<Pending> {
    match state {
        State::Persisting { pending, .. } | State::Calling { pending, .. } | State::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}
