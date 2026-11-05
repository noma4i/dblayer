//! Executable table of the owner `Session` in `specs/machines/lifecycle.md`.

use std::collections::VecDeque;

pub type RequestId = u64;
pub type BatchId = u64;
pub type ReplyId = u64;
pub type Digest = u64;

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
    SequenceConflict,
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
pub struct Pending {
    pub request_id: RequestId,
    pub sequence: u64,
    pub operation: String,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Open { credential: Option<Credential>, pending: Option<Pending> },
    SessionCall { operation: String, reply: ReplyId },
    Response { request_id: RequestId, body: String, sequence: Option<u64>, credential: Option<Digest>, revoked: bool },
    HttpError { request_id: RequestId, temporary: bool, auth: bool },
    Committed(BatchId),
    Rejected(BatchId, Reason),
    Tick,
    Online(bool),
    Published { epoch: u64 },
    EpochEnded { epoch: u64, cause: EndCause },
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Store `Commit{encrypted_pending}`.
    CommitPending { batch: BatchId, request_id: RequestId, sequence: u64 },
    /// Store `Commit{encrypted_session{digest or the previous one, sequence}, remove pending}`.
    CommitLanding { batch: BatchId, digest: Option<Digest>, sequence: u64 },
    /// Store `Commit{remove pending}`.
    RemovePending { batch: BatchId },
    HttpRequest { request_id: RequestId },
    HttpCancel { request_id: RequestId },
    ScheduleTick { attempts: u64 },
    CredentialCommitted(Digest),
    ServerEnded { epoch: u64 },
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    Persisting { pending: Pending, batch: BatchId },
    Calling { pending: Pending, attempts: u64, sent: bool },
    Landing { pending: Pending, batch: BatchId, answer: String, identity: Option<Digest> },
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
    pub queue: VecDeque<(String, ReplyId)>,
    pub online: bool,
    pub epoch: u64,
    declared: Vec<String>,
    next_request: RequestId,
    next_batch: BatchId,
    /// Epoch of the call in `Persisting`, `Calling`, `Landing`.
    call_epoch: u64,
}

impl Session {
    pub fn new(declared: &[&str], online: bool) -> Self {
        Self { state: State::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, declared: declared.iter().map(|name| name.to_string()).collect(), next_request: 1, next_batch: 1, call_epoch: 0 }
    }

    fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    fn reply(out: &mut Out, reply: Option<ReplyId>, answer: Answer) { out.replies.extend(reply.map(|id| (id, answer))); }

    /// `Idle`, then the next call of the queue as `Idle x SessionCall`.
    fn idle(&mut self, out: &mut Out) {
        self.state = State::Idle;
        if let Some((operation, reply)) = self.queue.pop_front() { self.session_call(operation, reply, out); }
    }

    fn session_call(&mut self, operation: String, reply: ReplyId, out: &mut Out) {
        if !self.declared.contains(&operation) {
            out.replies.push((reply, Err(Fail::Invalid)));
            self.idle(out);
            return;
        }
        let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
        let request_id = self.next_request;
        self.next_request += 1;
        let batch = self.batch();
        self.call_epoch = self.epoch;
        out.effects.push(Effect::CommitPending { batch, request_id, sequence });
        self.state = State::Persisting { pending: Pending { request_id, sequence, operation, reply: Some(reply) }, batch };
    }

    /// One row of the table per match arm, in table order.
    pub fn step(&mut self, input: Input) -> Out {
        let mut out = Out::default();
        let state = std::mem::replace(&mut self.state, State::Closed);
        match (state, input) {
            (State::Closed, Input::SessionCall { reply, .. }) => out.replies.push((reply, Err(Fail::Closed))),
            (State::Closed, _) => {}
            (state, Input::Closing) => {
                if let Some(Pending { request_id, reply, .. }) = pending_of(&state) {
                    Self::reply(&mut out, reply, Err(Fail::Closed));
                    out.effects.push(Effect::HttpCancel { request_id });
                }
                out.replies.extend(self.queue.drain(..).map(|(_, reply)| (reply, Err(Fail::Closed))));
                out.effects.push(Effect::Drained);
            }
            (state, Input::Online(online)) => {
                self.online = online;
                self.state = state;
                if online {
                    if let State::Calling { pending, sent: sent @ false, .. } = &mut self.state {
                        *sent = true;
                        out.effects.push(Effect::HttpRequest { request_id: pending.request_id });
                    }
                }
            }
            (state, Input::Published { epoch }) => {
                self.epoch = epoch;
                self.state = state;
            }
            (State::Idle, Input::Open { credential, pending }) => {
                self.credential = credential;
                match pending {
                    None => self.state = State::Idle,
                    Some(pending) => {
                        self.next_request = self.next_request.max(pending.request_id + 1);
                        if self.online { out.effects.push(Effect::HttpRequest { request_id: pending.request_id }); }
                        self.state = State::Calling { pending: Pending { reply: None, ..pending }, attempts: 0, sent: self.online };
                    }
                }
            }
            (state, Input::Open { .. }) => self.state = state,
            (State::Idle, Input::SessionCall { operation, reply }) => self.session_call(operation, reply, &mut out),
            (state, Input::SessionCall { operation, reply }) => {
                self.state = state;
                self.queue.push_back((operation, reply));
            }
            (State::Idle, Input::EpochEnded { cause, .. }) => {
                if cause != EndCause::Other { self.credential = None; }
                self.state = State::Idle;
            }
            (state, Input::EpochEnded { epoch, cause }) => {
                if epoch == self.call_epoch && cause != EndCause::Other {
                    let pending = pending_of(&state).expect("a busy state has a pending request");
                    Self::reply(&mut out, pending.reply, Err(Fail::Interrupted));
                    out.effects.push(Effect::HttpCancel { request_id: pending.request_id });
                    out.replies.extend(self.queue.drain(..).map(|(_, reply)| (reply, Err(Fail::NoSession))));
                    self.credential = None;
                    self.state = State::Idle;
                } else {
                    self.state = state;
                }
            }
            (State::Persisting { pending, batch }, Input::Committed(committed)) if committed == batch => {
                let sent = self.online;
                if sent { out.effects.push(Effect::HttpRequest { request_id: pending.request_id }); }
                self.state = State::Calling { pending, attempts: 0, sent };
            }
            (State::Persisting { pending, batch }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::reply(&mut out, pending.reply, Err(Fail::Reason(reason)));
                self.idle(&mut out);
            }
            (State::Calling { pending, attempts, sent }, Input::Response { request_id, body, sequence, credential, revoked }) if request_id == pending.request_id => {
                if revoked {
                    Self::reply(&mut out, pending.reply, Err(Fail::Revoked));
                    let batch = self.batch();
                    out.effects.push(Effect::RemovePending { batch });
                    out.effects.push(Effect::ServerEnded { epoch: self.call_epoch });
                    self.idle(&mut out);
                } else if sequence == Some(pending.sequence) {
                    let batch = self.batch();
                    out.effects.push(Effect::CommitLanding { batch, digest: credential, sequence: pending.sequence });
                    self.state = State::Landing { pending, batch, answer: body, identity: credential };
                } else {
                    Self::reply(&mut out, pending.reply, Err(Fail::SequenceConflict));
                    self.idle(&mut out);
                }
                let _ = (attempts, sent);
            }
            (State::Calling { pending, attempts, sent }, Input::HttpError { request_id, temporary, auth }) if request_id == pending.request_id => {
                if temporary && self.online {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, sent: false };
                } else if temporary {
                    self.state = State::Calling { pending, attempts, sent: false };
                } else {
                    let batch = self.batch();
                    out.effects.push(Effect::RemovePending { batch });
                    Self::reply(&mut out, pending.reply, Err(if auth { Fail::Unauthorized } else { Fail::Http }));
                    self.idle(&mut out);
                }
                let _ = sent;
            }
            (State::Calling { pending, attempts, .. }, Input::Tick) => {
                let sent = self.online;
                if sent { out.effects.push(Effect::HttpRequest { request_id: pending.request_id }); }
                self.state = State::Calling { pending, attempts, sent };
            }
            (State::Landing { pending, batch, answer, identity }, Input::Committed(committed)) if committed == batch => {
                match identity {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        out.effects.push(Effect::CredentialCommitted(digest));
                    }
                    None => {
                        if let Some(credential) = &mut self.credential { credential.sequence = pending.sequence; }
                    }
                }
                Self::reply(&mut out, pending.reply, Ok(answer));
                self.idle(&mut out);
            }
            (State::Landing { pending, batch, .. }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::reply(&mut out, pending.reply, Err(Fail::Reason(reason)));
                self.idle(&mut out);
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
