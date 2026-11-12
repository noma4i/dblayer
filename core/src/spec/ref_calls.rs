//! Executable table of the `calls` machine.

use std::collections::{BTreeMap, BTreeSet};

pub type CallId = u64;
pub type RequestId = u64;
pub type ReplyId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Query,
    Mutate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fail {
    NoSession,
    Invalid,
    Offline,
    Network,
    Graphql(Vec<String>),
    Http,
    Dropped,
    NotSent,
    InvalidResponse,
    Interrupted,
    SessionEnded,
    Closed,
}

pub type Answer = Result<String, Fail>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End { Session, Close }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Run { epoch: u64, operation: String, reply: ReplyId },
    Declare(Vec<(String, Kind)>),
    Response { call_id: CallId, body: Option<String>, errors: Vec<String> },
    Transient { call_id: CallId, reason: String },
    TransportError { call_id: CallId, error: Fail },
    Dropped { call_id: CallId },
    RetryDue { call_id: CallId, attempt: u32 },
    Deadline { call_id: CallId },
    CallerCancel { reply: ReplyId },
    Online(bool),
    Snapshot(bool),
    EpochStarted(u64),
    EpochEnded(End),
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Request { call_id: CallId, request_id: RequestId, operation: String, kind: Kind },
    Cancel { request_id: RequestId, kind: Kind },
    Release { request_id: RequestId },
    SnapshotRequest { epoch: u64 },
    Drained,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallState {
    Sending { request_id: RequestId, kind: Kind, generation: u64 },
    Done,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Out {
    pub effects: Vec<Effect>,
    pub replies: Vec<(ReplyId, Answer)>,
}

pub struct Calls {
    pub epoch: u64,
    pub online: Option<bool>,
    pub declared: BTreeMap<String, Kind>,
    pub generation: u64,
    pub calls: BTreeMap<CallId, (CallState, ReplyId)>,
    awaiting: Vec<(CallId, String, ReplyId, Kind, u64)>,
    retries: BTreeMap<CallId, (u32, Fail, bool, String)>,
    releasing: BTreeSet<CallId>,
    next: u64,
    drained: bool,
}

impl Calls {
    pub fn new(epoch: u64, online: Option<bool>, declared: &[(&str, Kind)]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|(name, kind)| (name.to_string(), *kind)).collect(), generation: 0, calls: BTreeMap::new(), awaiting: Vec::new(), retries: BTreeMap::new(), releasing: BTreeSet::new(), next: 1, drained: false }
    }

    pub fn state(&self, call: CallId) -> Option<CallState> { self.calls.get(&call).map(|(state, _)| *state) }

    fn done(&mut self, call_id: CallId, answer: Answer, out: &mut Out) {
        self.releasing.remove(&call_id);
        if let Some((state, reply)) = self.calls.get_mut(&call_id) {
            *state = CallState::Done;
            out.replies.push((*reply, answer));
        }
    }

    fn sending(&self, call_id: CallId) -> bool { matches!(self.state(call_id), Some(CallState::Sending { .. })) }

    /// One row of the table per match arm.
    pub fn step(&mut self, input: Input) -> Out {
        let mut out = Out::default();
        match input {
            Input::Declare(operations) => {
                self.declared = operations.into_iter().collect();
                self.generation += 1;
            }
            Input::Run { epoch, reply, .. } if epoch != self.epoch => out.replies.push((reply, Err(Fail::NoSession))),
            Input::Run { reply, ref operation, .. } if !self.declared.contains_key(operation) => out.replies.push((reply, Err(Fail::Invalid))),
            Input::Run { reply, operation, .. } if self.online.is_none() => {
                let call_id = self.next;
                self.next += 1;
                self.awaiting.push((call_id, operation.clone(), reply, self.declared[&operation], self.generation));
            }
            Input::Run { reply, operation, .. } => self.start(operation, reply, &mut out),
            Input::Response { call_id, body, errors } if self.sending(call_id) => {
                let answer = match (errors.is_empty(), body) {
                    (false, _) => Err(Fail::Graphql(errors)),
                    (true, Some(body)) => Ok(body),
                    (true, None) => Err(Fail::InvalidResponse),
                };
                self.done(call_id, answer, &mut out);
            }
            Input::TransportError { call_id, error } if self.sending(call_id) && self.releasing.contains(&call_id) => self.done(call_id, Err(if error == Fail::NotSent { Fail::Network } else { Fail::Dropped }), &mut out),
            Input::Dropped { call_id } if self.sending(call_id) && self.releasing.contains(&call_id) => self.done(call_id, Err(Fail::Dropped), &mut out),
            Input::TransportError { call_id, error } if self.sending(call_id) => self.failed(call_id, error, &mut out),
            Input::Transient { call_id, reason } if matches!(self.state(call_id), Some(CallState::Sending { kind: Kind::Query, .. })) => self.failed(call_id, Fail::Graphql(vec![reason]), &mut out),
            Input::Transient { call_id, .. } if self.sending(call_id) => self.done(call_id, Err(Fail::InvalidResponse), &mut out),
            Input::Dropped { call_id } if self.sending(call_id) => self.failed(call_id, Fail::Dropped, &mut out),
            Input::RetryDue { call_id, attempt } => self.retry(call_id, attempt, &mut out),
            Input::Deadline { call_id } => {
                if let Some(position) = self.awaiting.iter().position(|(awaiting, ..)| *awaiting == call_id) {
                    let (_, _, reply, _, _) = self.awaiting.remove(position);
                    out.replies.push((reply, Err(Fail::Network)));
                }
                if let Some(CallState::Sending { request_id, kind, .. }) = self.state(call_id) {
                    let (attempt, last) = self.retries.get(&call_id).map_or((1, Fail::Network), |entry| (entry.0, entry.1.clone()));
                    if kind == Kind::Query {
                        out.effects.push(Effect::Cancel { request_id, kind });
                        self.done(call_id, Err(if last == Fail::Dropped { Fail::Network } else { last }), &mut out);
                    } else if attempt > 1 {
                        out.effects.push(Effect::Release { request_id });
                        self.done(call_id, Err(last), &mut out);
                    } else if self.releasing.insert(call_id) {
                        out.effects.push(Effect::Release { request_id });
                    }
                }
            }
            Input::CallerCancel { reply } => {
                if let Some((_, (CallState::Sending { request_id, kind, .. }, _))) = self.calls.iter().find(|(_, (_, found))| *found == reply) {
                    out.effects.push(if *kind == Kind::Query { Effect::Cancel { request_id: *request_id, kind: *kind } } else { Effect::Release { request_id: *request_id } });
                }
                if let Some((_, (state, _))) = self.calls.iter_mut().find(|(_, (_, found))| *found == reply) { *state = CallState::Done; }
                self.awaiting.retain(|(_, _, found, _, _)| *found != reply);
            }
            Input::Response { .. } | Input::Transient { .. } | Input::TransportError { .. } | Input::Dropped { .. } => {}
            Input::Online(online) if self.online.is_some() => {
                self.online = Some(online);
                if online {
                    let waiting: Vec<_> = self.retries.iter().filter_map(|(id, (attempt, _, waiting, _))| waiting.then_some((*id, *attempt))).collect();
                    for (id, attempt) in waiting {
                        if let Some(entry) = self.retries.get_mut(&id) { entry.2 = false; }
                        self.retry(id, attempt - 1, &mut out);
                    }
                }
            }
            Input::Online(_) => {}
            Input::Snapshot(online) => {
                self.online = Some(online);
                for (call_id, operation, reply, kind, generation) in std::mem::take(&mut self.awaiting) {
                    self.start_as(call_id, operation, reply, Some((kind, generation)), &mut out);
                }
            }
            Input::EpochStarted(epoch) => {
                self.epoch = epoch;
                self.online = None;
                self.calls.clear();
                self.awaiting.clear();
                self.retries.clear();
                self.releasing.clear();
                out.effects.push(Effect::SnapshotRequest { epoch });
            }
            Input::EpochEnded(end) => self.interrupt(match end { End::Session => Fail::SessionEnded, End::Close => Fail::Closed }, &mut out),
            Input::Closing => {
                self.interrupt(Fail::Closed, &mut out);
                if !self.drained {
                    self.drained = true;
                    out.effects.push(Effect::Drained);
                }
            }
        }
        out
    }

    fn start(&mut self, operation: String, reply: ReplyId, out: &mut Out) {
        let call_id = self.next;
        self.next += 1;
        self.start_as(call_id, operation, reply, None, out);
    }

    fn start_as(&mut self, call_id: CallId, operation: String, reply: ReplyId, accepted: Option<(Kind, u64)>, out: &mut Out) {
        if self.online != Some(true) {
            out.replies.push((reply, Err(Fail::Offline)));
            return;
        }
        let Some((kind, generation)) = accepted.or_else(|| self.declared.get(&operation).copied().map(|kind| (kind, self.generation))) else {
            out.replies.push((reply, Err(Fail::Invalid)));
            return;
        };
        let request_id = call_id;
        self.calls.insert(call_id, (CallState::Sending { request_id, kind, generation }, reply));
        self.retries.insert(call_id, (1, Fail::Network, false, operation.clone()));
        out.effects.push(Effect::Request { call_id, request_id, operation, kind });
    }

    fn failed(&mut self, call_id: CallId, error: Fail, out: &mut Out) {
        let Some((attempt, _, _, operation)) = self.retries.get(&call_id).cloned() else { return };
        let Some(CallState::Sending { request_id, kind, .. }) = self.state(call_id) else { return };
        let limit = if kind == Kind::Mutate { 40 } else if matches!(error, Fail::Graphql(_)) { 2 } else { 3 };
        if !matches!(error, Fail::Network | Fail::Dropped | Fail::Graphql(_)) || attempt >= limit {
            if kind == Kind::Mutate && error == Fail::Dropped { out.effects.push(Effect::Release { request_id }); }
            self.done(call_id, Err(error), out);
        } else {
            self.retries.insert(call_id, (attempt + 1, error, self.online != Some(true), operation));
        }
    }

    fn retry(&mut self, call_id: CallId, attempt: u32, out: &mut Out) {
        let Some((next, error, waiting_network, operation)) = self.retries.get(&call_id).cloned() else { return };
        if next != attempt + 1 || waiting_network || self.releasing.contains(&call_id) { return; }
        let Some(CallState::Sending { request_id, kind, generation }) = self.state(call_id) else { return };
        let request_id = if kind == Kind::Query { let id = self.next; self.next += 1; id } else { request_id };
        if let Some((state, _)) = self.calls.get_mut(&call_id) { *state = CallState::Sending { request_id, kind, generation }; }
        self.retries.insert(call_id, (next, error, false, operation.clone()));
        out.effects.push(Effect::Request { call_id, request_id, operation, kind });
    }

    fn interrupt(&mut self, cause: Fail, out: &mut Out) {
        for (_, _, reply, _, _) in std::mem::take(&mut self.awaiting) {
            out.replies.push((reply, Err(cause.clone())));
        }
        let sending: Vec<(CallId, RequestId, Kind)> = self.calls.iter().filter_map(|(id, (state, _))| match state {
            CallState::Sending { request_id, kind, .. } => Some((*id, *request_id, *kind)),
            CallState::Done => None,
        }).collect();
        for (call_id, request_id, kind) in sending {
            self.done(call_id, Err(cause.clone()), out);
            out.effects.push(Effect::Cancel { request_id, kind });
        }
    }
}
