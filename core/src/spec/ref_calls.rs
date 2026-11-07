//! Executable table of the `calls` machine.

use std::collections::BTreeMap;

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
    InvalidResponse,
    Interrupted,
}

pub type Answer = Result<String, Fail>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Run { epoch: u64, operation: String, reply: ReplyId },
    Declare(Vec<(String, Kind)>),
    Response { call_id: CallId, body: Option<String>, errors: Vec<String> },
    TransportError { call_id: CallId, error: Fail },
    Dropped { call_id: CallId },
    Online(bool),
    Snapshot(bool),
    EpochStarted(u64),
    EpochEnded,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Request { call_id: CallId, request_id: RequestId, operation: String, kind: Kind },
    Cancel { request_id: RequestId, kind: Kind },
    Release { request_id: RequestId },
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
    next: u64,
    drained: bool,
}

impl Calls {
    pub fn new(epoch: u64, online: Option<bool>, declared: &[(&str, Kind)]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|(name, kind)| (name.to_string(), *kind)).collect(), generation: 0, calls: BTreeMap::new(), next: 1, drained: false }
    }

    pub fn state(&self, call: CallId) -> Option<CallState> { self.calls.get(&call).map(|(state, _)| *state) }

    fn done(&mut self, call_id: CallId, answer: Answer, out: &mut Out) {
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
            Input::Run { reply, .. } if self.online != Some(true) => out.replies.push((reply, Err(Fail::Offline))),
            Input::Run { reply, operation, .. } => {
                let (call_id, request_id) = (self.next, self.next);
                self.next += 1;
                let kind = self.declared[&operation];
                self.calls.insert(call_id, (CallState::Sending { request_id, kind, generation: self.generation }, reply));
                out.effects.push(Effect::Request { call_id, request_id, operation, kind });
            }
            Input::Response { call_id, body, errors } if self.sending(call_id) => {
                let answer = match (errors.is_empty(), body) {
                    (false, _) => Err(Fail::Graphql(errors)),
                    (true, Some(body)) => Ok(body),
                    (true, None) => Err(Fail::InvalidResponse),
                };
                self.done(call_id, answer, &mut out);
            }
            Input::TransportError { call_id, error } if self.sending(call_id) => self.done(call_id, Err(error), &mut out),
            Input::Dropped { call_id } if self.sending(call_id) => {
                if let Some(CallState::Sending { request_id, kind: Kind::Mutate, .. }) = self.state(call_id) { out.effects.push(Effect::Release { request_id }); }
                self.done(call_id, Err(Fail::Dropped), &mut out);
            }
            Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => {}
            Input::Online(online) if self.online.is_some() => self.online = Some(online),
            Input::Online(_) => {}
            Input::Snapshot(online) => self.online = Some(online),
            Input::EpochStarted(epoch) => {
                self.epoch = epoch;
                self.online = None;
                self.calls.clear();
            }
            Input::EpochEnded => self.interrupt(&mut out),
            Input::Closing => {
                self.interrupt(&mut out);
                if !self.drained {
                    self.drained = true;
                    out.effects.push(Effect::Drained);
                }
            }
        }
        out
    }

    fn interrupt(&mut self, out: &mut Out) {
        let sending: Vec<(CallId, RequestId, Kind)> = self.calls.iter().filter_map(|(id, (state, _))| match state {
            CallState::Sending { request_id, kind, .. } => Some((*id, *request_id, *kind)),
            CallState::Done => None,
        }).collect();
        for (call_id, request_id, kind) in sending {
            self.done(call_id, Err(Fail::Interrupted), out);
            out.effects.push(Effect::Cancel { request_id, kind });
        }
    }
}
