//! Executable table of the `calls` machine.

use std::collections::{BTreeMap, BTreeSet};

pub type CallId = u64;
pub type RequestId = u64;
pub type ReplyId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fail {
    NoSession,
    Invalid,
    Offline,
    Network,
    Graphql(Vec<String>),
    Http,
    Interrupted,
}

pub type Answer = Result<String, Fail>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Run { epoch: u64, operation: String, reply: ReplyId },
    Response { call_id: CallId, body: String, errors: Vec<String> },
    TransportError { call_id: CallId, error: Fail },
    Dropped { call_id: CallId },
    Online(bool),
    EpochEnded,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Request { call_id: CallId, request_id: RequestId, operation: String },
    Cancel { request_id: RequestId },
    Drained,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallState {
    Sending { request_id: RequestId },
    Done,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Out {
    pub effects: Vec<Effect>,
    pub replies: Vec<(ReplyId, Answer)>,
}

pub struct Calls {
    pub epoch: u64,
    pub online: bool,
    pub declared: BTreeSet<String>,
    pub calls: BTreeMap<CallId, (CallState, ReplyId)>,
    next: u64,
    drained: bool,
}

impl Calls {
    pub fn new(epoch: u64, online: bool, declared: &[&str]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|name| name.to_string()).collect(), calls: BTreeMap::new(), next: 1, drained: false }
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
            Input::Run { epoch, reply, .. } if epoch != self.epoch => out.replies.push((reply, Err(Fail::NoSession))),
            Input::Run { reply, ref operation, .. } if !self.declared.contains(operation) => out.replies.push((reply, Err(Fail::Invalid))),
            Input::Run { reply, .. } if !self.online => out.replies.push((reply, Err(Fail::Offline))),
            Input::Run { reply, operation, .. } => {
                let (call_id, request_id) = (self.next, self.next);
                self.next += 1;
                self.calls.insert(call_id, (CallState::Sending { request_id }, reply));
                out.effects.push(Effect::Request { call_id, request_id, operation });
            }
            Input::Response { call_id, body, errors } if self.sending(call_id) => {
                let answer = if errors.is_empty() { Ok(body) } else { Err(Fail::Graphql(errors)) };
                self.done(call_id, answer, &mut out);
            }
            Input::TransportError { call_id, error } if self.sending(call_id) => self.done(call_id, Err(error), &mut out),
            Input::Dropped { call_id } if self.sending(call_id) => self.done(call_id, Err(Fail::Network), &mut out),
            Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => {}
            Input::Online(online) => self.online = online,
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
        let sending: Vec<(CallId, RequestId)> = self.calls.iter().filter_map(|(id, (state, _))| match state {
            CallState::Sending { request_id } => Some((*id, *request_id)),
            CallState::Done => None,
        }).collect();
        for (call_id, request_id) in sending {
            self.done(call_id, Err(Fail::Interrupted), out);
            out.effects.push(Effect::Cancel { request_id });
        }
    }
}
