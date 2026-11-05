//! Owner `Calls` (`specs/machines/calls.md`): the arbitrary operation `run`, one transport request per call, no cache and no Store write.

use std::collections::{BTreeMap, BTreeSet};

use super::contract::{Drained, Online, RequestId};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub type CallId = u64;
use super::lifecycle::ReplyId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    NoSession,
    Invalid,
    Offline,
    Network,
    Graphql(Vec<String>),
    Http,
    Interrupted,
}

pub type CallAnswer = Result<String, CallError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallPhase {
    Sending { request_id: RequestId },
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallsEffect {
    Request { call_id: CallId, request_id: RequestId, operation: String },
    Cancel { request_id: RequestId },
    Reply { id: ReplyId, answer: CallAnswer },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum CallsInput {
    /// `epoch` is the epoch of the caller: the table answers `NoSession` when it is not the current one.
    Run { epoch: Epoch, operation: String, reply: ReplyId },
    Response { call_id: CallId, body: String, errors: Vec<String> },
    TransportError { call_id: CallId, error: CallError },
    Dropped { call_id: CallId },
    Online(Online),
    EpochEnded,
    Closing,
    Idle,
}

pub struct CallsState {
    epoch: Epoch,
    online: bool,
    declared: BTreeSet<String>,
    calls: BTreeMap<CallId, (CallPhase, ReplyId)>,
    next: u64,
    drained: bool,
}

impl CallsState {
    pub fn new(epoch: Epoch, online: bool, declared: &[&str]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|name| name.to_string()).collect(), calls: BTreeMap::new(), next: 1, drained: false }
    }

    pub fn phase(&self, call: CallId) -> Option<CallPhase> { self.calls.get(&call).map(|(phase, _)| *phase) }

    fn sending(&self, call_id: CallId) -> bool { matches!(self.phase(call_id), Some(CallPhase::Sending { .. })) }

    fn finish(&mut self, call_id: CallId, answer: CallAnswer, fx: &mut Vec<CallsEffect>) {
        if let Some((phase, reply)) = self.calls.get_mut(&call_id) {
            *phase = CallPhase::Done;
            fx.push(CallsEffect::Reply { id: *reply, answer });
        }
    }

    fn interrupt(&mut self, fx: &mut Vec<CallsEffect>) {
        let sending: Vec<(CallId, RequestId)> = self.calls.iter().filter_map(|(id, (phase, _))| match phase {
            CallPhase::Sending { request_id } => Some((*id, *request_id)),
            CallPhase::Done => None,
        }).collect();
        for (call_id, request_id) in sending {
            self.finish(call_id, Err(CallError::Interrupted), fx);
            fx.push(CallsEffect::Cancel { request_id });
        }
    }
}

pub struct Calls;

impl StateOwner for Calls {
    const EPOCHED: bool = true;

    type State = CallsState;
    type Input = CallsInput;
    type Effect = CallsEffect;

    fn step(state: &mut CallsState, input: CallsInput) -> Effects<Self> {
        let mut fx = Vec::new();
        match input {
            CallsInput::Run { epoch, reply, .. } if epoch != state.epoch => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::NoSession) }),
            CallsInput::Run { reply, ref operation, .. } if !state.declared.contains(operation) => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Invalid) }),
            CallsInput::Run { reply, .. } if !state.online => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Offline) }),
            CallsInput::Run { reply, operation, .. } => {
                let id = state.next;
                state.next += 1;
                state.calls.insert(id, (CallPhase::Sending { request_id: id }, reply));
                fx.push(CallsEffect::Request { call_id: id, request_id: id, operation });
            }
            CallsInput::Response { call_id, body, errors } if state.sending(call_id) => {
                let answer = if errors.is_empty() { Ok(body) } else { Err(CallError::Graphql(errors)) };
                state.finish(call_id, answer, &mut fx);
            }
            CallsInput::TransportError { call_id, error } if state.sending(call_id) => state.finish(call_id, Err(error), &mut fx),
            CallsInput::Dropped { call_id } if state.sending(call_id) => state.finish(call_id, Err(CallError::Network), &mut fx),
            CallsInput::Online(Online(online)) => state.online = online,
            CallsInput::EpochEnded => state.interrupt(&mut fx),
            CallsInput::Closing => {
                state.interrupt(&mut fx);
                if !state.drained {
                    state.drained = true;
                    fx.push(CallsEffect::Drained(Drained { owner: "calls" }));
                }
            }
            CallsInput::Response { .. } | CallsInput::TransportError { .. } | CallsInput::Dropped { .. } | CallsInput::Idle => {}
        }
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(_now: Time) -> CallsInput { CallsInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> CallsInput { CallsInput::EpochEnded }

    fn closing() -> CallsInput { CallsInput::Closing }
}
