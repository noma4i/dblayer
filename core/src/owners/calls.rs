//! Owner `Calls`: the arbitrary operation `run`, one transport request per call, no cache and no Store write.

use std::collections::BTreeMap;

use super::contract::{Drained, Online, RequestId};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub type CallId = u64;
use super::contract::ReplyId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    NoSession,
    Invalid,
    Offline,
    Network,
    Graphql { messages: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    Http { status: u16, detail: String },
    /// A query the server refused with a session code: the call ends, the session stays.
    Session { code: crate::transport::SessionCode, detail: String },
    Dropped,
    InvalidResponse,
    Interrupted,
    Storage,
}

pub type CallAnswer = Result<String, CallError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    Query,
    Mutate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallPhase {
    Sending { request_id: RequestId, kind: CallKind, generation: u64 },
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallsEffect {
    Request { call_id: CallId, request_id: RequestId, operation: String, variables: String, kind: CallKind, epoch: Epoch },
    Cancel { request_id: RequestId, kind: CallKind },
    /// Session `Release{req_id}`: the pair of a dropped mutation stays with Session, Calls gives up its outcome.
    Release { request_id: RequestId },
    Reply { id: ReplyId, answer: CallAnswer },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum CallsInput {
    /// `epoch` is the epoch of the caller: the table answers `NoSession` when it is not the current one.
    Run { epoch: Epoch, operation: String, variables: String, reply: ReplyId },
    /// Operations declared by `register_operations` after the open.
    Declare(Vec<(String, CallKind)>),
    /// `body` is the root of the operation, `None` when the response has no root.
    Response { call_id: CallId, body: Option<String>, errors: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    TransportError { call_id: CallId, error: CallError },
    Dropped { call_id: CallId },
    Online(Online),
    EpochEnded,
    Closing,
    Idle,
}

pub struct CallsState {
    epoch: Epoch,
    online: Option<bool>,
    declared: BTreeMap<String, CallKind>,
    generation: u64,
    calls: BTreeMap<CallId, (CallPhase, ReplyId)>,
    next: u64,
    drained: bool,
}

impl CallsState {
    pub fn new(epoch: Epoch, online: Option<bool>, declared: &[(&str, CallKind)]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|(name, kind)| (name.to_string(), *kind)).collect(), generation: 0, calls: BTreeMap::new(), next: 1, drained: false }
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
        let sending: Vec<(CallId, RequestId, CallKind)> = self.calls.iter().filter_map(|(id, (phase, _))| match phase {
            CallPhase::Sending { request_id, kind, .. } => Some((*id, *request_id, *kind)),
            CallPhase::Done => None,
        }).collect();
        for (call_id, request_id, kind) in sending {
            self.finish(call_id, Err(CallError::Interrupted), fx);
            fx.push(CallsEffect::Cancel { request_id, kind });
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
            CallsInput::Run { reply, ref operation, .. } if !state.declared.contains_key(operation) => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Invalid) }),
            CallsInput::Run { reply, .. } if state.online != Some(true) => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Offline) }),
            CallsInput::Run { reply, operation, variables, .. } => {
                let id = state.next;
                state.next += 1;
                let kind = state.declared[&operation];
                state.calls.insert(id, (CallPhase::Sending { request_id: id, kind, generation: state.generation }, reply));
                fx.push(CallsEffect::Request { call_id: id, request_id: id, operation, variables, kind, epoch: state.epoch });
            }
            CallsInput::Response { call_id, body, errors, extensions } if state.sending(call_id) => {
                let answer = match (errors.is_empty(), body) {
                    (false, _) => Err(CallError::Graphql { messages: errors, extensions }),
                    (true, Some(body)) => Ok(body),
                    (true, None) => Err(CallError::InvalidResponse),
                };
                state.finish(call_id, answer, &mut fx);
            }
            CallsInput::TransportError { call_id, error } if state.sending(call_id) => state.finish(call_id, Err(error), &mut fx),
            CallsInput::Dropped { call_id } if state.sending(call_id) => {
                if let Some(CallPhase::Sending { request_id, kind: CallKind::Mutate, .. }) = state.phase(call_id) { fx.push(CallsEffect::Release { request_id }); }
                state.finish(call_id, Err(CallError::Dropped), &mut fx);
            }
            CallsInput::Declare(operations) => {
                state.declared = operations.into_iter().collect();
                state.generation += 1;
            }
            CallsInput::Online(Online(online)) => state.online = Some(online),
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

    fn epoch_started(state: &mut CallsState, epoch: Epoch) {
        state.epoch = epoch;
        state.calls.clear();
    }

    fn tick(_now: Time) -> CallsInput { CallsInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> CallsInput { CallsInput::EpochEnded }

    fn closing() -> CallsInput { CallsInput::Closing }
}
