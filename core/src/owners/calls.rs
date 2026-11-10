//! Owner `Calls`: the arbitrary operation `run`, one transport request per call, no cache and no Store write.

use std::collections::BTreeMap;
use std::time::Duration;

use super::contract::{Drained, Online, RequestId};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};
const CALL_DEADLINE: Duration = Duration::from_secs(30);

pub type CallId = u64;
use super::contract::ReplyId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    NoSession,
    Invalid,
    Offline,
    Network,
    Unauthorized,
    Graphql { messages: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    Http { status: u16, detail: String },
    /// A query the server refused with a session code: the call ends, the session stays.
    Session { code: crate::transport::SessionCode, detail: String },
    Dropped,
    InvalidResponse,
    Interrupted,
    SessionEnded,
    Closed,
    Storage,
    /// A refusal that a transport or the owner Session already typed: it reaches the platform as it is.
    Failed(crate::ffi::DbError),
}

pub type CallAnswer = Result<String, CallError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    Query,
    Mutate,
}

impl CallKind {
    /// The operation type a GraphQL document declares: `query`, `mutation` or the anonymous selection `{`.
    pub fn declared_by(document: &str) -> Option<Self> {
        let mut rest = document;
        loop {
            rest = rest.trim_start_matches(|character: char| character.is_whitespace() || character == ',' || character == '\u{feff}');
            match rest.strip_prefix('#') {
                Some(comment) => rest = comment.split_once('\n').map_or("", |(_, after)| after),
                None => break,
            }
        }
        let word_end = rest.find(|character: char| !(character.is_alphanumeric() || character == '_')).unwrap_or(rest.len());
        match &rest[..word_end] {
            "query" => Some(Self::Query),
            "mutation" => Some(Self::Mutate),
            "" if rest.starts_with('{') => Some(Self::Query),
            _ => None,
        }
    }
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
    SnapshotRequest { epoch: Epoch },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum CallsInput {
    /// `epoch` is the epoch of the caller: the table answers `NoSession` when it is not the current one.
    Run { epoch: Epoch, operation: String, variables: String, reply: ReplyId },
    /// Operations declared by `register_operations` after the open.
    Declare(Vec<(String, CallKind)>),
    /// `body` is the root of the operation, `None` when the response has no root.
    Response { call_id: CallId, body: Option<String>, data: Option<serde_json::Value>, errors: Vec<crate::transport::GraphQLError>, kind: crate::transport::AnswerKind },
    TransportError { call_id: CallId, error: CallError },
    Dropped { call_id: CallId },
    Deadline { call_id: CallId },
    RetryDue { call_id: CallId, attempt: u32 },
    CallerCancel { reply: ReplyId },
    /// The broadcast of `Network`: it changes `online` only after the addressed `Snapshot` of the epoch.
    Online(Online),
    /// The addressed answer of `Network` to `SnapshotRequest{Calls}`.
    Snapshot(Online),
    ActivateEpoch(Epoch),
    EpochEnded(Cause),
    Closing,
    Idle,
}

pub struct CallsState {
    epoch: Epoch,
    online: Option<bool>,
    declared: BTreeMap<String, CallKind>,
    generation: u64,
    calls: BTreeMap<CallId, (CallPhase, ReplyId)>,
    by_reply: BTreeMap<ReplyId, CallId>,
    attempts: BTreeMap<CallId, CallAttempt>,
    /// `Run` calls that came before the addressed snapshot of the epoch.
    awaiting: BTreeMap<ReplyId, (CallId, String, String)>,
    awaiting_by_id: BTreeMap<CallId, ReplyId>,
    next: u64,
    drained: bool,
}

struct CallAttempt {
    operation: String,
    variables: String,
    epoch: Epoch,
    attempt: u32,
    last: Option<CallError>,
    waiting_network: bool,
    waiting_retry: bool,
}

fn retry_policy(error: &CallError, kind: CallKind) -> Option<crate::runtime::RetryPolicy> {
    use crate::runtime::RetryPolicy;
    match error {
        CallError::Network | CallError::Dropped => Some(if kind == CallKind::Mutate { RetryPolicy::Http } else { RetryPolicy::QueryNetwork }),
        CallError::Failed(crate::ffi::DbError::ResponseTooLarge { .. }) if kind == CallKind::Mutate => Some(RetryPolicy::Http),
        CallError::Http { status, .. } if matches!(crate::transport::classify_answer(&crate::transport::WireOutcome::Status(*status), false, true), crate::transport::AnswerKind::Transient { .. }) => Some(if kind == CallKind::Mutate { RetryPolicy::Http } else { RetryPolicy::QueryNetwork }),
        _ => None,
    }
}

impl CallsState {
    pub fn new(epoch: Epoch, online: Option<bool>, declared: &[(&str, CallKind)]) -> Self {
        Self { epoch, online, declared: declared.iter().map(|(name, kind)| (name.to_string(), *kind)).collect(), generation: 0, calls: BTreeMap::new(), by_reply: BTreeMap::new(), attempts: BTreeMap::new(), awaiting: BTreeMap::new(), awaiting_by_id: BTreeMap::new(), next: 1, drained: false }
    }

    pub fn phase(&self, call: CallId) -> Option<CallPhase> { self.calls.get(&call).map(|(phase, _)| *phase) }

    fn sending(&self, call_id: CallId) -> bool { matches!(self.phase(call_id), Some(CallPhase::Sending { .. })) }

    fn finish(&mut self, call_id: CallId, answer: CallAnswer, fx: &mut Vec<CallsEffect>, finished: &mut Vec<CallId>) {
        if let Some((phase, reply)) = self.calls.get_mut(&call_id) {
            *phase = CallPhase::Done;
            self.by_reply.remove(reply);
            self.attempts.remove(&call_id);
            finished.push(call_id);
            fx.push(CallsEffect::Reply { id: *reply, answer });
        }
    }

    fn run(&mut self, (operation, variables, reply): (String, String, ReplyId), held: Option<CallId>, fx: &mut Vec<CallsEffect>, deadlines: &mut Vec<CallId>, finished: &mut Vec<CallId>) {
        if self.online != Some(true) {
            fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Offline) });
            if let Some(id) = held { finished.push(id); }
            return;
        }
        let Some(kind) = self.declared.get(&operation).copied() else {
            fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Invalid) });
            if let Some(id) = held { finished.push(id); }
            return;
        };
        let id = held.unwrap_or_else(|| { let id = self.next; self.next += 1; deadlines.push(id); id });
        self.calls.insert(id, (CallPhase::Sending { request_id: id, kind, generation: self.generation }, reply));
        self.by_reply.insert(reply, id);
        self.attempts.insert(id, CallAttempt { operation: operation.clone(), variables: variables.clone(), epoch: self.epoch, attempt: 1, last: None, waiting_network: false, waiting_retry: false });
        fx.push(CallsEffect::Request { call_id: id, request_id: id, operation, variables, kind, epoch: self.epoch });
    }

    fn interrupt(&mut self, end: CallError, fx: &mut Vec<CallsEffect>, finished: &mut Vec<CallId>) {
        for (reply, (id, _, _)) in std::mem::take(&mut self.awaiting) {
            fx.push(CallsEffect::Reply { id: reply, answer: Err(end.clone()) });
            finished.push(id);
        }
        self.awaiting_by_id.clear();
        let sending: Vec<(CallId, RequestId, CallKind)> = self.calls.iter().filter_map(|(id, (phase, _))| match phase {
            CallPhase::Sending { request_id, kind, .. } => Some((*id, *request_id, *kind)),
            CallPhase::Done => None,
        }).collect();
        for (call_id, request_id, kind) in sending {
            self.finish(call_id, Err(end.clone()), fx, finished);
            fx.push(CallsEffect::Cancel { request_id, kind });
        }
    }

    fn retry(&mut self, call_id: CallId, error: CallError, policy: crate::runtime::RetryPolicy, fx: &mut Vec<CallsEffect>, scheduled: &mut Vec<(CallId, u32, crate::runtime::RetryPolicy)>, finished: &mut Vec<CallId>) {
        let Some(attempt) = self.attempts.get_mut(&call_id) else { return };
        if !policy.allows(attempt.attempt + 1) {
            let phase = self.phase(call_id);
            self.finish(call_id, Err(error), fx, finished);
            if let Some(CallPhase::Sending { request_id, kind: CallKind::Mutate, .. }) = phase { fx.push(CallsEffect::Release { request_id }); }
            return;
        }
        attempt.last = Some(error);
        let previous = attempt.attempt;
        attempt.attempt += 1;
        attempt.waiting_retry = true;
        if self.online != Some(true) { attempt.waiting_network = true; return; }
        scheduled.push((call_id, previous, policy));
    }

    fn send_again(&mut self, call_id: CallId, fx: &mut Vec<CallsEffect>) {
        let Some(CallPhase::Sending { mut request_id, kind, generation }) = self.phase(call_id) else { return };
        if kind == CallKind::Query {
            request_id = self.next;
            self.next += 1;
            if let Some((phase, _)) = self.calls.get_mut(&call_id) { *phase = CallPhase::Sending { request_id, kind, generation }; }
        }
        let Some(attempt) = self.attempts.get_mut(&call_id) else { return };
        attempt.waiting_retry = false;
        attempt.waiting_network = false;
        fx.push(CallsEffect::Request { call_id, request_id, operation: attempt.operation.clone(), variables: attempt.variables.clone(), kind, epoch: attempt.epoch });
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
        let (mut deadlines, mut scheduled, mut finished) = (Vec::new(), Vec::new(), Vec::new());
        match input {
            CallsInput::Run { epoch, reply, .. } if epoch != state.epoch => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::NoSession) }),
            CallsInput::Run { reply, ref operation, .. } if !state.declared.contains_key(operation) => fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Invalid) }),
            CallsInput::Run { operation, variables, reply, .. } if state.online.is_none() => {
                let id = state.next;
                state.next += 1;
                state.awaiting.insert(reply, (id, operation, variables));
                state.awaiting_by_id.insert(id, reply);
                deadlines.push(id);
            }
            CallsInput::Run { operation, variables, reply, .. } => state.run((operation, variables, reply), None, &mut fx, &mut deadlines, &mut finished),
            CallsInput::Response { call_id, body, data, errors, kind } if state.sending(call_id) => {
                let response = crate::transport::Response { data, errors, kind: Some(kind.clone()) };
                let answer = match (&kind, response.errors.is_empty(), body) {
                    (crate::transport::AnswerKind::Transient { reason }, true, None) if matches!(state.phase(call_id), Some(CallPhase::Sending { kind: CallKind::Query, .. })) => Err(CallError::Graphql { messages: vec![reason.clone()], extensions: None }),
                    (_, false, _) => Err(CallError::Graphql { messages: response.errors.iter().map(|error| error.message.clone()).collect(), extensions: response.extensions() }),
                    (_, true, Some(body)) => Ok(body),
                    (_, true, None) => Err(CallError::InvalidResponse),
                };
                let retry = match (&answer, state.phase(call_id), kind) { (Err(_), Some(CallPhase::Sending { kind: CallKind::Query, .. }), crate::transport::AnswerKind::Transient { .. }) => Some(crate::runtime::RetryPolicy::QueryServer), _ => None };
                if let (Some(policy), Err(error)) = (retry, &answer) { state.retry(call_id, error.clone(), policy, &mut fx, &mut scheduled, &mut finished); }
                else { state.finish(call_id, answer, &mut fx, &mut finished); }
            }
            CallsInput::TransportError { call_id, error } if state.sending(call_id) => {
                let policy = match state.phase(call_id) { Some(CallPhase::Sending { kind, .. }) => retry_policy(&error, kind), _ => None };
                if let Some(policy) = policy {
                    state.retry(call_id, error, policy, &mut fx, &mut scheduled, &mut finished);
                } else { state.finish(call_id, Err(error), &mut fx, &mut finished); }
            }
            CallsInput::Dropped { call_id } if state.sending(call_id) => {
                let policy = if matches!(state.phase(call_id), Some(CallPhase::Sending { kind: CallKind::Mutate, .. })) { crate::runtime::RetryPolicy::Http } else { crate::runtime::RetryPolicy::QueryNetwork };
                state.retry(call_id, CallError::Dropped, policy, &mut fx, &mut scheduled, &mut finished);
            }
            CallsInput::Deadline { call_id } if state.sending(call_id) => {
                let phase = state.phase(call_id);
                let error = state.attempts.get(&call_id).and_then(|attempt| attempt.last.clone()).filter(|error| *error != CallError::Dropped).unwrap_or(CallError::Network);
                state.finish(call_id, Err(error), &mut fx, &mut finished);
                if let Some(CallPhase::Sending { request_id, kind: CallKind::Query, .. }) = phase { fx.push(CallsEffect::Cancel { request_id, kind: CallKind::Query }); }
                if let Some(CallPhase::Sending { request_id, kind: CallKind::Mutate, .. }) = phase { fx.push(CallsEffect::Release { request_id }); }
            }
            CallsInput::Deadline { call_id } if state.awaiting_by_id.contains_key(&call_id) => {
                if let Some(reply) = state.awaiting_by_id.remove(&call_id) {
                    state.awaiting.remove(&reply);
                    fx.push(CallsEffect::Reply { id: reply, answer: Err(CallError::Network) });
                    finished.push(call_id);
                }
            }
            CallsInput::CallerCancel { reply } => {
                if let Some((id, _, _)) = state.awaiting.remove(&reply) { state.awaiting_by_id.remove(&id); finished.push(id); }
                if let Some((call_id, phase)) = state.by_reply.remove(&reply).and_then(|id| state.phase(id).map(|phase| (id, phase))) {
                    state.calls.insert(call_id, (CallPhase::Done, reply));
                    state.attempts.remove(&call_id);
                    finished.push(call_id);
                    match phase {
                        CallPhase::Sending { request_id, kind: CallKind::Query, .. } => fx.push(CallsEffect::Cancel { request_id, kind: CallKind::Query }),
                        CallPhase::Sending { request_id, kind: CallKind::Mutate, .. } => fx.push(CallsEffect::Release { request_id }),
                        CallPhase::Done => {}
                    }
                }
            }
            CallsInput::RetryDue { call_id, attempt } if state.sending(call_id) && state.attempts.get(&call_id).is_some_and(|current| current.waiting_retry && !current.waiting_network && current.attempt == attempt + 1) => {
                if state.online == Some(true) { state.send_again(call_id, &mut fx); }
                else if let Some(current) = state.attempts.get_mut(&call_id) { current.waiting_network = true; }
            }
            CallsInput::Declare(operations) => {
                state.declared = operations.into_iter().collect();
                state.generation += 1;
            }
            CallsInput::Online(Online(online)) if state.online.is_some() => {
                state.online = Some(online);
                if online {
                    let waiting: Vec<_> = state.attempts.iter().filter(|(_, attempt)| attempt.waiting_network).map(|(id, _)| *id).collect();
                    for id in waiting { state.send_again(id, &mut fx); }
                }
            }
            CallsInput::Online(_) => {}
            CallsInput::Snapshot(Online(online)) => {
                state.online = Some(online);
                for (reply, (id, operation, variables)) in std::mem::take(&mut state.awaiting) {
                    state.awaiting_by_id.remove(&id);
                    state.run((operation, variables, reply), Some(id), &mut fx, &mut deadlines, &mut finished);
                }
            }
            CallsInput::ActivateEpoch(epoch) => { state.epoch = epoch;
                state.online = None;
                state.calls.clear();
                state.by_reply.clear();
                state.attempts.clear();
                state.awaiting.clear();
                state.awaiting_by_id.clear();
                fx.push(CallsEffect::SnapshotRequest { epoch });
            }
            CallsInput::EpochEnded(cause) => state.interrupt(if cause == Cause::Close { CallError::Closed } else { CallError::SessionEnded }, &mut fx, &mut finished),
            CallsInput::Closing => {
                state.interrupt(CallError::Closed, &mut fx, &mut finished);
                if !state.drained {
                    state.drained = true;
                    fx.push(CallsEffect::Drained(Drained { owner: "calls" }));
                }
            }
            CallsInput::Response { .. } | CallsInput::TransportError { .. } | CallsInput::Dropped { .. } | CallsInput::Deadline { .. } | CallsInput::RetryDue { .. } | CallsInput::Idle => {}
        }
        let mut out: Effects<Self> = fx.into_iter().map(Effect::Run).collect();
        out.extend(deadlines.into_iter().map(|call_id| Effect::Schedule { id: crate::runtime::ScheduleId(call_id * 2), after: CALL_DEADLINE, input: CallsInput::Deadline { call_id } }));
        out.extend(scheduled.into_iter().map(|(call_id, attempt, policy)| Effect::Backoff { id: crate::runtime::ScheduleId(call_id * 2 + 1), policy, attempt, input: CallsInput::RetryDue { call_id, attempt } }));
        for call_id in finished { out.push(Effect::Cancel(crate::runtime::ScheduleId(call_id * 2))); out.push(Effect::Cancel(crate::runtime::ScheduleId(call_id * 2 + 1))); }
        out
    }

    fn epoch_started(epoch: Epoch) -> Option<CallsInput> { Some(CallsInput::ActivateEpoch(epoch)) }

    fn tick(_now: Time) -> CallsInput { CallsInput::Idle }

    fn epoch_ended(_epoch: Epoch, cause: Cause) -> CallsInput { CallsInput::EpochEnded(cause) }

    fn closing() -> CallsInput { CallsInput::Closing }
}
