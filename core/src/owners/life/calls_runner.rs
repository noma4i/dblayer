//! Executor of the effects of Calls: a query goes to the transport, a mutation goes through the owner Session.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Ports, Wiring};
use crate::owners::calls::{CallError, CallKind, Calls, CallsEffect, CallsInput};
use crate::owners::contract::RequestId;
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::session::{AuthorOutcome, SessionError, SessionInput, CALLS_AUTHOR};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::{GraphQLRequest, ResponseOutcome, SessionCode, TransportError};

pub(super) struct CallsRunner {
    wiring: Arc<Wiring>,
    transport: Option<Arc<dyn crate::transport::Upstream>>,
    session: Option<Arc<crate::transport::Exchange>>,
    executor: tokio::runtime::Handle,
    operations: super::OperationLookup,
    running: HashMap<u64, AbortHandle>,
}

fn input_of(call_id: u64, root: &str, result: Result<crate::transport::Response, TransportError>) -> CallsInput {
    match result {
        Ok(response) => {
            if response.shape().outcome() != ResponseOutcome::Commit {
                let messages: Vec<String> = response.errors.iter().map(|error| error.message.clone()).collect();
                return CallsInput::Response { call_id, body: None, errors: if messages.is_empty() { vec!["empty response".to_string()] } else { messages }, extensions: response.extensions() };
            }
            let body = response.data.and_then(|data| data.get(root).filter(|value| !value.is_null()).map(Value::to_string));
            CallsInput::Response { call_id, body, errors: Vec::new(), extensions: None }
        }
        Err(TransportError::Graphql { messages, extensions }) => CallsInput::TransportError { call_id, error: CallError::Graphql { messages, extensions } },
        Err(TransportError::Session { code, detail }) => CallsInput::TransportError { call_id, error: CallError::Session { code, detail } },
        Err(TransportError::Http { status, body }) => CallsInput::TransportError { call_id, error: CallError::Http { status, detail: body } },
        Err(TransportError::Interrupted(_)) => CallsInput::TransportError { call_id, error: CallError::Interrupted },
        Err(_) => CallsInput::TransportError { call_id, error: CallError::Network },
    }
}

fn session_call_error(code: SessionCode) -> CallError { CallError::Session { code, detail: code.name().to_string() } }

fn answer_input(call_id: u64, root: &str, outcome: AuthorOutcome) -> CallsInput {
    match outcome {
        AuthorOutcome::Response(text) => match serde_json::from_str::<Value>(&text) {
            Ok(body) => input_of(call_id, root, crate::transport::encrypted::response_of(crate::transport::encrypted::Answer { sequence: None, body })),
            Err(error) => input_of(call_id, root, Err(TransportError::Decode(error.to_string()))),
        },
        AuthorOutcome::Dropped => CallsInput::Dropped { call_id },
        AuthorOutcome::Error { error, .. } => CallsInput::TransportError { call_id, error: match error {
            SessionError::NoSession => CallError::NoSession,
            SessionError::Interrupted | SessionError::Closed => CallError::Interrupted,
            SessionError::Http { status } => CallError::Http { status, detail: "the server refused the request".into() },
            SessionError::Unauthorized => CallError::Unauthorized,
            SessionError::Session(code) => session_call_error(code),
            SessionError::Revoked => session_call_error(SessionCode::SessionEnded),
            SessionError::SequenceConflict => session_call_error(SessionCode::SequenceConflict),
            SessionError::StateLost => session_call_error(SessionCode::StateLost),
            SessionError::NoCredential => session_call_error(SessionCode::NoCredential),
            _ => CallError::Network,
        } },
    }
}

struct CallRequest {
    call_id: u64,
    request_id: RequestId,
    epoch: crate::runtime::Epoch,
    operation: String,
    variables: String,
    document: String,
    root: String,
    kind: CallKind,
}

impl CallsRunner {
    pub fn new(wiring: Arc<Wiring>, ports: &Ports) -> Self {
        Self { wiring, transport: ports.transport.clone(), session: ports.exchange.clone(), executor: ports.executor.clone(), operations: ports.operations.clone(), running: HashMap::new() }
    }

    fn query(&mut self, call: CallRequest, back: &Back<Calls>) {
        let CallRequest { call_id, request_id, epoch, operation, variables, document, root, kind } = call;
        let Some(transport) = self.transport.clone() else { return back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Invalid }) };
        let back = back.clone();
        let (launch, gate) = (self.wiring.handle.launch(), self.wiring.gate.clone());
        #[cfg(test)]
        let flight = crate::owners::read::InFlight::start(&self.wiring.flights);
        let task = crate::task::spawn_on(&self.executor, crate::runtime::counted(Some(launch), async move {
            #[cfg(test)]
            let _flight = flight;
            let variables: Value = serde_json::from_str(&variables).unwrap_or(Value::Null);
            let request = GraphQLRequest::new(document.clone(), operation, variables).by(crate::transport::WireIds::new(crate::transport::Author::Calls, request_id, crate::transport::WireIds::next_attempt()));
            crate::lock::assert_no_library_lock("Transport::request");
            let result = match kind {
                CallKind::Mutate => crate::runtime::send_in_epoch(&gate, Some(epoch), || transport.mutate(request)).await,
                CallKind::Query => crate::runtime::send_in_epoch(&gate, Some(epoch), || transport.query(request)).await,
            };
            back.reply(EpochStamp::Control, input_of(call_id, &root, result));
        }));
        self.running.insert(request_id, task.abort_handle());
    }

    fn forget(&mut self, request_id: RequestId) {
        self.wiring.calls_answers.forget(OutcomeId::from_raw(request_id));
        if let Some(task) = self.running.remove(&request_id) {
            crate::lock::effect(|| task.abort());
        }
    }

    fn mutate(&mut self, call: CallRequest, back: &Back<Calls>) {
        let Some(client) = self.session.clone() else { return self.query(call, back) };
        let CallRequest { request_id, epoch, operation, variables, document, root, .. } = call;
        let variables: Value = serde_json::from_str(&variables).unwrap_or(Value::Null);
        let body = crate::transport::encrypted::graphql_body(&GraphQLRequest::new(document, operation, variables), true);
        let waiter = self.wiring.calls_answers.register_as(request_id, crate::runtime::Epoch::MAX);
        let back = back.clone();
        let task = crate::task::spawn_on(&self.executor, crate::runtime::counted(None, async move {
            if let crate::runtime::Outcome::Value(outcome) = waiter.wait().await { back.reply(EpochStamp::Control, answer_input(request_id, &root, outcome)); }
        }));
        self.running.insert(request_id, task.abort_handle());
        self.wiring.to_session(mutate_input(request_id, epoch, client.graphql_path().to_string(), body.to_string()));
    }
}

pub(super) fn mutate_input(req_id: RequestId, epoch: crate::runtime::Epoch, path: String, body: String) -> SessionInput {
    SessionInput::Mutate { author: CALLS_AUTHOR, owner: Some(crate::owners::session::MutateOwner::Detached), req_id, epoch, path, body, purpose: 0 }
}

impl EffectRunner<Calls> for CallsRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: CallsEffect, back: &Back<Calls>) {
        match effect {
            CallsEffect::Request { call_id, request_id, operation, variables, kind, epoch } => {
                let Some((document, root)) = (self.operations)(&operation) else {
                    back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Invalid });
                    return;
                };
                let call = CallRequest { call_id, request_id, epoch, operation, variables, document, root, kind };
                match kind {
                    CallKind::Query => self.query(call, back),
                    CallKind::Mutate => self.mutate(call, back),
                };
            }
            CallsEffect::Cancel { request_id, kind } => match kind {
                CallKind::Query => {
                    if let Some(task) = self.running.remove(&request_id) {
                        crate::lock::effect(|| task.abort());
                    }
                }
                CallKind::Mutate => {
                    self.forget(request_id);
                    self.wiring.to_session(SessionInput::Cancel { req_id: request_id });
                }
            },
            CallsEffect::Release { request_id } => {
                self.forget(request_id);
                self.wiring.to_session(SessionInput::Release { req_id: request_id });
            }
            CallsEffect::Reply { id, answer } => {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Call(answer));
            }
            CallsEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}
