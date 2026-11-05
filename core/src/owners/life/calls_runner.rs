//! Executor of the effects of Calls: a query goes to the transport, a mutation goes through the owner Session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value;
use tokio::task::AbortHandle;

use super::{LifeReply, Ports, Wiring};
use crate::db::Db;
use crate::owners::calls::{CallError, CallKind, Calls, CallsEffect, CallsInput};
use crate::owners::contract::RequestId;
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::session::{AuthorOutcome, SessionError, SessionInput, CALLS_AUTHOR};
use crate::runtime::{Back, EffectRunner, EpochStamp, OutcomeId};
use crate::transport::{GraphQLRequest, ResponseOutcome, SessionCode, TransportError};

type Roots = Arc<Mutex<HashMap<RequestId, String>>>;

pub(super) struct CallsRunner {
    wiring: Arc<Wiring>,
    transport: Option<Arc<dyn crate::transport::Transport>>,
    session: Option<Arc<crate::transport::EncryptedClient>>,
    executor: tokio::runtime::Handle,
    operations: super::OperationLookup,
    db: Db,
    running: HashMap<u64, AbortHandle>,
    roots: Roots,
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
            SessionError::Http => CallError::Http { status: 0, detail: "the server refused the request".into() },
            SessionError::Unauthorized => CallError::Http { status: 401, detail: String::new() },
            SessionError::Session(code) => session_call_error(code),
            SessionError::Revoked => session_call_error(SessionCode::SessionEnded),
            SessionError::SequenceConflict => session_call_error(SessionCode::SequenceConflict),
            SessionError::StateLost => session_call_error(SessionCode::StateLost),
            SessionError::NoCredential => session_call_error(SessionCode::NoCredential),
            _ => CallError::Network,
        } },
    }
}

impl CallsRunner {
    pub fn new(wiring: Arc<Wiring>, ports: &Ports) -> Self {
        let roots = Roots::default();
        let (sink_wiring, sink_roots) = (wiring.clone(), roots.clone());
        wiring.on_mutate_answer(CALLS_AUTHOR, move |request_id, outcome| {
            let Some(root) = sink_roots.lock().unwrap_or_else(PoisonError::into_inner).remove(&request_id) else { return };
            sink_wiring.to_calls(answer_input(request_id, &root, outcome));
        });
        Self { wiring, transport: ports.transport.clone(), session: ports.session.clone(), executor: ports.executor.clone(), operations: ports.operations.clone(), db: ports.db.clone(), running: HashMap::new(), roots }
    }

    fn query(&mut self, call_id: u64, request_id: RequestId, operation: String, variables: String, document: String, root: String, back: &Back<Calls>) {
        let Some(transport) = self.transport.clone() else { return back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Invalid }) };
        let back = back.clone();
        let lease = self.db.shared.store.lifecycle.clone();
        let launch = self.wiring.handle.launch();
        let task = crate::task::spawn_on(crate::task::Carry::Detached, &self.executor, crate::runtime::counted(Some(launch), async move {
            let admitted = match lease.admit(crate::ffi::admission::Origin::Explicit, "run") {
                Ok(admitted) => admitted,
                Err(_) => return back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Interrupted }),
            };
            let variables: Value = serde_json::from_str(&variables).unwrap_or(Value::Null);
            let request = GraphQLRequest::new(document.clone(), operation, variables);
            crate::db::lock::assert_no_library_lock("Transport::request");
            let result = admitted.run(async {
                if document.trim_start().starts_with("mutation") { transport.mutate(&admitted, request).await } else { transport.query(&admitted, request).await }
            }).await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(TransportError::Interrupted("the call was cancelled".into())),
            };
            back.reply(EpochStamp::Control, input_of(call_id, &root, result));
        }));
        self.running.insert(request_id, task.abort_handle());
    }

    fn mutate(&mut self, call_id: u64, request_id: RequestId, epoch: crate::runtime::Epoch, operation: String, variables: String, document: String, root: String, back: &Back<Calls>) {
        let Some(client) = self.session.clone() else { return self.query(call_id, request_id, operation, variables, document, root, back) };
        let variables: Value = serde_json::from_str(&variables).unwrap_or(Value::Null);
        let body = crate::transport::encrypted::graphql_body(&GraphQLRequest::new(document, operation, variables), true);
        self.roots.lock().unwrap_or_else(PoisonError::into_inner).insert(request_id, root);
        self.wiring.to_session(SessionInput::Mutate { author: CALLS_AUTHOR, owner: None, req_id: request_id, epoch, path: client.graphql_path().to_string(), body: body.to_string(), purpose: 0 });
    }
}

impl EffectRunner<Calls> for CallsRunner {
    fn run(&mut self, _stamp: EpochStamp, effect: CallsEffect, back: &Back<Calls>) {
        match effect {
            CallsEffect::Request { call_id, request_id, operation, variables, kind, epoch } => {
                let Some((document, root)) = (self.operations)(&operation) else {
                    back.reply(EpochStamp::Control, CallsInput::TransportError { call_id, error: CallError::Invalid });
                    return;
                };
                match kind {
                    CallKind::Query => self.query(call_id, request_id, operation, variables, document, root, back),
                    CallKind::Mutate => self.mutate(call_id, request_id, epoch, operation, variables, document, root, back),
                };
            }
            CallsEffect::Cancel { request_id, kind } => match kind {
                CallKind::Query => {
                    if let Some(task) = self.running.remove(&request_id) {
                        crate::db::lock::effect(|| task.abort());
                    }
                }
                CallKind::Mutate => {
                    self.roots.lock().unwrap_or_else(PoisonError::into_inner).remove(&request_id);
                    self.wiring.to_session(SessionInput::Cancel { req_id: request_id });
                }
            },
            CallsEffect::Release { request_id } => {
                self.roots.lock().unwrap_or_else(PoisonError::into_inner).remove(&request_id);
                self.wiring.to_session(SessionInput::Release { req_id: request_id });
            }
            CallsEffect::Reply { id, answer } => {
                self.wiring.outcomes.settle(OutcomeId::from_raw(id), LifeReply::Call(answer));
            }
            CallsEffect::Drained(drained) => self.wiring.to_lifecycle(LifecycleInput::Drained(drained)),
        }
    }
}
