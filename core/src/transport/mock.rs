use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc::{self, UnboundedSender};

use super::error::TransportError;
use super::request::{GraphQLError, GraphQLRequest, Response};
use super::subscription::{Subscription, SubscriptionEvent, SubscriptionHandle};
use super::Transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Query,
    Mutation,
    Subscription,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordedCall {
    pub kind: CallKind,
    pub operation_name: String,
    pub variables: Value,
}

#[derive(Debug, Clone)]
pub struct Reply {
    delay: Duration,
    result: Result<Response, TransportError>,
}

impl Reply {
    pub fn data(value: Value) -> Self {
        Self { delay: Duration::ZERO, result: Ok(Response::data(value)) }
    }

    pub fn graphql_error(message: &str, code: &str) -> Self {
        let error = GraphQLError {
            message: message.to_string(),
            path: None,
            extensions: json!({ "code": code }).as_object().cloned(),
        };
        Self { delay: Duration::ZERO, result: Ok(Response { data: None, errors: vec![error] }) }
    }

    pub fn fail(error: TransportError) -> Self {
        Self { delay: Duration::ZERO, result: Err(error) }
    }

    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

#[derive(Default)]
struct MockState {
    replies: HashMap<String, VecDeque<Reply>>,
    scripts: HashMap<String, Vec<SubscriptionEvent>>,
    senders: HashMap<String, Vec<UnboundedSender<SubscriptionEvent>>>,
    cancellations: HashMap<String, usize>,
    calls: Vec<RecordedCall>,
}

#[derive(Clone, Default)]
pub struct MockTransport {
    state: Arc<Mutex<MockState>>,
}

impl MockTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, operation_name: &str, reply: Reply) {
        self.lock().replies.entry(operation_name.to_string()).or_default().push_back(reply);
    }

    pub fn script_subscription(&self, operation_name: &str, events: Vec<SubscriptionEvent>) {
        self.lock().scripts.insert(operation_name.to_string(), events);
    }

    pub fn emit(&self, operation_name: &str, event: SubscriptionEvent) {
        if let Some(senders) = self.lock().senders.get(operation_name) {
            for sender in senders {
                let _ = sender.send(event.clone());
            }
        }
    }

    pub fn disconnect(&self, operation_name: &str) {
        self.lock().senders.remove(operation_name);
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.lock().calls.clone()
    }

    pub fn call_count(&self, operation_name: &str) -> usize {
        self.lock().calls.iter().filter(|call| call.operation_name == operation_name).count()
    }

    pub fn cancellations(&self, operation_name: &str) -> usize {
        self.lock().cancellations.get(operation_name).copied().unwrap_or(0)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().expect("mock transport state")
    }

    fn record(&self, kind: CallKind, request: &GraphQLRequest) {
        self.lock().calls.push(RecordedCall {
            kind,
            operation_name: request.operation_name.clone(),
            variables: request.variables.clone(),
        });
    }

    async fn reply(&self, kind: CallKind, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.record(kind, &request);
        let reply = {
            let mut state = self.lock();
            let queue = state.replies.get_mut(&request.operation_name);
            match queue {
                Some(queue) if queue.len() > 1 => queue.pop_front(),
                Some(queue) => queue.front().cloned(),
                None => None,
            }
        };
        let reply = reply.ok_or_else(|| TransportError::Unscripted(request.operation_name.clone()))?;
        if !reply.delay.is_zero() {
            tokio::time::sleep(reply.delay).await;
        }
        reply.result
    }
}

#[async_trait]
impl Transport for MockTransport {
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(CallKind::Query, request).await
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(CallKind::Mutation, request).await
    }

    async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> {
        self.record(CallKind::Subscription, &request);
        let (sender, receiver) = mpsc::unbounded_channel();
        let name = request.operation_name;
        let mut state = self.lock();
        for event in state.scripts.get(&name).cloned().unwrap_or_default() {
            let _ = sender.send(event);
        }
        state.senders.entry(name.clone()).or_default().push(sender);
        drop(state);
        let shared = self.state.clone();
        let handle = SubscriptionHandle::new(move || {
            *shared.lock().expect("mock transport state").cancellations.entry(name.clone()).or_default() += 1;
        });
        Ok(Subscription::new(receiver, handle))
    }
}
