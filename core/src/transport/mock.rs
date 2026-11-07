use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::error::TransportError;
use super::put::{PutRequest, PutReply};
use super::request::{GraphQLError, GraphQLRequest, Response};
use super::Upstream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedKind {
    Query,
    Mutation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordedCall {
    pub kind: RecordedKind,
    pub operation_name: String,
    pub variables: Value,
    pub purpose: super::NetworkPurpose,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedPut {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub identity: super::TransferIdentity,
}

#[derive(Debug, Clone)]
pub struct PutScript {
    delay: Duration,
    result: Result<PutReply, TransportError>,
}

impl PutScript {
    pub fn etag(etag: &str) -> Self {
        Self { delay: Duration::ZERO, result: Ok(PutReply::with_etag(etag)) }
    }

    pub fn status(status: u16) -> Self {
        Self::fail(TransportError::Http { status, body: String::new() })
    }

    pub fn fail(error: TransportError) -> Self {
        Self { delay: Duration::ZERO, result: Err(error) }
    }

    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

const PUT_STEPS: u32 = 4;

type Responder = Arc<dyn Fn(&GraphQLRequest) -> Option<Reply> + Send + Sync>;

#[derive(Default)]
struct MockState {
    puts: HashMap<String, VecDeque<PutScript>>,
    put_calls: Vec<RecordedPut>,
    puts_running: usize,
    puts_peak: usize,
    replies: HashMap<String, VecDeque<Reply>>,
    calls: Vec<RecordedCall>,
    responder: Option<Responder>,
    put_progress: Option<Arc<dyn Fn() + Send + Sync>>,
    session: Option<Result<Option<String>, TransportError>>,
    clock: Option<Arc<dyn crate::runtime::Clock>>,
}

#[derive(Clone, Default)]
pub struct MockTransport {
    state: Arc<Mutex<MockState>>,
    wake: Arc<Notify>,
}

impl MockTransport {
    pub fn new() -> Self {
        Self::default()
    }

    async fn wait(&self, delay: Duration) {
        let clock = self.lock().clock.clone();
        match clock {
            Some(clock) => clock.sleep(delay).await,
            None => tokio::time::sleep(delay).await,
        }
    }

    pub fn enqueue(&self, operation_name: &str, reply: Reply) {
        self.lock().replies.entry(operation_name.to_string()).or_default().push_back(reply);
    }

    pub fn respond_with(&self, responder: impl Fn(&GraphQLRequest) -> Option<Reply> + Send + Sync + 'static) {
        self.lock().responder = Some(Arc::new(responder));
    }

    pub async fn await_calls(&self, operation_name: &str, count: usize) {
        self.until(&format!("{count} calls of {operation_name}"), |state| state.calls.iter().filter(|call| call.operation_name == operation_name).count() >= count).await;
    }

    pub async fn await_puts(&self, url: &str, count: usize) {
        self.until(&format!("{count} puts to {url}"), |state| state.put_calls.iter().filter(|call| call.url == url).count() >= count).await;
    }

    pub async fn await_put_calls(&self, count: usize) {
        self.until(&format!("{count} puts"), |state| state.put_calls.len() >= count).await;
    }

    async fn until(&self, what: &str, ready: impl Fn(&MockState) -> bool) {
        let wait = async {
            loop {
                let notified = self.wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if ready(&self.lock()) {
                    return;
                }
                notified.await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(60), wait).await.is_err() {
            panic!("mock transport: waited for {what}");
        }
    }

    pub fn enqueue_put(&self, url: &str, reply: PutScript) {
        self.lock().puts.entry(url.to_string()).or_default().push_back(reply);
    }

    pub fn put_calls(&self) -> Vec<RecordedPut> {
        self.lock().put_calls.clone()
    }

    pub fn put_count(&self, url: &str) -> usize {
        self.lock().put_calls.iter().filter(|call| call.url == url).count()
    }

    pub fn parallel_puts_peak(&self) -> usize {
        self.lock().puts_peak
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.lock().calls.clone()
    }

    pub fn call_count(&self, operation_name: &str) -> usize {
        self.lock().calls.iter().filter(|call| call.operation_name == operation_name).count()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().expect("mock transport state")
    }

    fn record(&self, kind: RecordedKind, request: &GraphQLRequest) {
        self.lock().calls.push(RecordedCall {
            kind,
            operation_name: request.operation_name.clone(),
            variables: request.variables.clone(),
            purpose: request.purpose,
        });
        self.wake.notify_waiters();
    }

    async fn reply(&self, kind: RecordedKind, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.record(kind, &request);
        let responder = self.lock().responder.clone();
        let answered = responder.and_then(|answer| answer(&request));
        let reply = answered.or_else(|| {
            let mut state = self.lock();
            let queue = state.replies.get_mut(&request.operation_name);
            match queue {
                Some(queue) if queue.len() > 1 => queue.pop_front(),
                Some(queue) => queue.front().cloned(),
                None => None,
            }
        });
        let reply = reply.ok_or_else(|| TransportError::Unscripted(request.operation_name.clone()))?;
        if !reply.delay.is_zero() {
            self.wait(reply.delay).await;
        }
        reply.result
    }
}

impl MockTransport {
    async fn transfer(&self, request: PutRequest) -> Result<PutReply, TransportError> {
        let length = request.file.length;
        let body = std::fs::File::open(&request.file.path).and_then(|mut file| {
            use std::io::{Read, Seek, SeekFrom};
            file.seek(SeekFrom::Start(request.file.offset))?;
            let mut body = Vec::new();
            file.take(length).read_to_end(&mut body)?;
            Ok(body)
        }).map_err(|error| TransportError::Network(error.to_string()))?;
        let reply = {
            let mut state = self.lock();
            state.put_calls.push(RecordedPut { url: request.url.clone(), headers: request.headers.clone(), body, identity: request.identity.clone() });
            state.puts_running += 1;
            state.puts_peak = state.puts_peak.max(state.puts_running);
            self.wake.notify_waiters();
            match state.puts.get_mut(&request.url) {
                Some(queue) if queue.len() > 1 => queue.pop_front(),
                Some(queue) => queue.front().cloned(),
                None => None,
            }
        };
        let reply = reply.unwrap_or_else(|| PutScript::etag(&format!("etag:{}", request.url)));
        let progress = self.lock().put_progress.clone();
        let run = async {
            for step in 1..=PUT_STEPS {
                if !reply.delay.is_zero() {
                    self.wait(reply.delay / PUT_STEPS).await;
                }
                (request.sent)(length * u64::from(step) / u64::from(PUT_STEPS));
                if let Some(progress) = &progress { progress(); }
            }
            reply.result.clone()
        };
        let outcome = tokio::select! {
            result = run => result,
            () = request.cancel.cancelled() => Err(TransportError::Interrupted("cancelled".to_string())),
        };
        self.lock().puts_running -= 1;
        outcome
    }
}

#[async_trait]
impl Upstream for MockTransport {
    fn session(&self) -> Result<Option<String>, TransportError> {
        self.lock().session.clone().unwrap_or_else(|| Ok(Some(String::new())))
    }

    async fn put(&self, request: PutRequest) -> Result<PutReply, TransportError> {
        self.transfer(request).await
    }

    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(RecordedKind::Query, request).await
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(RecordedKind::Mutation, request).await
    }
}
