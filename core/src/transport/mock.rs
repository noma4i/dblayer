use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Notify;

use super::error::TransportError;
use super::put::{PutRequest, PutResponse};
use super::request::{GraphQLError, GraphQLRequest, Response};
use super::subscription::{ConsumerSignal, ServerSubscription, SubscriptionEvent, ServerSubscriptionHandle};
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
    pub upload: bool,
}

#[derive(Debug, Clone)]
pub struct Reply {
    delay: Duration,
    result: Result<Response, TransportError>,
}

impl Reply {
    #[cfg(test)]
    pub(crate) fn response(response: Response) -> Self {
        Self { delay: Duration::ZERO, result: Ok(response) }
    }

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

    pub fn graphql_messages(messages: &[&str]) -> Self {
        let errors = messages.iter().map(|message| GraphQLError { message: (*message).to_string(), path: None, extensions: None }).collect();
        Self { delay: Duration::ZERO, result: Ok(Response { data: None, errors }) }
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
    pub tag: String,
}

#[derive(Debug, Clone)]
pub struct PutReply {
    delay: Duration,
    result: Result<PutResponse, TransportError>,
}

impl PutReply {
    pub fn etag(etag: &str) -> Self {
        Self { delay: Duration::ZERO, result: Ok(PutResponse::with_etag(etag)) }
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
    puts: HashMap<String, VecDeque<PutReply>>,
    put_calls: Vec<RecordedPut>,
    puts_running: usize,
    puts_peak: usize,
    replies: HashMap<String, VecDeque<Reply>>,
    scripts: HashMap<String, Vec<SubscriptionEvent>>,
    senders: HashMap<String, Vec<(usize, UnboundedSender<SubscriptionEvent>)>>,
    streams: Vec<Stream>,
    cancellations: HashMap<String, usize>,
    calls: Vec<RecordedCall>,
    responder: Option<Responder>,
    put_progress: Option<Arc<dyn Fn() + Send + Sync>>,
    session: Option<Result<Option<String>, TransportError>>,
}

struct Stream {
    operation_name: String,
    sent: usize,
    received: usize,
    entered: usize,
    waiting: bool,
    open: bool,
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

    pub fn set_session(&self, session: Option<&str>) {
        self.lock().session = Some(Ok(session.map(str::to_string)));
    }

    pub fn fail_session(&self, message: &str) {
        self.lock().session = Some(Err(TransportError::Network(message.to_string())));
    }

    pub fn enqueue(&self, operation_name: &str, reply: Reply) {
        self.lock().replies.entry(operation_name.to_string()).or_default().push_back(reply);
    }

    pub fn respond_with(&self, responder: impl Fn(&GraphQLRequest) -> Option<Reply> + Send + Sync + 'static) {
        self.lock().responder = Some(Arc::new(responder));
    }

    pub fn on_put_progress(&self, progress: impl Fn() + Send + Sync + 'static) {
        self.lock().put_progress = Some(Arc::new(progress));
    }

    pub fn script_subscription(&self, operation_name: &str, events: Vec<SubscriptionEvent>) {
        self.lock().scripts.insert(operation_name.to_string(), events);
    }

    pub fn emit(&self, operation_name: &str, event: SubscriptionEvent) {
        let mut state = self.lock();
        let senders = state.senders.get(operation_name).cloned().unwrap_or_default();
        for (index, sender) in senders {
            if sender.send(event.clone()).is_ok() {
                state.streams[index].sent += 1;
            }
        }
        drop(state);
        self.wake.notify_waiters();
    }

    pub fn emit_on(&self, operation_name: &str, connection: usize, event: SubscriptionEvent) {
        let mut state = self.lock();
        if let Some((index, sender)) = state.senders.get(operation_name).and_then(|senders| senders.get(connection)).cloned() {
            if sender.send(event).is_ok() { state.streams[index].sent += 1; }
        }
        drop(state);
        self.wake.notify_waiters();
    }

    pub fn disconnect(&self, operation_name: &str) {
        let mut state = self.lock();
        for (index, _) in state.senders.remove(operation_name).unwrap_or_default() {
            state.streams[index].open = false;
        }
        drop(state);
        self.wake.notify_waiters();
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

    pub async fn await_cancellations(&self, operation_name: &str, count: usize) {
        self.until(&format!("{count} cancellations of {operation_name}"), |state| state.cancellations.get(operation_name).copied().unwrap_or(0) >= count).await;
    }

    pub async fn await_consumed(&self, operation_name: &str) {
        self.until(&format!("every event of {operation_name} consumed"), |state| {
            state.streams.iter().filter(|stream| stream.operation_name == operation_name && stream.open).all(|stream| stream.received == stream.sent && stream.waiting)
        })
        .await;
    }

    pub fn waits(&self, operation_name: &str) -> usize {
        self.lock().streams.iter().filter(|stream| stream.operation_name == operation_name).map(|stream| stream.entered).sum()
    }

    pub async fn await_waits(&self, operation_name: &str, count: usize) {
        self.until(&format!("{count} reads of {operation_name}"), |state| state.streams.iter().filter(|stream| stream.operation_name == operation_name).map(|stream| stream.entered).sum::<usize>() >= count).await;
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

    pub fn enqueue_put(&self, url: &str, reply: PutReply) {
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
            upload: request.upload,
        });
        self.wake.notify_waiters();
    }

    async fn reply(&self, kind: CallKind, request: GraphQLRequest) -> Result<Response, TransportError> {
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
            tokio::time::sleep(reply.delay).await;
        }
        reply.result
    }
}

impl MockTransport {
    async fn transfer(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
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
            state.put_calls.push(RecordedPut { url: request.url.clone(), headers: request.headers.clone(), body, tag: request.tag.clone() });
            state.puts_running += 1;
            state.puts_peak = state.puts_peak.max(state.puts_running);
            self.wake.notify_waiters();
            match state.puts.get_mut(&request.url) {
                Some(queue) if queue.len() > 1 => queue.pop_front(),
                Some(queue) => queue.front().cloned(),
                None => None,
            }
        };
        let reply = reply.unwrap_or_else(|| PutReply::etag(&format!("etag:{}", request.url)));
        let progress = self.lock().put_progress.clone();
        let run = async {
            for step in 1..=PUT_STEPS {
                if !reply.delay.is_zero() {
                    tokio::time::sleep(reply.delay / PUT_STEPS).await;
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
impl Transport for MockTransport {
    fn session(&self) -> Result<Option<String>, TransportError> {
        self.lock().session.clone().unwrap_or_else(|| Ok(Some(String::new())))
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
        self.transfer(request).await
    }

    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(CallKind::Query, request).await
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.reply(CallKind::Mutation, request).await
    }

    async fn subscribe(&self, request: GraphQLRequest) -> Result<ServerSubscription, TransportError> {
        self.record(CallKind::Subscription, &request);
        let (sender, receiver) = mpsc::unbounded_channel();
        let name = request.operation_name;
        let mut state = self.lock();
        let index = state.streams.len();
        state.streams.push(Stream { operation_name: name.clone(), sent: 0, received: 0, entered: 0, waiting: false, open: true });
        for event in state.scripts.get(&name).cloned().unwrap_or_else(|| vec![SubscriptionEvent::Ready]) {
            let _ = sender.send(event);
            state.streams[index].sent += 1;
        }
        state.senders.entry(name.clone()).or_default().push((index, sender));
        drop(state);
        self.wake.notify_waiters();
        let (shared, wake) = (self.state.clone(), self.wake.clone());
        let handle = ServerSubscriptionHandle::new(move || {
            let mut state = shared.lock().expect("mock transport state");
            *state.cancellations.entry(name.clone()).or_default() += 1;
            state.streams[index].open = false;
            drop(state);
            wake.notify_waiters();
        });
        let (shared, wake) = (self.state.clone(), self.wake.clone());
        Ok(ServerSubscription::new(receiver, handle).on_next(move |signal| {
            let mut state = shared.lock().expect("mock transport state");
            let stream = &mut state.streams[index];
            match signal {
                ConsumerSignal::Waiting => {
                    stream.entered += 1;
                    stream.waiting = true;
                }
                ConsumerSignal::Received => stream.received += 1,
                ConsumerSignal::Left => stream.waiting = false,
            }
            drop(state);
            wake.notify_waiters();
        }))
    }
}
