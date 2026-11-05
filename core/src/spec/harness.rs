use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::{Notify, Semaphore};

use crate::ffi::tape::{Reader, StreamDelivery, Watched};
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, PutReply, ReadEnd, ReadPhase, ReadRequest, ReadValue, SubscriptionSink, TestClock, Transport, TransportRequest};
use crate::schema::{Coverage, FieldSpec, Membership, ModelSpec, NodeRead, PageDirection, RelationSpec, RemoteShape, RemoteSpec, VariableSource, NODE_PARAM};
use crate::value::ScalarKind;

pub(super) const LIMIT_SECS: u64 = 20;

pub(super) async fn bound<T>(work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(LIMIT_SECS), work).await.unwrap_or_else(|_| panic!("did not finish within {LIMIT_SECS} seconds"))
}

/// Observables shared by the references and the core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WState {
    pub(super) phase: Phase,
    pub(super) error: bool,
    pub(super) loaded: bool,
}

impl WState {
    pub(super) const IDLE: WState = WState { phase: Phase::Idle, error: false, loaded: false };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
    OutOfSync,
}

/// A value a platform reads from a stream, without revisions (they are compared by the properties).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Val {
    Snapshot { rows: Vec<String>, state: WState },
    Delta { upserted: Vec<String>, removed: Vec<String> },
    Count { count: u64, state: WState },
    StateOnly(WState),
    Ready,
    Event { position: u64, id: String },
    Lost { first: u64, last: u64 },
    Ended(Cause),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    Loaded,
    Failed,
    Interrupted,
    Pending,
}

pub(super) fn outcome_of(result: &Result<(), DbError>) -> Outcome {
    match result {
        Ok(()) => Outcome::Loaded,
        Err(DbError::Interrupted { .. }) => Outcome::Interrupted,
        Err(_) => Outcome::Failed,
    }
}

fn wstate_of(state: &crate::ffi::ReadState) -> WState {
    let phase = match state.phase {
        ReadPhase::Idle => Phase::Idle,
        ReadPhase::Loading => Phase::Loading,
        ReadPhase::Loaded => Phase::Loaded,
        ReadPhase::Failed => Phase::Failed,
    };
    WState { phase, error: state.error.is_some(), loaded: state.loaded.is_some() }
}

pub(super) fn val_of(delivery: &StreamDelivery) -> Val {
    match &delivery.value {
        ReadValue::Snapshot { rows, state, .. } => Val::Snapshot { rows: rows.iter().map(|row| row.id.clone()).collect(), state: wstate_of(state) },
        ReadValue::Delta { rows, removed, .. } => Val::Delta { upserted: rows.iter().map(|row| row.id.clone()).collect(), removed: removed.clone() },
        ReadValue::State { state, .. } => Val::StateOnly(wstate_of(state)),
        ReadValue::Count { count, state, .. } => Val::Count { count: *count, state: wstate_of(state) },
        ReadValue::Ready => Val::Ready,
        ReadValue::Event { position, payload } => Val::Event { position: *position, id: serde_json::from_str::<serde_json::Value>(payload).ok().and_then(|value| value["id"].as_str().map(str::to_string)).unwrap_or_else(|| payload.clone()) },
        ReadValue::Lost { first, last } => Val::Lost { first: *first, last: *last },
        ReadValue::Gap | ReadValue::Status { .. } | ReadValue::Retry { .. } => Val::StateOnly(WState::IDLE),
        ReadValue::Ended { cause } => Val::Ended(match cause {
            ReadEnd::SessionEnded => Cause::SessionEnded,
            ReadEnd::Closed => Cause::Closed,
            ReadEnd::Cancelled => Cause::Cancelled,
            ReadEnd::Invalid => Cause::Invalid,
        }),
    }
}

pub(super) fn revision_of(delivery: &StreamDelivery) -> Option<(Option<i64>, i64)> {
    match &delivery.value {
        ReadValue::Snapshot { revision, .. } => Some((None, *revision)),
        ReadValue::Delta { base, revision, .. } => Some((Some(*base), *revision)),
        _ => None,
    }
}

/// Every sequence of `symbols` symbols up to `depth` inputs, at most `cap` of them, in a fixed order.
pub(super) fn sequences(symbols: usize, depth: usize, cap: usize) -> Vec<Vec<usize>> {
    let mut all: Vec<Vec<usize>> = Vec::new();
    let mut level: Vec<Vec<usize>> = vec![Vec::new()];
    for _ in 0..depth {
        let mut next = Vec::new();
        for prefix in &level {
            for symbol in 0..symbols {
                let mut trace = prefix.clone();
                trace.push(symbol);
                next.push(trace);
            }
        }
        all.extend(next.iter().cloned());
        level = next;
    }
    all.truncate(cap);
    all
}

pub(super) struct Gate {
    entered: Notify,
    release: Semaphore,
}

impl Gate {
    pub(super) fn new() -> Arc<Self> { Arc::new(Self { entered: Notify::new(), release: Semaphore::new(0) }) }
    async fn pass(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    pub(super) async fn entered(&self) { bound(self.entered.notified()).await }
    pub(super) fn release(&self) { self.release.add_permits(1) }
}

#[derive(Clone)]
pub(super) enum Reply {
    Page(Vec<String>, bool),
    Fail(DbError),
    Hold(Arc<Gate>, Box<Reply>),
}

impl Reply {
    pub(super) fn page(ids: &[&str], has_next: bool) -> Reply { Reply::Page(ids.iter().map(|id| id.to_string()).collect(), has_next) }
    pub(super) fn held(gate: &Arc<Gate>, then: Reply) -> Reply { Reply::Hold(gate.clone(), Box::new(then)) }
    pub(super) fn network() -> Reply { Reply::Fail(DbError::Network { detail: "down".into() }) }
    pub(super) fn refused() -> Reply { Reply::Fail(DbError::Http { status: 404, detail: "gone".into() }) }
}

#[derive(Clone, Debug)]
pub(super) struct Seen {
    pub(super) operation: String,
}

struct CancelCount<'a> {
    net: &'a Net,
    armed: bool,
}

impl Drop for CancelCount<'_> {
    fn drop(&mut self) {
        self.net.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.armed { self.net.cancelled.fetch_add(1, Ordering::SeqCst); }
    }
}

/// The server. It records requests, answers the script in order and keeps the subscription sinks.
pub(super) struct Net {
    seen: Mutex<Vec<Seen>>,
    script: Mutex<VecDeque<Reply>>,
    cancelled: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    sinks: Mutex<Vec<(String, Arc<SubscriptionSink>)>>,
    unsubscribed: Mutex<Vec<String>>,
    tokens: AtomicUsize,
    subscribe_fails: AtomicBool,
    subscribed: Notify,
}

impl Net {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::default(),
            script: Mutex::default(),
            cancelled: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            sinks: Mutex::default(),
            unsubscribed: Mutex::default(),
            tokens: AtomicUsize::new(0),
            subscribe_fails: AtomicBool::new(false),
            subscribed: Notify::new(),
        })
    }
    pub(super) fn script(&self, reply: Reply) { self.script.lock().unwrap().push_back(reply); }
    pub(super) fn requests(&self, operation: &str) -> usize { self.seen.lock().unwrap().iter().filter(|seen| seen.operation == operation).count() }
    pub(super) fn feed_requests(&self) -> usize { self.requests("Feed") }
    pub(super) fn subscribes(&self) -> usize { self.requests("subscribe:RecordEvents") }
    pub(super) fn cancelled(&self) -> usize { self.cancelled.load(Ordering::SeqCst) }
    pub(super) fn max_in_flight(&self) -> usize { self.max_in_flight.load(Ordering::SeqCst) }
    pub(super) fn unsubscribes(&self) -> usize { self.unsubscribed.lock().unwrap().len() }
    pub(super) fn fail_subscribes(&self, fail: bool) { self.subscribe_fails.store(fail, Ordering::SeqCst); }
    pub(super) async fn wait_subscribes(&self, count: usize) {
        bound(async {
            loop {
                let notified = self.subscribed.notified();
                if self.subscribes() >= count { return; }
                notified.await;
            }
        }).await
    }
    /// A frame on the newest subscription.
    pub(super) fn emit(&self, entries: Vec<serde_json::Value>) -> Result<(), DbError> {
        let sink = self.sinks.lock().unwrap().last().map(|(_, sink)| sink.clone()).expect("a subscription is open");
        sink.event(json!({"data":{"recordEvents":{"entries":entries}}}).to_string())
    }
    /// A frame on the oldest subscription, which the core no longer holds as current once a newer one opened.
    pub(super) fn emit_on_first(&self, entries: Vec<serde_json::Value>) -> Result<(), DbError> {
        let sink = self.sinks.lock().unwrap().first().map(|(_, sink)| sink.clone()).expect("a subscription is open");
        sink.event(json!({"data":{"recordEvents":{"entries":entries}}}).to_string())
    }
    /// The transport drops the newest subscription.
    pub(super) fn drop_connection(&self) {
        let sink = self.sinks.lock().unwrap().last().map(|(_, sink)| sink.clone()).expect("a subscription is open");
        sink.failed(DbError::Network { detail: "dropped".into() });
    }
}

pub(super) fn chat_entry(index: usize, title: &str) -> serde_json::Value {
    json!({"topic":"MESSAGE","action":"UPDATED","recordId":format!("m{index}"),"chat":{"id":format!("c{index}"),"title":title,"revision":1,"lists":["PRIMARY"]}})
}

#[async_trait]
impl Transport for Net {
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        let number = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(Seen { operation: request.operation_name.clone() });
            seen.len()
        };
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let mut guard = CancelCount { net: self, armed: true };
        let mut reply = self.script.lock().unwrap().pop_front().unwrap_or_else(|| Reply::Page(vec!["r1".to_string()], false));
        while let Reply::Hold(gate, then) = reply {
            gate.pass().await;
            reply = *then;
        }
        guard.armed = false;
        drop(guard);
        match (request.operation_name.as_str(), reply) {
            (_, Reply::Fail(error)) => Err(error),
            ("Feed", Reply::Page(ids, has_next)) => {
                let nodes: Vec<_> = ids.iter().map(|id| json!({"id":id,"text":"t"})).collect();
                Ok(json!({"data":{"feed":{"nodes":nodes,"pageInfo":{"hasNextPage":has_next,"endCursor":format!("c{number}")}}}}).to_string())
            }
            ("ChatNode", _) => Ok(json!({"data":{"node":{"__typename":"Chat","id":"c1","title":"t","revision":1,"lists":["PRIMARY"]}}}).to_string()),
            _ => Ok(json!({"data":{"chats":[]}}).to_string()),
        }
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Network { detail: "no mutations".into() }) }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> {
        self.seen.lock().unwrap().push(Seen { operation: format!("subscribe:{}", request.operation_name) });
        if self.subscribe_fails.load(Ordering::SeqCst) {
            self.subscribed.notify_one();
            return Err(DbError::Network { detail: "subscribe refused".into() });
        }
        let token = format!("t{}", self.tokens.fetch_add(1, Ordering::SeqCst));
        sink.opened(token.clone())?;
        sink.ready();
        self.sinks.lock().unwrap().push((token, sink));
        self.subscribed.notify_one();
        Ok(())
    }
    fn unsubscribe(&self, token: String) { self.unsubscribed.lock().unwrap().push(token); }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { Err(DbError::Network { detail: "no put".into() }) }
}

fn models_json() -> String {
    let mut moment = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    moment.relations.push(feed);

    let mut revision = FieldSpec::scalar("serverRevision", ScalarKind::Int);
    revision.from = Some("revision".into());
    let mut chat = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), revision, FieldSpec::list("lists", ScalarKind::Str)]);
    chat.server_revision = Some("serverRevision".into());
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("ChatNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    let mut listed = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    listed.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(listed);
    chat.relations.extend([node, list]);
    chat.changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"node","payload":"chat"})).unwrap());
    chat.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    serde_json::to_string(&vec![moment, chat]).unwrap()
}

const OPERATIONS: &str = r#"[
    {"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"},
    {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
    {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
    {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
]"#;

pub(super) const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

#[derive(Default)]
struct Heard {
    seen: Mutex<Vec<StreamDelivery>>,
    wake: Notify,
}

impl Reader for Heard {
    fn delivered(&self, delivery: StreamDelivery) {
        self.seen.lock().unwrap().push(delivery);
        self.wake.notify_one();
    }
}

/// A stream the test reads the way a platform does: it pulls values and keeps them.
pub(super) struct Reading {
    watched: Arc<Watched>,
    heard: Arc<Heard>,
}

impl Reading {
    /// Everything pulled so far.
    pub(super) fn all(&self) -> Vec<StreamDelivery> { self.heard.seen.lock().unwrap().clone() }
    /// The values read so far. A state-only value that repeats the state of the one before shows nothing the spec names, so it is left out.
    pub(super) fn vals(&self) -> Vec<Val> {
        let mut last: Option<WState> = None;
        let mut vals = Vec::new();
        for delivery in self.all() {
            let val = val_of(&delivery);
            match &val {
                Val::Snapshot { state, .. } => last = Some(*state),
                Val::StateOnly(state) => {
                    if last == Some(*state) { continue; }
                    last = Some(*state);
                }
                _ => {}
            }
            vals.push(val);
        }
        vals
    }
    /// Reads until `done` holds for everything read so far.
    pub(super) async fn until(&self, mut done: impl FnMut(&[Val]) -> bool) -> Vec<Val> {
        bound(async {
            loop {
                let notified = self.heard.wake.notified();
                let vals = self.vals();
                if done(&vals) { return vals; }
                notified.await;
            }
        }).await
    }
    /// Reads until the first settled snapshot or state of a rows stream.
    pub(super) async fn first_screen(&self) -> Vec<Val> {
        self.until(|vals| vals.iter().any(|val| matches!(val, Val::Snapshot { state, .. } | Val::StateOnly(state) if state.phase != Phase::Loading && state.phase != Phase::Idle))).await
    }
    pub(super) fn cancel(&self) { self.watched.cancel(); }
}

pub(super) struct Call {
    task: tokio::task::JoinHandle<Result<(), DbError>>,
    cancel: Box<dyn Fn() + Send + Sync>,
    done: Option<Outcome>,
}

impl Call {
    pub(super) fn cancel(&self) { (self.cancel)(); }
    /// The outcome now, waiting at most `millis` for it.
    pub(super) async fn outcome_within(&mut self, millis: u64) -> Outcome {
        if let Some(done) = self.done { return done; }
        match tokio::time::timeout(std::time::Duration::from_millis(millis), &mut self.task).await {
            Err(_) => Outcome::Pending,
            Ok(joined) => {
                let outcome = joined.map(|result| outcome_of(&result)).unwrap_or(Outcome::Interrupted);
                self.done = Some(outcome);
                outcome
            }
        }
    }
    pub(super) async fn outcome(&mut self) -> Outcome {
        let outcome = self.outcome_within(LIMIT_SECS * 1000).await;
        assert_ne!(outcome, Outcome::Pending, "the call never ended");
        outcome
    }
}

pub(super) struct World {
    pub(super) db: Arc<Database>,
    pub(super) net: Arc<Net>,
    pub(super) clock: Arc<TestClock>,
}

impl World {
    pub(super) async fn open() -> World {
        let net = Net::new();
        let clock = TestClock::new(1_000_000);
        let db = bound(Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 32 }, models_json(), Some(net.clone()), None, clock.clone(), None, None, None, None)).await.unwrap();
        db.register_operations(OPERATIONS.to_string()).unwrap();
        World { db, net, clock }
    }
    pub(super) fn epoch(&self) -> u64 { self.db.session_epoch() }
    pub(super) fn open_stream(&self, request: ReadRequest) -> Reading {
        let heard = Arc::new(Heard::default());
        let watched = self.db.watch(request, heard.clone());
        Reading { watched, heard }
    }
    pub(super) fn rows_stream(&self) -> Reading { self.open_stream(ReadRequest::List { query_json: FEED.into() }) }
    pub(super) fn events_stream(&self) -> Reading { self.open_stream(ReadRequest::Events { model: "Chat".into() }) }
    pub(super) fn call(&self) -> Call {
        let request = self.db.begin_load(self.epoch(), "Moment".into(), "feed".into(), "{}".into());
        match request {
            Err(error) => Call { task: tokio::spawn(async move { Err(error) }), cancel: Box::new(|| {}), done: None },
            Ok(request) => {
                let waiting = request.clone();
                Call { task: tokio::spawn(async move { waiting.result().await }), cancel: Box::new(move || request.cancel()), done: None }
            }
        }
    }
    /// A load that started before, ends with its outcome and shows the rows the server answered.
    pub(super) async fn load_ok(&self) {
        self.db.load(self.epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    }
    pub(super) async fn load_more(&self) -> Result<crate::ffi::LoadMoreResult, DbError> {
        self.db.load_more(self.epoch(), "Moment".into(), "feed".into(), "{}".into()).await
    }
    pub(super) async fn feed_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = bound(self.db.read_query(self.epoch(), FEED.into())).await.unwrap().into_iter().map(|row| row.id).collect();
        ids.sort();
        ids
    }
    pub(super) async fn chat_ids(&self) -> Vec<String> {
        let query = json!({"model":"Chat"}).to_string();
        let mut ids: Vec<String> = bound(self.db.read_query(self.epoch(), query)).await.unwrap().into_iter().map(|row| row.id).collect();
        ids.sort();
        ids
    }
    /// Reads `stream` until `done` holds, moving the clock a step each round so that a debounce or a backoff on it passes.
    pub(super) async fn pump_until(&self, stream: &Reading, mut done: impl FnMut(&[Val]) -> bool) -> Vec<Val> {
        for _ in 0..300 {
            let vals = stream.vals();
            if done(&vals) { return vals; }
            self.clock.advance(5_000);
            self.breathe().await;
        }
        let vals = stream.vals();
        assert!(done(&vals), "the condition never held, read: {vals:?}");
        vals
    }
    pub(super) async fn settle(&self) { bound(crate::ffi::settled(&self.db)).await.unwrap(); }
    /// Lets the library run what is ready without waiting for a held request.
    pub(super) async fn breathe(&self) {
        for _ in 0..20 { tokio::task::yield_now().await; }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    pub(super) async fn close(&self) { bound(self.db.close_database()).await.unwrap(); }
}

/// Rows and ids sorted, so an order the spec does not name does not decide.
pub(super) fn canon(vals: Vec<Val>) -> Vec<Val> {
    vals.into_iter().map(|val| match val {
        Val::Snapshot { mut rows, state } => { rows.sort(); Val::Snapshot { rows, state } }
        Val::Delta { mut upserted, mut removed } => { upserted.sort(); removed.sort(); Val::Delta { upserted, removed } }
        other => other,
    }).collect()
}

pub(super) fn loaded() -> WState { WState { phase: Phase::Loaded, error: false, loaded: true } }

/// Every window state the stream showed, also the one a Delta carries.
pub(super) fn states_of(stream: &Reading) -> Vec<WState> {
    stream.all().iter().filter_map(|delivery| match &delivery.value {
        ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } | ReadValue::Count { state, .. } => Some(wstate_of(state)),
        _ => None,
    }).collect()
}

/// The ids a reader holds after applying `vals` in order.
pub(super) fn held_ids(vals: &[Val]) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for val in vals {
        match val {
            Val::Snapshot { rows, .. } => ids = rows.clone(),
            Val::Delta { upserted, removed } => {
                ids.retain(|id| !removed.contains(id));
                for id in upserted { if !ids.contains(id) { ids.push(id.clone()); } }
            }
            _ => {}
        }
    }
    ids.sort();
    ids
}
