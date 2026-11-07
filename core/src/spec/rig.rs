use std::collections::{HashMap, VecDeque};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::ffi::{ByteProgress, Config, Database, DbError, FakeHttp, EncryptedReply, PutReply, TestClock, FakeTransport, TransportRequest, WireOperationOutcome, WireOperationReceipt, WireRow};
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, OnFailure, RootShape};
use crate::owners::write::upload::{MultipartOperations, UploadOperation, UploadOperations};
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response, MemoryCredentials};
use crate::transport::CredentialSlot;
use super::harness_life::Held;
use crate::value::ScalarKind;

pub const LIMIT_SECS: u64 = 10;
const STEP_SECS: u64 = 25;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reply { Ok, Temp, Refuse, NotFound }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind { Mutate = 0, Put = 1, Query = 2 }

#[derive(Clone, Debug)]
pub(super) struct Seen { pub kind: Kind, pub operation: String, pub variables: Value }

pub(super) type Pair = (String, String, Option<i64>);

#[derive(Default)]
struct State {
    seen: Vec<Seen>,
    held: [VecDeque<(String, oneshot::Sender<Reply>)>; 3],
    hold: [bool; 3],
    hold_ops: Vec<String>,
    auto: [VecDeque<Reply>; 3],
    script: HashMap<String, VecDeque<Reply>>,
    cancelled: usize,
    next: u64,
    in_flight: HashMap<String, usize>,
    overlap: bool,
    settled: HashMap<Pair, Value>,
}

pub(super) struct Server { state: Mutex<State> }

struct Flight { server: Arc<Server>, row: Option<String>, done: bool }

impl Drop for Flight {
    fn drop(&mut self) {
        let mut state = self.server.state.lock().unwrap();
        if let Some(row) = &self.row {
            if let Some(count) = state.in_flight.get_mut(row) { *count -= 1; }
        }
        if !self.done { state.cancelled += 1; }
    }
}

impl Server {
    pub fn new() -> Arc<Self> { Arc::new(Self { state: Mutex::new(State::default()) }) }
    pub fn hold(&self, kind: Kind, on: bool) { self.state.lock().unwrap().hold[kind as usize] = on; }
    pub fn release_all(&self) {
        let mut state = self.state.lock().unwrap();
        state.hold = [false, false, false];
        state.hold_ops.clear();
        state.held = Default::default();
    }
    pub fn script(&self, operation: &str, reply: Reply) { self.state.lock().unwrap().script.entry(operation.into()).or_default().push_back(reply); }
    pub fn seen(&self) -> Vec<Seen> { self.state.lock().unwrap().seen.clone() }
    pub fn count(&self, operation: &str) -> usize { self.seen().iter().filter(|seen| seen.operation == operation).count() }
    pub fn mutations(&self) -> usize { self.seen().iter().filter(|seen| seen.kind == Kind::Mutate).count() }
    pub fn cancelled(&self) -> usize { self.state.lock().unwrap().cancelled }
    pub fn overlapped(&self) -> bool { self.state.lock().unwrap().overlap }
    pub fn held(&self, kind: Kind) -> usize {
        let mut state = self.state.lock().unwrap();
        state.held[kind as usize].retain(|(_, sender)| !sender.is_closed());
        state.held[kind as usize].len()
    }

    pub fn answer(&self, kind: Kind, reply: Reply) {
        let mut state = self.state.lock().unwrap();
        state.held[kind as usize].retain(|(_, sender)| !sender.is_closed());
        let sender = state.held[kind as usize].pop_front();
        let sender = sender.expect("no held request to answer").1;
        drop(state);
        crate::lock::effect(|| { let _ = sender.send(reply); });
    }

    pub fn answer_text(&self, text: &str, reply: Reply) {
        let mut state = self.state.lock().unwrap();
        let held = &mut state.held[Kind::Mutate as usize];
        held.retain(|(_, sender)| !sender.is_closed());
        let position = held.iter().position(|(held_text, _)| held_text == text).unwrap_or_else(|| panic!("no held request for {text}"));
        let sender = held.remove(position).unwrap().1;
        drop(state);
        crate::lock::effect(|| { let _ = sender.send(reply); });
    }

    pub fn holds_text(&self, text: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        state.held[Kind::Mutate as usize].retain(|(_, sender)| !sender.is_closed());
        state.held[Kind::Mutate as usize].iter().any(|(held_text, _)| held_text == text)
    }

    pub async fn wait_held(&self, kind: Kind, count: usize) { until("held requests", || self.held(kind) >= count).await }
    pub async fn wait_seen(&self, operation: &str, count: usize) { until(operation, || self.count(operation) >= count).await }

    async fn decide(self: &Arc<Self>, kind: Kind, operation: &str, variables: Value) -> Reply {
        let row = variables.get("momentId").and_then(Value::as_str).map(str::to_string);
        let text = variables.get("text").and_then(Value::as_str).unwrap_or("").to_string();
        let (receiver, reply) = {
            let mut state = self.state.lock().unwrap();
            state.seen.push(Seen { kind, operation: operation.into(), variables });
            if let Some(row) = &row {
                let count = state.in_flight.entry(row.clone()).or_default();
                *count += 1;
                if *count > 1 { state.overlap = true; }
            }
            let scripted = state.script.get_mut(operation).and_then(VecDeque::pop_front);
            match scripted {
                Some(reply) => (None, Some(reply)),
                None if state.hold[kind as usize] || state.hold_ops.iter().any(|held| held == operation) => {
                    let (sender, receiver) = oneshot::channel();
                    state.held[kind as usize].push_back((text.clone(), sender));
                    (Some(receiver), None)
                }
                None => (None, Some(state.auto[kind as usize].pop_front().unwrap_or(Reply::Ok))),
            }
        };
        let mut flight = Flight { server: self.clone(), row, done: false };
        let reply = match (receiver, reply) {
            (Some(receiver), _) => receiver.await.unwrap_or(Reply::Temp),
            (None, Some(reply)) => reply,
            (None, None) => Reply::Ok,
        };
        flight.done = true;
        reply
    }

    /// The server answers a repeated pair `(operation, request id, sequence)` with the answer it gave first. A temporary failure settles nothing.
    pub(super) fn known(&self, pair: &Pair) -> Option<Value> { self.state.lock().unwrap().settled.get(pair).cloned() }

    pub(super) fn settle(&self, pair: Pair, answer: Value) -> Value { self.state.lock().unwrap().settled.entry(pair).or_insert(answer).clone() }

    fn next(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        state.next
    }
}

pub(super) struct Wire(pub Arc<Server>);

fn answer_of(operation: &str, variables: &Value, number: u64) -> Value {
    let text = variables.get("text").cloned().unwrap_or(Value::Null);
    match operation {
        "MakeMoment" => json!({"data":{"makeMoment":{"__typename":"Moment","id":format!("srv{number}"),"text":text}}}),
        "DeleteMoment" => json!({"data":{"deleteMoment":{"id":variables["id"]}}}),
        "UpdateMoment" => json!({"data":{"updateMoment":{"__typename":"Moment","id":variables["momentId"],"text":text}}}),
        "CreateBlob" => json!({"data":{"createBlob":{"url":format!("https://put.invalid/{number}"),"headers":[],"signedId":format!("sig{number}"),"blobId":format!("blob{number}")}}}),
        "InitMultipart" => json!({"data":{"initMultipart":{"key":"k1","uploadId":"u1"}}}),
        "PresignParts" => {
            let parts: Vec<Value> = variables["partNumbers"].as_array().cloned().unwrap_or_default().into_iter().map(|part| json!({"partNumber":part,"url":format!("https://put.invalid/part/{part}"),"headers":[]})).collect();
            json!({"data":{"presignParts":{"parts":parts}}})
        }
        "CompleteMultipart" => json!({"data":{"completeMultipart":{"signedId":"sigm","blobId":"blobm"}}}),
        _ => json!({"data":{"abortMultipart":{"ok":true}}}),
    }
}

#[async_trait]
impl FakeTransport for Wire {
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        self.0.state.lock().unwrap().seen.push(Seen { kind: Kind::Query, operation: request.operation_name, variables: serde_json::from_str(&request.variables_json)? });
        Ok(json!({"data":{"moments":[]}}).to_string())
    }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        let variables: Value = serde_json::from_str(&request.variables_json).unwrap_or(Value::Null);
        let reply = self.0.decide(Kind::Mutate, &request.operation_name, variables.clone()).await;
        match reply {
            Reply::Ok => Ok(answer_of(&request.operation_name, &variables, self.0.next()).to_string()),
            Reply::Temp => Err(DbError::Network { detail: "temporary".into() }),
            Reply::Refuse => Err(DbError::Graphql { messages: vec!["refused".into()], extensions: None }),
            Reply::NotFound => Err(DbError::Graphql { messages: vec!["gone".into()], extensions: None }),
        }
    }
    async fn put(&self, url: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let reply = self.0.decide(Kind::Put, "put", json!({"url": url})).await;
        match reply {
            Reply::Ok => Ok(PutReply { status: 200, etag: Some(format!("etag-{}", self.0.next())) }),
            Reply::Temp => Err(DbError::Network { detail: "temporary".into() }),
            Reply::Refuse => Err(DbError::Http { status: 400, detail: "refused".into() }),
            Reply::NotFound => Err(DbError::Http { status: 404, detail: "gone".into() }),
        }
    }
}

#[async_trait]
impl FakeHttp for Wire {
    async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let sequence = request["sequence"].as_i64();
        if request["body"]["operationName"] == "Feed" {
            self.0.state.lock().unwrap().seen.push(Seen { kind: Kind::Query, operation: "Feed".into(), variables: request["body"]["variables"].clone() });
            return Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, json!({"data":{"moments":[]}})) });
        }
        let answer = match path.as_str() {
            "/api/auth/login_link" => json!({"sessionId": request["body"]["account"]}),
            "/api/logout" => json!({"revoked": true}),
            _ => {
                let name = request["body"]["operationName"].as_str().unwrap_or_default().to_string();
                let variables = request["body"]["variables"].clone();
                let pair = (name.clone(), request["requestId"].as_str().unwrap_or_default().to_string(), sequence);
                if let Some(known) = self.0.known(&pair) { return Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, known) }); }
                let answer = match self.0.decide(Kind::Mutate, &name, variables.clone()).await {
                    Reply::Ok => answer_of(&name, &variables, self.0.next()),
                    Reply::Temp => return Err(DbError::Network { detail: "temporary".into() }),
                    Reply::Refuse => json!({"errors": [{"message": "refused", "extensions": {"code": "REFUSED"}}]}),
                    Reply::NotFound => json!({"errors": [{"message": "gone", "extensions": {"code": "NOT_FOUND"}}]}),
                };
                self.0.settle(pair, answer)
            }
        };
        Ok(EncryptedReply { status: 200, body: seal_response(&request, sequence, answer) })
    }
}

pub const OPERATIONS: &str = r#"[
{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"},
{"name":"UpdateMoment","document":"mutation UpdateMoment { updateMoment { id text } }","rootPath":"updateMoment"},
{"name":"DeleteMoment","document":"mutation DeleteMoment { deleteMoment { id } }","rootPath":"deleteMoment"},
{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url } }","rootPath":"createBlob"},
{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key } }","rootPath":"initMultipart"},
{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts } }","rootPath":"presignParts"},
{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId } }","rootPath":"completeMultipart"},
{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }","rootPath":"abortMultipart"}]"#;

pub fn moment_models(on_failure: OnFailure) -> String {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.upload_steps = Some(upload_operations());
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("MakeMoment".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.on_failure = on_failure;
    create.variables.insert("text".into(), "text".into());
    model.actions.push(create);
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("UpdateMoment".into());
    update.has_optimistic = true;
    update.has_temp_row_retention = true;
    update.on_failure = on_failure;
    update.variables.insert("momentId".into(), "id".into());
    update.variables.insert("text".into(), "text".into());
    model.actions.push(update);
    let mut destroy = ActionDeclaration::new("destroy", RootShape::Destroy);
    destroy.mutation = Some("DeleteMoment".into());
    destroy.variables.insert("id".into(), "id".into());
    model.actions.push(destroy);
    let mut upload = ActionDeclaration::new("upload", RootShape::Upsert);
    upload.upload = true;
    upload.mutation = Some("MakeMoment".into());
    upload.on_failure = on_failure;
    model.actions.push(upload);
    serde_json::to_string(&vec![model]).unwrap()
}

pub fn upload_operations() -> UploadOperations {
    UploadOperations {
        create_blob: Some(UploadOperation::new("CreateBlob")),
        multipart: Some(MultipartOperations { initiate: UploadOperation::new("InitMultipart"), presign: UploadOperation::new("PresignParts"), complete: UploadOperation::new("CompleteMultipart"), abort: UploadOperation::new("AbortMultipart") }),
    }
}

pub(super) struct Rig { pub db: Arc<Database>, pub server: Arc<Server>, pub clock: Arc<TestClock> }

impl Drop for Rig {
    fn drop(&mut self) {
        self.server.release_all();
        let db = self.db.clone();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let closed = runtime.block_on(async { tokio::time::timeout(Duration::from_secs(LIMIT_SECS), db.close_database()).await });
            let _ = done.send(closed.is_ok());
        });
        let closed = finished.recv_timeout(Duration::from_secs(LIMIT_SECS + 5));
        if !std::thread::panicking() {
            assert!(matches!(closed, Ok(true)), "the database did not close within the limit: a holder close still waits");
        }
    }
}

impl Rig {
    pub async fn open(on_failure: OnFailure) -> Rig { Self::open_at(on_failure, None, Server::new()).await }

    pub async fn open_at(on_failure: OnFailure, path: Option<String>, server: Arc<Server>) -> Rig {
        let models = moment_models(on_failure);
        Self::open_models(models, path, server).await
    }

    pub async fn open_models(models: String, path: Option<String>, server: Arc<Server>) -> Rig {
        let clock = TestClock::new(1_000_000);
        let mut encrypted = crate::transport::encrypted::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
        let credentials = credentials_of(path.as_deref());
        let signed = CredentialSlot::get(&*credentials).unwrap().is_some();
        let held = Arc::new(Held(credentials));
        let db = within(Database::open_with_clock(Config { path, data_version: None }, models.clone(), crate::ffi::OpenPorts { transport: Some(Arc::new(Wire(server.clone()))), encrypted: Some(encrypted), credentials: Some(held), http: Some(Arc::new(Wire(server.clone()))) }, clock.clone())).await.unwrap();
        db.register_operations(OPERATIONS.into()).await.unwrap();
        db.set_online(true).unwrap();
        if !signed { within(db.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap(); }
        Rig { db, server, clock }
    }

    pub fn epoch(&self) -> u64 { self.db.session_epoch() }

    pub async fn create(&self, text: &str) -> WireOperationReceipt { within(self.db.send_create("Moment", &json!({"text": text}).to_string())).await }

    pub async fn update(&self, row: &str, text: &str) -> Result<WireOperationReceipt, DbError> {
        within(self.db.update(self.epoch(), "Moment".into(), row.into(), json!({"text": text}).to_string(), "{}".into())).await
    }

    pub async fn row(&self, id: &str) -> Option<WireRow> { within(self.db.read_row(self.epoch(), "Moment".into(), id.into())).await.unwrap() }

    pub async fn outcome(&self, receipt: &WireOperationReceipt) -> WireOperationOutcome { within(self.db.outcome(receipt.clone())).await.unwrap() }

    pub async fn pending(&self, receipt: &WireOperationReceipt) -> bool {
        tokio::time::timeout(Duration::from_millis(200), self.db.outcome(receipt.clone())).await.is_err()
    }
}

/// The credential of a file database outlives its reopen, as the credential store of the app does.
fn credentials_of(path: Option<&str>) -> Arc<MemoryCredentials> {
    static KEPT: std::sync::LazyLock<Mutex<HashMap<String, Arc<MemoryCredentials>>>> = std::sync::LazyLock::new(Default::default);
    let Some(path) = path else { return Arc::default() };
    KEPT.lock().unwrap().entry(path.to_string()).or_default().clone()
}

pub async fn within<T>(work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(LIMIT_SECS), work).await.expect("did not finish within the limit")
}

pub async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(LIMIT_SECS);
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "{what}: the event did not come");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

impl Rig {
    /// Returns once the owners handled every input and every started effect task, also while a server request is held.
    pub async fn quiet(&self) {
        let owners = self.db.life_handle();
        let idle = within(async { tokio::task::spawn_blocking(move || owners.barrier(Duration::from_secs(10))).await.unwrap() }).await;
        assert!(idle, "the owners still hold inputs or started effect tasks");
    }
}

struct Steps { count: AtomicU64, label: Mutex<String> }

thread_local! { static STEPS: RefCell<Option<Arc<Steps>>> = const { RefCell::new(None) }; }

pub fn step(label: &str) {
    STEPS.with(|steps| {
        if let Some(steps) = steps.borrow().as_ref() {
            *steps.label.lock().unwrap() = label.to_string();
            steps.count.fetch_add(1, Ordering::SeqCst);
        }
    });
}

pub fn bounded<Fut: std::future::Future<Output = ()>>(name: &'static str, make: impl FnOnce() -> Fut + Send + 'static) {
    let steps = Arc::new(Steps { count: AtomicU64::new(0), label: Mutex::new("start".into()) });
    let (done, finished) = std::sync::mpsc::channel();
    let own = steps.clone();
    std::thread::spawn(move || {
        STEPS.with(|slot| *slot.borrow_mut() = Some(own));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            runtime.block_on(make());
        }));
        let _ = done.send(result);
    });
    let mut seen = u64::MAX;
    loop {
        match finished.recv_timeout(Duration::from_secs(STEP_SECS)) {
            Ok(Ok(())) => return,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => {
                let count = steps.count.load(Ordering::SeqCst);
                assert!(count != seen, "{name}: step {count} ({}) did not finish within {STEP_SECS} seconds: the event did not come", steps.label.lock().unwrap());
                seen = count;
            }
        }
    }
}
