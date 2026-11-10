//! The stand of the wrapper upload scenarios: the models of `fixtures/*.dbl`, the real session path, operations answered from a script per operation name, a transport that records the PUTs.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::tests::encrypted_tests::ThroughServer;
use crate::ffi::tests::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::tape::Rows;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::wire::WireSyncState;
use crate::ffi::{ByteProgress, Config, Database, DbError, EncryptedConfig, FakeHttp, EncryptedReply, OpenPorts, PutReply, ReadFetch, ReadRequest, TestClock, FakeTransport, TransportRequest};
use crate::owners::write::upload::UploadFile;
use crate::server_contract::{Admission, Contract, Request};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{NetworkPurpose, PutFile, TransferIdentity};

pub(super) const BLOB: &str = r#"{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#;

const STEPS: [(&str, &str); 6] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("InitMultipart", "mutation InitMultipart { initMultipart { key uploadId } }"),
    ("PresignParts", "mutation PresignParts { presignParts { parts { partNumber url } } }"),
    ("CompleteMultipart", "mutation CompleteMultipart { completeMultipart { signedId blobId } }"),
    ("AbortMultipart", "mutation AbortMultipart { abortMultipart { ok } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];

#[derive(Clone)]
pub(super) enum Answer {
    Data(String),
    Unconfirmed(String),
    Unreadable,
    Status(u16),
    ResponseTooLarge(u64),
    Lost,
    Held(Arc<Hold>, Box<Answer>),
}

#[derive(Default)]
pub(super) struct Hold {
    entered: AtomicBool,
    dropped: AtomicBool,
    gate: Notify,
    signal: Notify,
}

struct Dropped(Arc<Hold>);

impl Drop for Dropped {
    fn drop(&mut self) { self.0.dropped.store(true, Ordering::SeqCst); crate::lock::effect(|| self.0.signal.notify_waiters()); }
}

impl Hold {
    pub(super) async fn entered(&self) { crate::test_wait::signalled(&self.signal, "the held request arrived", || self.entered.load(Ordering::SeqCst)).await }
    pub(super) async fn dropped(&self) { crate::test_wait::signalled(&self.signal, "the library dropped the held request", || self.dropped.load(Ordering::SeqCst)).await }
    pub(super) fn release(&self) { crate::lock::effect(|| self.gate.notify_one()) }
}

pub(super) struct Call {
    pub(super) operation: String,
    pub(super) pair: (String, Option<i64>),
    pub(super) variables: Value,
    pub(super) purpose: NetworkPurpose,
}

pub(super) struct Script {
    contract: Mutex<Contract<Answer>>,
    calls: Mutex<Vec<Call>>,
    wires: Mutex<Vec<(crate::transport::Author, u64, u32)>>,
}

impl Default for Script {
    fn default() -> Self {
        Script { contract: Mutex::new(Contract::adopting()), calls: Mutex::default(), wires: Mutex::default() }
    }
}

/// The hash the core sends for a document: sha256 of its text, the same as `graphql_body`.
fn document_hash(document: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(document.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn settled_of(answer: &Answer) -> Option<Answer> {
    match answer {
        Answer::Data(text) => Some(Answer::Data(text.clone())),
        Answer::Status(_) | Answer::ResponseTooLarge(_) | Answer::Unconfirmed(_) | Answer::Unreadable | Answer::Lost => None,
        Answer::Held(_, next) => settled_of(next),
    }
}

impl Script {
    pub(super) fn enqueue(&self, operation: &str, answer: Answer) { self.contract.lock().unwrap().script(operation, answer) }
    pub(super) fn data(&self, operation: &str, body: &str) { self.enqueue(operation, Answer::Data(body.into())) }
    pub(super) fn hold(&self, operation: &str, reply: &str) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.enqueue(operation, Answer::Held(hold.clone(), Box::new(Answer::Data(reply.into()))));
        hold
    }
    pub(super) fn wires(&self) -> Vec<(crate::transport::Author, u64, u32)> { self.wires.lock().unwrap().clone() }
    pub(super) fn names(&self) -> Vec<String> { self.calls.lock().unwrap().iter().map(|call| call.operation.clone()).collect() }
    pub(super) fn count(&self, operation: &str) -> usize { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).count() }
    pub(super) fn pairs(&self, operation: &str, filename: &str) -> Vec<(String, Option<i64>)> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation && call.variables["filename"] == filename).map(|call| call.pair.clone()).collect() }
    pub(super) fn operation_pairs(&self, operation: &str) -> Vec<(String, Option<i64>)> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.pair.clone()).collect() }
    pub(super) fn variables(&self, operation: &str) -> Vec<Value> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.variables.clone()).collect() }
    pub(super) fn purposes(&self, operation: &str) -> Vec<NetworkPurpose> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.purpose).collect() }
    /// The documents the app declared: a real server knows them, an undeclared document gets `PERSISTED_QUERY_NOT_FOUND`.
    pub(super) fn register_documents(&self, named: &[(&str, &str)]) {
        let mut contract = self.contract.lock().unwrap();
        for (_, document) in named { contract.register(&document_hash(document)); }
    }
}

struct ScriptedHttp {
    script: Arc<Script>,
    session: ThroughServer,
}

#[async_trait]
impl FakeHttp for ScriptedHttp {
    fn seen(&self, request: &crate::transport::HttpRequest) { self.script.wires.lock().unwrap().push((request.author, request.request_id, request.attempt)) }

    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let Some(operation) = request["body"]["operationName"].as_str().map(str::to_string) else { return self.session.post(path, body, purpose).await };
        self.script.calls.lock().unwrap().push(Call { operation: operation.clone(), pair: (request["requestId"].as_str().unwrap_or_default().to_string(), request["sequence"].as_i64()), variables: request["body"]["variables"].clone(), purpose });
        let contract_request = Request::from_wire("upload", &request);
        let admission = self.script.contract.lock().unwrap().admit(&contract_request);
        let mut answer = match admission {
            Admission::Stored(stored) => Some(stored),
            Admission::TwoBodies => panic!("the pair {} went with two bodies", contract_request.request_id),
            Admission::Refused(refusal) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, None, refusal) }),
            Admission::Fresh => {
                let mut contract = self.script.contract.lock().unwrap();
                let taken = contract.take(&operation);
                if let Some(known) = taken.as_ref().and_then(settled_of) { contract.settle(&contract_request, known); }
                taken
            }
        };
        loop {
            match answer {
                Some(Answer::Held(hold, next)) => {
                    hold.entered.store(true, Ordering::SeqCst);
            crate::lock::effect(|| hold.signal.notify_waiters());
                    let _dropped = Dropped(hold.clone());
                    hold.gate.notified().await;
                    answer = Some(*next);
                }
                Some(Answer::Lost) => return Err(DbError::Network { detail: "lost answer".into() }),
                Some(Answer::Status(status)) => return Ok(EncryptedReply { status, body: String::new() }),
                Some(Answer::ResponseTooLarge(limit)) => return Err(DbError::ResponseTooLarge { limit }),
                Some(Answer::Data(text)) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), serde_json::from_str(&text).unwrap()) }),
                Some(Answer::Unconfirmed(text)) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, None, serde_json::from_str(&text).unwrap()) }),
                Some(Answer::Unreadable) => return Ok(EncryptedReply { status: 200, body: "not a sealed answer".into() }),
                None => panic!("the request {operation} has no scripted answer"),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Put {
    pub(super) url: String,
    pub(super) operation: i64,
    pub(super) item: String,
    pub(super) step: crate::transport::TransferStep,
    pub(super) path: String,
    pub(super) offset: u64,
    pub(super) length: u64,
}

#[derive(Default)]
pub(super) struct Puts {
    pub(super) seen: Mutex<Vec<Put>>,
    held: Mutex<VecDeque<Arc<Hold>>>,
    pub(super) tokens: Mutex<Vec<Arc<ByteProgress>>>,
}

impl Puts {
    pub(super) fn hold_next(&self) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.held.lock().unwrap().push_back(hold.clone());
        hold
    }
}

#[async_trait]
impl FakeTransport for Puts {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, url: String, _: HashMap<String, String>, file: PutFile, identity: TransferIdentity, progress: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.tokens.lock().unwrap().push(progress);
        let held = self.held.lock().unwrap().pop_front();
        self.seen.lock().unwrap().push(Put { url, operation: identity.operation_id, item: identity.item_id, step: identity.step, path: file.path, offset: file.offset, length: file.length });
        if let Some(hold) = held {
            hold.entered.store(true, Ordering::SeqCst);
            crate::lock::effect(|| hold.signal.notify_waiters());
            let _dropped = Dropped(hold.clone());
            hold.gate.notified().await;
        }
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

/// The order of the inputs the platform gives an opening database. A restored session needs no session call: that step is then a no-op.
#[derive(Clone, Copy, Debug)]
pub(super) enum OpenOrder {
    OnlineFirst,
    RegisterFirst,
    OnlineBetweenRegisterAndSession,
}

impl OpenOrder {
    pub(super) const ALL: [OpenOrder; 3] = [Self::OnlineFirst, Self::RegisterFirst, Self::OnlineBetweenRegisterAndSession];
}

pub(super) struct Stand {
    pub(super) rig: SessionRig,
    pub(super) script: Arc<Script>,
    pub(super) puts: Arc<Puts>,
    root: Option<Option<&'static str>>,
    keep_moment: bool,
}

impl Stand {
    pub(super) fn new() -> Self { Self { rig: SessionRig::new(), script: Arc::default(), puts: Arc::default(), root: None, keep_moment: false } }

    pub(super) fn keep_moment(mut self) -> Self { self.keep_moment = true; self }

    pub(super) fn declaring_root(mut self, root: Option<&'static str>) -> Self {
        self.root = Some(root);
        self
    }

    fn models(&self) -> String {
        let text = include_str!("upload_wrapper_models.json");
        if self.root.is_none() && !self.keep_moment { return text.to_string() }
        let mut models: Value = serde_json::from_str(text).unwrap();
        for model in models.as_array_mut().unwrap() {
            if self.keep_moment && model["key"] == "Moment" {
                let action = model["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "create").unwrap();
                action["on_failure"] = json!("keep");
            }
            let Some(root) = self.root else { continue };
            let Some(create) = model["upload_steps"].get_mut("create_blob") else { continue };
            match root {
                Some(root) => create["root_path"] = json!(root),
                None => { create.as_object_mut().unwrap().remove("root_path"); }
            }
        }
        models.to_string()
    }

    pub(super) async fn open(&self, path: Option<&Path>, operations: &[(&str, &str)]) -> Arc<Database> { self.open_with(path, operations, None).await }

    pub(super) async fn open_with(&self, path: Option<&Path>, operations: &[(&str, &str)], clock: Option<Arc<TestClock>>) -> Arc<Database> { self.open_ordered(path, operations, clock, OpenOrder::OnlineFirst).await }

    pub(super) async fn open_ordered(&self, path: Option<&Path>, operations: &[(&str, &str)], clock: Option<Arc<TestClock>>, order: OpenOrder) -> Arc<Database> {
        let restored = self.rig.credentials.inner.value().is_some();
        let models = self.models();
        let ports = OpenPorts { transport: Some(self.puts.clone()), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(self.rig.credentials.clone()), http: Some(Arc::new(ScriptedHttp { script: self.script.clone(), session: ThroughServer(self.rig.server.clone()) })), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
        let config = Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None };
        let database = match clock {
            Some(clock) => Database::open_with_clock(config, crate::ffi::specs(models), ports, clock).await,
            None => Database::open(config, crate::ffi::specs(models), ports).await,
        }
        .unwrap();
        let named: Vec<(&str, &str)> = STEPS.iter().copied().filter(|(name, _)| !operations.iter().any(|(declared, _)| declared == name)).chain(operations.iter().copied()).collect();
        self.script.register_documents(&named);
        let declared: Vec<Value> = named.iter().map(|(name, document)| json!({"name": name, "document": document})).collect();
        let register = || database.register_operations(Value::Array(declared.clone()).to_string());
        let session = || async { if !restored { super::rig_tests::bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap(); } };
        match order {
            OpenOrder::OnlineFirst => { database.set_online(true).unwrap(); register().await.unwrap(); session().await; }
            OpenOrder::RegisterFirst => { register().await.unwrap(); session().await; database.set_online(true).unwrap(); }
            OpenOrder::OnlineBetweenRegisterAndSession => { register().await.unwrap(); database.set_online(true).unwrap(); session().await; }
        }
        database
    }
}

pub(super) fn request(path: &Path) -> WireUploadRequest { WireUploadRequest::of(&UploadFile::new(path.to_str().unwrap())) }

pub(super) fn clip(model: &str, values: &str, action: &str, path: &Path, epoch: u64) -> CommandRequest {
    CommandRequest { epoch, model: model.into(), values_json: values.into(), args_json: "{}".into(), upload: Some(CommandUpload { action: action.into(), request: request(path) }) }
}

pub(super) fn plain(model: &str, values: &str, epoch: u64) -> CommandRequest {
    CommandRequest { epoch, model: model.into(), values_json: values.into(), args_json: "{}".into(), upload: None }
}

pub(super) const CREATE_MOMENT: (&str, &str) = ("CreateMoment", "mutation CreateMoment { createMoment { id uuid likedByMe media createdAt } }");

pub(super) fn watched(database: &Database, model: &str, id: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    drop(database.watch(ReadRequest::Entity { model: model.into(), id: id.into(), fetch: ReadFetch::Never }, rows.clone()));
    rows
}

pub(super) fn shows(rows: &Rows, wanted: WireSyncState) -> bool { rows.0.lock().unwrap().first().is_some_and(|row| row.sync.state == wanted) }
