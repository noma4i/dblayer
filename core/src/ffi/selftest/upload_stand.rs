//! The stand of the wrapper upload scenarios: the models of `fixtures/*.dbl`, the real session path, operations answered from a script per operation name, a transport that records the PUTs.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::tape::Rows;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::wire::WireSyncState;
use crate::ffi::{ByteProgress, Config, Database, DbError, EncryptedConfig, FakeHttp, EncryptedReply, OpenPorts, PutReply, ReadFetch, ReadRequest, TestClock, FakeTransport, TransportRequest};
use crate::owners::write::upload::UploadFile;
use crate::test_wait::yield_until;
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{NetworkPurpose, PutFile, TransferIdentity};

pub(super) const BLOB: &str = r#"{"data":{"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#;

const STEPS: [(&str, &str); 6] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("InitMultipart", "mutation InitMultipart { initMultipart { key uploadId } }"),
    ("PresignParts", "mutation PresignParts { presignParts { parts { partNumber url } } }"),
    ("CompleteMultipart", "mutation CompleteMultipart { completeMultipart { signedId blobId } }"),
    ("AbortMultipart", "mutation AbortMultipart { abortMultipart { ok } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];

pub(super) enum Answer {
    Data(String),
    Status(u16),
    Lost,
    Held(Arc<Hold>, Box<Answer>),
}

#[derive(Default)]
pub(super) struct Hold {
    entered: AtomicBool,
    dropped: AtomicBool,
    gate: Notify,
}

struct Dropped(Arc<Hold>);

impl Drop for Dropped {
    fn drop(&mut self) { self.0.dropped.store(true, Ordering::SeqCst) }
}

impl Hold {
    pub(super) async fn entered(&self) { yield_until("the held request arrived", || self.entered.load(Ordering::SeqCst)).await }
    pub(super) async fn dropped(&self) { yield_until("the library dropped the held request", || self.dropped.load(Ordering::SeqCst)).await }
    pub(super) fn release(&self) { crate::lock::effect(|| self.gate.notify_one()) }
}

pub(super) struct Call {
    pub(super) operation: String,
    pub(super) variables: Value,
    pub(super) purpose: NetworkPurpose,
}

#[derive(Default)]
pub(super) struct Script {
    queues: Mutex<HashMap<String, VecDeque<Answer>>>,
    calls: Mutex<Vec<Call>>,
    settled: Mutex<HashMap<Pair, Answer>>,
}

type Pair = (String, String, Option<i64>);

fn settled_of(answer: &Answer) -> Option<Answer> {
    match answer {
        Answer::Data(text) => Some(Answer::Data(text.clone())),
        Answer::Status(status) => Some(Answer::Status(*status)),
        Answer::Held(_, next) => settled_of(next),
        Answer::Lost => None,
    }
}

impl Script {
    pub(super) fn enqueue(&self, operation: &str, answer: Answer) { self.queues.lock().unwrap().entry(operation.into()).or_default().push_back(answer) }
    pub(super) fn data(&self, operation: &str, body: &str) { self.enqueue(operation, Answer::Data(body.into())) }
    pub(super) fn hold(&self, operation: &str, reply: &str) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.enqueue(operation, Answer::Held(hold.clone(), Box::new(Answer::Data(reply.into()))));
        hold
    }
    /// The server answers a repeated pair `(request id, sequence)` of an operation with the answer it gave the first time. A different pair takes the next scripted answer. A lost answer settles nothing.
    pub(super) fn answer(&self, operation: &str, request_id: &str, sequence: Option<i64>) -> Option<Answer> {
        let pair = (operation.to_string(), request_id.to_string(), sequence);
        if let Some(known) = self.settled.lock().unwrap().get(&pair) { return settled_of(known); }
        let answer = self.queues.lock().unwrap().get_mut(operation).and_then(VecDeque::pop_front)?;
        if let Some(known) = settled_of(&answer) { self.settled.lock().unwrap().insert(pair, known); }
        Some(answer)
    }
    pub(super) fn names(&self) -> Vec<String> { self.calls.lock().unwrap().iter().map(|call| call.operation.clone()).collect() }
    pub(super) fn count(&self, operation: &str) -> usize { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).count() }
    pub(super) fn variables(&self, operation: &str) -> Vec<Value> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.variables.clone()).collect() }
    pub(super) fn purposes(&self, operation: &str) -> Vec<NetworkPurpose> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.purpose).collect() }
}

struct ScriptedHttp {
    script: Arc<Script>,
    session: ThroughServer,
}

#[async_trait]
impl FakeHttp for ScriptedHttp {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let Some(operation) = request["body"]["operationName"].as_str().map(str::to_string) else { return self.session.post(path, body, purpose).await };
        self.script.calls.lock().unwrap().push(Call { operation: operation.clone(), variables: request["body"]["variables"].clone(), purpose });
        let mut answer = self.script.answer(&operation, request["requestId"].as_str().unwrap_or_default(), request["sequence"].as_i64());
        loop {
            match answer {
                Some(Answer::Held(hold, next)) => {
                    hold.entered.store(true, Ordering::SeqCst);
                    let _dropped = Dropped(hold.clone());
                    hold.gate.notified().await;
                    answer = Some(*next);
                }
                Some(Answer::Lost) => return Err(DbError::Network { detail: "lost answer".into() }),
                Some(Answer::Status(status)) => return Ok(EncryptedReply { status, body: String::new() }),
                Some(Answer::Data(text)) => return Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), serde_json::from_str(&text).unwrap()) }),
                None => return Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), json!({"errors":[{"message":format!("unscripted {operation}")}]})) }),
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
}

#[derive(Default)]
pub(super) struct Puts {
    pub(super) seen: Mutex<Vec<Put>>,
    held: Mutex<VecDeque<Arc<Hold>>>,
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
    async fn put(&self, url: String, _: HashMap<String, String>, file: PutFile, identity: TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let held = self.held.lock().unwrap().pop_front();
        self.seen.lock().unwrap().push(Put { url, operation: identity.operation_id, item: identity.item_id, step: identity.step, path: file.path });
        if let Some(hold) = held {
            hold.entered.store(true, Ordering::SeqCst);
            hold.gate.notified().await;
        }
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

pub(super) struct Stand {
    pub(super) rig: SessionRig,
    pub(super) script: Arc<Script>,
    pub(super) puts: Arc<Puts>,
}

impl Stand {
    pub(super) fn new() -> Self { Self { rig: SessionRig::new(), script: Arc::default(), puts: Arc::default() } }

    pub(super) async fn open(&self, path: Option<&Path>, operations: &[(&str, &str)]) -> Arc<Database> { self.open_with(path, operations, None).await }

    pub(super) async fn open_with(&self, path: Option<&Path>, operations: &[(&str, &str)], clock: Option<Arc<TestClock>>) -> Arc<Database> {
        let restored = self.rig.credentials.inner.value().is_some();
        let models = include_str!("upload_wrapper_models.json").to_string();
        let ports = OpenPorts { transport: Some(self.puts.clone()), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), credentials: Some(self.rig.credentials.clone()), http: Some(Arc::new(ScriptedHttp { script: self.script.clone(), session: ThroughServer(self.rig.server.clone()) })), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
        let config = Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None };
        let database = match clock {
            Some(clock) => Database::open_with_clock(config, crate::ffi::specs(models), ports, clock).await,
            None => Database::open(config, crate::ffi::specs(models), ports).await,
        }
        .unwrap();
        let named: Vec<(&str, &str)> = STEPS.iter().copied().filter(|(name, _)| !operations.iter().any(|(declared, _)| declared == name)).chain(operations.iter().copied()).collect();
        let declared: Vec<Value> = named.iter().map(|(name, document)| json!({"name": name, "document": document})).collect();
        database.set_online(true).unwrap();
        database.register_operations(Value::Array(declared).to_string()).await.unwrap();
        if !restored {
            super::rig_tests::bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
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
