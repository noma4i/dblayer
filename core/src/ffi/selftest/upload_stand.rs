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
use crate::ffi::{PrepareJob, PrepareOutcome, PrepareProgress, Preparer, ProbeOutcome, ByteProgress, Config, Database, DbError, EncryptedConfig, FakeHttp, EncryptedReply, OpenPorts, PutExecutor, PutReply, PutTarget, ReadFetch, ReadRequest, TestClock, FakeTransport, TransportRequest};
use crate::owners::write::upload::UploadFile;
use crate::server_contract::{Admission, Contract, Request};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{NetworkPurpose, PutFile, PutIdentity, RawOutcome, TransferIdentity};

pub(super) const BLOB: &str = r#"{"data":{"blobCreate":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#;

pub(super) const STEPS: [(&str, &str); 6] = [
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
    pub(super) full: bool,
}

pub(super) struct Script {
    contract: Mutex<Contract<Answer>>,
    forgotten: AtomicBool,
    calls: Mutex<Vec<Call>>,
    wires: Mutex<Vec<(crate::transport::Author, u64, u32)>>,
}

impl Default for Script {
    fn default() -> Self {
        Script { contract: Mutex::new(Contract::adopting()), forgotten: AtomicBool::new(false), calls: Mutex::default(), wires: Mutex::default() }
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
    /// The server registry loses every hash and registers none from now on: only a full document registers again.
    pub(super) fn forget_documents(&self) {
        self.forgotten.store(true, Ordering::SeqCst);
        self.contract.lock().unwrap().forget();
    }
    /// For each send of the operation: does it carry the full document.
    pub(super) fn full_pairs(&self, operation: &str) -> Vec<bool> { self.calls.lock().unwrap().iter().filter(|call| call.operation == operation).map(|call| call.full).collect() }
    /// The documents the app declared: a real server knows them, an undeclared document gets `PERSISTED_QUERY_NOT_FOUND`.
    pub(super) fn register_documents(&self, named: &[(&str, &str)]) {
        if self.forgotten.load(Ordering::SeqCst) { return }
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
        self.script.calls.lock().unwrap().push(Call { operation: operation.clone(), pair: (request["requestId"].as_str().unwrap_or_default().to_string(), request["sequence"].as_i64()), variables: request["body"]["variables"].clone(), purpose, full: request["body"]["query"].is_string() });
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
    pub(super) bodies: Mutex<Vec<Vec<u8>>>,
    reports: AtomicBool,
    failing: Mutex<VecDeque<u64>>,
}

impl Puts {
    pub(super) fn hold_next(&self) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.held.lock().unwrap().push_back(hold.clone());
        hold
    }

    pub(super) fn fail_once_at(&self, offset: u64) { self.failing.lock().unwrap().push_back(offset) }
}

#[async_trait]
impl FakeTransport for Puts {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, url: String, _: HashMap<String, String>, file: PutFile, identity: TransferIdentity, progress: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        if self.reports.load(Ordering::SeqCst) {
            progress.sent(file.length / 4);
            progress.sent(file.length / 2);
        }
        self.tokens.lock().unwrap().push(progress);
        let body = std::fs::read(&file.path).unwrap_or_default();
        self.bodies.lock().unwrap().push(body[(file.offset as usize).min(body.len())..((file.offset + file.length) as usize).min(body.len())].to_vec());
        let held = self.held.lock().unwrap().pop_front();
        self.seen.lock().unwrap().push(Put { url, operation: identity.operation_id, item: identity.item_id, step: identity.step, path: file.path, offset: file.offset, length: file.length });
        if let Some(hold) = held {
            hold.entered.store(true, Ordering::SeqCst);
            crate::lock::effect(|| hold.signal.notify_waiters());
            let _dropped = Dropped(hold.clone());
            hold.gate.notified().await;
        }
        let failed = { let mut failing = self.failing.lock().unwrap(); failing.iter().position(|offset| *offset == file.offset).map(|index| failing.remove(index)) };
        if failed.is_some() { return Err(DbError::Network { detail: "the part was cut".into() }) }
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

/// A system task of the executor outside the process: what started it and how it ended.
#[derive(Clone)]
pub(super) struct SystemTask {
    pub(super) identity: PutIdentity,
    pub(super) outcome: Option<RawOutcome>,
}

/// The executor of the PUTs outside the process. It outlives `close` and `open` of the database, like the system does: a task goes on while no process runs, and `resume_puts` finds it by transfer and attempt.
#[derive(Default)]
pub(super) struct Detached {
    tasks: Mutex<Vec<SystemTask>>,
    started: Mutex<Vec<(PutIdentity, PutTarget, PutFile)>>,
    cancelled: Mutex<Vec<PutIdentity>>,
    resumed: Mutex<Vec<Vec<PutIdentity>>>,
    database: Mutex<Option<std::sync::Weak<Database>>>,
    /// The answers to `resume_puts` that came while the opening database was not attached yet.
    queued: Mutex<Vec<(PutIdentity, Option<RawOutcome>)>>,
}

fn same_task(left: &PutIdentity, right: &PutIdentity) -> bool { left.transfer_id == right.transfer_id && left.attempt_id == right.attempt_id }

impl Detached {
    /// A new open starts: the database of the process before it is gone.
    pub(super) fn detach(&self) { *self.database.lock().unwrap() = None; }

    pub(super) fn attach(&self, database: &Arc<Database>) {
        *self.database.lock().unwrap() = Some(Arc::downgrade(database));
        for (identity, outcome) in std::mem::take(&mut *self.queued.lock().unwrap()) { Self::answer(database, identity, outcome); }
    }

    fn answer(database: &Database, identity: PutIdentity, outcome: Option<RawOutcome>) {
        match outcome {
            Some(outcome) => database.put_ended(identity, outcome),
            None => database.put_missing(identity),
        }
    }
    pub(super) fn started(&self) -> Vec<(PutIdentity, PutTarget, PutFile)> { self.started.lock().unwrap().clone() }
    pub(super) fn cancelled(&self) -> Vec<PutIdentity> { self.cancelled.lock().unwrap().clone() }
    pub(super) fn resumed(&self) -> Vec<Vec<PutIdentity>> { self.resumed.lock().unwrap().clone() }
    pub(super) fn ok() -> RawOutcome { RawOutcome::Response { status: 200, etag: Some("etag".into()), body: String::new() } }

    /// The task `n` ends and the live database hears it.
    pub(super) fn finish(&self, n: usize, outcome: RawOutcome) {
        let identity = self.finish_offline(n, outcome.clone());
        let database = self.database.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade).expect("a database is attached");
        database.put_ended(identity, outcome);
    }

    /// The task `n` ends while no process runs: `resume_puts` reports it.
    pub(super) fn finish_offline(&self, n: usize, outcome: RawOutcome) -> PutIdentity {
        let mut tasks = self.tasks.lock().unwrap();
        tasks[n].outcome = Some(outcome);
        tasks[n].identity
    }

    /// The system lost the task `n`: `resume_puts` finds nothing.
    pub(super) fn lose(&self, n: usize) { self.tasks.lock().unwrap().remove(n); }

    /// A task that a process started before: the identity as the platform keeps it.
    pub(super) fn plant(&self, task: SystemTask) { self.tasks.lock().unwrap().push(task); }
}

impl PutExecutor for Detached {
    fn start_put(&self, identity: PutIdentity, target: PutTarget, file: PutFile) {
        self.started.lock().unwrap().push((identity, target, file));
        self.tasks.lock().unwrap().push(SystemTask { identity, outcome: None });
    }

    fn cancel_put(&self, identity: PutIdentity) {
        self.cancelled.lock().unwrap().push(identity);
        self.tasks.lock().unwrap().retain(|task| task.identity != identity);
    }

    fn resume_puts(&self, identities: Vec<PutIdentity>) {
        self.resumed.lock().unwrap().push(identities.clone());
        self.tasks.lock().unwrap().retain(|task| identities.iter().any(|listed| same_task(&task.identity, listed)));
        let database = self.database.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
        for listed in identities {
            let found = self.tasks.lock().unwrap().iter().find(|task| same_task(&task.identity, &listed)).cloned();
            let answer = match found {
                None => Some(None),
                Some(SystemTask { outcome: Some(outcome), .. }) => Some(Some(outcome)),
                Some(_) => None,
            };
            let Some(answer) = answer else { continue };
            match &database {
                Some(database) => Self::answer(database, listed, answer),
                None => self.queued.lock().unwrap().push((listed, answer)),
            }
        }
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
    preparer: Option<Arc<dyn Preparer>>,
    signed_out: bool,
    /// The registration of the app is complete: the open checks the operations, as an app does.
    pub(super) complete: AtomicBool,
    /// The store the app gives instead of the memory one of the rig.
    pub(super) store: Option<Arc<dyn crate::ffi::CredentialStore>>,
    pub(super) detached: Option<Arc<Detached>>,
}

impl Drop for Stand {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("Stand::drop"); }
}

impl Stand {
    pub(super) fn new() -> Self { Self { rig: SessionRig::new(), script: Arc::default(), puts: Arc::default(), root: None, keep_moment: false, preparer: None, signed_out: false, complete: AtomicBool::new(false), store: None, detached: None } }


    /// The platform runs the PUTs outside the process through `executor`.
    pub(super) fn detached_through(mut self, executor: Arc<Detached>) -> Self { self.detached = Some(executor); self }

    /// The open does not sign in: the test signs in itself.
    pub(super) fn signed_out(mut self) -> Self { self.signed_out = true; self }

    pub(super) fn with_preparer(mut self, preparer: Arc<dyn Preparer>) -> Self {
        self.preparer = Some(preparer);
        self
    }

    pub(super) fn keep_moment(mut self) -> Self { self.keep_moment = true; self }

    pub(super) fn reporting_bytes(self) -> Self {
        self.puts.reports.store(true, Ordering::SeqCst);
        self
    }

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
        if let Some(detached) = &self.detached { detached.detach(); }
        let restored = self.signed_out || self.rig.credentials.inner.value().is_some();
        let models = self.models();
        let ports = OpenPorts { partial_operations: !self.complete.load(Ordering::SeqCst), transport: Some(self.puts.clone()), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(self.store.clone().unwrap_or_else(|| self.rig.credentials.clone())), http: Some(Arc::new(ScriptedHttp { script: self.script.clone(), session: ThroughServer(self.rig.server.clone()) })), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), preparer: self.preparer.clone(), detached_put: self.detached.is_some(), put_executor: self.detached.clone().map(|executor| executor as Arc<dyn PutExecutor>), ..Default::default() };
        let config = Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None };
        let database = match clock {
            Some(clock) => Database::open_with_clock(config, crate::ffi::specs(models), ports, clock).await,
            None => Database::open(config, crate::ffi::specs(models), ports).await,
        }
        .unwrap();
        if let Some(detached) = &self.detached { detached.attach(&database); }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PrepareEvent {
    Probe(Vec<u8>, crate::ffi::PrepareKind),
    Run(u64),
    Cancel(u64),
}

pub(super) struct ScriptedPreparer {
    probe: ProbeOutcome,
    bytes: Vec<u8>,
    fractions: Vec<f64>,
    holds: Mutex<VecDeque<Arc<Hold>>>,
    failures: Mutex<VecDeque<String>>,
    outcomes: Mutex<VecDeque<PrepareOutcome>>,
    jobs: Mutex<Vec<PrepareJob>>,
    events: Mutex<Vec<PrepareEvent>>,
    cancelled: Mutex<std::collections::HashSet<u64>>,
    running: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl ScriptedPreparer {
    pub(super) fn new(probe: ProbeOutcome, bytes: &[u8], fractions: &[f64]) -> Arc<Self> {
        Arc::new(Self { probe, bytes: bytes.to_vec(), fractions: fractions.to_vec(), holds: Mutex::default(), failures: Mutex::default(), outcomes: Mutex::default(), jobs: Mutex::default(), events: Mutex::default(), cancelled: Mutex::default(), running: Default::default(), peak: Default::default() })
    }

    pub(super) fn hold_next(&self) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.holds.lock().unwrap().push_back(hold.clone());
        hold
    }

    pub(super) fn fail_next(&self, message: &str) { self.failures.lock().unwrap().push_back(message.into()) }
    pub(super) fn outcome_next(&self, outcome: PrepareOutcome) { self.outcomes.lock().unwrap().push_back(outcome) }
    pub(super) fn jobs(&self) -> Vec<PrepareJob> { self.jobs.lock().unwrap().clone() }
    pub(super) fn events(&self) -> Vec<PrepareEvent> { self.events.lock().unwrap().clone() }
    pub(super) fn probes(&self) -> Vec<Vec<u8>> { self.events().into_iter().filter_map(|event| if let PrepareEvent::Probe(content, _) = event { Some(content) } else { None }).collect() }
    pub(super) fn kinds(&self) -> Vec<crate::ffi::PrepareKind> { self.events().into_iter().filter_map(|event| if let PrepareEvent::Probe(_, kind) = event { Some(kind) } else { None }).collect() }
    pub(super) fn runs(&self) -> Vec<u64> { self.events().into_iter().filter_map(|event| if let PrepareEvent::Run(attempt) = event { Some(attempt) } else { None }).collect() }
    pub(super) fn cancels(&self) -> Vec<u64> { self.events().into_iter().filter_map(|event| if let PrepareEvent::Cancel(attempt) = event { Some(attempt) } else { None }).collect() }
    pub(super) fn peak_runs(&self) -> usize { self.peak.load(Ordering::SeqCst) }
    pub(super) fn calls(&self) -> usize { self.events().iter().filter(|event| !matches!(event, PrepareEvent::Cancel(_))).count() }
}

struct Running<'a>(&'a ScriptedPreparer);

impl Drop for Running<'_> {
    fn drop(&mut self) { self.0.running.fetch_sub(1, Ordering::SeqCst); }
}

#[async_trait]
impl Preparer for ScriptedPreparer {
    async fn probe(&self, path: String, kind: crate::ffi::PrepareKind) -> ProbeOutcome {
        self.events.lock().unwrap().push(PrepareEvent::Probe(std::fs::read(&path).unwrap_or_default(), kind));
        self.probe.clone()
    }

    async fn run(&self, job: PrepareJob, progress: Arc<PrepareProgress>) -> PrepareOutcome {
        self.events.lock().unwrap().push(PrepareEvent::Run(job.attempt_id));
        self.jobs.lock().unwrap().push(job.clone());
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let _running = Running(self);
        let hold = self.holds.lock().unwrap().pop_front();
        let (early, late) = self.fractions.split_at(if hold.is_some() { self.fractions.len().min(1) } else { self.fractions.len() });
        for fraction in early { progress.report(*fraction); }
        if let Some(hold) = hold {
            hold.entered.store(true, Ordering::SeqCst);
            crate::lock::effect(|| hold.signal.notify_waiters());
            let _dropped = Dropped(hold.clone());
            hold.gate.notified().await;
        }
        if self.cancelled.lock().unwrap().contains(&job.attempt_id) { return PrepareOutcome::Failed { message: "cancelled".into() } }
        for fraction in late { progress.report(*fraction); }
        if let Some(outcome) = self.outcomes.lock().unwrap().pop_front() { return outcome }
        if let Some(message) = self.failures.lock().unwrap().pop_front() { return PrepareOutcome::Failed { message } }
        std::fs::write(&job.destination, &self.bytes).unwrap();
        PrepareOutcome::Done { name: Some("ready.jpg".into()), mime: Some("image/jpeg".into()) }
    }

    fn cancel(&self, attempt_id: u64) {
        self.cancelled.lock().unwrap().insert(attempt_id);
        self.events.lock().unwrap().push(PrepareEvent::Cancel(attempt_id));
    }
}

/// The source of `supply_source` that carries the prepare settings of the transfer.
pub(super) fn source_of(path: &Path, values_json: &str, prepare: &crate::ffi::upload_wire::WirePrepare) -> crate::ffi::upload_wire::WireUploadSource {
    crate::ffi::upload_wire::WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: values_json.into(), variables_json: "{}".into(), prepare: Some(prepare.clone()) }
}

/// The video the platform read: `reencode` is a source that the table of limits cannot keep as it is, `keys` are the metadata keys of the file.
pub(super) fn video_probe(keys: &[&str], reencode: bool) -> ProbeOutcome {
    ProbeOutcome::Video { source: crate::prepare::VideoSource { width: 1280, height: 720, rotation_degrees: if reencode { 90 } else { 0 }, frame_rate: 30.0, bitrate: Some(1_000_000), mp4_h264_aac: true }, metadata_keys: keys.iter().map(|key| key.to_string()).collect() }
}

/// The settings of a video that say what a device that cannot decode the source does: the field of the settings comes with the type.
pub(super) fn cannot_decode_settings(upload_source: bool) -> crate::ffi::upload_wire::WirePrepare {
    crate::ffi::upload_wire::WirePrepare::Video { limits: Some(crate::prepare::default_video_limits()), mirror: false, on_cannot_decode: if upload_source { crate::ffi::upload_wire::CannotDecode::UploadSource } else { crate::ffi::upload_wire::CannotDecode::Fail }, max_bytes: None }
}

/// The typed reason of a failed upload: the field of the failure comes with the type.
pub(super) fn failure_reason(upload: &crate::ffi::wire::WireUpload) -> Option<String> {
    upload.error.as_ref().and_then(|error| error.reason.clone())
}
