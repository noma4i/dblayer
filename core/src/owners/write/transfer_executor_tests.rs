//! Tests of `TransferExecutor` on `DetScheduler`: every IO effect gives its own input to `Transfers`.
//! A file directory lives in a temp folder, the transport is a stub. Names `exec_xfer_*`.
//! Copy in chunks outside the writer -> `exec_xfer_copies_big_file_to_directory`, `exec_xfer_copy_cancel_leaves_nothing`, `exec_xfer_copy_failed_removes_transfer`
//! · step, complete and abort with `NetworkPurpose::Transfer` and data by value -> `exec_xfer_steps_carry_transfer_purpose`, `exec_xfer_step_error_aborts_with_upload_id`
//! · `Transport Cancel` of the step -> `exec_xfer_offline_cancels_step` · `uploads[action]` and local copy deletion -> `exec_xfer_view_and_local_copy`.
//! Store batches, Outbox inputs and the retry timer are owner effects, tested in the owner tests of the machine.

use crate::schema::OnFailure;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use serde_json::json;

use super::outbox::{Fault, NewOp, Origin, OutboxOutcome};
use super::rig::{admit, peer, settle, Act, ManualSpawn, Stub};
use crate::owners::support::{StoreStub, StoreStubState};
use super::transfer::{file_id, BatchItem, Shown, Source, Steps, TransferAnswer, TransferInput, TransferPeers, TransferStage, TransferState, Transfers};
use super::transfer_executor::{Link, StepCodec, StepDone, TransferExecutor};
use super::wire::Wire;
use crate::files::FileDirectories;
use crate::owners::contract::{Online, ReplyId};
use crate::runtime::{Address, DetScheduler, EpochStamp, Time};
use crate::transport::{GraphQLRequest, NetworkPurpose, Response, TransportError};

const EPOCH: u64 = 1;
const KEY: &str = "Moment:r1:upload";
const MIB: usize = 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }

/// What the codec was asked, by value: step, part, upload id, source path.
type Asked = (String, Option<u32>, Option<String>, String);

#[derive(Default)]
struct Codec {
    asked: Mutex<Vec<Asked>>,
}

#[async_trait]
impl StepCodec for Codec {
    async fn step(&self, link: &Link, _key: &str, part: Option<u32>, upload_id: Option<&str>, source: &Source) -> Result<StepDone, TransportError> {
        lock(&self.asked).push(("Step".into(), part, upload_id.map(str::to_string), source.path.clone()));
        link.mutate(GraphQLRequest::new("mutation Step", "Step", json!({ "part": part }))).await?;
        Ok(StepDone { upload_id: (part == Some(1)).then(|| "u1".to_string()), etag: None })
    }

    async fn complete(&self, link: &Link, _key: &str, upload_id: Option<&str>, _parts: &[(u32, Option<String>)]) -> Result<Option<String>, TransportError> {
        lock(&self.asked).push(("Complete".into(), None, upload_id.map(str::to_string), String::new()));
        link.mutate(GraphQLRequest::new("mutation Complete", "Complete", json!({}))).await.map(|_| Some("signed".to_string()))
    }

    async fn discard_upload(&self, link: &Link, _key: &str, upload_id: Option<&str>) -> Result<(), TransportError> {
        lock(&self.asked).push(("Abort".into(), None, upload_id.map(str::to_string), String::new()));
        link.mutate(GraphQLRequest::new("mutation Abort", "Abort", json!({}))).await.map(|_| ())
    }
}

struct World {
    det: DetScheduler,
    spawn: ManualSpawn,
    stub: Stub,
    transfers: Address<Transfers>,
    files: Arc<FileDirectories>,
    codec: Arc<Codec>,
    answers: Arc<Mutex<Vec<(ReplyId, TransferAnswer)>>>,
    shown: Arc<Mutex<Vec<Shown>>>,
    dir: tempfile::TempDir,
}

fn mutation() -> NewOp {
    NewOp { model: "Moment".into(), row_id: "r1".into(), action: "upload".into(), on_failure: OnFailure::Keep, origin: Origin::Api, path: "/moments/r1".into(), body: String::new(), effects: Default::default(), durable: true }
}

fn world() -> World {
    let dir = tempfile::tempdir().expect("temp dir");
    let (spawn, stub) = (ManualSpawn::default(), Stub::default());
    let files = Arc::new(FileDirectories::beside(&dir.path().join("db.sqlite")));
    let codec = Arc::new(Codec::default());
    let (answers, shown) = (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new())));
    let (to_answers, to_shown) = (answers.clone(), shown.clone());
    let executor = TransferExecutor::new(
        Wire { transport: Arc::new(stub.clone()), admit: Arc::new(admit), tasks: Arc::new(spawn.clone()) },
        codec.clone(),
        files.clone(),
        Arc::new(move |reply, answer| lock(&to_answers).push((reply, answer))),
        Arc::new(move |_key, shown| lock(&to_shown).push(shown)),
        Arc::new(|_| {}),
        Arc::new(|_| {}),
    );
    let mut det = DetScheduler::new(13);
    let generation = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let store_stub = StoreStubState { to: Address::<Transfers>::new(2), generation, revision: 0, only: None, committed: TransferInput::Committed, rejected: TransferInput::Rejected, note: Box::new(|_| {}) };
    let store = Address::new(det.add::<StoreStub<Transfers>>("store", store_stub, crate::owners::support::Silent).index());
    let (outbox, _) = peer(&mut det, "outbox");
    let transfers = det.add::<Transfers>("transfers", TransferState::new(EPOCH, 2, TransferPeers { store, outbox, rounds: None }), executor);
    det.start_epoch(EPOCH);
    let mut world = World { det, spawn, stub, transfers, files, codec, answers, shown, dir };
    world.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default() });
    world.post(TransferInput::RestoreReady { epoch: EPOCH });
    world.post(TransferInput::Online(Online(true)));
    world
}

impl World {
    fn source(&self, key: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.path().join(format!("source-{}", file_id(key)));
        std::fs::write(&path, bytes).expect("write source");
        path
    }

    fn accept_input(&self, key: &str, path: PathBuf, parts: u32) -> TransferInput {
        TransferInput::Accept { epoch: EPOCH, item: Box::new(BatchItem { key: key.into(), source: Source { path: path.to_string_lossy().into_owned(), parts }, mutation: mutation(), steps: Steps { path: "/upload".into(), body: String::new() } }), reply: 1, now: Time(0) }
    }

    fn post(&mut self, input: TransferInput) {
        self.det.post(self.transfers, EpochStamp::Epoch(EPOCH), input);
        self.settle();
    }

    fn accept(&mut self, key: &str, path: PathBuf, parts: u32) {
        let input = self.accept_input(key, path, parts);
        self.post(input);
    }

    fn stage(&self, key: &str) -> Option<TransferStage> { self.det.state(self.transfers).expect("transfer state").transfers.values().rev().find(|transfer| transfer.key == key).map(|transfer| transfer.stage) }

    fn calls(&self) -> Vec<(String, NetworkPurpose)> { self.stub.calls().into_iter().map(|call| (call.operation_name, call.purpose)).collect() }

    fn asked(&self) -> Vec<Asked> { lock(&self.codec.asked).clone() }

    fn settle(&mut self) {
        let transfers = self.transfers;
        settle(&mut self.det, &self.spawn, |det| det.post(transfers, EpochStamp::Control, TransferInput::Clock(Time(0))));
    }

    fn late(&self, prefixes: &[&str]) -> usize {
        self.det.trace().iter().filter(|entry| entry.owner == "transfers" && prefixes.iter().any(|prefix| entry.input.starts_with(prefix))).count()
    }
}

#[test]
fn exec_xfer_copies_big_file_to_directory() {
    let mut w = world();
    let big: Vec<u8> = (0..3 * MIB + 17).map(|i| (i % 251) as u8).collect();
    let (big_key, small_key) = ("big:r1:upload", "small:r1:upload");
    let (big_path, small_path) = (w.source(big_key, &big), w.source(small_key, b"0123456789"));
    w.stub.script([Act::Hang, Act::Hang]);
    w.accept(big_key, big_path, 2);
    w.accept(small_key, small_path, 1);
    assert_eq!(w.files.read(&file_id(big_key)).expect("copy in the directory"), big);
    assert_eq!(w.files.directory_entries().expect("entries"), vec![file_id(big_key)], "a file over 256 KiB is on disk, a small one stays in the table");
    assert_eq!((w.stage(big_key), w.stage(small_key)), (Some(TransferStage::Uploading), Some(TransferStage::Uploading)));
}

#[test]
fn exec_xfer_copy_cancel_leaves_nothing() {
    let mut w = world();
    let path = w.source(KEY, &vec![7u8; 2 * MIB]);
    let accept = w.accept_input(KEY, path, 1);
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), accept);
    while w.det.step() {}
    assert_eq!((w.stage(KEY), w.spawn.pending()), (Some(TransferStage::Copying), 1));
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::Cancel { key: KEY.into(), reply: 2 });
    while w.det.step() {}
    w.settle();
    assert_eq!((w.stage(KEY), w.spawn.pending()), (Some(TransferStage::Done), 0));
    assert!(w.files.directory_entries().expect("entries").is_empty());
    assert_eq!(w.late(&["CopyDone", "CopyFailed"]), 0);
    assert!(lock(&w.answers).contains(&(2, TransferAnswer::Cancelled(true))));
}

#[test]
fn exec_xfer_copy_failed_removes_transfer() {
    let mut w = world();
    let missing = w.dir.path().join("missing");
    w.accept(KEY, missing, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Done));
    assert!(lock(&w.answers).iter().any(|(reply, answer)| *reply == 1 && matches!(answer, TransferAnswer::Err(Fault::Copy(_)))));
    assert_eq!(lock(&w.shown).last(), Some(&Shown::Gone));
    assert!(w.files.directory_entries().expect("entries").is_empty());
}

#[test]
fn exec_xfer_steps_carry_transfer_purpose() {
    let mut w = world();
    let path = w.source(KEY, b"abc");
    w.accept(KEY, path.clone(), 2);
    assert_eq!(w.stage(KEY), Some(TransferStage::Mutating));
    let (step, complete) = (("Step".to_string(), NetworkPurpose::Transfer), ("Complete".to_string(), NetworkPurpose::Transfer));
    assert_eq!(w.calls(), vec![step.clone(), step, complete]);
    let source = path.to_string_lossy().into_owned();
    assert_eq!(
        w.asked(),
        vec![
            ("Step".to_string(), Some(1), None, source.clone()),
            ("Step".to_string(), Some(2), Some("u1".to_string()), source),
            ("Complete".to_string(), None, Some("u1".to_string()), String::new()),
        ],
        "the upload id of the first step reaches the next effects by value"
    );
}

#[test]
fn exec_xfer_step_error_aborts_with_upload_id() {
    let mut w = world();
    let path = w.source(KEY, b"abc");
    w.stub.script([Act::Reply(Ok(Response::data(json!({})))), Act::Reply(Err(TransportError::Graphql { messages: vec!["refused".into()], extensions: None }))]);
    w.accept(KEY, path, 3);
    assert_eq!(w.stage(KEY), Some(TransferStage::Failed));
    let step = ("Step".to_string(), NetworkPurpose::Transfer);
    assert_eq!(w.calls(), vec![step.clone(), step, ("Abort".to_string(), NetworkPurpose::Transfer)]);
    assert_eq!(w.asked().last(), Some(&("Abort".to_string(), None, Some("u1".to_string()), String::new())));
}

#[test]
fn exec_xfer_offline_cancels_step() {
    let mut w = world();
    let path = w.source(KEY, b"abc");
    w.stub.script([Act::Hang]);
    w.accept(KEY, path, 2);
    assert_eq!((w.stage(KEY), w.spawn.pending()), (Some(TransferStage::Uploading), 1));
    w.post(TransferInput::Online(Online(false)));
    assert_eq!((w.stage(KEY), w.stub.dropped(), w.spawn.pending()), (Some(TransferStage::Ready), 1, 0));
    assert_eq!(w.late(&["StepOk", "StepError"]), 0, "a cancelled step gives no answer");
}

#[test]
fn exec_xfer_view_and_local_copy() {
    let mut w = world();
    let path = w.source(KEY, &vec![5u8; 300 * 1024]);
    w.accept(KEY, path, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Mutating));
    assert_eq!(lock(&w.shown).first(), Some(&Shown::Copying));
    assert!(w.files.read(&file_id(KEY)).is_ok(), "the local copy is on disk before Confirmed");
    w.post(TransferInput::MutationOutcome { key: KEY.into(), outcome: OutboxOutcome::Confirmed { revision: 3 } });
    assert_eq!(w.stage(KEY), Some(TransferStage::Done));
    assert!(w.files.read(&file_id(KEY)).is_err(), "the local copy is deleted after Confirmed");
}
