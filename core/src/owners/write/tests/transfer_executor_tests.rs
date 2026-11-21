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

use super::outbox::{Fault, NewOp, OpSource, OutboxOutcome, ReserveTarget};
use super::rig::{open_gate, peer, settle, Act, ManualSpawn, Stub};
use crate::spec::support::{StoreStub, StoreStubState};
use super::transfer::{file_id, BatchItem, Shown, Source, Steps, TransferAnswer, TransferInput, TransferPeers, TransferStage, TransferTick, TransferState, Transfers};
use super::transfer_executor::{Link, StepCodec, StepDone, TransferExecutor, TransferSinks};
use super::wire::Wire;
use crate::store::files::FileDirectories;
use crate::owners::contract::{Online, ReplyId};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::transport::{CancelToken, GraphQLRequest, NetworkPurpose, PutFile, PutRequest, Response, TransferIdentity, TransferStep, TransportError};

const EPOCH: u64 = 1;
const KEY: &str = "Moment:r1:upload";
const MIB: usize = 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }

/// What the codec was asked, by value: step, part, upload id, source path.
type Asked = (String, Option<u32>, Option<String>, String);

#[derive(Default)]
struct Codec {
    asked: Mutex<Vec<Asked>>,
    /// The step is a PUT of the file instead of a mutation.
    put: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl StepCodec for Codec {
    async fn step(&self, link: &Link, _operation: u64, part: Option<u32>, upload_id: Option<&str>, source: &Source, _steps: &Steps) -> Result<StepDone, TransportError> {
        lock(&self.asked).push(("Step".into(), part, upload_id.map(str::to_string), source.path.clone()));
        if self.put.load(std::sync::atomic::Ordering::SeqCst) {
            let request = PutRequest { reason: crate::transport::RequestReason::Send, url: "https://storage/part".into(), headers: Vec::new(), file: PutFile { path: source.path.clone(), offset: 0, length: 1 }, identity: TransferIdentity { operation_id: 0, item_id: "k".into(), step: TransferStep::Put }, cancel: CancelToken::new(), sent: Arc::new(|_| {}) };
            link.put(request).await?;
        } else {
            link.mutate(GraphQLRequest::new("mutation Step", "Step", json!({ "part": part })), &step_key()).await?;
        }
        Ok(StepDone { upload_id: (part == Some(1)).then(|| "u1".to_string()), etag: None, ..Default::default() })
    }

    async fn complete(&self, link: &Link, _key: &str, upload_id: Option<&str>, _parts: &[(u32, Option<String>)]) -> Result<Option<String>, TransportError> {
        lock(&self.asked).push(("Complete".into(), None, upload_id.map(str::to_string), String::new()));
        link.mutate(GraphQLRequest::new("mutation Complete", "Complete", json!({})), &step_key()).await.map(|_| Some("signed".to_string()))
    }

    async fn discard_upload(&self, link: &Link, upload_id: Option<&str>) -> Result<(), TransportError> {
        lock(&self.asked).push(("Abort".into(), None, upload_id.map(str::to_string), String::new()));
        link.mutate(GraphQLRequest::new("mutation Abort", "Abort", json!({})), &step_key()).await.map(|_| ())
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
    reads: Arc<Mutex<Vec<(ReplyId, u64, u64)>>>,
    dir: tempfile::TempDir,
}

fn mutation() -> NewOp {
    NewOp { model: "Moment".into(), row_id: "r1".into(), action: "upload".into(), on_failure: OnFailure::Keep, origin: OpSource::Api, path: "/moments/r1".into(), body: "{}".into(), effects: Default::default(), expires_after: None }
}

fn world() -> World {
    let dir = tempfile::tempdir().expect("temp dir");
    let (spawn, stub) = (ManualSpawn::default(), Stub::default());
    let files = Arc::new(FileDirectories::beside(&dir.path().join("db.sqlite")));
    let codec = Arc::new(Codec::default());
    let (answers, shown) = (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new())));
    let (to_answers, to_shown) = (answers.clone(), shown.clone());
    let reads = Arc::new(Mutex::new(Vec::new()));
    let to_reads = reads.clone();
    let executor = TransferExecutor::new(
        Wire { gate: open_gate(EPOCH, Arc::new(stub.clone())), tasks: Arc::new(spawn.clone()) },
        codec.clone(),
        files.clone(),
        TransferSinks {
            settle: Arc::new(move |reply, answer| lock(&to_answers).push((reply, answer))),
            read_outcome: Arc::new(move |_, id, cycle, reply, _| lock(&to_reads).push((reply, id, cycle))),
            status: Arc::new(move |_stream, shown, _figures, _revision| lock(&to_shown).push(shown)),
            drained: Arc::new(|_| {}),
            signal: Arc::new(|_| {}),
        },
        Arc::new(|_| super::transfer::CachedBlob::Miss),
    );
    let mut det = DetScheduler::new(13);
    let store_stub = StoreStubState { to: Address::<Transfers>::new(2), revision: 0, committed: TransferInput::Committed, note: Box::new(|_| {}) };
    let store = Address::new(det.add::<StoreStub<Transfers>>("store", store_stub, crate::spec::support::Silent).index());
    let (outbox, _) = peer(&mut det, "outbox");
    let transfers = det.add::<Transfers>("transfers", TransferState::new(EPOCH, 2, TransferPeers { store, outbox, rounds: None }), executor);
    det.start_epoch(EPOCH);
    let mut world = World { det, spawn, stub, transfers, files, codec, answers, shown, reads, dir };
    world.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
    world.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    world.post(TransferInput::RestoreReady { epoch: EPOCH });
    world.post(TransferInput::Online(Online(true)));
    world.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 1 });
    lock(&world.shown).clear();
    world
}

impl World {
    fn source(&self, key: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.path().join(format!("source-{}", key.replace([':', '/'], "_")));
        std::fs::write(&path, bytes).expect("write source");
        path
    }

    fn accept_input(&self, key: &str, path: PathBuf, parts: u32) -> TransferInput {
        TransferInput::Accept { epoch: EPOCH, item: Box::new(BatchItem { key: key.into(), held: false, on_row: false, fields: Default::default(), urgent: false, source: Some(Source { path: path.to_string_lossy().into_owned(), parts }), prepare: None, mutation: mutation(), steps: Some(Steps { path: "/upload".into(), body: super::upload_codec::StepsBody { multipart: false, name: "clip.mp4".into(), mime: "video/mp4".into(), signed_variable: "signedId".into(), item: "id".into() }.steps().body }), dimensions: None, max_bytes: None }), reply: 1 }
    }

    fn post(&mut self, input: TransferInput) {
        self.det.post(self.transfers, EpochStamp::Epoch(EPOCH), input);
        self.settle();
    }

    fn accept(&mut self, key: &str, path: PathBuf, parts: u32) {
        let input = self.accept_input(key, path, parts);
        self.post(input);
        let id = self.det.state(self.transfers).unwrap().open_id(key).unwrap();
        self.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    }

    /// The id of the copy of the transfer of `key` in the file directory.
    fn copy(&self, key: &str) -> String { file_id(self.det.state(self.transfers).expect("transfer state").transfers.values().rev().find(|transfer| transfer.key == key).map(|transfer| transfer.transfer_id).expect("transfer")) }

    fn stage(&self, key: &str) -> Option<TransferStage> { self.det.state(self.transfers).expect("transfer state").transfers.values().rev().find(|transfer| transfer.key == key).map(|transfer| transfer.stage) }

    fn calls(&self) -> Vec<(String, NetworkPurpose)> { self.stub.calls().into_iter().map(|call| (call.operation_name, call.purpose)).collect() }

    fn asked(&self) -> Vec<Asked> { lock(&self.codec.asked).clone() }

    fn settle(&mut self) {
        let transfers = self.transfers;
        settle(&mut self.det, &self.spawn, |det| det.post(transfers, EpochStamp::Control, TransferInput::Tick { kind: TransferTick::Retry { transfer_id: u64::MAX, attempt_id: 0 } }));
    }

    fn late(&self, kind: fn(&TransferInput) -> bool) -> usize {
        self.det.trace().iter().filter(|entry| entry.owner == "transfers" && entry.input_is::<TransferInput>(kind)).count()
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
    assert_eq!(std::fs::read(w.files.path(&w.copy(big_key)).unwrap()).expect("copy in the directory"), big);
    assert_eq!(std::fs::read(w.files.path(&w.copy(small_key)).unwrap()).expect("small copy in the directory"), b"0123456789", "every copy lies in the directory: the steps read one place");
    assert_eq!((w.stage(big_key), w.stage(small_key)), (Some(TransferStage::Uploading), Some(TransferStage::Uploading)));
}

#[test]
fn exec_xfer_copy_cancel_leaves_nothing() {
    let mut w = world();
    let path = w.source(KEY, &vec![7u8; 2 * MIB]);
    let accept = w.accept_input(KEY, path, 1);
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), accept);
    while w.det.step() {}
    let id = w.det.state(w.transfers).unwrap().open_id(KEY).unwrap();
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    while w.det.step() {}
    assert_eq!((w.stage(KEY), w.spawn.pending()), (Some(TransferStage::Copying), 1));
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::Cancel { key: KEY.into(), reply: 2 });
    while w.det.step() {}
    w.settle();
    assert_eq!((w.stage(KEY), w.spawn.pending()), (None, 0));
    assert!(w.files.directory_entries().expect("entries").is_empty());
    assert_eq!(w.late(|input| matches!(input, TransferInput::CopyDone { .. } | TransferInput::CopyFailed { .. })), 0);
    assert!(lock(&w.answers).contains(&(2, TransferAnswer::Cancelled(true))));
}

#[test]
fn exec_xfer_copy_failed_removes_transfer() {
    let mut w = world();
    let missing = w.dir.path().join("missing");
    w.accept(KEY, missing, 1);
    assert_eq!(w.stage(KEY), None);
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
    let source = w.files.path(&w.copy(KEY)).expect("directory").to_string_lossy().into_owned();
    let _ = path;
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
    assert_eq!(w.late(|input| matches!(input, TransferInput::StepOk { .. } | TransferInput::StepError { .. })), 0, "a cancelled step gives no answer");
}

#[test]
fn exec_xfer_view_and_local_copy() {
    let mut w = world();
    let path = w.source(KEY, &vec![5u8; 300 * 1024]);
    w.accept(KEY, path, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Mutating));
    assert_eq!(lock(&w.shown).first(), Some(&Shown::Copying));
    let copy = w.copy(KEY);
    let receipt = lock(&w.answers).iter().find_map(|(reply, answer)| match (reply, answer) { (1, TransferAnswer::Receipt(receipt)) => Some(receipt.clone()), _ => None }).expect("receipt");
    assert!(std::fs::read(w.files.path(&copy).unwrap()).is_ok(), "the local copy is on disk before Confirmed");
    let id = w.det.state(w.transfers).unwrap().open_id(KEY).unwrap();
    w.post(TransferInput::MutationOutcome { transfer_id: id, cycle_id: 1, intent_removed: true, outcome: OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None }, row_never_created: false });
    assert_eq!(w.stage(KEY), None);
    assert!(std::fs::read(w.files.path(&copy).unwrap()).is_err(), "the local copy is deleted after Confirmed");
    w.post(TransferInput::Outcome { receipt, reply: 2 , discard_on_failure: false});
    assert_eq!(*lock(&w.reads), vec![(2, id, 1)], "a finished transfer leaves the memory: its receipt is read from the Store");
}

#[test]
fn xfer_confirmed_waits_file_delete() {
    let mut w = world();
    let path = w.source(KEY, &vec![5u8; 300 * 1024]);
    w.stub.script([Act::Reply(Err(TransportError::Http { status: 400, body: "refused".into() }))]);
    w.accept(KEY, path, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Failed));
    let copy = w.files.path(&w.copy(KEY)).unwrap();
    let receipt = lock(&w.answers).iter().find_map(|(reply, answer)| match (reply, answer) { (1, TransferAnswer::Receipt(receipt)) => Some(receipt.clone()), _ => None }).unwrap();
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::Outcome { receipt, reply: 2, discard_on_failure: true });
    while w.det.step() {}
    assert_eq!(w.spawn.pending(), 1);
    assert!(copy.exists());
    assert!(lock(&w.answers).iter().all(|(reply, _)| *reply != 2));
    w.settle();
    assert!(!copy.exists());
    assert!(lock(&w.answers).iter().any(|(reply, answer)| *reply == 2 && matches!(answer, TransferAnswer::Outcome(OutboxOutcome::Rejected { error }, None) if error.contains("400"))));
}

#[test]
fn xfer_confirmed_delete_spawn_failed() {
    let mut w = world();
    drop(crate::store::Store::open_models(Some(&w.dir.path().join("db.sqlite")), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let path = w.source(KEY, &vec![5u8; 300 * 1024]);
    w.stub.script([Act::Reply(Err(TransportError::Http { status: 400, body: "refused".into() }))]);
    w.accept(KEY, path, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Failed));
    let copy = w.files.path(&w.copy(KEY)).unwrap();
    let receipt = lock(&w.answers).iter().find_map(|(reply, answer)| match (reply, answer) { (1, TransferAnswer::Receipt(receipt)) => Some(receipt.clone()), _ => None }).unwrap();
    w.spawn.reject_next_blocking();
    w.post(TransferInput::Outcome { receipt, reply: 2, discard_on_failure: true });
    assert!(lock(&w.answers).iter().any(|(reply, answer)| *reply == 2 && matches!(answer, TransferAnswer::Outcome(OutboxOutcome::Rejected { error }, None) if error.contains("400"))));
    assert!(copy.exists());
    let reopened = crate::store::Store::open_models(Some(&w.dir.path().join("db.sqlite")), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(!copy.exists());
    drop(reopened);
}

#[test]
fn xfer_confirmed_delete_failure_keeps_rejection() {
    use std::os::unix::fs::PermissionsExt;
    let mut w = world();
    drop(crate::store::Store::open_models(Some(&w.dir.path().join("db.sqlite")), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
    let path = w.source(KEY, &vec![5u8; 300 * 1024]);
    w.stub.script([Act::Reply(Err(TransportError::Http { status: 400, body: "refused".into() }))]);
    w.accept(KEY, path, 1);
    assert_eq!(w.stage(KEY), Some(TransferStage::Failed));
    let copy = w.files.path(&w.copy(KEY)).unwrap();
    let receipt = lock(&w.answers).iter().find_map(|(reply, answer)| match (reply, answer) { (1, TransferAnswer::Receipt(receipt)) => Some(receipt.clone()), _ => None }).unwrap();
    w.post(TransferInput::Outcome { receipt: receipt.clone(), reply: 3, discard_on_failure: false });
    let directory = copy.parent().unwrap();
    let permissions = std::fs::metadata(directory).unwrap().permissions();
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o500)).unwrap();
    w.post(TransferInput::Outcome { receipt, reply: 2, discard_on_failure: true });
    std::fs::set_permissions(directory, permissions).unwrap();
    assert!(lock(&w.answers).iter().any(|(reply, answer)| *reply == 2 && matches!(answer, TransferAnswer::Outcome(OutboxOutcome::Rejected { error }, None) if error.contains("400"))));
    let kept = lock(&w.answers).iter().find_map(|(reply, answer)| match answer { TransferAnswer::Outcome(OutboxOutcome::Failed { error }, _) if *reply == 3 => Some(error.clone()), _ => None });
    let dropped = lock(&w.answers).iter().find_map(|(reply, answer)| match answer { TransferAnswer::Outcome(OutboxOutcome::Rejected { error }, None) if *reply == 2 => Some(error.clone()), _ => None });
    assert!(kept.is_some() && kept == dropped, "the read that keeps the failure and the refused delete give the same failure: {kept:?} {dropped:?}");
    assert!(copy.exists());
    let reopened = crate::store::Store::open_models(Some(&w.dir.path().join("db.sqlite")), Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(!copy.exists());
    drop(reopened);
}

#[test]
fn exec_xfer_key_path_chars() {
    let mut w = world();
    let key = "Moment:../../escape:upload";
    let path = w.source(key, b"abc");
    w.accept(key, path, 1);
    let (directory, copy) = (w.files.path("").expect("directory"), w.files.path(&w.copy(key)).expect("directory"));
    assert!(copy.starts_with(&directory) && copy.exists() && !copy.to_string_lossy().contains(".."), "the copy lies in the directory of the files: {copy:?}");
    assert!(w.asked().iter().any(|asked| asked.0 == "Step") && w.asked().iter().filter(|asked| asked.0 == "Step").all(|asked| std::path::Path::new(&asked.3).starts_with(&directory)), "every step reads the copy inside the directory of the files: {:?}", w.asked());
}

#[test]
fn xfer_prop_put_cancel() {
    for cause in ["offline", "saving", "discard"] {
        let mut w = world();
        w.codec.put.store(true, std::sync::atomic::Ordering::SeqCst);
        w.stub.hang_puts();
        let path = w.source(KEY, b"abc");
        w.accept(KEY, path, 2);
        let token = w.stub.put_tokens().remove(0);
        match cause {
            "offline" => w.post(TransferInput::Online(Online(false))),
            "saving" => w.post(TransferInput::PowerSaving(true)),
            _ => w.post(TransferInput::Discard { key: KEY.into(), reply: 9 }),
        }
        assert!(token.is_cancelled(), "{cause}: the attempt stays cancelled");
        if cause == "offline" { assert_eq!((w.stage(KEY), w.late(|input| matches!(input, TransferInput::StepOk { .. } | TransferInput::StepError { .. }))), (Some(TransferStage::Ready), 0)); }
        assert!(lock(&w.shown).iter().all(|shown| !matches!(shown, Shown::Uploading { progress } if *progress > 0)), "{cause}: the bytes of a cancelled attempt raise no progress");
    }
}

#[test]
fn xfer_prop_table_file_off_async() {
    let mut w = world();
    let path = w.source(KEY, &vec![3u8; 2 * MIB]);
    w.stub.script([Act::Hang]);
    let accept = w.accept_input(KEY, path, 1);
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), accept);
    while w.det.step() {}
    let id = w.det.state(w.transfers).unwrap().open_id(KEY).unwrap();
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    while w.det.step() {}
    assert_eq!((w.stage(KEY), w.spawn.pending()), (Some(TransferStage::Copying), 1), "the copy waits as a blocking job");
    assert!(w.files.directory_entries().expect("entries").is_empty(), "the owner thread wrote no byte");
    w.settle();
    assert_eq!(w.stage(KEY), Some(TransferStage::Uploading));
    let copy = w.copy(KEY);
    assert_eq!(std::fs::read(w.files.path(&copy).unwrap()).expect("copy").len(), 2 * MIB);
    w.det.post(w.transfers, EpochStamp::Epoch(EPOCH), TransferInput::Cancel { key: KEY.into(), reply: 2 });
    while w.det.step() {}
    assert!(std::fs::read(w.files.path(&copy).unwrap()).is_ok(), "the delete of the copy is a pending job, not a call of the owner thread");
    w.settle();
    assert!(std::fs::read(w.files.path(&copy).unwrap()).is_err(), "the pending job removed the copy");
}

fn step_key() -> crate::transport::RequestOwner { crate::transport::RequestOwner::Detached }

/// P-RT-14 for Transfer: a close while a PUT is inside the transport leaves that PUT alone and no later mutation or PUT of the step reaches the old transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_prop_close_against_query_no_old_transport_transfer() {
    let stub = Stub::default();
    stub.hang_puts();
    let gate = open_gate(EPOCH, Arc::new(stub.clone()));
    let link = Arc::new(Link::over(gate.clone(), EPOCH));
    let put = || PutRequest { reason: crate::transport::RequestReason::Send, url: "https://storage/part".into(), headers: Vec::new(), file: PutFile { path: "unused".into(), offset: 0, length: 1 }, identity: TransferIdentity { operation_id: 0, item_id: "k".into(), step: TransferStep::Put }, cancel: CancelToken::new(), sent: Arc::new(|_| {}) };
    let inside = crate::task::spawn_on(&tokio::runtime::Handle::current(), { let (link, request) = (link.clone(), put()); async move { link.put(request).await } });
    crate::test_wait::signalled(stub.signal(), "the PUT is inside the transport", || stub.put_tokens().len() == 1).await;
    gate.close(EPOCH);
    let late_mutation = link.mutate(GraphQLRequest::new("mutation Late", "Late", json!({})), &step_key()).await;
    let late_put = link.put(put()).await;
    assert!(matches!(late_mutation, Err(TransportError::Interrupted(_))), "{late_mutation:?}");
    assert!(matches!(late_put, Err(TransportError::Interrupted(_))), "{late_put:?}");
    let late: Vec<_> = stub.calls().iter().map(|call| call.operation_name.clone()).collect();
    assert!(late.is_empty() && stub.put_tokens().len() == 1, "late calls reached the old transport after the close: mutations {late:?}, PUTs {}", stub.put_tokens().len());
    inside.abort_handle().abort();
    crate::test_wait::signalled(stub.signal(), "the aborted PUT is dropped", || stub.dropped() == 1).await;
}

#[test]
fn exec_xfer_abort_result_carries_the_status_class() {
    use super::transfer_executor::abort_result;
    use super::transfer::AbortFault;
    let http = |status: u16| Err(TransportError::Http { status, body: String::new() });
    let refused = |temporary, status| Err(AbortFault { temporary, status });
    let table = [(Ok(()), Ok(())), (http(404), refused(false, Some(404))), (http(410), refused(false, Some(410))), (http(403), refused(false, Some(403))), (http(500), refused(true, Some(500))), (Err(TransportError::Network("down".into())), refused(true, None))];
    for (result, expected) in table {
        assert_eq!(abort_result(&result), expected, "{result:?}");
    }
}
