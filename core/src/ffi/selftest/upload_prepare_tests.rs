use std::sync::Arc;

use async_trait::async_trait;

use super::crash_image_support::{files_of, queue_now};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, OPERATIONS};
use super::upload_stand::{failure_reason, source_of, ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireSource, WireUploadBatch, WireUploadItem, WireUploadRequest};
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::wire::{WireUpload, WireUploadStatus};
use crate::ffi::{DbError, PrepareJob, PrepareOutcome, PrepareProgress, Preparer, ProbeOutcome, ReadRequest, ReadValue};

struct Refusing;

#[async_trait]
impl Preparer for Refusing {
    async fn probe(&self, _: String, _: Option<Vec<u8>>, _: Option<String>) -> ProbeOutcome { ProbeOutcome::Failed { message: "unused".into() } }
    async fn run(&self, _: PrepareJob, _: Arc<PrepareProgress>) -> PrepareOutcome { PrepareOutcome::Failed { message: "unused".into() } }
    fn cancel(&self, _: u64) {}
    fn import_descriptor(&self, _: crate::ffi::SavedPreparation) -> crate::ffi::PrepareImportOutcome { crate::ffi::PrepareImportOutcome::Failed { message: "unused".into() } }
}

pub(super) fn send(epoch: u64, path: &std::path::Path, prepare: Vec<u8>) -> CommandRequest { send_limited(epoch, path, Some(prepare), None) }

pub(super) fn send_limited(epoch: u64, path: &std::path::Path, prepare: Option<Vec<u8>>, max_bytes: Option<u64>) -> CommandRequest {
    let source = WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None, prepare, max_bytes };
    CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"clip"}"#.into(), args_json: "{}".into(), upload: Some(CommandUpload { action: "upload_clip".into(), request: WireUploadRequest { source, variables_json: "{}".into() } }) }
}

fn transfer_files(database: &std::path::Path) -> usize {
    std::fs::read_dir(format!("{}.files/transfers", database.display())).map_or(0, |found| found.count())
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_invalid_settings() {
    let directory = tempfile::tempdir().unwrap();
    let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
    std::fs::write(&source, "clip").unwrap();
    let stand = Stand::new().with_preparer(Arc::new(Refusing));
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let rows = |database: &crate::ffi::Database| database.db.lock().connection.query_row("SELECT (SELECT count(*) FROM sync_upload) + (SELECT count(*) FROM sync_upload_prepare)", [], |found| found.get::<_, i64>(0)).unwrap();
    let refused = bound(database.send(send_limited(epoch, &source, None, Some(0)))).await.map(|_| ());
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "max bytes of 0: {refused:?}");
    assert_eq!((rows(&database), transfer_files(&path)), (0, 0));
    database.close_now();

    let without = Stand::new();
    let database = without.open(Some(&directory.path().join("other.sqlite")), &[CREATE_MOMENT]).await;
    let refused = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.map(|_| ());
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "no preparer: {refused:?}");
    database.close_now();
}

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const LANDED_NEXT: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:01.000Z"}}}"#;
pub(super) const SOURCE: &[u8] = b"source-bytes-of-the-clip";
pub(super) const PREPARED: &[u8] = b"prepared";

pub(super) fn probed_image() -> ProbeOutcome { ProbeOutcome::File { width: Some(400), height: Some(400), mime: Some("image/jpeg".into()) } }

pub(super) fn descriptor() -> Vec<u8> { vec![0, 255, 128, 1, 0, 17] }

pub(super) fn upload_of(database: &crate::ffi::Database, id: &str) -> Option<WireUpload> {
    let row = database.peek_row_now(database.session_epoch(), "Moment".into(), id.into()).unwrap()?;
    row.uploads.into_iter().find(|entry| entry.action == "upload_clip").map(|entry| entry.upload)
}

fn shown(database: &crate::ffi::Database, id: &str) -> bool { database.peek_row_now(database.session_epoch(), "Moment".into(), id.into()).unwrap().is_some() }

pub(super) fn files_left(database: &std::path::Path) -> usize {
    fn walk(path: &std::path::Path) -> usize {
        std::fs::read_dir(path).map_or(0, |found| found.map(|entry| { let entry = entry.unwrap(); if entry.file_type().unwrap().is_dir() { walk(&entry.path()) } else { 1 } }).sum())
    }
    walk(&files_of(database))
}

async fn quiet(database: &crate::ffi::Database) {
    let life = database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
}

async fn checksum_of(bytes: &[u8]) -> String {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sum");
    std::fs::write(&path, bytes).unwrap();
    crate::owners::write::upload_io::md5_base64(&path, &crate::transport::CancelToken::new()).await.unwrap()
}

fn source_file(directory: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_runs_then_uploads() {
    for settings in [Vec::new(), descriptor()] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let source = source_file(directory.path(), "clip.jpg", SOURCE);
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.3, 0.6, 1.0]);
        let hold = preparer.hold_next();
        let stand = Stand::new().with_preparer(preparer.clone());
        let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", LANDED);
        let epoch = database.session_epoch();
        let receipt = bound(database.send(send(epoch, &source, settings.clone()))).await.unwrap();
        let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
        assert_eq!(upload_of(&database, &id).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the receipt comes with the copy and the preparing row");
        hold.entered().await;
        let watch = database.subscribe_values(epoch, ReadRequest::Blob { model: "Moment".into(), id: id.clone(), action: "upload_clip".into() });
        hold.release();
        let mut preparing = Vec::new();
        loop {
            match bound(watch.pull_next()).await {
                Some(ReadValue::Status { upload, .. }) if upload.status == WireUploadStatus::Preparing => preparing.push(upload.progress),
                Some(ReadValue::Status { .. }) => break,
                other => panic!("the status ended: {other:?}"),
            }
        }
        assert!(preparing.windows(2).all(|pair| pair[0] <= pair[1]) && preparing.iter().any(|percent| *percent >= 0.30), "the percent of the step never goes back: {preparing:?}");
        bound(database.confirmed(receipt, false)).await.unwrap();
        let blobs = stand.script.variables("CreateBlob");
        assert_eq!(blobs.len(), 1);
        assert_eq!((blobs[0]["byteSize"].as_u64(), blobs[0]["checksum"].as_str().map(str::to_string)), (Some(PREPARED.len() as u64), Some(checksum_of(PREPARED).await)), "the server gets the size and the checksum of the prepared file");
        assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![PREPARED.to_vec()], "the body of the PUT is the prepared file");
        assert_eq!(stand.script.count("CreateMoment"), 1);
        crate::test_wait::settled(&database, "no copy and no result stay", || files_left(&path) == 0).await;
        assert_eq!(preparer.jobs()[0].descriptor, settings, "the producer receives the descriptor without interpretation");
        assert_eq!(preparer.runs().len(), 1);
        database.close_now();

        database.close_now();

    }
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_pass_through() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
    preparer.outcome_next(PrepareOutcome::Unchanged { name: None, mime: None });
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    assert_eq!(preparer.started_sources(), vec![SOURCE.to_vec()], "the producer returns the existing copy without writing a result");
    assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![SOURCE.to_vec()], "the copy of the source goes");
    assert_eq!(stand.script.count("CreateMoment"), 1);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_failed_and_retry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = source_file(directory.path(), "clip.jpg", SOURCE);
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    preparer.fail_next("the codec refused");
    let stand = Stand::new().keep_moment().with_preparer(preparer.clone());
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let epoch = database.session_epoch();
    let receipt = bound(database.send(send(epoch, &source, descriptor()))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(database.confirmed(receipt, false)).await;
    crate::test_wait::settled(&database, "the failed step is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    let failed = upload_of(&database, &id).unwrap();
    assert_eq!(failed.error.map(|error| error.kind), Some("prepare_failed".to_string()));
    assert!(files_left(&path) > 0, "the copy of the source stays");
    assert_eq!(stand.script.count("CreateBlob"), 0);
    let again = crate::ffi::upload_wire::WireUploadSource { path: source.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
    assert!(matches!(bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), Some(again))).await, Err(DbError::Invalid { .. })), "a new source is a new upload");
    let retried = bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), None)).await.unwrap();
    bound(database.confirmed(retried, false)).await.unwrap();
    assert_eq!(preparer.runs().len(), 2, "the retry runs the step again from the copy");
    assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![PREPARED.to_vec()]);
    assert_eq!(stand.script.count("CreateMoment"), 1);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_cancel() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = source_file(directory.path(), "clip.jpg", SOURCE);
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    let hold = preparer.hold_next();
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let epoch = database.session_epoch();
    let receipt = bound(database.send(send(epoch, &source, descriptor()))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    assert_eq!(upload_of(&database, &id).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the accepted transfer waits in the prepare step");
    hold.entered().await;
    assert!(bound(database.cancel_upload(epoch, "Moment".into(), id.clone(), "upload_clip".into())).await.unwrap());
    assert_eq!(preparer.cancels(), preparer.runs());
    assert_eq!(preparer.cancels().len(), 1);
    assert!(!shown(&database, &id), "the optimistic row is gone");
    crate::test_wait::settled(&database, "no copy and no result stay", || files_left(&path) == 0).await;
    hold.release();
    quiet(&database).await;
    assert_eq!((files_left(&path), shown(&database, &id), stand.script.count("CreateBlob"), stand.puts.bodies.lock().unwrap().len()), (0, false, 0, 0), "the late end of the run changes nothing");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_one_at_a_time() {
    let directory = tempfile::tempdir().unwrap();
    let (first, second) = (source_file(directory.path(), "a.jpg", b"first-source"), source_file(directory.path(), "b.jpg", b"second-source"));
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    let hold = preparer.hold_next();
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    for landed in [LANDED, LANDED_NEXT] {
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", landed);
    }
    let epoch = database.session_epoch();
    let one = bound(database.send(send(epoch, &first, descriptor()))).await.unwrap();
    assert_eq!(upload_of(&database, one.optimistic_value.as_ref().map_or("", |value| value.id.as_str())).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the accepted transfer waits in the prepare step");
    hold.entered().await;
    let two = bound(database.send(send(epoch, &second, descriptor()))).await.unwrap();
    quiet(&database).await;
    assert_eq!(preparer.started_sources(), vec![b"first-source".to_vec()], "the second transfer waits for the step");
    hold.release();
    bound(database.confirmed(one, false)).await.unwrap();
    bound(database.confirmed(two, false)).await.unwrap();
    assert_eq!(preparer.started_sources(), vec![b"first-source".to_vec(), b"second-source".to_vec()], "the order of the acceptance");
    assert_eq!(preparer.peak_runs(), 1);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_end() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = source_file(directory.path(), "clip.jpg", SOURCE);
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    let hold = preparer.hold_next();
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let receipt = bound(database.send(send(epoch, &source, descriptor()))).await.unwrap();
    assert_eq!(upload_of(&database, receipt.optimistic_value.as_ref().map_or("", |value| value.id.as_str())).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the accepted transfer waits in the prepare step");
    hold.entered().await;
    let waiting = { let database = database.clone(); tokio::spawn(async move { database.confirmed(receipt, false).await }) };
    bound(database.session_reset()).await.unwrap();
    assert!(matches!(bound(waiting).await.unwrap(), Err(DbError::NoSession { .. })), "the wait ends with the end of the session");
    assert_eq!(preparer.cancels(), preparer.runs());
    assert_eq!(preparer.cancels().len(), 1);
    crate::test_wait::settled(&database, "no copy and no result stay", || files_left(&path) == 0).await;
    assert_eq!(queue_now(&database).await, 0);
    hold.release();
    database.close_now();
}

/// D-413: the declared attributes hold the size a user sees of the prepared file from the first commit of the row and go to the final mutation; the prepare step takes the probe of the acceptance.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_dimensions_of_the_result_from_the_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(ProbeOutcome::File { width: Some(360), height: Some(640), mime: Some("video/mp4".into()) }, PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone()).with_dimensions();
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.unwrap();
    let spec = database.db.spec("Moment").unwrap();
    let fields = crate::ffi::wire::wire_fields(spec);
    let row = receipt.optimistic_value.clone().expect("the row of the send");
    let size_of = |name: &str| row.cells[fields.iter().position(|field| field.name == name).unwrap()].clone();
    let shown = (size_of("width"), size_of("height"));
    bound(database.confirmed(receipt, false)).await.unwrap();
    let sent = &stand.script.variables("CreateMoment")[0];
    let sent = (sent["width"].clone(), sent["height"].clone());
    let int = |value: i64| crate::ffi::wire::Cell::Int { value };
    assert_eq!(preparer.descriptors(), vec![Some(descriptor())], "the producer receives the settings that determine the first row's dimensions");
    assert_eq!((shown, sent, preparer.probes().len()), ((int(360), int(640)), (serde_json::json!(360), serde_json::json!(640)), 1), "the producer's planned dimensions are committed before the receipt and sent once");
    database.close_now();
}


#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_cannot_decode() {
    const MESSAGE: &str = "decoder up to 2560x1440";
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip", SOURCE);
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
    preparer.outcome_next(PrepareOutcome::CannotDecode { message: MESSAGE.into() });
    let stand = Stand::new().keep_moment().with_preparer(preparer);
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    let receipt = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    assert!(bound(database.confirmed(receipt, false)).await.is_err());
    crate::test_wait::settled(&database, "the refusal is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    let failed = upload_of(&database, &id).unwrap();
    assert_eq!(failed.error.as_ref().map(|error| error.kind.as_str()), Some("prepare_failed"));
    assert_eq!(failure_reason(&failed).as_deref(), Some("device_cannot_decode"));
    assert!(failed.error.is_some_and(|error| error.message.contains(MESSAGE)));
    assert_eq!((stand.script.count("CreateBlob"), stand.puts.bodies.lock().unwrap().len()), (0, 0));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_of_a_supplied_source() {
    let directory = tempfile::tempdir().unwrap();
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    let hold = preparer.hold_next();
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("pending.sqlite")), &OPERATIONS).await;
    for index in 0..2 { stand.script.data("CreateBlob", &blob(index)); }
    stand.script.data("CreateAssets", r#"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":3,"position":0},{"id":"a1","clientKey":"key-1","size":3,"position":1}]}}}"#);
    let epoch = database.session_epoch();
    let items = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: WireSource::Pending, variables_json: "{}".into() }).collect();
    let batch = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    quiet(&database).await;
    let first = batch.items[0].optimistic_value.id.clone();
    assert_eq!((preparer.calls(), upload_of_asset(&database, &first).map(|upload| upload.status)), (0, Some(WireUploadStatus::Waiting)), "the step waits for the source");
    let supplied = source_file(directory.path(), "supplied.jpg", SOURCE);
    for item in &batch.items {
        bound(database.supply_source(epoch, "Asset".into(), item.optimistic_value.id.clone(), "create_files".into(), source_of(&supplied, "{}", &descriptor()))).await.unwrap();
    }
    quiet(&database).await;
    assert_eq!(upload_of_asset(&database, &first).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the supplied source goes through the step");
    hold.entered().await;
    hold.release();
    for item in batch.items { bound(database.confirmed(item.receipt, false)).await.unwrap(); }
    assert_eq!(preparer.started_sources(), vec![SOURCE.to_vec(); 2]);
    assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![PREPARED.to_vec(); 2], "the body of every PUT is the prepared file");
    database.close_now();
}

fn upload_of_asset(database: &crate::ffi::Database, id: &str) -> Option<WireUpload> {
    let row = database.peek_row_now(database.session_epoch(), "Asset".into(), id.into()).unwrap()?;
    row.uploads.into_iter().find(|entry| entry.action == "create_files").map(|entry| entry.upload)
}

/// D-412: the file that goes to the network is larger than the `max_bytes` of its source: the upload fails with the kind `too_large` and nothing is sent, with a prepare step or without, alone or as a member of a batch. A file of exactly the limit goes.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_too_large() {
    let cases: Vec<(&str, Option<Vec<u8>>, bool, u64, usize, bool)> = vec![
        ("no prepare, above the limit", None, false, SOURCE.len() as u64 - 1, 0, true),
        ("unchanged source above the limit", Some(descriptor()), true, SOURCE.len() as u64 - 1, 1, true),
        ("the prepared result above the limit", Some(descriptor()), false, PREPARED.len() as u64 - 1, 1, true),
        ("no prepare, exactly the limit", None, false, SOURCE.len() as u64, 0, false),
        ("the prepared result of exactly the limit", Some(descriptor()), false, PREPARED.len() as u64, 1, false),
    ];
    for (what, prepare, unchanged, limit, runs, refused) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let source = source_file(directory.path(), "clip", SOURCE);
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        if unchanged { preparer.outcome_next(PrepareOutcome::Unchanged { name: None, mime: None }); }
        let stand = if prepare.is_some() { Stand::new().keep_moment().with_preparer(preparer.clone()) } else { Stand::new().keep_moment() };
        let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", LANDED);
        let receipt = bound(database.send(send_limited(database.session_epoch(), &source, prepare, Some(limit)))).await.unwrap();
        let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
        let outcome = bound(database.confirmed(receipt, false)).await;
        assert_eq!(preparer.runs().len(), runs, "{what}");
        if refused {
            crate::test_wait::settled(&database, "the refused size is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
            let failed = upload_of(&database, &id).unwrap();
            assert_eq!((failed.error.as_ref().map(|error| error.kind.clone()), failure_reason(&failed)), (Some("too_large".to_string()), None), "{what}");
            assert_eq!((stand.script.count("CreateBlob"), stand.puts.bodies.lock().unwrap().len(), stand.script.count("CreateMoment")), (0, 0, 0), "{what}: nothing goes to the network");
        } else {
            outcome.unwrap();
            assert_eq!(stand.puts.bodies.lock().unwrap().len(), 1, "{what}");
            assert_eq!(stand.script.count("CreateMoment"), 1, "{what}");
        }
        database.close_now();
    }

    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "cached", SOURCE);
    let stand = Stand::new().keep_moment();
    let database = stand.open(Some(&directory.path().join("cached.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let first = bound(database.send(send_limited(database.session_epoch(), &source, None, None))).await.unwrap();
    bound(database.confirmed(first, false)).await.unwrap();
    stand.script.data("CreateMoment", LANDED_NEXT);
    let again = bound(database.send(send_limited(database.session_epoch(), &source, None, None))).await.unwrap();
    bound(database.confirmed(again, false)).await.unwrap();
    assert_eq!((stand.script.count("CreateBlob"), stand.script.count("CreateMoment"), stand.puts.bodies.lock().unwrap().len()), (1, 2, 1), "the same file again is a hit of the blob cache: the final mutation only");
    let receipt = bound(database.send(send_limited(database.session_epoch(), &source, None, Some(SOURCE.len() as u64 - 1)))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    crate::test_wait::settled(&database, "the refused size is shown over a cached blob", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    assert_eq!(upload_of(&database, &id).and_then(|upload| upload.error).map(|error| error.kind), Some("too_large".to_string()), "a cached blob");
    assert_eq!((stand.script.count("CreateBlob"), stand.script.count("CreateMoment"), stand.puts.bodies.lock().unwrap().len()), (1, 2, 1), "a cached blob: no request for the limited file");
    bound(database.retry(database.session_epoch(), "Moment".into(), id.clone(), Some("upload_clip".into()), None)).await.unwrap();
    quiet(&database).await;
    assert_eq!(upload_of(&database, &id).and_then(|upload| upload.error).map(|error| error.kind), Some("too_large".to_string()), "a retry ends at the same point with the same outcome");
    assert_eq!((stand.script.count("CreateBlob"), stand.script.count("CreateMoment"), stand.puts.bodies.lock().unwrap().len()), (1, 2, 1), "a retry: no request for the limited file");
    let deferred = bound(database.send(CommandRequest { epoch: database.session_epoch(), model: "Moment".into(), values_json: r#"{"text":"later"}"#.into(), args_json: "{}".into(), upload: Some(CommandUpload { action: "upload_clip".into(), request: WireUploadRequest { source: WireSource::Pending, variables_json: "{}".into() } }) })).await.unwrap();
    let deferred_id = deferred.optimistic_value.as_ref().unwrap().id.clone();
    let large_file = source_file(directory.path(), "late", b"late-bytes-above-the-limit");
    let late = crate::ffi::upload_wire::WireUploadSource { path: large_file.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: Some(10) };
    bound(database.supply_source(database.session_epoch(), "Moment".into(), deferred_id.clone(), "upload_clip".into(), late)).await.unwrap();
    crate::test_wait::settled(&database, "the refused size is shown for a supplied source", || upload_of(&database, &deferred_id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    assert_eq!(upload_of(&database, &deferred_id).and_then(|upload| upload.error).map(|error| error.kind), Some("too_large".to_string()), "a supplied source");
    assert_eq!((stand.script.count("CreateBlob"), stand.script.count("CreateMoment")), (1, 2), "a supplied source: no request");
    database.close_now();

    let directory = tempfile::tempdir().unwrap();
    let (large, small) = (source_file(directory.path(), "large", SOURCE), source_file(directory.path(), "small", b"abc"));
    let stand = Stand::new();
    let database = stand.open(Some(&directory.path().join("batch.sqlite")), &OPERATIONS).await;
    stand.script.data("CreateBlob", &blob(1));
    let items = [&large, &small].iter().enumerate().map(|(index, path)| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None, prepare: None, max_bytes: Some(10) }, variables_json: "{}".into() }).collect();
    let batch = bound(database.upload_create_batch(database.session_epoch(), "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let member = batch.items[0].optimistic_value.id.clone();
    crate::test_wait::settled(&database, "the large member failed", || upload_of_asset(&database, &member).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    assert_eq!(upload_of_asset(&database, &member).and_then(|upload| upload.error).map(|error| error.kind), Some("too_large".to_string()), "a member of a batch");
    assert!(stand.puts.bodies.lock().unwrap().iter().all(|body| body.as_slice() != SOURCE), "no byte of the large member goes");
    database.close_now();
}
