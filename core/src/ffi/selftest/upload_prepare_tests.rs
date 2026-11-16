use std::sync::Arc;

use async_trait::async_trait;

use super::crash_image_support::{files_of, queue_now};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, OPERATIONS};
use super::upload_stand::{cannot_decode_settings, failure_reason, source_of, video_probe, ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::upload_wire::{WirePrepare, WireSource, WireUploadBatch, WireUploadItem, WireUploadRequest};
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::wire::{WireUpload, WireUploadStatus};
use crate::ffi::{DbError, PreparePlan, PrepareJob, PrepareOutcome, PrepareProgress, Preparer, ProbeOutcome, ReadRequest, ReadValue};
use crate::prepare::{default_video_limits, CropFractions, ImageFormat, PhotoEdit, PixelSize, VideoLimits};

struct Refusing;

#[async_trait]
impl Preparer for Refusing {
    async fn probe(&self, _: String, _: crate::ffi::PrepareKind) -> ProbeOutcome { ProbeOutcome::Failed { message: "unused".into() } }
    async fn run(&self, _: PrepareJob, _: Arc<PrepareProgress>) -> PrepareOutcome { PrepareOutcome::Failed { message: "unused".into() } }
    fn cancel(&self, _: u64) {}
}

fn video(change: impl FnOnce(&mut VideoLimits)) -> WirePrepare {
    let mut limits = default_video_limits();
    change(&mut limits);
    WirePrepare::Video { limits: Some(limits), mirror: false, on_cannot_decode: crate::ffi::upload_wire::CannotDecode::Fail }
}

pub(super) fn image(change: impl FnOnce(&mut PhotoEdit)) -> WirePrepare {
    let mut edit = PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 };
    change(&mut edit);
    WirePrepare::Image { edit, quality: None, format: ImageFormat::Jpeg }
}

pub(super) fn send(epoch: u64, path: &std::path::Path, prepare: WirePrepare) -> CommandRequest { send_limited(epoch, path, Some(prepare), None) }

pub(super) fn send_limited(epoch: u64, path: &std::path::Path, prepare: Option<WirePrepare>, max_bytes: Option<u64>) -> CommandRequest {
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
    let rows = |database: &crate::ffi::Database| database.db.lock().connection.query_row("SELECT (SELECT count(*) FROM sync_upload) + (SELECT count(*) FROM sync_upload_prepare) + (SELECT count(*) FROM sync_upload_prepare_step)", [], |found| found.get::<_, i64>(0)).unwrap();
    let invalid: Vec<(&str, WirePrepare)> = vec![
        ("long side below 2", video(|limits| limits.max_long_side = 0)),
        ("no corridors", video(|limits| limits.corridors.clear())),
        ("min above max bitrate", video(|limits| limits.corridors[0].min_bps = limits.corridors[0].max_bps + 1)),
        ("frame rate of 0", video(|limits| limits.min_frame_rate = 0)),
        ("max size side 0", image(|edit| edit.max_size = Some(PixelSize { width: 0, height: 10 }))),
        ("output size side 0", image(|edit| edit.output_size = Some(PixelSize { width: 10, height: 0 }))),
        ("crop outside 0..1", image(|edit| edit.crop = Some(CropFractions { x: 1.5, y: 0.0, width: 0.5, height: 0.5 }))),
        ("crop without pixels", image(|edit| edit.crop = Some(CropFractions { x: 0.0, y: 0.0, width: 0.0, height: 0.5 }))),
        ("rotation 45", image(|edit| edit.rotation_degrees = 45)),
        ("rotation 360", image(|edit| edit.rotation_degrees = 360)),
        ("quality 1.5", WirePrepare::Image { edit: PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 }, quality: Some(1.5), format: ImageFormat::Jpeg }),
    ];
    for (name, prepare) in invalid {
        let refused = bound(database.send(send(epoch, &source, prepare))).await.map(|_| ());
        assert!(matches!(refused, Err(DbError::Invalid { .. })), "{name}: {refused:?}");
    }
    let refused = bound(database.send(send_limited(epoch, &source, None, Some(0)))).await.map(|_| ());
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "max bytes of 0: {refused:?}");
    assert_eq!((rows(&database), transfer_files(&path)), (0, 0));
    database.close_now();

    let without = Stand::new();
    let database = without.open(Some(&directory.path().join("other.sqlite")), &[CREATE_MOMENT]).await;
    let refused = bound(database.send(send(database.session_epoch(), &source, WirePrepare::Video { limits: None, mirror: false, on_cannot_decode: crate::ffi::upload_wire::CannotDecode::Fail }))).await.map(|_| ());
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "no preparer: {refused:?}");
    database.close_now();
}

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const LANDED_NEXT: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:01.000Z"}}}"#;
pub(super) const SOURCE: &[u8] = b"source-bytes-of-the-clip";
pub(super) const PREPARED: &[u8] = b"prepared";

pub(super) fn probed_image() -> ProbeOutcome { ProbeOutcome::Image { width: 400, height: 400, exif_orientation: 1, format: ImageFormat::Jpeg } }

pub(super) fn shrink() -> WirePrepare { image(|edit| edit.max_size = Some(PixelSize { width: 200, height: 200 })) }

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
    let receipt = bound(database.send(send(epoch, &source, shrink()))).await.unwrap();
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
    assert_eq!(preparer.kinds(), vec![crate::ffi::PrepareKind::Image], "the probe gets the kind the settings ask for");
    assert_eq!(preparer.runs().len(), 1);
    database.close_now();

    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_pass_through() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(video_probe(&[], false), PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(database.session_epoch(), &source, video(|_| {})))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    assert_eq!((preparer.probes(), preparer.runs()), (vec![SOURCE.to_vec()], Vec::new()), "the plan asks for no work: the metadata is read, nothing is written");
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
    let receipt = bound(database.send(send(epoch, &source, shrink()))).await.unwrap();
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
    let receipt = bound(database.send(send(epoch, &source, shrink()))).await.unwrap();
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
    let one = bound(database.send(send(epoch, &first, shrink()))).await.unwrap();
    assert_eq!(upload_of(&database, one.optimistic_value.as_ref().map_or("", |value| value.id.as_str())).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the accepted transfer waits in the prepare step");
    hold.entered().await;
    let two = bound(database.send(send(epoch, &second, shrink()))).await.unwrap();
    quiet(&database).await;
    assert_eq!(preparer.probes(), vec![b"first-source".to_vec()], "the second transfer waits for the step");
    hold.release();
    bound(database.confirmed(one, false)).await.unwrap();
    bound(database.confirmed(two, false)).await.unwrap();
    assert_eq!(preparer.probes(), vec![b"first-source".to_vec(), b"second-source".to_vec()], "the order of the acceptance");
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
    let receipt = bound(database.send(send(epoch, &source, shrink()))).await.unwrap();
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

const LOCATION: &str = "com.apple.quicktime.location.ISO6709";

async fn states_of(database: &crate::ffi::Database, id: &str) -> (Arc<std::sync::Mutex<Vec<WireUploadStatus>>>, tokio::task::JoinHandle<()>) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let watch = database.subscribe_values(database.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: id.into(), action: "upload_clip".into() });
    let sink = seen.clone();
    let task = tokio::spawn(async move {
        while let Some(value) = watch.pull_next().await {
            if let ReadValue::Status { upload, .. } = value { sink.lock().unwrap().push(upload.status); }
        }
    });
    (seen, task)
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_strips_private_metadata_without_reencode() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(video_probe(&[LOCATION, "major_brand"], false), PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(database.session_epoch(), &source, video(|_| {})))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    assert_eq!(preparer.kinds(), vec![crate::ffi::PrepareKind::Video], "the probe gets the kind the settings ask for");
    let jobs = preparer.jobs();
    assert_eq!(jobs.len(), 1, "the private key forces a write");
    match &jobs[0].plan {
        PreparePlan::Video { plan, strip_metadata, .. } => assert_eq!((plan.reencode, strip_metadata.clone()), (false, vec![LOCATION.to_string()]), "a repack without encoding that strips the private key and keeps the technical one"),
        other => panic!("not a video plan: {other:?}"),
    }
    assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![PREPARED.to_vec()], "the result goes, not the copy");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_mirror_makes_the_plan_reencode() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(video_probe(&[], false), PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone());
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let mirrored = WirePrepare::Video { limits: None, mirror: true, on_cannot_decode: crate::ffi::upload_wire::CannotDecode::Fail };
    let receipt = bound(database.send(send(database.session_epoch(), &source, mirrored))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    match &preparer.jobs()[0].plan {
        PreparePlan::Video { plan, mirror, .. } => assert_eq!((plan.reencode, *mirror), (true, true), "a clean source that the table keeps still goes through the encoder to be mirrored"),
        other => panic!("not a video plan: {other:?}"),
    }
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_recode_above_bytes() {
    let size = SOURCE.len() as u64;
    for (threshold, runs, body) in [(size, 0, SOURCE), (size - 1, 1, PREPARED)] {
        let directory = tempfile::tempdir().unwrap();
        let source = source_file(directory.path(), "clip.mp4", SOURCE);
        let preparer = ScriptedPreparer::new(video_probe(&[], true), PREPARED, &[]);
        let stand = Stand::new().with_preparer(preparer.clone());
        let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", LANDED);
        let receipt = bound(database.send(send(database.session_epoch(), &source, video(|limits| limits.recode_above_bytes = Some(threshold))))).await.unwrap();
        bound(database.confirmed(receipt, false)).await.unwrap();
        assert_eq!((preparer.jobs().len(), stand.puts.bodies.lock().unwrap().clone()), (runs, vec![body.to_vec()]), "threshold {threshold} for a copy of {size} bytes that the limits encode");
        database.close_now();
    }
}

/// D-413: the declared attributes hold the size a user sees of the prepared file from the first commit of the row and go to the final mutation; the prepare step takes the probe of the acceptance.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_dimensions_of_the_result_from_the_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let preparer = ScriptedPreparer::new(video_probe(&[], true), PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone()).with_dimensions();
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(database.session_epoch(), &source, video(|limits| limits.max_long_side = 640)))).await.unwrap();
    let spec = database.db.spec("Moment").unwrap();
    let fields = crate::ffi::wire::wire_fields(spec);
    let row = receipt.optimistic_value.clone().expect("the row of the send");
    let size_of = |name: &str| row.cells[fields.iter().position(|field| field.name == name).unwrap()].clone();
    let shown = (size_of("width"), size_of("height"));
    bound(database.confirmed(receipt, false)).await.unwrap();
    let sent = &stand.script.variables("CreateMoment")[0];
    let sent = (sent["width"].clone(), sent["height"].clone());
    let int = |value: i64| crate::ffi::wire::Cell::Int { value };
    assert_eq!((shown, sent, preparer.probes().len()), ((int(360), int(640)), (serde_json::json!(360), serde_json::json!(640)), 1), "a 1280x720 video turned by 90 and limited to 640 on its long side, probed once");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_refuses_a_rotation_outside_the_set() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = source_file(directory.path(), "clip.mp4", SOURCE);
    let ProbeOutcome::Video { source: facts, metadata_keys } = video_probe(&[], false) else { unreachable!() };
    let probe = ProbeOutcome::Video { source: crate::prepare::VideoSource { rotation_degrees: 45, ..facts }, metadata_keys };
    let preparer = ScriptedPreparer::new(probe, PREPARED, &[]);
    let stand = Stand::new().keep_moment().with_preparer(preparer.clone());
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let receipt = bound(database.send(send(epoch, &source, video(|_| {})))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(database.confirmed(receipt, false)).await;
    crate::test_wait::settled(&database, "the refusal is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    let failed = upload_of(&database, &id).unwrap();
    assert_eq!((failed.error.as_ref().map(|error| error.kind.clone()), failure_reason(&failed).as_deref()), (Some("prepare_failed".to_string()), Some("failed")));
    assert!(preparer.jobs().is_empty() && files_left(&path) > 0, "nothing ran and the copy of the source stays");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepare_cannot_decode() {
    const MESSAGE: &str = "decoder up to 2560x1440";
    let cases: [(&str, bool, &[&str]); 3] = [("refuse", false, &[]), ("send the source", true, &["major_brand"]), ("send a repack", true, &[LOCATION])];
    for (name, upload_source, keys) in cases {
        let directory = tempfile::tempdir().unwrap();
        let source = source_file(directory.path(), "clip.mp4", SOURCE);
        let preparer = ScriptedPreparer::new(video_probe(keys, true), PREPARED, &[]);
        preparer.outcome_next(PrepareOutcome::CannotDecode { message: MESSAGE.into() });
        let stand = Stand::new().keep_moment().with_preparer(preparer.clone());
        let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", LANDED);
        let epoch = database.session_epoch();
        let receipt = bound(database.send(send(epoch, &source, cannot_decode_settings(upload_source)))).await.unwrap();
        let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
        let (states, collector) = states_of(&database, &id).await;
        let outcome = bound(database.confirmed(receipt, false)).await;
        if !upload_source {
            crate::test_wait::settled(&database, "the refusal is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
            let failed = upload_of(&database, &id).unwrap();
            assert_eq!(failed.error.as_ref().map(|error| error.kind.clone()), Some("prepare_failed".to_string()), "{name}");
            assert_eq!(failure_reason(&failed).as_deref(), Some("device_cannot_decode"), "{name}: the reason is typed");
            assert!(failed.error.is_some_and(|error| error.message.contains(MESSAGE)), "{name}: the text carries the message of the platform");
            assert_eq!((stand.script.count("CreateBlob"), stand.puts.bodies.lock().unwrap().len()), (0, 0), "{name}: nothing goes");
        } else {
            outcome.unwrap_or_else(|error| panic!("{name}: the upload goes on: {error:?}"));
            let body = if keys == [LOCATION] { PREPARED } else { SOURCE };
            assert_eq!(*stand.puts.bodies.lock().unwrap(), vec![body.to_vec()], "{name}");
            assert_eq!(stand.script.count("CreateMoment"), 1, "{name}");
            assert!(!states.lock().unwrap().iter().any(|status| *status == WireUploadStatus::Failed), "{name}: no frame shows a failure: {:?}", states.lock().unwrap());
            let jobs = preparer.jobs();
            if keys == [LOCATION] {
                assert_eq!(jobs.len(), 2, "{name}: the second run repacks");
                assert_ne!(jobs[0].attempt_id, jobs[1].attempt_id, "{name}: a new attempt");
                match (&jobs[0].plan, &jobs[1].plan) {
                    (PreparePlan::Video { plan: first, .. }, PreparePlan::Video { plan: second, strip_metadata, .. }) => assert_eq!((first.reencode, second.reencode, strip_metadata.clone()), (true, false, vec![LOCATION.to_string()]), "{name}"),
                    other => panic!("{name}: not video plans: {other:?}"),
                }
            } else {
                assert_eq!(jobs.len(), 1, "{name}: nothing to strip, no second run");
            }
        }
        collector.abort();
        database.close_now();
    }
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
        bound(database.supply_source(epoch, "Asset".into(), item.optimistic_value.id.clone(), "create_files".into(), source_of(&supplied, "{}", &shrink()))).await.unwrap();
    }
    quiet(&database).await;
    assert_eq!(upload_of_asset(&database, &first).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the supplied source goes through the step");
    hold.entered().await;
    hold.release();
    for item in batch.items { bound(database.confirmed(item.receipt, false)).await.unwrap(); }
    assert_eq!(preparer.probes(), vec![SOURCE.to_vec(); 2]);
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
    let cases: Vec<(&str, ProbeOutcome, Option<WirePrepare>, u64, usize, bool)> = vec![
        ("no prepare, above the limit", probed_image(), None, SOURCE.len() as u64 - 1, 0, true),
        ("prepare, the source as it is above the limit", video_probe(&[], false), Some(video(|_| {})), SOURCE.len() as u64 - 1, 0, true),
        ("the prepared result above the limit", probed_image(), Some(shrink()), PREPARED.len() as u64 - 1, 1, true),
        ("no prepare, exactly the limit", probed_image(), None, SOURCE.len() as u64, 0, false),
        ("the prepared result of exactly the limit", probed_image(), Some(shrink()), PREPARED.len() as u64, 1, false),
    ];
    for (what, probe, prepare, limit, runs, refused) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let source = source_file(directory.path(), "clip", SOURCE);
        let preparer = ScriptedPreparer::new(probe, PREPARED, &[]);
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
