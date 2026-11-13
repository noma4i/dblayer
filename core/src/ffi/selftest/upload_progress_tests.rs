use std::sync::{Arc, Mutex};

use super::crash_image_support::{advancing, copy_tree, drained, files_of, over_images, reopen_timed, timed_stand, wal_length, Capture};
use super::rig_tests::bound;
use super::upload_prepare_tests::upload_of;
use super::upload_stand::{clip, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::{Database, ReadRequest, ReadValue};
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const TOTAL: u64 = 11 * MEBIBYTE;
const FIRST: f64 = (5 * MEBIBYTE) as f64 / TOTAL as f64;
const SECOND: f64 = (10 * MEBIBYTE) as f64 / TOTAL as f64;

fn answers(stand: &Stand) {
    stand.script.data("InitMultipart", INIT);
    for _ in 0..40 { stand.script.data("PresignParts", PRESIGN); }
}

fn shown(database: &Database, id: &str) -> Option<f64> { upload_of(database, id).map(|upload| upload.progress) }

fn watch_progress(database: &Database, id: &str) -> (Arc<Mutex<Vec<f64>>>, tokio::task::JoinHandle<()>) {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let watch = database.subscribe_values(database.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: id.into(), action: "upload_clip".into() });
    let sink = frames.clone();
    let task = tokio::spawn(async move {
        while let Some(value) = watch.pull_next().await {
            if let ReadValue::Status { progress, .. } = value { sink.lock().unwrap().push(progress); }
        }
    });
    (frames, task)
}

fn never_goes_back(frames: &[f64]) -> bool { frames.windows(2).all(|pair| pair[0] <= pair[1]) }

fn between(frames: &[f64], low: f64, high: f64) -> bool { frames.iter().any(|frame| *frame > low && *frame < high) }

fn sparse(path: &std::path::Path, length: u64) { std::fs::File::create(path).unwrap().set_len(length).unwrap(); }

/// The progress of an upload is the share of its bytes from 0 to 1: it grows inside a part, a repeated part and a restart do not take it back, and 1 comes only when the server has the whole file.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_progress_is_true_and_never_goes_back() {
    let (stand, clock) = timed_stand();
    let stand = stand.reporting_bytes();
    let directory = tempfile::tempdir().unwrap();
    let (live, source) = (directory.path().join("live.sqlite"), directory.path().join("video"));
    sparse(&source, TOTAL);
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let epoch = database.session_epoch();
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    answers(&stand);
    let complete = stand.script.hold("CompleteMultipart", COMPLETE);
    let create = stand.script.hold("CreateMoment", LANDED);
    let (first, second) = (stand.puts.hold_next(), stand.puts.hold_next());
    stand.puts.fail_once_at(5 * MEBIBYTE);
    let receipt = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let (frames, collector) = watch_progress(&database, &id);
    first.entered().await;
    first.release();
    second.entered().await;
    let in_flight = shown(&database, &id).unwrap();
    assert!(in_flight <= 1.0, "the progress is a share from 0 to 1: {in_flight}");
    crate::test_wait::settled(&database, "the share of part two in flight is shown", || shown(&database, &id).is_some_and(|share| share > FIRST && share < SECOND)).await;
    let before_death = shown(&database, &id).unwrap();
    let kept_files = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept_files);
    let cut_length = wal_length(&live);
    second.release();
    advancing(&clock, complete.entered()).await;
    crate::test_wait::settled(&database, "the whole file is sent, not yet complete", || shown(&database, &id).is_some_and(|share| share > SECOND)).await;
    assert!(shown(&database, &id).unwrap() < 1.0, "1 is not shown before the server completes the file");
    assert!(frames.lock().unwrap().iter().all(|frame| *frame < 1.0), "no frame shows 1 before the server completes the file");
    complete.release();
    advancing(&clock, create.entered()).await;
    crate::test_wait::settled(&database, "1 is shown after complete", || shown(&database, &id) == Some(1.0) && frames.lock().unwrap().last() == Some(&1.0)).await;
    create.release();
    advancing(&clock, async { crate::test_wait::settled(&database, "the upload landed", || database.peek_row_now(epoch, "Moment".into(), "m77".into()).unwrap().is_some()).await; drained(&database).await }).await;
    let live_frames = frames.lock().unwrap().clone();
    collector.abort();
    assert!(live_frames.iter().all(|frame| (0.0..=1.0).contains(frame)) && never_goes_back(&live_frames), "the share never goes back and stays in 0..1: {live_frames:?}");
    assert!(between(&live_frames, 0.0, FIRST) && between(&live_frames, FIRST, SECOND) && between(&live_frames, SECOND, 1.0), "the share grows inside every part: {live_frames:?}");
    assert_eq!(stand.puts.seen.lock().unwrap().iter().filter(|put| put.offset == 5 * MEBIBYTE).count(), 2, "part two went again");
    let (capture, _) = base.end(&live);
    let (secret, target) = (stand.rig.credentials.inner.value().unwrap(), capture.cut_at(cut_length));

    let restarted = over_images(&capture, &kept_files, |index, image| {
        let (secret, id) = (secret.clone(), id.clone());
        async move {
            if index != target { return None }
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &[CREATE_MOMENT], |fresh| {
                answers(fresh);
                fresh.script.data("CompleteMultipart", COMPLETE);
                fresh.script.data("CreateMoment", LANDED);
            }, false).await;
            let watch = reopened.subscribe_values(reopened.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id, action: "upload_clip".into() });
            let Some(ReadValue::Status { progress, .. }) = bound(watch.pull_next()).await else { panic!("the status ended") };
            let frames = Arc::new(Mutex::new(vec![progress]));
            let sink = frames.clone();
            let task = tokio::spawn(async move {
                while let Some(value) = watch.pull_next().await {
                    if let ReadValue::Status { progress, .. } = value { sink.lock().unwrap().push(progress); }
                }
            });
            advancing(&clock, async { crate::test_wait::settled(&reopened, "the upload landed", || reopened.peek_row_now(reopened.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some()).await; drained(&reopened).await }).await;
            task.abort();
            let seen = frames.lock().unwrap().clone();
            let parts = fresh.puts.seen.lock().unwrap().len();
            bound(reopened.close_database()).await.unwrap();
            Some((seen, parts))
        }
    }).await;
    let (after, parts) = restarted.into_iter().flatten().next().expect("the image cut after the share of part two was shown");
    assert!(after[0] >= before_death, "the first frame after the restart ({}) is not less than the last shown before it ({before_death})", after[0]);
    assert!(never_goes_back(&after) && after.iter().all(|frame| *frame <= 1.0), "after the restart the share never goes back: {after:?}");
    assert!(parts <= 2, "the part confirmed before the cut is not sent again");
    database.close_now();

    let stand = Stand::new().reporting_bytes();
    let (single, small) = (directory.path().join("single.sqlite"), directory.path().join("small"));
    sparse(&small, MEBIBYTE);
    let database = stand.open(Some(&single), &operations).await;
    let epoch = database.session_epoch();
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let held = stand.puts.hold_next();
    let receipt = bound(database.send(clip("Moment", r#"{"text":"small"}"#, "upload_clip", &small, epoch))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let (frames, collector) = watch_progress(&database, &id);
    held.entered().await;
    crate::test_wait::settled(&database, "the bytes of the single PUT in flight are shown", || shown(&database, &id).is_some_and(|share| share > 0.0 && share < 1.0)).await;
    held.release();
    bound(database.confirmed(receipt, false)).await.unwrap();
    let single_frames = frames.lock().unwrap().clone();
    collector.abort();
    assert!(between(&single_frames, 0.0, 1.0) && never_goes_back(&single_frames), "a single file does not jump from 0 to 1: {single_frames:?}");
    database.close_now();
}
