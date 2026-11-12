use std::sync::Arc;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen_with, Capture};
use super::rig_tests::bound;
use super::upload_stand::{clip, Detached, Hold, Stand, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::{Database, TestClock};
use crate::owners::write::upload::MEBIBYTE;
use crate::transport::{PutIdentity, RawFailure, RawOutcome};

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn script(stand: &Stand) { script_with(stand, false); }

/// The script of the server. With `holds` the answers of `Complete` and of the final mutation wait for the test.
fn script_with(stand: &Stand, holds: bool) -> Option<(Arc<Hold>, Arc<Hold>)> {
    stand.script.data("InitMultipart", INIT);
    for _ in 0..6 { stand.script.data("PresignParts", PRESIGN); }
    stand.script.data("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    if !holds {
        stand.script.data("CompleteMultipart", COMPLETE);
        stand.script.data("CreateMoment", LANDED);
        return None;
    }
    Some((stand.script.hold("CompleteMultipart", COMPLETE), stand.script.hold("CreateMoment", LANDED)))
}

fn status(database: &Database, id: &str) -> Option<WireUploadStatus> {
    let row = database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap()?;
    row.uploads.iter().find(|entry| entry.action == "upload_clip").map(|entry| entry.upload.status)
}

fn landed(database: &Database) -> bool { database.peek_row(database.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some() }

fn source(directory: &std::path::Path, length: u64) -> std::path::PathBuf {
    let source = directory.join("video");
    std::fs::File::create(&source).unwrap().set_len(length).unwrap();
    source
}

/// A database whose PUT runs outside the process, with a clip sent and its PUT started.
struct Started {
    stand: Stand,
    detached: Arc<Detached>,
    database: Arc<Database>,
    clock: Arc<TestClock>,
    temporary: String,
    directory: tempfile::TempDir,
    live: std::path::PathBuf,
}

async fn opened() -> Started { opened_with(false).await.0 }

async fn opened_with(holds: bool) -> (Started, Option<(Arc<Hold>, Arc<Hold>)>) {
    let detached = Arc::new(Detached::default());
    let stand = Stand::new().multipart_only().detached_through(detached.clone());
    let held = script_with(&stand, holds);
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let clock = TestClock::new(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    (Started { stand, detached, database, clock, temporary: String::new(), directory, live }, held)
}

/// The clip is sent and the executor got the start of its PUT.
async fn launch(run: &mut Started, length: u64) {
    let epoch = run.database.session_epoch();
    let file = source(run.directory.path(), length);
    let sent = bound(run.database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file, epoch))).await.unwrap();
    run.temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    let detached = run.detached.clone();
    crate::test_wait::settled(&run.database, "the PUT started outside the process", || detached.started().len() == 1).await;
}

async fn started(length: u64) -> Started {
    let mut run = opened().await;
    launch(&mut run, length).await;
    run
}

/// A file of one part of a multipart-only declaration, `Init` first, the PUT started with the presigned target by the executor and not by the transport, and no timer ends it.
/// Observable: the calls to the server, the start the executor got, the PUTs of the transport, the status after the time of the PUT timer passed. Catches: a PUT that goes through the transport, a part cut to the part size, a second step that starts without the recorded upload, a timer that cancels the system task.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_start() {
    let run = started(MEBIBYTE).await;
    assert_eq!(run.stand.script.names(), ["InitMultipart", "PresignParts"], "Init answers and is written before the presign of part 1");
    assert_eq!(run.stand.script.variables("PresignParts")[0]["partNumbers"], serde_json::json!([1]));
    let started = run.detached.started();
    assert_eq!(started[0].1.url, "https://storage/1");
    assert_eq!((started[0].2.offset, started[0].2.length), (0, MEBIBYTE));
    assert!(run.stand.puts.seen.lock().unwrap().is_empty(), "the transport runs no PUT");
    run.clock.advance(10 * 60 * 1000);
    crate::test_wait::settled(&run.database, "the time of the PUT timer passed", || status(&run.database, &run.temporary) == Some(WireUploadStatus::Uploading)).await;
    assert_eq!((run.detached.started().len(), run.detached.cancelled().len()), (1, 0), "no timer restarts or cancels the task");
    run.database.close_now();
}

/// The outcome of the system task ends the step as the answer of a PUT in the process does: the part is confirmed with its etag, `Complete` goes with it and the row lands.
/// Observable: the arguments of `CompleteMultipart` and the landed row. Catches: an outcome that is dropped, an etag that is lost.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_ended_ok() {
    let run = started(MEBIBYTE).await;
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    assert_eq!(run.stand.script.variables("CompleteMultipart")[0]["parts"], serde_json::json!([{"partNumber": 1, "etag": "etag"}]));
    assert_eq!(run.stand.script.count("InitMultipart"), 1);
    run.database.close_now();
}

/// A permanent refusal of the system task and a 2xx answer without an etag fail the transfer as a refusal of a PUT in the process does, and `Complete` never goes.
/// Observable: the status of the row and the calls. Catches: an outcome that is taken as a success, a refusal that is retried.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_ended_failed() {
    for status_code in [400, 410, 200, 204] {
        let run = started(MEBIBYTE).await;
        run.detached.finish(0, RawOutcome::Response { status: status_code, etag: None, body: String::new() });
        crate::test_wait::settled(&run.database, "the upload failed", || status(&run.database, &run.temporary) == Some(WireUploadStatus::Failed)).await;
        assert_eq!((run.stand.script.count("CompleteMultipart"), run.detached.started().len()), (0, 1), "status {status_code}");
        run.database.close_now();
    }
}

/// An outcome for a cycle, a transfer or an attempt that is not the live one changes nothing and the executor is told to cancel it. The live outcome still lands the row.
/// Observable: the cancels the executor got, the calls, the landed row. Catches: a foreign outcome that confirms the part of another attempt.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_stray() {
    let run = started(MEBIBYTE).await;
    let live = run.detached.started()[0].0;
    let strays = [PutIdentity { cycle_id: live.cycle_id + 1, ..live }, PutIdentity { transfer_id: live.transfer_id + 90, ..live }, PutIdentity { attempt: live.attempt + 1, ..live }];
    for stray in strays { run.database.put_ended(stray, Detached::ok()); }
    crate::test_wait::settled(&run.database, "the strays were answered", || run.detached.cancelled().len() == 3).await;
    assert_eq!(run.detached.cancelled(), strays);
    assert_eq!(run.stand.script.count("CompleteMultipart"), 0);
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    run.database.close_now();
}

/// Leaving the network does not touch the system task: the transfer stays `Uploading`, nothing is cancelled and nothing starts again.
/// Observable: the status, the cancels, the starts. Catches: the lines of the PUT in the process (back to `Queued`, cancel of the step) applied to a task of the system.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_keeps_put_offline() {
    let run = started(MEBIBYTE).await;
    run.database.set_online(false).unwrap();
    crate::test_wait::settled(&run.database, "offline", || status(&run.database, &run.temporary) == Some(WireUploadStatus::Uploading)).await;
    run.database.set_online(true).unwrap();
    crate::test_wait::settled(&run.database, "online again", || status(&run.database, &run.temporary) == Some(WireUploadStatus::Uploading)).await;
    assert_eq!((run.detached.started().len(), run.detached.cancelled().len()), (1, 0));
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    run.database.close_now();
}

/// Cancelling an upload in the network cancels the system task with its identity.
/// Observable: the cancel the executor got. Catches: a task of the system that goes on for a transfer that is gone.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_cancel() {
    let run = started(MEBIBYTE).await;
    let identity = run.detached.started()[0].0;
    assert!(bound(run.database.cancel_upload(run.database.session_epoch(), "Moment".into(), run.temporary.clone(), "upload_clip".into())).await.unwrap());
    crate::test_wait::settled(&run.database, "the task was cancelled", || run.detached.cancelled() == [identity]).await;
    run.database.close_now();
}

/// A process that starts on a file with a recorded attempt waits for the task: no second `Init`, no second PUT, the transfer shows `Uploading`, `ResumePuts` lists the task once, and the outcome that came while no process ran lands the row.
/// Observable: the starts, the calls, the status, the lists of `ResumePuts`. Catches: a second PUT of the same part, a restored transfer that shows `Queued`, a list that goes twice.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_restored() {
    let run = started(MEBIBYTE).await;
    run.database.close_now();
    run.detached.finish_offline(0, Detached::ok());
    let secret = run.stand.rig.credentials.inner.value().unwrap();
    let (again, database) = reopen_with(&run.live, &secret, &[CREATE_MOMENT], run.detached.clone(), script).await;
    crate::test_wait::settled(&database, "the row landed", || landed(&database)).await;
    assert_eq!(run.detached.started().len(), 1, "no second PUT");
    assert_eq!((again.script.count("InitMultipart"), again.script.count("PresignParts")), (0, 0));
    let lists = run.detached.resumed();
    assert_eq!((lists.len(), lists[0].len(), lists[1].len()), (2, 0, 1), "the first open lists nothing, the open on the file lists the task once");
    database.close_now();
}

/// The system has no task for the recorded attempt: the step starts again with the recorded upload, so no second `Init`, and the new task lands the row.
/// Observable: the starts, the calls, the landed row. Catches: a transfer that waits for a task that does not exist, an upload created twice.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_missing() {
    let run = started(MEBIBYTE).await;
    run.database.close_now();
    run.detached.lose(0);
    let secret = run.stand.rig.credentials.inner.value().unwrap();
    let (again, database) = reopen_with(&run.live, &secret, &[CREATE_MOMENT], run.detached.clone(), script).await;
    crate::test_wait::settled(&database, "the PUT started again", || run.detached.started().len() == 2).await;
    assert_eq!((again.script.count("InitMultipart"), again.script.count("PresignParts")), (0, 1));
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&database, "the row landed", || landed(&database)).await;
    assert_eq!(again.script.count("CompleteMultipart"), 1);
    database.close_now();
}

/// The executor ends an attempt whose target is not a URL with a failure: the core classifies it as any failed PUT (`Io`, a temporary one) and the next attempt waits for its Backoff.
/// Observable: the starts the executor got before and after the time of the first retry passed. Catches: an attempt that starts again at once and runs through the attempt limit without a pause.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_bad_target() {
    let run = started(MEBIBYTE).await;
    let live = run.detached.started()[0].0;
    run.database.put_ended(live, RawOutcome::Failed { failure: RawFailure { domain: "url".into(), code: None, message: "the PUT target is not a URL".into() } });
    bound(run.database.writes_quiet(run.database.session_epoch())).await;
    assert_eq!(run.detached.started().len(), 1, "no attempt starts before the Backoff");
    run.clock.advance(60 * 1000);
    let detached = run.detached.clone();
    crate::test_wait::settled(&run.database, "the next attempt started", || detached.started().len() == 2).await;
    assert_ne!(run.detached.started()[1].0.attempt, live.attempt);
    run.database.close_now();
}

/// `PutMissing` counts only for the attempt that the opening restored and listed: for an attempt this process started it changes nothing.
/// Observable: the starts and the calls after a `PutMissing` for the live attempt. Catches: an executor input that restarts the step without a pause and without the answer of `ResumePuts`.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_missing_needs_resume() {
    let run = started(MEBIBYTE).await;
    let live = run.detached.started()[0].0;
    run.database.put_missing(live);
    bound(run.database.writes_quiet(run.database.session_epoch())).await;
    assert_eq!((run.detached.started().len(), run.stand.script.count("PresignParts")), (1, 1));
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    run.database.close_now();
}

/// A transfer of an earlier build with 3 parts, 1 of them confirmed in the process, opens on a build that runs PUTs outside the process: the other parts go on in the process with their own slices and `Complete` carries every etag.
/// Observable: the PUTs of the transport with their ranges, the starts the executor got, the arguments of `CompleteMultipart`. Catches: a part sent as the whole file, a part started outside the process, an object assembled from a wrong list.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_old_multipart() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = source(directory.path(), 11 * MEBIBYTE);
    let presign = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
    let earlier = Stand::new();
    earlier.script.data("InitMultipart", INIT);
    for _ in 0..3 { earlier.script.data("PresignParts", presign); }
    let (first_part, second_part) = (earlier.puts.hold_next(), earlier.puts.hold_next());
    let database = earlier.open(Some(&live), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file, epoch))).await.unwrap();
    first_part.entered().await;
    first_part.release();
    second_part.entered().await;
    database.close_now();
    let secret = earlier.rig.credentials.inner.value().unwrap();
    let detached = Arc::new(Detached::default());
    let (later, reopened) = reopen_with(&live, &secret, &[CREATE_MOMENT], detached.clone(), |stand| {
        for _ in 0..3 { stand.script.data("PresignParts", presign); }
        stand.script.data("CompleteMultipart", COMPLETE);
        stand.script.data("CreateMoment", LANDED);
        stand.script.data("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    }).await;
    crate::test_wait::settled(&reopened, "the row landed", || landed(&reopened)).await;
    let ranges: Vec<(u64, u64)> = later.puts.seen.lock().unwrap().iter().map(|put| (put.offset, put.length)).collect();
    assert_eq!(ranges, [(5 * MEBIBYTE, 5 * MEBIBYTE), (10 * MEBIBYTE, MEBIBYTE)], "the parts that are left go as slices in the process");
    assert_eq!(detached.started().len(), 0, "no PUT outside the process");
    assert_eq!(later.script.variables("CompleteMultipart")[0]["parts"].as_array().map(Vec::len), Some(3));
    reopened.close_now();
}

/// P-XFER-77: `writes_quiet` answers only when the writes have no work that runs without a new external event, and always answers.
/// Observable: when the answer comes against the held `Complete` and final mutation, the landed row at the answer, the answer with only a system task in flight, with the network away, and at `close`. Catches: an answer before the final mutation landed (the app goes back to the system with work left), an answer that waits for a system task or for the network, a wait that `close` does not end.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_writes_quiet() {
    let (mut run, held) = opened_with(true).await;
    let (complete, final_mutation) = held.unwrap();
    launch(&mut run, MEBIBYTE).await;
    let epoch = run.database.session_epoch();
    bound(run.database.writes_quiet(epoch)).await;
    run.detached.finish(0, Detached::ok());
    let database = run.database.clone();
    let quiet = tokio::spawn(async move { database.writes_quiet(epoch).await });
    complete.entered().await;
    assert!(!quiet.is_finished(), "Complete is on its way");
    complete.release();
    final_mutation.entered().await;
    assert!(!quiet.is_finished(), "the final mutation is on its way");
    final_mutation.release();
    bound(quiet).await.unwrap();
    assert!(landed(&run.database), "the answer comes after the final mutation landed");
    run.database.close_now();

    let away = opened().await;
    let away_epoch = away.database.session_epoch();
    away.database.set_online(false).unwrap();
    let file = source(away.directory.path(), MEBIBYTE);
    bound(away.database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file, away_epoch))).await.unwrap();
    bound(away.database.writes_quiet(away_epoch)).await;
    assert_eq!(away.detached.started().len(), 0, "the step waits for the network");
    away.database.close_now();

    let (mut closing, held) = opened_with(true).await;
    launch(&mut closing, MEBIBYTE).await;
    closing.detached.finish(0, Detached::ok());
    held.as_ref().unwrap().0.entered().await;
    let database = closing.database.clone();
    let closing_epoch = closing.database.session_epoch();
    let waits = tokio::spawn(async move { database.writes_quiet(closing_epoch).await });
    closing.database.close_now();
    bound(waits).await.unwrap();

    let restart = started(MEBIBYTE).await;
    restart.database.close_now();
    restart.detached.finish_offline(0, Detached::ok());
    let secret = restart.stand.rig.credentials.inner.value().unwrap();
    let (_again, reopened) = reopen_with(&restart.live, &secret, &[CREATE_MOMENT], restart.detached.clone(), script).await;
    bound(reopened.writes_quiet(reopened.session_epoch())).await;
    assert!(landed(&reopened), "the answer after the opening comes after the restored transfer applied the outcome of its task");
    reopened.close_now();
}

/// P-XFER-74: a process killed at any commit between the start of the PUT and its outcome leaves a file that never sends the PUT of the part a second time before `PutMissing`, and finishes the upload from the outcome of the same task.
/// Observable: the starts of a process opened on every image and its landed row. Catches: a restart that starts the part again beside a live task, an upload created twice after the id was recorded.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_prop_one_put_through_restart() {
    let mut run = opened().await;
    let secret = run.stand.rig.credentials.inner.value().unwrap();
    rusqlite::Connection::open(&run.live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&run.live);
    launch(&mut run, MEBIBYTE).await;
    let kept = run.directory.path().join("kept.files");
    copy_tree(&files_of(&run.live), &kept);
    run.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    drained(&run.database).await;
    let (capture, _) = base.end(&run.live);
    let cuts = over_images(&capture, &kept, |index, image| {
        let secret = secret.clone();
        async move {
            let copies = [image.clone(), twin(&image)];
            for (lost, image) in [false, true].into_iter().zip(copies) {
                let detached = Arc::new(Detached::default());
                if !lost { detached.finishing_listed(); }
                let (fresh, reopened) = reopen_with(&image, &secret, &[CREATE_MOMENT], detached.clone(), script).await;
                crate::test_wait::settled(&reopened, "the writes are quiet", || true).await;
                let listed: usize = detached.resumed().iter().map(Vec::len).sum();
                let new_starts = detached.started().len();
                match (listed, lost) {
                    (0, _) => assert!(new_starts <= 1, "cut {index}: a part starts at most once"),
                    (1, false) => assert_eq!((new_starts, landed(&reopened)), (0, true), "cut {index}: a listed attempt with a finished task starts no second PUT and lands"),
                    (1, true) => assert_eq!(new_starts, 1, "cut {index}: after PutMissing the part starts once"),
                    (many, _) => panic!("cut {index}: {many} tasks listed for one part"),
                }
                assert!(listed == 0 || fresh.script.count("InitMultipart") == 0, "cut {index}: the recorded upload is not created twice");
                bound(reopened.close_database()).await.unwrap();
            }
            index
        }
    }).await;
    assert_eq!(cuts.len(), capture.cuts());
    run.database.close_now();
    let running = started(MEBIBYTE).await;
    running.database.close_now();
    let secret = running.stand.rig.credentials.inner.value().unwrap();
    let (again, database) = reopen_with(&running.live, &secret, &[CREATE_MOMENT], running.detached.clone(), script).await;
    crate::test_wait::settled(&database, "the task is listed", || running.detached.resumed().len() == 2).await;
    running.detached.finish(0, Detached::ok());
    crate::test_wait::settled(&database, "the row landed", || landed(&database)).await;
    assert_eq!((running.detached.started().len(), again.script.count("InitMultipart"), again.script.count("CompleteMultipart")), (1, 0, 1), "a task that runs at the opening and ends later with the identity of the old process lands the row without a second PUT");
    database.close_now();
}

/// P-XFER-75: the outcome of a task counts once. A repeat of the outcome, an outcome after the part is confirmed and an outcome after the transfer was cancelled change nothing: `Complete` and the final mutation go once.
/// Observable: the calls and the cancels. Catches: a repeated outcome that confirms a second part or starts `Complete` twice.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_prop_ended_once() {
    let run = started(MEBIBYTE).await;
    let identity = run.detached.started()[0].0;
    run.detached.finish(0, Detached::ok());
    run.database.put_ended(identity, Detached::ok());
    run.database.put_ended(identity, Detached::ok());
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    run.database.put_ended(identity, Detached::ok());
    crate::test_wait::settled(&run.database, "the late outcome was answered", || run.detached.cancelled().len() >= 1).await;
    assert_eq!((run.stand.script.count("CompleteMultipart"), run.stand.script.count("CreateMoment")), (1, 1));
    run.database.close_now();
    let cancelled = started(MEBIBYTE).await;
    let identity = cancelled.detached.started()[0].0;
    assert!(bound(cancelled.database.cancel_upload(cancelled.database.session_epoch(), "Moment".into(), cancelled.temporary.clone(), "upload_clip".into())).await.unwrap());
    cancelled.database.put_ended(identity, Detached::ok());
    crate::test_wait::settled(&cancelled.database, "the outcome after the cancel was answered", || cancelled.detached.cancelled().len() == 2).await;
    assert_eq!((cancelled.stand.script.count("CompleteMultipart"), cancelled.stand.script.count("CreateMoment")), (0, 0));
    cancelled.database.close_now();
    let failed = started(MEBIBYTE).await;
    let past = failed.detached.started()[0].0;
    failed.detached.finish(0, RawOutcome::Response { status: 400, etag: None, body: String::new() });
    crate::test_wait::settled(&failed.database, "the upload failed", || status(&failed.database, &failed.temporary) == Some(WireUploadStatus::Failed)).await;
    failed.stand.script.data("InitMultipart", INIT);
    let epoch = failed.database.session_epoch();
    bound(failed.database.retry(epoch, "Moment".into(), failed.temporary.clone(), Some("upload_clip".into()), None)).await.unwrap();
    let detached = failed.detached.clone();
    crate::test_wait::settled(&failed.database, "the retry started a task", || detached.started().len() == 2).await;
    failed.database.put_ended(past, Detached::ok());
    crate::test_wait::settled(&failed.database, "the outcome of the past cycle was answered", || failed.detached.cancelled().contains(&past)).await;
    assert_eq!(failed.stand.script.count("CompleteMultipart"), 0, "the outcome of the cycle before retry confirms nothing");
    failed.database.close_now();
    let ended = started(MEBIBYTE).await;
    let task = ended.detached.started()[0].0;
    bound(ended.database.session_reset()).await.unwrap();
    crate::test_wait::settled(&ended.database, "the end of the session cancelled the task", || ended.detached.cancelled() == [task]).await;
    ended.database.put_ended(task, Detached::ok());
    crate::test_wait::settled(&ended.database, "the outcome of the past session was dropped", || true).await;
    assert_eq!(ended.detached.cancelled(), [task], "the task was cancelled when the session ended");
    assert_eq!((ended.stand.script.count("CompleteMultipart"), ended.stand.script.count("CreateMoment")), (0, 0));
    ended.database.close_now();
}

/// A second copy of an image with its log and files directory: each variant of a cut opens its own file.
fn twin(image: &std::path::Path) -> std::path::PathBuf {
    let twin = image.with_extension("twin.sqlite");
    std::fs::copy(image, &twin).unwrap();
    let wal = |path: &std::path::Path| std::path::PathBuf::from(format!("{}-wal", path.display()));
    if wal(image).exists() { std::fs::copy(wal(image), wal(&twin)).unwrap(); }
    copy_tree(&files_of(image), &files_of(&twin));
    twin
}

/// The progress of the row of a transfer: the share the system task reported, never below the share shown, 1.0 only after the server confirmed the file.
fn progress(database: &Database, id: &str) -> Option<f64> {
    let row = database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap()?;
    row.uploads.iter().find(|entry| entry.action == "upload_clip").map(|entry| entry.upload.progress)
}

/// The bytes a system task sent show on the row through the one count of the PUT in the process.
/// Observable: `uploads[action].progress` after `put_progress`. Catches: a progress that falls after a smaller value, a stray input that moves a row, a share of 1.0 before `Complete` and the final mutation are confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_progress_reaches_the_row() {
    let (mut run, held) = opened_with(true).await;
    let (complete, landed_hold) = held.unwrap();
    launch(&mut run, MEBIBYTE).await;
    let live = run.detached.started()[0].0;
    let stray_attempt = PutIdentity { attempt: live.attempt + 1, ..live };
    let stray_transfer = PutIdentity { transfer_id: live.transfer_id + 90, ..live };
    for (identity, sent, shown) in [(live, MEBIBYTE / 2, 0.5), (live, MEBIBYTE / 4, 0.5), (stray_attempt, MEBIBYTE - 1, 0.5), (stray_transfer, MEBIBYTE - 1, 0.5), (live, MEBIBYTE, 0.99)] {
        run.database.put_progress(identity, sent);
        crate::test_wait::settled(&run.database, "the progress was counted", || progress(&run.database, &run.temporary) == Some(shown)).await;
    }
    run.detached.finish(0, Detached::ok());
    complete.entered().await;
    assert_eq!(progress(&run.database, &run.temporary), Some(0.99), "before the server confirmed the file");
    complete.release();
    landed_hold.entered().await;
    assert_eq!(progress(&run.database, &run.temporary), Some(1.0), "after the server confirmed the file");
    landed_hold.release();
    crate::test_wait::settled(&run.database, "the row landed", || landed(&run.database)).await;
    run.database.close_now();
}
