use std::sync::Arc;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen_with, Capture};
use super::rig_tests::bound;
use super::upload_stand::{clip, Detached, Stand, SystemTask, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::{Database, TestClock};
use crate::owners::write::upload::MEBIBYTE;
use crate::transport::{PutIdentity, RawOutcome};

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn script(stand: &Stand) {
    stand.script.data("InitMultipart", INIT);
    for _ in 0..6 { stand.script.data("PresignParts", PRESIGN); }
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    stand.script.data("CreateMoment", LANDED);
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

async fn opened() -> Started {
    let detached = Arc::new(Detached::default());
    let stand = Stand::new().detached_through(detached.clone());
    script(&stand);
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let clock = TestClock::new(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    Started { stand, detached, database, clock, temporary: String::new(), directory, live }
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

/// A file below the multipart threshold still goes as one part as large as the file, `Init` first, the PUT started with the presigned target by the executor and not by the transport, and no timer ends it.
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

/// A permanent refusal of the system task fails the transfer as a refusal of a PUT in the process does, and `Complete` never goes.
/// Observable: the status of the row and the calls. Catches: an outcome that is taken as a success, a refusal that is retried.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_ended_failed() {
    for status_code in [400, 410] {
        let run = started(MEBIBYTE).await;
        run.detached.finish(0, RawOutcome::Response { status: status_code, etag: None, body: String::new() });
        crate::test_wait::settled(&run.database, "the upload failed", || status(&run.database, &run.temporary) == Some(WireUploadStatus::Failed)).await;
        assert_eq!((run.stand.script.count("CompleteMultipart"), run.detached.started().len()), (0, 1), "status {status_code}");
        run.database.close_now();
    }
}

/// An outcome for an epoch, a transfer or an attempt that is not the live one changes nothing and the executor is told to cancel it. The live outcome still lands the row.
/// Observable: the cancels the executor got, the calls, the landed row. Catches: a foreign outcome that confirms the part of another attempt.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_detached_stray() {
    let run = started(MEBIBYTE).await;
    let live = run.detached.started()[0].0;
    let strays = [PutIdentity { epoch: live.epoch + 1, ..live }, PutIdentity { transfer_id: live.transfer_id + 90, ..live }, PutIdentity { attempt_id: live.attempt_id + 1, ..live }];
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
    let temporary = run.temporary.clone();
    let cuts = over_images(&capture, &kept, |index, image| {
        let secret = secret.clone();
        let temporary = temporary.clone();
        async move {
            let task = recorded_task(&image, &temporary);
            let copies = [image.clone(), twin(&image)];
            for (lost, image) in [false, true].into_iter().zip(copies) {
                let detached = Arc::new(Detached::default());
                if let (Some(identity), false) = (task, lost) { detached.plant(SystemTask { identity, outcome: Some(Detached::ok()) }); }
                let (fresh, reopened) = reopen_with(&image, &secret, &[CREATE_MOMENT], detached.clone(), script).await;
                if task.is_some() && !lost { drained(&reopened).await; } else { crate::test_wait::settled(&reopened, "opened", || true).await; }
                let new_starts = detached.started().len();
                match (task.is_some(), lost) {
                    (true, false) => assert_eq!((new_starts, landed(&reopened)), (0, true), "cut {index}: a recorded attempt with a finished task starts no second PUT and lands"),
                    (true, true) => assert_eq!(new_starts, 1, "cut {index}: after PutMissing the part starts once"),
                    (false, _) => assert!(new_starts <= 1, "cut {index}: a part starts at most once"),
                }
                assert!(task.is_none() || fresh.script.count("InitMultipart") == 0, "cut {index}: the recorded upload is not created twice");
                bound(reopened.close_database()).await.unwrap();
            }
            index
        }
    }).await;
    assert_eq!(cuts.len(), capture.cuts());
    run.database.close_now();
}

/// The id of the task a file records: the transfer and the count of attempts, when the attempt has the upload recorded and no presign waits for its answer, so the executor may hold a task of it.
fn recorded_task(image: &std::path::Path, _temporary: &str) -> Option<PutIdentity> {
    let connection = rusqlite::Connection::open(image).unwrap();
    rusqlite::OptionalExtension::optional(connection.query_row("SELECT transfer_id, attempts FROM sync_upload WHERE upload_id IS NOT NULL AND attempts > 0 AND completed IS NOT 1 AND NOT EXISTS (SELECT 1 FROM encrypted_pending) AND (SELECT count(*) FROM sync_upload_part WHERE etag IS NOT NULL) < total", [], |row| Ok(PutIdentity { epoch: 0, transfer_id: row.get::<_, i64>(0)? as u64, attempt_id: row.get::<_, i64>(1)? as u64 }))).unwrap()
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
