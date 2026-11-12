//! A file accepted before its row (D-372) through the public entry: the library moves it to the last part, `commit_staged` gives it its row and its final mutation.

use std::path::Path;

use super::crash_image_support::{files_of, over_images, reopen, Capture};
use super::upload_prepare_tests::{probed_image, shrink, upload_of, PREPARED, SOURCE};
use super::rig_tests::bound;
use super::upload_stand::{request, ScriptedPreparer, Stand, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
use crate::ffi::{Database, ReadRequest, ReadValue};
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
pub(super) const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;
pub(super) const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
pub(super) const OPERATIONS: [(&str, &str); 1] = [CREATE_MOMENT];

pub(super) fn answers(stand: &Stand) {
    stand.script.data("InitMultipart", INIT);
    for _ in 0..8 { stand.script.data("PresignParts", PRESIGN); }
}

fn file_of(directory: &Path, length: u64) -> std::path::PathBuf {
    let path = directory.join("clip");
    std::fs::File::create(&path).unwrap().set_len(length).unwrap();
    path
}

async fn staged(database: &Database, path: &Path) -> crate::ffi::upload_wire::WireStagedUpload {
    bound(database.stage(database.session_epoch(), "Moment".into(), "upload_clip".into(), request(path))).await.unwrap()
}

async fn held(stand: &Stand, database: &Database, parts: usize) {
    crate::test_wait::settled(database, "every part is on the server", || stand.puts.seen.lock().unwrap().len() == parts).await;
    let life = database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
}

fn row_exists(database: &Database, id: &str) -> bool { database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap().is_some() }

/// A file staged before its row goes up as a multipart upload from 1 byte, shows `uploading` at 0.99 to the stream of its key and sends neither `complete` nor the mutation; `commit_staged` then shows the row and finishes the upload once.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_accept() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clip");
    std::fs::write(&path, b"data").unwrap();
    answers(&stand);
    let staged = staged(&database, &path).await;
    assert!(staged.receipt.optimistic_value.is_none(), "the receipt has no row");
    held(&stand, &database, 1).await;
    assert!(!row_exists(&database, &staged.row_id), "the row does not exist before the commit");
    assert_eq!(stand.script.names(), ["InitMultipart", "PresignParts"], "multipart from 1 byte, no complete, no mutation");
    let status = database.subscribe_values(database.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: staged.row_id.clone(), action: "upload_clip".into() });
    let mut last = None;
    for _ in 0..8 {
        if let Some(ReadValue::Status { state, progress, .. }) = bound(status.pull_next()).await { last = Some((state, progress)); if progress >= 0.99 { break; } }
    }
    assert_eq!(last, Some(("uploading".to_string(), 0.99)));
    database.close_now();
}

/// After `commit_staged` the transfer sends the requests of a `send` of the same file: the same series of steps and the same puts.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_matches_send() {
    let directory = tempfile::tempdir().unwrap();
    let source = file_of(directory.path(), 11 * MEBIBYTE);
    let sent = Stand::new();
    let database = sent.open(None, &OPERATIONS).await;
    answers(&sent);
    sent.script.data("CompleteMultipart", COMPLETE);
    sent.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(super::upload_stand::clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    database.close_now();
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    answers(&stand);
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
    let staged = staged(&database, &source).await;
    held(&stand, &database, 3).await;
    let committed = bound(database.commit_staged(database.session_epoch(), "Moment".into(), staged.row_id, "upload_clip".into(), r#"{"text":"video"}"#.into(), "{}".into())).await.unwrap();
    bound(database.confirmed(committed, false)).await.unwrap();
    assert_eq!(stand.script.names(), sent.script.names());
    assert_eq!(stand.puts.seen.lock().unwrap().len(), sent.puts.seen.lock().unwrap().len());
    database.close_now();
}

/// `commit_staged` in any stage of the file gives the row its values at once and sends the final mutation once, after every part is on the server: while the file is prepared, while its parts go and while it waits in `Held`.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_commit_early() {
    for case in ["preparing", "parts", "held"] {
        let directory = tempfile::tempdir().unwrap();
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        let stand = if case == "preparing" { Stand::new().with_preparer(preparer.clone()) } else { Stand::new() };
        let database = stand.open(None, &OPERATIONS).await;
        answers(&stand);
        stand.script.data("CompleteMultipart", COMPLETE);
        stand.script.data("CreateMoment", LANDED);
        let (source, parts) = match case {
            "preparing" => { let path = directory.path().join("clip"); std::fs::write(&path, SOURCE).unwrap(); (path, 1) }
            _ => (file_of(directory.path(), 11 * MEBIBYTE), 3),
        };
        let (run_hold, put_hold) = (preparer.hold_next(), stand.puts.hold_next());
        let staged = if case == "preparing" {
            let request = WireUploadRequest { source: WireSource::File { path: source.to_str().unwrap().into(), name: None, mime: None, prepare: Some(shrink()) }, variables_json: "{}".into() };
            bound(database.stage(database.session_epoch(), "Moment".into(), "upload_clip".into(), request)).await.unwrap()
        } else { staged(&database, &source).await };
        match case {
            "preparing" => run_hold.entered().await,
            "parts" => put_hold.entered().await,
            _ => { put_hold.release(); held(&stand, &database, parts).await; }
        }
        assert_eq!(stand.script.count("CompleteMultipart") + stand.script.count("CreateMoment"), 0, "{case}: nothing is completed before the commit");
        let committed = bound(database.commit_staged(database.session_epoch(), "Moment".into(), staged.row_id.clone(), "upload_clip".into(), r#"{"text":"hi"}"#.into(), "{}".into())).await.unwrap();
        let shown = database.peek_row(database.session_epoch(), "Moment".into(), staged.row_id.clone()).unwrap().expect("the row is shown at once");
        assert_eq!(committed.optimistic_value.as_ref().map(|row| row.id.clone()), Some(staged.row_id.clone()), "{case}");
        assert!(format!("{:?}", shown.cells).contains("hi"), "{case}: the row shows the values of the commit");
        assert!(upload_of(&database, &staged.row_id).is_some(), "{case}: the row shows its upload");
        run_hold.release();
        put_hold.release();
        bound(database.confirmed(committed, false)).await.unwrap();
        let names = stand.script.names();
        assert_eq!(names.iter().filter(|name| *name == "CreateMoment").count(), 1, "{case}: the mutation goes once");
        assert_eq!(names.iter().filter(|name| *name == "CompleteMultipart").count(), 1, "{case}");
        assert!(names.iter().rposition(|name| name == "PresignParts") < names.iter().position(|name| name == "CompleteMultipart") && names.last().map(String::as_str) == Some("CreateMoment"), "{case}: {names:?}");
        assert_eq!(stand.puts.seen.lock().unwrap().len(), parts, "{case}: every part was sent");
        database.close_now();
    }
}

/// A process killed at any commit of `stage` -> parts -> `Held` leaves a base that the next opening cleans: no held row, no file of a transfer, no optimistic row, one abort for every multipart the server created, the one the image knew and the one the saved `Init` pair answers; the only other requests are the saved pairs of the steps in flight, which Session sends again (D-246).
#[tokio::test(flavor = "multi_thread")]
async fn crash_image_held() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let source = file_of(directory.path(), 11 * MEBIBYTE);
    let stand = Stand::new();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    answers(&stand);
    let staged = staged(&database, &source).await;
    held(&stand, &database, 3).await;
    crate::test_wait::settled(&database, "the last part is written", || rusqlite::Connection::open(&live).unwrap().query_row("SELECT count(*) FROM sync_upload WHERE held = 1 AND sent = total", [], |row| row.get::<_, i64>(0)).unwrap() == 1).await;
    let (capture, kept_files) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();
    let id = staged.row_id.clone();
    let cuts = over_images(&capture, &kept_files, |index, image: std::path::PathBuf| {
        let (secret, id) = (secret.clone(), id.clone());
        async move {
            let known = rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM (SELECT upload_id FROM sync_upload WHERE upload_id IS NOT NULL UNION SELECT upload_id FROM upload_orphan)", [], |row| row.get::<_, i64>(0)).unwrap();
            let pairs = rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get::<_, i64>(0)).unwrap();
            let init_pair = rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM encrypted_pending WHERE owner_step = 'init'", [], |row| row.get::<_, i64>(0)).unwrap() > 0;
            let (fresh, reopened) = reopen(&image, &secret, &OPERATIONS, |fresh| {
                fresh.script.data("AbortMultipart", ABORTED);
                fresh.script.data("InitMultipart", INIT);
                fresh.script.data("PresignParts", PRESIGN);
            }).await;
            let rows = || rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM sync_upload", [], |row| row.get::<_, i64>(0)).unwrap();
            let files = || std::fs::read_dir(files_of(&image).join("transfers")).map_or(0, |found| found.count());
            let aborts = usize::try_from(known).unwrap().max(usize::from(init_pair));
            crate::test_wait::settled(&reopened, "the held transfer is gone", || { rows() == 0 && files() == 0 && fresh.script.count("AbortMultipart") == aborts }).await;
            assert!(reopened.peek_row(reopened.session_epoch(), "Moment".into(), id).unwrap().is_none(), "cut {index}: no optimistic row");
            let sent = fresh.script.names();
            assert!(sent.iter().all(|name| matches!(name.as_str(), "AbortMultipart" | "InitMultipart" | "PresignParts")), "cut {index}: nothing completes and nothing is created: {sent:?}");
            assert!(pairs > 0 || sent.iter().all(|name| name == "AbortMultipart"), "cut {index}: only the pair the image saved goes again: {sent:?}");
            bound(reopened.close_database()).await.unwrap();
            aborts
        }
    }).await;
    assert!(cuts.contains(&0) && cuts.contains(&1), "the cuts hold images before and after the multipart exists: {cuts:?}");
    database.close_now();
}

/// A second `commit_staged` of the same key is `Invalid`: the row stays one and the final mutation goes once.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_commit_twice() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("clip");
    std::fs::write(&path, b"data").unwrap();
    answers(&stand);
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
    let staged = staged(&database, &path).await;
    held(&stand, &database, 1).await;
    let commit = |text: &str| database.commit_staged(database.session_epoch(), "Moment".into(), staged.row_id.clone(), "upload_clip".into(), format!(r#"{{"text":"{text}"}}"#), "{}".into());
    bound(commit("first")).await.unwrap();
    let second = bound(commit("second")).await;
    assert!(matches!(second, Err(crate::ffi::DbError::Invalid { .. })), "{second:?}");
    let shown = database.peek_row(database.session_epoch(), "Moment".into(), staged.row_id.clone()).unwrap().expect("the row of the first commit");
    assert!(format!("{:?}", shown.cells).contains("first"));
    crate::test_wait::settled(&database, "the final mutation went", || stand.script.count("CreateMoment") == 1).await;
    assert_eq!(stand.script.count("CreateMoment"), 1);
}
