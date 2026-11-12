//! A file accepted before its row (D-372) through the public entry: the library moves it to the last part, `commit_staged` gives it its row and its final mutation.

use std::path::Path;

use super::crash_image_support::{copy_tree, files_of, reopen};
use super::rig_tests::bound;
use super::upload_stand::{request, Stand, CREATE_MOMENT};
use crate::ffi::{Database, ReadRequest, ReadValue};
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const OPERATIONS: [(&str, &str); 1] = [CREATE_MOMENT];

fn answers(stand: &Stand) {
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
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
    let committed = bound(database.commit_staged(database.session_epoch(), "Moment".into(), staged.row_id.clone(), "upload_clip".into(), r#"{"text":"hi"}"#.into(), "{}".into())).await.unwrap();
    assert_eq!(committed.optimistic_value.as_ref().map(|row| row.id.clone()), Some(staged.row_id.clone()), "the receipt shows the new row");
    bound(database.confirmed(committed, false)).await.unwrap();
    assert_eq!(stand.script.names(), ["InitMultipart", "PresignParts", "CompleteMultipart", "CreateMoment"]);
    assert!(stand.script.variables("CreateMoment")[0].to_string().contains("signed"), "the mutation carries the signed id of the file");
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

/// A process killed while a staged file waits for its commit leaves a base that the next opening cleans: the row of the transfer is gone, no file of the transfer is left, and the upload it minted is aborted.
#[tokio::test(flavor = "multi_thread")]
async fn crash_image_held() {
    let directory = tempfile::tempdir().unwrap();
    let (live, image) = (directory.path().join("live.sqlite"), directory.path().join("image.sqlite"));
    let source = directory.path().join("clip");
    std::fs::write(&source, b"data").unwrap();
    let stand = Stand::new();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    answers(&stand);
    staged(&database, &source).await;
    held(&stand, &database, 1).await;
    crate::test_wait::settled(&database, "the last part is written", || rusqlite::Connection::open(&live).unwrap().query_row("SELECT count(*) FROM sync_upload WHERE held = 1 AND sent = total", [], |row| row.get::<_, i64>(0)).unwrap() == 1).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    std::fs::copy(&live, &image).unwrap();
    copy_tree(&files_of(&live), &files_of(&image));
    let secret = stand.rig.credentials.inner.value().unwrap();
    database.close_now();
    let (fresh, reopened) = reopen(&image, &secret, &OPERATIONS, |fresh| fresh.script.data("AbortMultipart", ABORTED)).await;
    let rows = || rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM sync_upload", [], |row| row.get::<_, i64>(0)).unwrap();
    let files = || std::fs::read_dir(files_of(&image).join("transfers")).map_or(0, |found| found.count());
    crate::test_wait::settled(&reopened, "the held transfer is gone", || rows() == 0 && files() == 0 && fresh.script.count("AbortMultipart") == 1).await;
    assert_eq!(fresh.script.count("CompleteMultipart") + fresh.script.count("CreateMoment"), 0);
    reopened.close_now();
}
