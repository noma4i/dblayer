use std::sync::Arc;

use super::rig_tests::bound;
use super::upload_stand::{clip, shows, watched, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::{WireRow, WireSyncState, WireUploadStatus};
use crate::ffi::{Database, TestClock, WireOperationOutcome};
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::yield_until;

const ROW: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const WEEK_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;
const EXPIRY_OPERATIONS: [(&str, &str); 2] = [("CreateBlob", "mutation CreateBlob { createBlob { signedId } }"), ("CreateMoment", "mutation CreateMoment { createMoment { id } }")];

fn now_millis() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 }

async fn row(database: &Database, id: &str) -> Option<WireRow> { bound(database.read_row(database.session_epoch(), "Moment".into(), id.into())).await.unwrap() }

async fn until_state(database: &Database, id: &str, wanted: WireSyncState) {
    let rows = watched(database, "Moment", id);
    yield_until("the row reaches the state", || shows(&rows, wanted)).await;
}

async fn until_gone(database: &Database, id: &str) {
    let rows = watched(database, "Moment", id);
    yield_until("the row is gone", || rows.len() == 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_blob_fails_the_row_of_its_upload() {
    let stand = Stand::new();
    let database = stand.open(None, &EXPIRY_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    stand.script.enqueue("CreateBlob", Answer::Status(422));

    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;

    until_state(&database, &id, WireSyncState::Failed).await;
    let failed = row(&database, &id).await.unwrap();
    let upload = &failed.uploads.iter().find(|entry| entry.action == "upload_clip").unwrap().upload;
    assert_eq!(upload.status, WireUploadStatus::Failed);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn reopening_after_seven_days_expires_a_failed_blob() {
    let (stand, start) = (Stand::new(), now_millis());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    let database = stand.open_with(Some(&path), &EXPIRY_OPERATIONS, Some(TestClock::new(start))).await;
    stand.script.enqueue("CreateBlob", Answer::Status(422));
    let id = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap().optimistic_value.unwrap().id;
    until_state(&database, &id, WireSyncState::Failed).await;
    database.close_now();

    let reopened = stand.open_with(Some(&path), &EXPIRY_OPERATIONS, Some(TestClock::new(start + WEEK_MILLIS))).await;

    until_gone(&reopened, &id).await;
    reopened.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_blob_expires_when_the_network_returns_after_seven_days() {
    let (stand, start) = (Stand::new(), now_millis());
    let clock = TestClock::new(start);
    let database = stand.open_with(None, &EXPIRY_OPERATIONS, Some(clock.clone())).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    stand.script.enqueue("CreateBlob", Answer::Status(422));
    let id = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap().optimistic_value.unwrap().id;
    until_state(&database, &id, WireSyncState::Failed).await;
    database.set_online(false).unwrap();
    bound(crate::ffi::settled(&database)).await.unwrap();
    clock.advance(WEEK_MILLIS);

    database.set_online(true).unwrap();

    until_gone(&database, &id).await;
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_http_refusal_of_a_photo_keeps_the_failed_record_until_discard() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("photo.jpg");
    std::fs::write(&source, b"image").unwrap();
    stand.script.data("CreateBlob", BLOB);
    stand.script.enqueue("CreateMoment", Answer::Status(422));
    let epoch = database.session_epoch();

    let receipt = bound(database.send(clip("Moment", r#"{"text":"group"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = receipt.optimistic_value.clone().unwrap().id;
    assert!(matches!(bound(database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));

    assert_eq!(row(&database, &id).await.map(|row| row.sync.state), Some(WireSyncState::Failed));
    bound(database.discard(epoch, "Moment".into(), id.clone(), None)).await.unwrap();
    assert!(row(&database, &id).await.is_none());
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_complete_failure_then_retry_keeps_the_confirmed_parts() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    for _ in 0..3 {
        stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#);
    }
    stand.script.enqueue("CompleteMultipart", Answer::Status(422));
    stand.script.data("CompleteMultipart", r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#);
    stand.script.data("CreateMoment", ROW);
    let epoch = database.session_epoch();

    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    until_state(&database, &id, WireSyncState::Failed).await;
    let again = bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), None)).await.unwrap();
    let WireOperationOutcome::Confirmed { value, .. } = bound(database.outcome(again)).await.unwrap() else { panic!("the retry was not confirmed") };
    assert_eq!(value.map(|row| (row.id, row.sync.state)), Some(("m1".to_string(), WireSyncState::Synced)));

    let names = stand.script.names();
    let count = |name: &str| names.iter().filter(|seen| *seen == name).count();
    assert_eq!((count("InitMultipart"), count("PresignParts"), count("CompleteMultipart")), (1, 3, 2));
    let mut urls: Vec<String> = stand.puts.seen.lock().unwrap().iter().map(|put| put.url.clone()).collect();
    urls.sort();
    assert_eq!(urls, ["https://storage/1", "https://storage/2", "https://storage/3"]);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_discard_routes_its_reopened_abort_as_a_transfer() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&path), &operations).await;
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    let files = directory.path().join("db.sqlite.files/transfers");
    assert_eq!(std::fs::read_dir(&files).unwrap().count(), 1);
    database.set_online(false).unwrap();
    presign.dropped().await;

    bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();

    yield_until("the copy is gone", || std::fs::read_dir(&files).unwrap().count() == 0).await;
    assert!(source.exists());
    presign.release();
    database.close_now();
    let abort = stand.script.hold("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    let reopened: Arc<Database> = stand.open(Some(&path), &operations).await;
    reopened.set_online(false).unwrap();
    reopened.set_online(true).unwrap();
    abort.entered().await;
    abort.release();
    yield_until("the abort was answered", || stand.script.count("AbortMultipart") == 1).await;
    reopened.set_online(false).unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    reopened.set_online(true).unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.count("AbortMultipart"), 1);
    assert_eq!(stand.script.purposes("AbortMultipart"), [crate::transport::NetworkPurpose::Transfer]);
    reopened.close_now();
}
