use std::path::Path;

use serde_json::Value;

use super::rig_tests::bound;
use super::upload_stand::{shows, watched, Answer, Stand};
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem};
use crate::ffi::wire::WireSyncState;
use crate::ffi::{Database, DbError, TestClock, WireBatchReceipt, WireOperationReceipt};
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::settled_showing;
use crate::transport::{NetworkPurpose, TransferStep};

const ALBUM_OPERATIONS: [(&str, &str); 3] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("CreateAssets", "mutation CreateAssets { createAssets { assets { id clientKey size position } } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];
const REFUSED_BLOB: &str = r#"{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"#;

#[tokio::test(flavor = "multi_thread")]
async fn xfer_confirmed_group_member_exact_cycle() {
    let mut observed = Vec::new();
    for automatic in [false, true] {
        let stand = Stand::new();
        let database = stand.open(None, &ALBUM_OPERATIONS).await;
        let directory = tempfile::tempdir().unwrap();
        let files = album_files(directory.path(), &["0.jpg", "1.jpg"], "image");
        stand.script.data("CreateBlob", REFUSED_BLOB);
        stand.script.data("CreateBlob", &blob(1));
        stand.script.data("CreateAsset", r#"{"data":{"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2,"position":1}}}}"#);
        let epoch = database.session_epoch();
        let receipt = upload_batch(&database, items(&[&files[0], &files[1]], &["key-0", "key-1"], 2)).await;
        let first = receipt.items[0].optimistic_value.id.clone();
        let rows = watched(&database, "Asset", &first);
        settled_showing(&database, "the refused file failed", || state_report(&stand, &database, &[first.as_str()]), || shows(&rows, WireSyncState::Failed)).await;
        if automatic {
            assert!(matches!(bound(database.confirmed(receipt.items[0].receipt.clone(), true)).await, Err(DbError::Rejected { kept: false, .. })));
        } else {
            bound(database.discard(epoch, "Asset".into(), first, Some("create_files".into()))).await.unwrap();
        }
        observed.push((outcomes(&database, &receipt).await, stand.script.count("CreateBlob"), stand.script.count("CreateAssets"), stand.script.variables("CreateAsset")));
        bound(database.close_database()).await.unwrap();
    }
    assert_eq!(observed[0], observed[1], "confirmed discards the same group member as explicit discard");
}

fn blob(index: usize) -> String { format!(r#"{{"data":{{"blobCreate":{{"url":"https://storage/file","headers":{{}},"signedId":"signed-{index}","blobId":"b{index}"}}}}}}"#) }

fn assets(rows: &[(&str, &str, usize)]) -> String {
    let listed: Vec<String> = rows.iter().map(|(id, key, position)| format!(r#"{{"id":"{id}","clientKey":"{key}","size":2,"position":{position}}}"#)).collect();
    format!(r#"{{"data":{{"createAssets":{{"assets":[{}]}}}}}}"#, listed.join(","))
}

fn items(paths: &[&Path], keys: &[&str], size: u32) -> Vec<WireUploadItem> {
    paths.iter().zip(keys).enumerate().map(|(position, (path, key))| WireUploadItem { values_json: format!(r#"{{"clientKey":"{key}","size":{size},"position":{position}}}"#), source: crate::ffi::upload_wire::WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None }, variables_json: "{}".into() }).collect()
}

fn batch() -> WireUploadBatch { WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() } }

async fn upload_batch(database: &Database, items: Vec<WireUploadItem>) -> WireBatchReceipt { bound(database.upload_create_batch(database.session_epoch(), "Asset".into(), items, batch())).await.unwrap() }

async fn outcome_of(database: &Database, receipt: &WireOperationReceipt) -> String {
    match bound(database.confirmed(receipt.clone(), false)).await {
        Ok(confirmed) => format!("confirmed {}", confirmed.value.map(|row| row.id).unwrap_or_default()),
        Err(DbError::Rejected { failure, .. }) => format!("rejected {}", failure.message),
        Err(DbError::NoSession { .. } | DbError::Interrupted { .. }) => "interrupted".into(),
        Err(error) => panic!("unexpected receipt error: {error:?}"),
    }
}

async fn outcomes(database: &Database, receipt: &WireBatchReceipt) -> Vec<String> {
    let mut seen = Vec::new();
    for item in &receipt.items { seen.push(outcome_of(database, &item.receipt).await); }
    seen
}

fn album_files(directory: &Path, names: &[&str], body: &str) -> Vec<std::path::PathBuf> {
    names.iter().map(|name| { let path = directory.join(name); std::fs::write(&path, body).unwrap(); path }).collect()
}

fn sent_items(stand: &Stand) -> Vec<Value> { stand.script.variables("CreateAssets")[0]["items"].as_array().unwrap().clone() }

fn state_report(stand: &Stand, database: &Database, ids: &[&str]) -> String {
    let calls: Vec<String> = stand.script.names().into_iter().enumerate().map(|(index, name)| format!("{index}:{name}")).collect();
    let per_operation: Vec<String> = ["CreateBlob", "CreateAssets", "CreateAsset"].iter().map(|name| format!("{name}={}", stand.script.count(name))).collect();
    let files: Vec<Value> = stand.script.variables("CreateBlob").iter().map(|variables| variables["filename"].clone()).collect();
    let puts = stand.puts.seen.lock().unwrap().clone();
    let rows: Vec<String> = ids.iter().map(|id| match database.peek_row(database.session_epoch(), "Asset".into(), (*id).into()) {
        Ok(Some(row)) => format!("{id}: sync={:?} uploads={:?}", row.sync, row.uploads),
        other => format!("{id}: {other:?}"),
    }).collect();
    format!("\ncalls {calls:?}\nrequests {per_operation:?} blob files {files:?}\nputs {puts:?}\nrows {rows:#?}\nopen_report {:?}", database.open_report())
}

fn keys_of(sent: &[Value]) -> Vec<String> { sent.iter().map(|item| item["clientKey"].as_str().unwrap().to_string()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_owns_its_sources_and_confirms_every_member_with_one_request() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("image.png");
    std::fs::write(&source, b"image").unwrap();

    let receipt = upload_batch(&database, items(&[&source, &source], &["key-0", "key-1"], 2)).await;

    let rows: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.clone()).collect();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.id.starts_with("temp-") && row.sync.state == WireSyncState::Sending && !row.uploads.is_empty()), "{rows:?}");
    std::fs::remove_file(&source).unwrap();
    for _ in 0..2 { stand.script.data("CreateBlob", &blob(0).replace("signed-0", "signed")); }
    stand.script.data("CreateAssets", &assets(&[("a1", "key-1", 1), ("a0", "key-0", 0)]));
    database.set_online(true).unwrap();

    assert_eq!(outcomes(&database, &receipt).await, ["confirmed a0", "confirmed a1"]);
    assert_eq!(stand.script.count("CreateAssets"), 1);
    let sent = sent_items(&stand);
    assert_eq!(keys_of(&sent), ["key-0", "key-1"]);
    assert!(sent.iter().all(|item| item["signedId"] == "signed"));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_member_that_landed_before_the_accepted_row_is_read_gives_the_landed_row() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("image.png");
    std::fs::write(&source, b"image").unwrap();
    let receipt = upload_batch(&database, items(&[&source, &source], &["key-0", "key-1"], 2)).await;
    for _ in 0..2 { stand.script.data("CreateBlob", &blob(0).replace("signed-0", "signed")); }
    stand.script.data("CreateAssets", &assets(&[("a0", "key-0", 0), ("a1", "key-1", 1)]));
    database.set_online(true).unwrap();
    assert_eq!(outcomes(&database, &receipt).await, ["confirmed a0", "confirmed a1"]);

    let landed = bound(database.confirmed(receipt.items[0].receipt.clone(), false)).await.unwrap().value.expect("the item is confirmed with its row");
    assert_eq!((landed.id, landed.sync.state), ("a0".to_string(), WireSyncState::Synced));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multipart_batch_accepts_a_correlation_key_with_path_characters() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    for _ in 0..2 {
        stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
        for _ in 0..3 { stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#); }
        stand.script.data("CompleteMultipart", r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#);
    }
    stand.script.data("CreateAssets", &assets(&[("a0", "../../../outside", 0), ("a1", "other", 1)]));
    let path = directory.path().join("db.sqlite");
    let database = stand.open(Some(&path), &ALBUM_OPERATIONS).await;

    let receipt = upload_batch(&database, items(&[&source, &source], &["../../../outside", "other"], 2)).await;

    assert_eq!(outcomes(&database, &receipt).await, ["confirmed a0", "confirmed a1"]);
    let puts = stand.puts.seen.lock().unwrap().clone();
    let outside: Vec<_> = puts.iter().filter(|put| put.item == "../../../outside").collect();
    assert_eq!(outside.len(), 3, "{puts:?}");
    let files = directory.path().join("db.sqlite.files");
    assert!(outside.iter().all(|put| Path::new(&put.path).starts_with(&files)), "{outside:?}");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn discarding_a_refused_file_sends_the_remaining_file_as_single() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let files = album_files(directory.path(), &["0.jpg", "1.jpg"], "image");
    stand.script.data("CreateBlob", REFUSED_BLOB);
    stand.script.data("CreateBlob", &blob(1));
    stand.script.data("CreateAsset", r#"{"data":{"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2,"position":1}}}}"#);
    let epoch = database.session_epoch();

    let receipt = upload_batch(&database, items(&[&files[0], &files[1]], &["key-0", "key-1"], 2)).await;
    let first = receipt.items[0].optimistic_value.id.clone();
    let rows = watched(&database, "Asset", &first);
    settled_showing(&database, "the refused file failed", || state_report(&stand, &database, &[first.as_str()]), || shows(&rows, WireSyncState::Failed)).await;
    bound(database.discard(epoch, "Asset".into(), first, Some("create_files".into()))).await.unwrap();

    assert_eq!(outcomes(&database, &receipt).await, ["rejected no storage", "confirmed a1"]);
    assert_eq!(stand.script.count("CreateBlob"), 2);
    assert_eq!(stand.script.count("CreateAssets"), 0);
    assert_eq!(stand.script.variables("CreateAsset")[0]["clientKey"], "key-1");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn retrying_a_refused_file_keeps_the_other_album_files() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let files = album_files(directory.path(), &["0.jpg", "1.jpg", "2.jpg"], "image");
    stand.script.data("CreateBlob", REFUSED_BLOB);
    for index in 1..4 { stand.script.data("CreateBlob", &blob(index)); }
    stand.script.data("CreateAssets", &assets(&[("a2", "key-2", 2), ("a1", "key-1", 1), ("a0", "key-0", 0)]));
    let epoch = database.session_epoch();

    let receipt = upload_batch(&database, items(&[&files[0], &files[1], &files[2]], &["key-0", "key-1", "key-2"], 3)).await;
    let first = receipt.items[0].optimistic_value.id.clone();
    let rows = watched(&database, "Asset", &first);
    settled_showing(&database, "the refused file failed", || state_report(&stand, &database, &[first.as_str()]), || shows(&rows, WireSyncState::Failed)).await;
    let again = bound(database.retry(epoch, "Asset".into(), first, Some("create_files".into()), None)).await.unwrap();

    assert_eq!(again.operation, receipt.items[0].receipt.operation);
    assert_eq!(outcome_of(&database, &again).await, "confirmed a0");
    assert_eq!(&outcomes(&database, &receipt).await[1..], ["confirmed a1", "confirmed a2"]);
    assert_eq!(keys_of(&sent_items(&stand)), ["key-0", "key-1", "key-2"]);
    assert_eq!(stand.script.count("CreateBlob"), 4);
    let mut names: Vec<String> = stand.script.variables("CreateBlob").iter().map(|variables| variables["filename"].as_str().unwrap().to_string()).collect();
    names.sort();
    assert_eq!(names, ["0.jpg", "0.jpg", "1.jpg", "2.jpg"]);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_middle_file_keeps_the_other_files_in_order() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let files = album_files(directory.path(), &["0.jpg", "1.jpg", "2.jpg"], "image");
    let first = stand.script.hold("CreateBlob", &blob(0));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", &assets(&[("a2", "key-2", 1), ("a0", "key-0", 0)]));
    let epoch = database.session_epoch();

    let receipt = upload_batch(&database, items(&[&files[0], &files[1], &files[2]], &["key-0", "key-1", "key-2"], 3)).await;
    first.entered().await;
    let middle = receipt.items[1].optimistic_value.id.clone();
    assert!(bound(database.cancel_upload(epoch, "Asset".into(), middle, "create_files".into())).await.unwrap());
    first.release();

    assert_eq!(outcomes(&database, &receipt).await, ["confirmed a0", "interrupted", "confirmed a2"]);
    assert_eq!(keys_of(&sent_items(&stand)), ["key-0", "key-2"]);
    assert_eq!(stand.script.count("CreateBlob"), 2);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn shifting_a_batch_during_a_held_request_keeps_the_file_owner() {
    let stand = Stand::new();
    let database = stand.open(None, &ALBUM_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let files = album_files(directory.path(), &["0.jpg", "1.jpg", "2.jpg"], "image-x");
    stand.script.data("CreateBlob", &blob(0));
    let held = stand.script.hold("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", &assets(&[("a2", "key-2", 1), ("a1", "key-1", 0)]));
    let epoch = database.session_epoch();

    let receipt = upload_batch(&database, items(&[&files[0], &files[1], &files[2]], &["key-0", "key-1", "key-2"], 3)).await;
    held.entered().await;
    bound(database.discard(epoch, "Asset".into(), receipt.items[0].optimistic_value.id.clone(), Some("create_files".into()))).await.unwrap();
    held.release();

    assert_eq!(outcomes(&database, &receipt).await, ["interrupted", "confirmed a1", "confirmed a2"]);
    let puts = stand.puts.seen.lock().unwrap().clone();
    let (second, third) = (puts.iter().find(|put| put.item == "key-1").expect("the second file"), puts.iter().find(|put| put.item == "key-2").expect("the third file"));
    assert_eq!(second.operation, third.operation);
    assert_eq!((&second.step, &third.step), (&TransferStep::Put, &TransferStep::Put));
    let sent = sent_items(&stand);
    assert_eq!(keys_of(&sent), ["key-1", "key-2"]);
    assert_eq!(sent.iter().map(|item| item["signedId"].as_str().unwrap()).collect::<Vec<_>>(), ["signed-1", "signed-2"]);
    assert!(stand.script.purposes("CreateBlob").iter().all(|purpose| *purpose == NetworkPurpose::Transfer));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_answer_before_a_close_is_replayed_after_the_reopen_and_the_batch_completes() {
    let (stand, now) = (Stand::new(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let database = stand.open_with(Some(&path), &ALBUM_OPERATIONS, Some(TestClock::new(now))).await;
    let files = album_files(directory.path(), &["0.jpg", "1.jpg", "2.jpg"], "image-x");
    stand.script.data("CreateBlob", &blob(0));
    let held = std::sync::Arc::new(super::upload_stand::Hold::default());
    stand.script.enqueue("CreateBlob", Answer::Held(held.clone(), Box::new(Answer::Lost)));
    let epoch = database.session_epoch();
    let receipt = upload_batch(&database, items(&[&files[0], &files[1], &files[2]], &["key-0", "key-1", "key-2"], 3)).await;
    held.entered().await;
    bound(database.discard(epoch, "Asset".into(), receipt.items[0].optimistic_value.id.clone(), Some("create_files".into()))).await.unwrap();
    database.set_online(false).unwrap();
    held.release();
    held.dropped().await;
    database.close_now();
    stand.script.data("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", &assets(&[("a2", "key-2", 1), ("a1", "key-1", 0)]));

    let reopened = stand.open_with(Some(&path), &ALBUM_OPERATIONS, Some(TestClock::new(now))).await;

    let (first, second) = (watched(&reopened, "Asset", "a1"), watched(&reopened, "Asset", "a2"));
    let optimistic: Vec<&str> = receipt.items.iter().map(|item| item.optimistic_value.id.as_str()).chain(["a1", "a2"]).collect();
    settled_showing(&reopened, "both assets are stored", || state_report(&stand, &reopened, &optimistic), || !first.0.lock().unwrap().is_empty() && !second.0.lock().unwrap().is_empty()).await;
    let attempts = stand.script.variables("CreateBlob").iter().filter(|variables| variables["filename"] == "1.jpg").count();
    assert_eq!(attempts, 2, "the lost step is requested once more");
    let filenames: Vec<String> = stand.script.variables("CreateBlob").iter().map(|variables| variables["filename"].as_str().unwrap().to_string()).collect();
    assert_eq!(filenames, ["0.jpg", "1.jpg", "1.jpg", "2.jpg"], "calls: {:?}", stand.script.names());
    assert_eq!(stand.script.count("CreateAssets"), 1);
    assert_eq!(keys_of(&sent_items(&stand)), ["key-1", "key-2"]);
    reopened.close_now();
}
