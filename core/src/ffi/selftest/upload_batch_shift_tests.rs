use super::rig_tests::bound;
use super::upload_stand::Stand;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem};
use crate::ffi::{WireOperationOutcome, WireOperationReceipt};
use crate::test_wait::yield_until;

pub(super) const OPERATIONS: [(&str, &str); 3] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("CreateAssets", "mutation CreateAssets { createAssets { assets { id clientKey size position } } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];
pub(super) const ASSETS: &str = r#"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a1","clientKey":"key-1","size":2,"position":0}]}}}"#;

pub(super) fn blob(index: usize) -> String { format!(r#"{{"data":{{"blobCreate":{{"url":"https://storage/file","headers":{{}},"signedId":"signed-{index}","blobId":"b{index}"}}}}}}"#) }

pub(super) fn items(directory: &std::path::Path) -> Vec<WireUploadItem> {
    (0..3).map(|index| {
        let path = directory.join(format!("{index}.jpg"));
        std::fs::write(&path, format!("image-{index}")).unwrap();
        WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), path: path.to_str().unwrap().into(), name: None, mime: None, variables_json: "{}".into() }
    }).collect()
}

async fn confirmed(database: &crate::ffi::Database, receipt: &WireOperationReceipt) -> Option<String> {
    match bound(database.outcome(receipt.clone())).await.unwrap() {
        WireOperationOutcome::Confirmed { value, .. } => value.map(|row| row.id),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn discarding_the_first_member_of_a_batch_during_a_held_request_keeps_the_others() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    stand.script.data("CreateBlob", &blob(0));
    let held = stand.script.hold("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", ASSETS);
    let epoch = database.session_epoch();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items(directory.path()), WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    held.entered().await;

    bound(database.discard(epoch, "Asset".into(), receipt.items[0].optimistic_value.id.clone(), Some("create_files".into()))).await.unwrap();
    held.release();

    let mut ids = Vec::new();
    for item in &receipt.items { ids.push(confirmed(&database, &item.receipt).await); }
    assert_eq!(ids, [None, Some("a1".to_string()), Some("a2".to_string())]);
    let sent = stand.script.variables("CreateAssets")[0]["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap().to_string()).collect::<Vec<_>>();
    assert_eq!(sent, ["key-1", "key-2"]);
    let puts: Vec<String> = stand.puts.seen.lock().unwrap().iter().map(|put| put.item.clone()).collect();
    assert_eq!(puts.iter().filter(|item| *item == "key-1").count() + puts.iter().filter(|item| *item == "key-2").count(), 2, "{puts:?}");
    yield_until("nothing else is in flight", || stand.script.count("CreateBlob") == 3).await;
    database.close_now();
}
