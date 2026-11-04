use std::sync::Arc;

use serde_json::json;

use super::batch_tests::spec;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::sync::{OperationDef, Sync, SyncConfig};
use crate::transport::{MockTransport, Reply, TransportError};
use crate::upload::{UploadBatchInput, UploadItem, UploadOperations};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_receipt_ends_with_the_refusal_when_every_item_fails() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    sync.register(OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    transport.respond_with(move |request| {
        if request.operation_name != "CreateBlob" { return None; }
        Some(Reply::fail(TransportError::Http { status: 422, body: String::new() }))
    });
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let receipt = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap();
    sync.set_online(true);
    let waiter = tokio::spawn(async move { receipt.outcome().await });
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await;
    assert!(matches!(&outcome, Ok(Ok(Ok(outcome))) if matches!(outcome.result, crate::sync::SendResult::Rejected(_))), "the receipt must wake with the failure: {outcome:?}");
}
