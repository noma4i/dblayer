use std::sync::{Arc, OnceLock};

use serde_json::json;

use super::batch_tests::spec;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::sync::{OperationDef, Sync, SyncConfig};
use crate::transport::{MockTransport, Reply, TransportError};
use crate::upload::{UploadBatchInput, UploadItem, UploadOperations};
use crate::test_wait::spin_until;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_wakes_when_an_item_fails_after_settled_parked() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    sync.register(OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    let observer = Arc::new(OnceLock::<Sync>::new());
    let watched = observer.clone();
    transport.respond_with(move |request| {
        if request.operation_name != "CreateBlob" { return None; }
        spin_until("the settle call parked", || watched.get().is_some_and(|sync| sync.counters().settle_parks > 0));
        Some(Reply::fail(TransportError::Http { status: 422, body: String::new() }))
    });
    observer.set(sync.clone()).ok();
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().iter().map(|row| row.id.clone()).collect();
    sync.set_online(true);
    let waiter = { let sync = sync.clone(); let id = ids[0].clone(); tokio::spawn(async move { sync.settled(&sync.inner.db.admit().unwrap(), "Asset", &id, None).await }) };
    let settled = tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await;
    assert!(matches!(settled, Ok(Ok(Err(_)))), "settled must wake with the failure: {settled:?}");
}
