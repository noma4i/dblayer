use std::sync::Arc;

use serde_json::json;

use super::batch_tests::spec;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::sync::{Sync, SyncConfig};
use crate::transport::MockTransport;
use crate::upload::{UploadBatchInput, UploadItem, UploadSteps};

#[tokio::test]
async fn upload_runs_commit_their_rows_after_the_batch_call_returned() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"file").unwrap();
    let items = (0..3).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":4}).as_object().unwrap().clone(),
        path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let rows = sync.upload_create_batch("Asset", items, request).await.unwrap();
    let created: Vec<_> = rows.iter().map(|row| format!("{:?}", db.find("Asset", &row.id).unwrap().unwrap().sync.upload)).collect();
    let start = db.revision_seq().unwrap();
    while db.revision_seq().unwrap() == start {
        tokio::task::yield_now().await;
    }
    let committed: Vec<_> = rows.iter().map(|row| format!("{:?}", db.find("Asset", &row.id).unwrap().unwrap().sync.upload)).collect();
    assert!(committed.iter().zip(&created).any(|(after, before)| after != before));
}
