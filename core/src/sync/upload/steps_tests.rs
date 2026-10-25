use super::*;
use std::path::Path;
use std::time::Duration;
use serde_json::json;
use crate::db::Db;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use crate::transport::{MockTransport, PutReply, Reply};
use crate::upload::{UploadConfig, UploadFile, UploadOperations, UploadStatus};
use super::super::{OperationDef, SyncConfig};
use super::super::rig::{Rig, rig_without_operations, put, await_outbox};
use super::upload_tests::{await_released, file, rid, small};

fn reopen(path: &Path, budget: u32) -> Rig {
    let mut model = ModelSpec::new("assets", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut action = crate::sync::rig::upload_declared("upload");
    action.mutation = Some("Publish".into());
    model.actions.push(action);
    let config = UploadConfig { backoff: small().backoff.limited(budget), parallel_parts: 3, ..small() };
    let rig = rig_without_operations(Db::open(path, vec![model], None).unwrap(), MockTransport::new(), SyncConfig { upload: config, uploads: Some(UploadOperations::empty().create_blob("Grant").multipart("Start", "Sign", "Finish", "Abort")), ..SyncConfig::default() });
    for (name, root) in [("Grant", "grant"), ("Start", "start"), ("Sign", "sign"), ("Finish", "finish"), ("Abort", "abort")] {
        rig.sync.register(OperationDef::new(name, "mutation { result }", root));
    }
    rig
}

fn source_input(source: &tempfile::NamedTempFile) -> UploadFile {
    UploadFile::new(source.path().to_str().unwrap())
}

fn grants(rig: &Rig) {
    rig.transport.enqueue("Grant", Reply::data(json!({"grant": {"url": "https://storage/file", "signedId": "signed", "blobId": "blob"}})));
    rig.transport.enqueue("Start", Reply::data(json!({"start": {"key": "key", "uploadId": "upload"}})));
    rig.transport.enqueue("Sign", Reply::data(json!({"sign": {"parts": [{"partNumber": 1, "url": "https://storage/file"}, {"partNumber": 2, "url": "https://storage/second"}, {"partNumber": 3, "url": "https://storage/third"}]}})));
    rig.transport.enqueue("Abort", Reply::data(json!({"abort": {"ok": true}})));
}

async fn shutdown(rig: Rig) {
    let database = Arc::downgrade(&rig.db.shared);
    let mut tasks = std::mem::take(&mut locked(&rig.sync.inner.uploads).tasks);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    drop(rig);
    await_released(&database).await;
    assert!(database.upgrade().is_none(), "the database remained open");
}

#[tokio::test(start_paused = true)]
async fn restart_preserves_each_put_budget_and_deadline() {
    let mut observed = Vec::new();
    for multipart in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.sqlite");
        let source = file(if multipart { b"0123456789ab" } else { b"abcdef" });
        let rig = reopen(&path, 1);
        put(&rig, "assets", json!({"id": "a"}));
        grants(&rig);
        rig.transport.enqueue_put("https://storage/file", PutReply::status(503));
        let mut outbox = rig.db.outbox_changes();
        rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
        await_outbox(&mut outbox, || rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap().attempts == 1).await;
        assert_eq!(rig.transport.put_count("https://storage/file"), 1);
        shutdown(rig).await;
        let rig = reopen(&path, 1);
        grants(&rig);
        for _ in 0..2 { rig.transport.enqueue_put("https://storage/file", PutReply::status(503)); }
        rig.sync.inner.start_uploads().unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(rig.transport.put_calls().is_empty(), "restart skipped saved deadline");
        rig.sync.set_online(false);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(rig.transport.put_calls().is_empty(), "offline during the saved wait sent a PUT");
        rig.sync.set_online(true);
        rig.sync.wait_uploads().await;
        observed.push((multipart, rig.transport.put_calls().iter().filter(|call| call.url == "https://storage/file").count()));
        assert_eq!(rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap().status, UploadStatus::Failed);
        shutdown(rig).await;
    }
    assert_eq!(observed, [(false, 1), (true, 1)], "restart granted another retry");
}

fn retry_rows(rig: &Rig) -> u32 {
    rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT count(*) FROM op_item_retry", [], |row| row.get(0))?)).unwrap()
}

#[tokio::test(start_paused = true)]
async fn restart_preserves_each_graphql_step_budget() {
    let mut observed = Vec::new();
    for operation in ["Grant", "Start", "Sign", "Finish"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retry.sqlite");
        let source = file(if operation == "Grant" { b"abcdef" } else { b"0123456789ab" });
        let rig = reopen(&path, 1);
        put(&rig, "assets", json!({"id": "a"}));
        rig.transport.enqueue(operation, Reply::fail(crate::transport::TransportError::Network("unavailable".into())));
        grants(&rig);
        let mut outbox = rig.db.outbox_changes();
        rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
        await_outbox(&mut outbox, || retry_rows(&rig) == 1).await;
        assert_eq!(rig.transport.call_count(operation), 1);
        shutdown(rig).await;
        let rig = reopen(&path, 1);
        for _ in 0..2 { rig.transport.enqueue(operation, Reply::fail(crate::transport::TransportError::Network("unavailable".into()))); }
        grants(&rig);
        rig.sync.inner.start_uploads().unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(rig.transport.call_count(operation), 0, "{operation} skipped its deadline");
        rig.sync.wait_uploads().await;
        observed.push((operation, rig.transport.call_count(operation)));
        assert_eq!(rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap().status, UploadStatus::Failed);
        shutdown(rig).await;
    }
    assert_eq!(observed, [("Grant", 1), ("Start", 1), ("Sign", 1), ("Finish", 1)]);
}

#[tokio::test(start_paused = true)]
async fn concurrent_parts_keep_independent_retry_records() {
    let directory = tempfile::tempdir().unwrap();
    let rig = reopen(&directory.path().join("retry.sqlite"), 1);
    let source = file(b"0123456789ab");
    put(&rig, "assets", json!({"id": "a"}));
    grants(&rig);
    for url in ["https://storage/file", "https://storage/second", "https://storage/third"] {
        rig.transport.enqueue_put(url, PutReply::status(503));
        rig.transport.enqueue_put(url, PutReply::etag("etag"));
    }
    rig.transport.enqueue("Finish", Reply::data(json!({"finish": {"signedId": "signed", "blobId": "blob"}})));
    rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 6);
    assert!(rig.db.outbox().unwrap()[0].operation.upload.as_ref().unwrap().is_transferred());
    assert_eq!(retry_rows(&rig), 0);
    shutdown(rig).await;
}

#[tokio::test(start_paused = true)]
async fn manual_retry_refreshes_the_budget_and_success_removes_its_record() {
    let directory = tempfile::tempdir().unwrap();
    let rig = reopen(&directory.path().join("retry.sqlite"), 1);
    let source = file(b"abcdef");
    put(&rig, "assets", json!({"id": "a"}));
    grants(&rig);
    for _ in 0..2 { rig.transport.enqueue_put("https://storage/file", PutReply::status(503)); }
    rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(retry_rows(&rig), 1);
    assert_eq!(rig.db.retry("assets", &rid("a"), None).unwrap(), 1);
    assert_eq!(retry_rows(&rig), 0, "manual retry retained the exhausted budget");
    rig.transport.enqueue_put("https://storage/file", PutReply::etag("etag"));
    rig.sync.inner.start_uploads().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 4);
    assert!(rig.db.outbox().unwrap()[0].operation.upload.as_ref().unwrap().is_transferred());
    assert_eq!(retry_rows(&rig), 0, "successful PUT retained its retry record");
    assert_eq!(rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap().attempts, 0);
    shutdown(rig).await;
}

#[tokio::test(start_paused = true)]
async fn terminal_operations_delete_retry_records() {
    use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
    for action in ["close", "discard", "cancel"] {
        let directory = tempfile::tempdir().unwrap();
        let rig = reopen(&directory.path().join("retry.sqlite"), 1);
        let source = file(b"abcdef");
        put(&rig, "assets", json!({"id": "a"}));
        grants(&rig);
        for _ in 0..2 { rig.transport.enqueue_put("https://storage/file", PutReply::status(503)); }
        let seq = rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
        rig.sync.wait_uploads().await;
        assert_eq!(retry_rows(&rig), 1);
        match action {
            "close" => {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
                envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::Committed });
                rig.db.land(envelope).unwrap();
            }
            "discard" => { rig.db.discard("assets", &rid("a"), None).unwrap(); }
            _ => { rig.sync.cancel_upload("assets", &rid("a"), "upload").await.unwrap(); }
        }
        assert!(rig.db.outbox().unwrap().is_empty());
        assert_eq!(retry_rows(&rig), 0, "{action} left retry records");
        shutdown(rig).await;
    }
}

#[tokio::test(start_paused = true)]
async fn alternating_grant_failures_and_put_expiry_share_only_the_put_budget() {
    let directory = tempfile::tempdir().unwrap();
    let rig = reopen(&directory.path().join("retry.sqlite"), 1);
    let source = file(b"abcdef");
    put(&rig, "assets", json!({"id": "a"}));
    for _ in 0..2 {
        rig.transport.enqueue("Grant", Reply::fail(crate::transport::TransportError::Network("offline".into())));
        grants(&rig);
    }
    rig.transport.enqueue_put("https://storage/file", PutReply::status(403));
    rig.transport.enqueue_put("https://storage/file", PutReply::status(503));
    rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("Grant"), 4);
    assert_eq!(rig.transport.put_calls().len(), 2, "url renewal reset the PUT budget");
    let info = rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap();
    assert_eq!((info.status, info.attempts), (UploadStatus::Failed, 1));
    assert_eq!(retry_rows(&rig), 1);
    shutdown(rig).await;
}

#[tokio::test(start_paused = true)]
async fn fifty_retries_exhaust_the_step_budget() {
    let directory = tempfile::tempdir().unwrap();
    let rig = reopen(&directory.path().join("retry.sqlite"), UploadConfig::default().backoff.max_attempts);
    let source = file(b"abcdef");
    put(&rig, "assets", json!({"id": "a"}));
    grants(&rig);
    for _ in 0..52 { rig.transport.enqueue_put("https://storage/file", PutReply::status(503)); }
    rig.sync.upload("assets", &rid("a"), "upload", source_input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 51);
    let info = rig.db.sync_info("assets", &rid("a")).unwrap().uploads.get("upload").cloned().unwrap();
    assert_eq!((info.status, info.attempts), (UploadStatus::Failed, 50));
    shutdown(rig).await;
}
