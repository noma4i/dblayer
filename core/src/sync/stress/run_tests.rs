use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::sync::rig::{rig_on, specs};
use crate::sync::{OperationDef, SyncConfig};
use crate::transport::MockTransport;
use crate::upload::{UploadInput, UploadSteps};

use super::{run, runtime, scratch_dir, settle_uploads, shrink, FakeServer, Findings, StressConfig};

#[test]
fn finishing_a_run_keeps_another_runs_uploads() {
    let first = super::scratch_dir();
    let second = super::scratch_dir();
    let upload = second.path().join("upload.bin");
    std::fs::write(&upload, b"upload").unwrap();
    drop(first);
    assert_eq!(std::fs::read(&upload).unwrap(), b"upload");
}

#[test]
fn shrink_returns_the_smallest_operation_count_that_still_fails() {
    assert_eq!(shrink(500, |ops| ops >= 37, Duration::from_secs(5)), Some(37));
    assert_eq!(shrink(500, |ops| ops >= 500, Duration::from_secs(5)), Some(500));
    assert_eq!(shrink(500, |_| false, Duration::from_secs(5)), None);
}

#[test]
fn one_thread_and_one_seed_end_in_the_same_state() {
    let config = StressConfig { seed: 7, threads: 1, seconds: 60, ops: Some(150), trace: false };
    let first = run(&config);
    let second = run(&config);
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.ops, [150]);
}

#[test]
fn another_seed_ends_in_another_state() {
    let first = run(&StressConfig { seed: 7, threads: 1, seconds: 60, ops: Some(150), trace: false });
    let other = run(&StressConfig { seed: 8, threads: 1, seconds: 60, ops: Some(150), trace: false });
    assert_ne!(first.digest, other.digest);
}

#[test]
fn settling_uploads_waits_for_an_upload_started_offline_and_released_online() {
    runtime(1).block_on(async {
        let transport = MockTransport::new();
        let server = FakeServer::install(&transport, 1, Findings::default());
        server.set_calm(true);
        let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig::default());
        rig.sync.register(OperationDef::new("CreateBlob", "operation CreateBlob { x }", "createBlob"));
        server.set_offline(true);
        rig.sync.set_online(false);
        let row = rig.db.create("moments", json!({"text": "w0:1"}).as_object().unwrap().clone()).unwrap();
        let scratch = scratch_dir();
        let path = scratch.path().join("upload.bin");
        std::fs::write(&path, b"abcdef").unwrap();
        let input = UploadInput::new(&path.to_string_lossy(), UploadSteps::new("CreateMoment").single("CreateBlob")).variable("text", json!("w0:2"));
        rig.sync.upload("moments", &row.id, "upload", input).await.unwrap();
        server.set_offline(false);
        server.hold_blob_replies(std::time::Duration::from_secs(10));
        rig.sync.set_online(true);
        settle_uploads(&rig.sync).await;
        assert!(rig.db.outbox().unwrap().iter().all(|entry| entry.operation.upload.is_none()));
        assert_eq!(*rig.sync.inner.file_tasks.borrow(), 0);
    });
}
