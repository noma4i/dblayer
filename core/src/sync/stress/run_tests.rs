use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::sync::rig::{await_outbox, moment, put, rig_on, specs};
use crate::sync::{OperationDef, SyncConfig};
use crate::transport::MockTransport;
use crate::upload::{UploadFile, UploadOperations, UploadStatus};

use super::{check, run, runtime, scratch_dir, settle_uploads, shrink, FakeServer, Findings, StressConfig};
use crate::test_wait::{recv_or_closed, recv_within};

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
        let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        rig.sync.register(OperationDef::new("CreateBlob", "operation CreateBlob { x }", "createBlob"));
        server.set_offline(true);
        rig.sync.set_online(false);
        let row = rig.db.create("moments", json!({"text": "w0:1"}).as_object().unwrap().clone()).unwrap();
        let scratch = scratch_dir();
        let path = scratch.path().join("upload.bin");
        std::fs::write(&path, b"abcdef").unwrap();
        let input = UploadFile::new(&path.to_string_lossy()).variable("text", json!("w0:2"));
        rig.sync.upload("moments", &row.id, "upload", input).await.unwrap();
        server.set_offline(false);
        server.hold_blob_replies(std::time::Duration::from_secs(10));
        rig.sync.set_online(true);
        settle_uploads(&rig.sync).await;
        assert!(rig.db.outbox().unwrap().iter().all(|entry| entry.operation.upload.is_none()));
        assert_eq!(*rig.sync.inner.file_tasks.borrow(), 0);
    });
}

#[derive(Clone, Copy)]
enum Stage {
    PreparationHeld,
    Parked,
}

#[derive(Clone, Copy)]
enum Cancel {
    Discard,
    Reset,
}

fn outcome_of_cancelling(stage: Stage, cancel: Cancel) -> String {
    let runtime = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_time().start_paused(true).build().unwrap();
    runtime.block_on(async {
        let transport = MockTransport::new();
        let server = FakeServer::install(&transport, 1, Findings::default());
        server.set_calm(true);
        let findings = Findings::default();
        let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        rig.sync.register(OperationDef::new("CreateBlob", "operation CreateBlob { x }", "createBlob"));
        server.set_offline(true);
        rig.sync.set_online(false);
        put(&rig, "moments", moment("m1", "w0:1"));
        let row = rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap();
        let scratch = scratch_dir();
        let path = scratch.path().join("upload.bin");
        std::fs::write(&path, b"abcdef").unwrap();
        let input = UploadFile::new(&path.to_string_lossy()).variable("text", json!("w0:2"));
        rig.sync.upload("moments", &row.id, "upload", input).await.unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let mut release_tx = Some(release_tx);
        let mut occupied = Some(tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            let _ = recv_or_closed(&release_rx);
        }));
        recv_within(&entered_rx);
        let mut changes = rig.db.outbox_changes();
        let status = |wanted: UploadStatus| {
            let rig = &rig;
            let id = row.id.clone();
            move || rig.db.sync_info("moments", &id).unwrap().uploads.get("upload").is_some_and(|upload| upload.status == wanted)
        };
        await_outbox(&mut changes, status(UploadStatus::Preparing)).await;
        if matches!(stage, Stage::Parked) {
            drop(release_tx.take());
            occupied.take().unwrap().await.unwrap();
            await_outbox(&mut changes, status(UploadStatus::Uploading)).await;
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            tokio::task::yield_now().await;
        }
        match cancel {
            Cancel::Discard => {
                rig.db.discard("moments", &row.id, None).unwrap();
            }
            Cancel::Reset => rig.db.reset(&["moments"]).unwrap(),
        }
        if let Some(held) = occupied.take() {
            drop(release_tx.take());
            held.await.unwrap();
        }
        server.set_offline(false);
        rig.sync.set_online(true);
        settle_uploads(&rig.sync).await;
        check::queue(&rig.db, &findings);
        check::lists(&rig.db, &findings);
        let rows: Vec<String> = rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().map(|row| row.text("text").unwrap_or_default().to_string()).collect();
        format!("rows {rows:?} server {:?} requests {} outbox {} file tasks {} findings {:?}", server.state(), server.received(), rig.db.outbox().unwrap().len(), *rig.sync.inner.file_tasks.borrow(), findings.all())
    })
}

#[test]
fn cancelling_an_offline_upload_ends_the_same_whether_preparation_is_held_or_parked() {
    for cancel in [Cancel::Discard, Cancel::Reset] {
        let held = outcome_of_cancelling(Stage::PreparationHeld, cancel);
        let parked = outcome_of_cancelling(Stage::Parked, cancel);
        assert_eq!(held, parked);
        assert!(held.ends_with("findings []"), "{held}");
    }
}
