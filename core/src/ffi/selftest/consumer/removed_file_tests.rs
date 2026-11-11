use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
use crate::ffi::{ReadRequest, ReadValue};

async fn assert_failed_operations(stand: &Stand, ids: &[String]) {
    let stream = stand.database.subscribe_values(stand.database.session_epoch(), ReadRequest::Operations);
    let Some(ReadValue::Operations { items, .. }) = bound(stream.pull_next()).await else { panic!("operations snapshot missing") };
    for id in ids {
        assert!(items.iter().any(|item| item.model == "Moment" && item.row_id == *id && item.action == "create_file" && item.status == crate::ffi::queue::OperationQueueStatus::Failed && item.error.as_ref().is_some_and(|error| error.message == "RestoreFailed")), "{id}: {items:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_discard_unrestorable_transfer_removes_retained_file() {
    for case in ["valid", "unreadable", "no_seq", "no_transfer_id", "neither"] {
        check_format_change(case).await;
    }
}

async fn check_format_change(case: &str) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let source = directory.path().join("source");
    std::fs::write(&source, b"retained file").unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let epoch = stand.database.session_epoch();
    let receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(),
        values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
        args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfer: i64 = db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1", [&id], |row| row.get(0)).unwrap();
    drop(db);
    let file = std::path::PathBuf::from(format!("{}.files/transfers/{transfer}", path.display()));
    assert!(file.exists(), "the accepted upload must have its owned copy");
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE old_upload AS SELECT * FROM sync_upload; DROP TABLE sync_upload; ALTER TABLE old_upload RENAME TO sync_upload; UPDATE manifest SET value = 'old' WHERE key = 'format_version';").unwrap();
    let change = match case {
        "valid" => "SELECT 1",
        "unreadable" => "UPDATE sync_upload SET total = 'unreadable'",
        "no_seq" => "UPDATE sync_upload SET seq = NULL",
        "no_transfer_id" => "UPDATE sync_upload SET transfer_id = NULL",
        "neither" => "UPDATE sync_upload SET seq = NULL, transfer_id = NULL",
        _ => unreachable!(),
    };
    db.execute_batch(change).unwrap();
    drop(db);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    reopened.settle().await;
    let report = reopened.database.open_report().unwrap();
    if case == "valid" {
        assert!(!report.dropped.iter().any(|row| row.table == "sync_upload"), "valid upload was dropped: {:?}", report.dropped);
    } else {
        assert!(report.dropped.iter().any(|row| row.table == "sync_upload"), "{case}: upload must be retained as RestoreFailed: {:?}", report.dropped);
        let stream = reopened.database.subscribe_values(reopened.database.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: id.clone(), action: "create_file".into() });
        let shown = bound(stream.pull_next()).await;
        assert!(matches!(shown, Some(ReadValue::Status { ref state, ref error, .. }) if state == "failed" && error.as_deref() == Some("RestoreFailed")), "{case}: {shown:?}");
        assert_failed_operations(&reopened, std::slice::from_ref(&id)).await;
    }
    if matches!(case, "no_transfer_id" | "neither") {
        assert!(!file.exists(), "{case}: the copy has no transfer_id holder");
    } else {
        assert!(file.exists(), "{case}: the owned copy must remain until discard");
    }
    if case == "valid" {
        reopened.hold(&["CreateBlob"]);
        let mark = reopened.request_mark();
        reopened.database.set_online(true).unwrap();
        reopened.receive_after(mark, "CreateBlob", None, 1).await;
        assert!(file.exists(), "the resumed upload still owns its copy");
        reopened.release(&["CreateBlob"]);
        bound(reopened.database.close_database()).await.unwrap();
        return;
    }
    let retained = if case == "unreadable" {
        let db = rusqlite::Connection::open(&path).unwrap();
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0);
        Some(std::fs::read(&file).unwrap())
    } else { None };
    let epoch = reopened.database.session_epoch();
    bound(reopened.database.discard(epoch, "Moment".into(), id, Some("create_file".into()))).await.unwrap();
    assert!(!file.exists(), "discard must remove the retained copy");
    if let Some(retained) = retained {
        let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
        assert!(wal.len() >= 32, "discard WAL header missing");
        let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
        assert_eq!((wal.len() - 32) % frame_size, 0);
        let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
        assert!(!commits.is_empty(), "discard must commit");
        for (index, end) in std::iter::once(32).chain(commits).enumerate() {
            let image = directory.path().join(format!("image-{index}.sqlite"));
            std::fs::copy(&path, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
            let copy = std::path::PathBuf::from(format!("{}.files/transfers/{transfer}", image.display()));
            std::fs::create_dir_all(copy.parent().unwrap()).unwrap();
            std::fs::write(&copy, &retained).unwrap();
            let image_stand = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            image_stand.settle().await;
            let db = rusqlite::Connection::open(&image).unwrap();
            let held: i64 = db.query_row("SELECT count(*) FROM restore_failed WHERE source_table = 'sync_upload'", [], |row| row.get(0)).unwrap();
            assert_eq!(copy.exists(), held > 0, "commit {index}: file and restore_failed holder differ");
            let inline: i64 = db.query_row("SELECT count(*) FROM file WHERE id = ?1", [format!("transfers/{transfer}")], |row| row.get(0)).unwrap();
            assert_eq!(inline, 0, "commit {index}: no orphan inline file");
            drop(db);
            bound(image_stand.database.close_database()).await.unwrap();
        }
    }
    bound(reopened.database.close_database()).await.unwrap();
    drop(reopened);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials, None, false).await;
    assert!(!file.exists(), "{case}: reopening must not restore a discarded file");
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_unidentified_transfers_discard_independently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let source = directory.path().join("source");
    std::fs::write(&source, b"file").unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let epoch = stand.database.session_epoch();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let receipt = bound(stand.database.send(CommandRequest {
            epoch, model: "Moment".into(),
            values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
            upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
        })).await.unwrap();
        ids.push(receipt.optimistic_value.unwrap().id);
    }
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfers: Vec<i64> = db.prepare("SELECT transfer_id FROM sync_upload ORDER BY transfer_id").unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(transfers.len(), 2);
    let files: Vec<_> = transfers.iter().map(|id| std::path::PathBuf::from(format!("{}.files/transfers/{id}", path.display()))).collect();
    assert!(files.iter().all(|file| file.exists()));
    drop(db);
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE old_upload AS SELECT * FROM sync_upload; DROP TABLE sync_upload; ALTER TABLE old_upload RENAME TO sync_upload; UPDATE sync_upload SET seq = NULL, transfer_id = NULL, op_id = NULL; UPDATE manifest SET value = 'old' WHERE key = 'format_version';").unwrap();
    drop(db);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    reopened.settle().await;
    assert_eq!(reopened.database.open_report().unwrap().dropped.iter().filter(|row| row.table == "sync_upload").count(), 2);
    assert!(files.iter().all(|file| !file.exists()), "unidentified files must not survive Boot");
    assert_failed_operations(&reopened, &ids).await;
    let epoch = reopened.database.session_epoch();
    for id in ids {
        let stream = reopened.database.subscribe_values(epoch, ReadRequest::Blob { model: "Moment".into(), id: id.clone(), action: "create_file".into() });
        let shown = bound(stream.pull_next()).await;
        assert!(matches!(shown, Some(ReadValue::Status { ref state, ref error, .. }) if state == "failed" && error.as_deref() == Some("RestoreFailed")), "{shown:?}");
        bound(reopened.database.discard(epoch, "Moment".into(), id, Some("create_file".into()))).await.unwrap();
    }
    let db = rusqlite::Connection::open(&path).unwrap();
    let left: i64 = db.query_row("SELECT count(*) FROM restore_failed WHERE source_table = 'sync_upload'", [], |row| row.get(0)).unwrap();
    assert_eq!(left, 0);
    drop(db);
    bound(reopened.database.close_database()).await.unwrap();
}
