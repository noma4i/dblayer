use std::collections::BTreeSet;

use rusqlite::Connection;

use super::*;
use crate::ddl::migrate_core;
use crate::upload::{UploadStage, UploadState, UploadStatus};

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn insert_upload(connection: &Connection, status: &str, started: i64, state: &UploadState) -> i64 {
    connection
        .execute(
            "INSERT INTO op (action_key, status, row_ids, input, transport_started, created_at, since, upload, error) VALUES ('moments:upload', ?1, ?2, ?3, ?4, 5, 5, ?5, ?6)",
            rusqlite::params![
                status,
                br#"["m1"]"#.as_slice(),
                b"{}".as_slice(),
                started,
                serde_json::to_vec(state).unwrap(),
                (status == "failed").then(|| serde_json::to_vec(&OperationFailure::new(FailureKind::Network, "offline")).unwrap()),
            ],
        )
        .unwrap();
    connection.last_insert_rowid()
}

fn state(phase: UploadStatus, sent: u64) -> UploadState {
    UploadState { phase, stage: UploadStage::Put, total_bytes: 100, sent_bytes: sent, ..UploadState::queued() }
}

fn keys() -> BTreeSet<SyncKey> {
    BTreeSet::from([("moments".to_string(), "m1".to_string())])
}

fn info(connection: &Connection) -> Option<SyncInfo> {
    read_sync(connection, "moments", &["m1".to_string()]).unwrap().remove("m1")
}

#[test]
fn a_queued_upload_is_pending_and_a_running_one_is_sending() {
    let connection = database();
    let seq = insert_upload(&connection, "pending", 0, &state(UploadStatus::Queued, 0));
    recompute_sync(&connection, &keys()).unwrap();
    let queued = info(&connection).unwrap();
    assert_eq!((queued.state, queued.upload.as_ref().map(|upload| upload.status)), (SyncState::Pending, Some(UploadStatus::Queued)));
    connection.execute("UPDATE op SET upload = ?1 WHERE seq = ?2", rusqlite::params![serde_json::to_vec(&state(UploadStatus::Uploading, 40)).unwrap(), seq]).unwrap();
    assert_eq!(recompute_sync(&connection, &keys()).unwrap(), keys());
    let running = info(&connection).unwrap();
    let upload = running.upload.unwrap();
    assert_eq!((running.state, upload.status, upload.stage, upload.sent_bytes, upload.total_bytes), (SyncState::Sending, UploadStatus::Uploading, UploadStage::Put, 40, 100));
    assert_eq!(upload.fraction(), 0.4);
}

#[test]
fn a_progress_tick_rewrites_the_row_and_an_unchanged_one_does_not() {
    let connection = database();
    let seq = insert_upload(&connection, "pending", 0, &state(UploadStatus::Uploading, 10));
    recompute_sync(&connection, &keys()).unwrap();
    assert!(recompute_sync(&connection, &keys()).unwrap().is_empty());
    connection.execute("UPDATE op SET upload = ?1 WHERE seq = ?2", rusqlite::params![serde_json::to_vec(&state(UploadStatus::Uploading, 11)).unwrap(), seq]).unwrap();
    assert_eq!(recompute_sync(&connection, &keys()).unwrap(), keys());
}

#[test]
fn a_failed_upload_carries_the_error_and_a_closed_one_leaves_no_upload() {
    let connection = database();
    let seq = insert_upload(&connection, "failed", 0, &state(UploadStatus::Uploading, 10));
    recompute_sync(&connection, &keys()).unwrap();
    let failed = info(&connection).unwrap();
    let upload = failed.upload.unwrap();
    assert_eq!((failed.state, upload.status, upload.error.map(|error| error.message)), (SyncState::Failed, UploadStatus::Failed, Some("offline".to_string())));
    connection.execute("DELETE FROM op WHERE seq = ?1", [seq]).unwrap();
    recompute_sync(&connection, &keys()).unwrap();
    assert_eq!(info(&connection), None);
}

#[test]
fn an_operation_without_upload_state_has_no_upload() {
    let connection = database();
    connection
        .execute("INSERT INTO op (action_key, status, row_ids, input, created_at, since) VALUES ('moments:update', 'pending', X'5B226D31225D', X'7B7D', 5, 5)", [])
        .unwrap();
    recompute_sync(&connection, &keys()).unwrap();
    assert_eq!(info(&connection).unwrap().upload, None);
}
