use std::collections::BTreeSet;

use rusqlite::Connection;

use super::*;
use crate::ddl::migrate_core;
use crate::operation::{delete_operation, failure_cells, upload_cells, write_rows, UPLOAD_COLUMNS};
use crate::upload::{UploadStage, UploadState, UploadStatus};

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn insert_upload(connection: &Connection, status: &str, started: i64, state: &UploadState) -> i64 {
    let failure = (status == "failed").then(|| OperationFailure::new(FailureKind::Network, "offline"));
    let [kind, message] = failure_cells(failure.as_ref());
    connection
        .execute(
            "INSERT INTO op (action_key, status, transport_started, created_at, since, error_kind, error_message) VALUES ('moments:upload', ?1, ?2, 5, 5, ?3, ?4)",
            rusqlite::params![status, started, kind, message],
        )
        .unwrap();
    let seq = connection.last_insert_rowid();
    write_rows(connection, seq, &[crate::identity::RowId::new("m1").unwrap()]).unwrap();
    set_upload(connection, seq, state);
    seq
}

fn set_upload(connection: &Connection, seq: i64, state: &UploadState) {
    let assignments: Vec<String> = UPLOAD_COLUMNS.split(", ").enumerate().map(|(at, column)| format!("{column} = ?{}", at + 2)).collect();
    let mut values = vec![rusqlite::types::Value::Integer(seq)];
    values.extend(upload_cells(Some(state)));
    connection.execute(&format!("UPDATE op SET {} WHERE seq = ?1", assignments.join(", ")), rusqlite::params_from_iter(values)).unwrap();
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
    set_upload(&connection, seq, &state(UploadStatus::Uploading, 40));
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
    set_upload(&connection, seq, &state(UploadStatus::Uploading, 11));
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
    delete_operation(&connection, seq).unwrap();
    recompute_sync(&connection, &keys()).unwrap();
    assert_eq!(info(&connection), None);
}

#[test]
fn an_operation_without_upload_state_has_no_upload() {
    let connection = database();
    connection
        .execute("INSERT INTO op (action_key, status, created_at, since) VALUES ('moments:update', 'pending', 5, 5)", [])
        .unwrap();
    write_rows(&connection, connection.last_insert_rowid(), &[crate::identity::RowId::new("m1").unwrap()]).unwrap();
    recompute_sync(&connection, &keys()).unwrap();
    assert_eq!(info(&connection).unwrap().upload, None);
}
