use super::*;
use crate::ddl::migrate_core;
use crate::identity::RowId;
use crate::schema::ActionMode;
use rusqlite::Connection;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn operation(row: &str, status: OperationStatus) -> Operation {
    let mut operation = Operation::new("messages.send", ActionMode::Request, vec![RowId::new(row).unwrap()], 0, 1);
    operation.status = status;
    operation
}

fn stored(connection: &Connection, seq: i64) -> Option<(String, String, i64)> {
    connection
        .query_row("SELECT action_key, status, transport_started FROM op WHERE seq = ?", [seq], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .ok()
}

#[test]
fn append_stores_action_and_status_then_status_transport_and_delete_apply() {
    let connection = database();
    let first = append_operation(&connection, &operation("m1", OperationStatus::Pending), b"{}").unwrap();
    let second = append_operation(&connection, &operation("m2", OperationStatus::Pending), b"{}").unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(stored(&connection, first), Some(("messages.send".into(), "pending".into(), 0)));
    set_status(&connection, first, OperationStatus::Committed).unwrap();
    assert_eq!(stored(&connection, first).unwrap().1, "committed");
    mark_transport(&connection, second).unwrap();
    assert_eq!(stored(&connection, second).unwrap().2, 1);
    delete_operation(&connection, second).unwrap();
    assert_eq!(stored(&connection, second), None);
}

#[test]
fn open_sequences_exclude_committed_operations() {
    let connection = database();
    for (row, status) in [("m1", OperationStatus::Pending), ("m2", OperationStatus::Pending), ("m3", OperationStatus::Committed)] {
        append_operation(&connection, &operation(row, status), b"{}").unwrap();
    }
    assert_eq!(open_sequences(&connection, "messages.send").unwrap(), [1, 2]);
}

#[test]
fn open_operations_empty_for_empty_database() {
    assert!(open_operations(&database()).unwrap().is_empty());
}

#[test]
fn open_operations_restores_pending_operation_and_excludes_committed() {
    let connection = database();
    let mut pending = operation("1", OperationStatus::Pending);
    pending.row_ids.push(RowId::new("2").unwrap());
    pending.temp_id = Some(RowId::new("3").unwrap());
    pending.dedupe_key = Some("k".into());
    pending.created_at = 9;
    let seq = append_operation(&connection, &pending, b"{}").unwrap();
    let loaded = open_operations(&connection).unwrap();
    assert_eq!(loaded.len(), 1);
    pending.id = OperationId(seq);
    assert_eq!(loaded[0], pending);
    set_status(&connection, seq, OperationStatus::Committed).unwrap();
    assert!(open_operations(&connection).unwrap().is_empty());
}

#[test]
fn input_is_stored_and_read_back() {
    let connection = database();
    let seq = append_operation(&connection, &operation("1", OperationStatus::Pending), b"{\"a\":1}").unwrap();
    assert_eq!(operation_input(&connection, seq).unwrap(), Some(b"{\"a\":1}".to_vec()));
    assert_eq!(operation_input(&connection, 99).unwrap(), None);
}

#[test]
fn apply_fsck_counts_only_status_actions() {
    let connection = database();
    assert_eq!(apply_fsck(&connection, &[]).unwrap(), 0);
    let seq = append_operation(&connection, &operation("row-1", OperationStatus::Pending), b"{}").unwrap();
    let skipped = [FsckAction::Keep(OperationId(seq)), FsckAction::QuarantineOrphan(RowId::new("orphan-1").unwrap())];
    assert_eq!(apply_fsck(&connection, &skipped).unwrap(), 0);
    assert_eq!(open_operations(&connection).unwrap()[0].status, OperationStatus::Pending);
    assert_eq!(apply_fsck(&connection, &[FsckAction::SetStatus(OperationId(seq), OperationStatus::Failed)]).unwrap(), 1);
    assert_eq!(open_operations(&connection).unwrap()[0].status, OperationStatus::Failed);
}

#[test]
fn once_keys_are_recorded_once_and_read_back() {
    let connection = database();
    let request = OnceRequest::new("deleteGroup", Some("chat-1"));
    record_once(&connection, &request).unwrap();
    record_once(&connection, &request).unwrap();
    record_once(&connection, &OnceRequest::new("deleteGroup", None)).unwrap();
    assert_eq!(committed_once_keys(&connection).unwrap(), [request].into());
}

#[test]
fn open_once_keys_come_from_pending_and_delivery_unknown_operations() {
    let connection = database();
    let mut pending = operation("1", OperationStatus::Pending);
    pending.dedupe_key = Some("chat-1".into());
    let mut failed = operation("2", OperationStatus::Failed);
    failed.dedupe_key = Some("chat-2".into());
    append_operation(&connection, &pending, b"{}").unwrap();
    append_operation(&connection, &failed, b"{}").unwrap();
    assert_eq!(open_once_keys(&connection).unwrap(), [OnceRequest::new("messages.send", Some("chat-1"))].into());
}

#[test]
fn op_columns_match_spec_and_nullability() {
    let connection = database();
    let mut statement = connection.prepare("PRAGMA table_info(op)").unwrap();
    let columns: Vec<(String, bool)> = statement.query_map([], |r| Ok((r.get(1)?, r.get::<_, i64>(3)? == 0 && r.get::<_, i64>(5)? == 0))).unwrap().map(Result::unwrap).collect();
    let names: Vec<&str> = columns.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(names, OP_COLUMNS);
    let nullable: Vec<&str> = columns.iter().filter(|c| c.1).map(|c| c.0.as_str()).collect();
    assert_eq!(nullable, ["temp_id", "base_revision", "dedupe_key", "rollback"]);
}
