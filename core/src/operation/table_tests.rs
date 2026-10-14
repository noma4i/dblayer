use super::*;
use crate::ddl::{create_model_tables, migrate_core};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use std::collections::HashSet;
use crate::identity::RowId;
use rusqlite::Connection;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn operation(row: &str, status: OperationStatus) -> Operation {
    let mut operation = Operation::new("messages.send", vec![RowId::new(row).unwrap()], 0, 1);
    operation.status = status;
    operation
}

fn stored(connection: &Connection, seq: i64) -> Option<(String, String, i64)> {
    connection
        .query_row("SELECT action_key, status, transport_started FROM op WHERE seq = ?", [seq], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .ok()
}

#[test]
fn append_stores_action_and_status_then_status_applies() {
    let connection = database();
    let first = append_operation(&connection, &operation("m1", OperationStatus::Pending), b"{}").unwrap();
    let second = append_operation(&connection, &operation("m2", OperationStatus::Pending), b"{}").unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(stored(&connection, first), Some(("messages.send".into(), "pending".into(), 0)));
    set_status(&connection, first, OperationStatus::Committed).unwrap();
    assert_eq!(stored(&connection, first).unwrap().1, "committed");
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
    pending.since = 9;
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
    assert_eq!(apply_fsck(&connection, &[], &Orphans::new()).unwrap(), 0);
    let seq = append_operation(&connection, &operation("row-1", OperationStatus::Pending), b"{}").unwrap();
    let skipped = [FsckAction::Keep(OperationId(seq)), FsckAction::QuarantineOrphan(RowId::new("orphan-1").unwrap())];
    assert_eq!(apply_fsck(&connection, &skipped, &Orphans::new()).unwrap(), 0);
    assert_eq!(open_operations(&connection).unwrap()[0].status, OperationStatus::Pending);
    assert_eq!(apply_fsck(&connection, &[FsckAction::SetStatus(OperationId(seq), OperationStatus::Failed)], &Orphans::new()).unwrap(), 1);
    assert_eq!(open_operations(&connection).unwrap()[0].status, OperationStatus::Failed);
}

#[test]
fn op_columns_match_spec_and_nullability() {
    let connection = database();
    let mut statement = connection.prepare("PRAGMA table_info(op)").unwrap();
    let columns: Vec<(String, bool)> = statement.query_map([], |r| Ok((r.get(1)?, r.get::<_, i64>(3)? == 0 && r.get::<_, i64>(5)? == 0))).unwrap().map(Result::unwrap).collect();
    let names: Vec<&str> = columns.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(
        names,
        ["seq", "action_key", "status", "row_ids", "temp_id", "input", "transport_started", "base_revision", "created_at", "dedupe_key", "rollback", "attempts", "error", "since", "upload"]
    );
    let nullable: Vec<&str> = columns.iter().filter(|c| c.1).map(|c| c.0.as_str()).collect();
    assert_eq!(nullable, ["temp_id", "base_revision", "dedupe_key", "rollback", "error", "upload"]);
}

fn with_messages_table() -> Connection {
    let connection = database();
    let messages = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    create_model_tables(&connection, &[messages]).unwrap();
    connection
}

fn insert_message(connection: &Connection, id: &str) {
    connection
        .execute(
            "INSERT INTO row_messages (id, revision, born_rev) VALUES (?1, 1, 1)",
            rusqlite::params![id],
        )
        .unwrap();
    connection.execute("INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES ('messages', 'list', ?1, ?1)", [id]).unwrap();
}

fn orphans() -> Orphans {
    ["temp-orphan", "temp-kept"]
        .iter()
        .map(|id| (RowId::new(id).unwrap(), Orphan { model: "messages".into(), table: "row_messages".into(), payload: format!("{{\"id\":\"{id}\"}}").into_bytes() }))
        .collect()
}

#[test]
fn apply_fsck_quarantines_and_removes_orphan_temp_rows() {
    let connection = with_messages_table();
    insert_message(&connection, "temp-orphan");
    insert_message(&connection, "temp-kept");
    let actions = [FsckAction::QuarantineOrphan(RowId::new("temp-orphan").unwrap()), FsckAction::QuarantineOrphan(RowId::new("temp-unknown").unwrap())];
    assert_eq!(apply_fsck(&connection, &actions, &orphans()).unwrap(), 1);
    let left: Vec<String> = connection.prepare("SELECT id FROM row_messages").unwrap().query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect();
    assert_eq!(left, vec!["temp-kept"]);
    let members: i64 = connection.query_row("SELECT COUNT(*) FROM scope_member WHERE row_id = 'temp-orphan'", [], |r| r.get(0)).unwrap();
    assert_eq!(members, 0);
    let entry: (String, String, Vec<u8>) = connection.query_row("SELECT reason, model, payload FROM quarantine", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!((entry.0.as_str(), entry.1.as_str(), entry.2), ("orphan-temp-row", "messages", b"{\"id\":\"temp-orphan\"}".to_vec()));
}

fn create_operation(connection: &Connection, temp: &str, input: &[u8]) -> OperationId {
    let mut operation = Operation::new("messages:create", vec![RowId::new(temp).unwrap()], 0, 1);
    operation.temp_id = Some(RowId::new(temp).unwrap());
    let seq = append_operation(connection, &operation, input).unwrap();
    OperationId(seq)
}

fn known() -> HashSet<String> {
    ["messages".to_string()].into()
}

#[test]
fn rebuilt_rows_come_from_the_input_of_the_create_operation() {
    let connection = with_messages_table();
    let id = create_operation(&connection, "temp-1", b"{\"id\":\"temp-1\",\"body\":\"hi\"}");
    let operations = open_operations(&connection).unwrap();
    let rows = rebuilt_rows(&connection, &[FsckAction::RebuildTempRow(id)], &operations, &known()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].model.as_str(), rows[0].id.as_str()), ("messages", "temp-1"));
    assert_eq!(rows[0].row["body"], "hi");
}

#[test]
fn rebuilt_rows_skip_inputs_that_do_not_describe_the_temp_row() {
    let connection = with_messages_table();
    let wrong_id = create_operation(&connection, "temp-1", b"{\"id\":\"temp-2\"}");
    let not_object = create_operation(&connection, "temp-3", b"[1]");
    let broken = create_operation(&connection, "temp-4", b"nope");
    let operations = open_operations(&connection).unwrap();
    let actions: Vec<FsckAction> = [wrong_id, not_object, broken, OperationId(99)].into_iter().map(FsckAction::RebuildTempRow).collect();
    assert!(rebuilt_rows(&connection, &actions, &operations, &known()).unwrap().is_empty());
    let unknown = HashSet::new();
    let id = create_operation(&connection, "temp-5", b"{\"id\":\"temp-5\"}");
    let operations = open_operations(&connection).unwrap();
    assert!(rebuilt_rows(&connection, &[FsckAction::RebuildTempRow(id)], &operations, &unknown).unwrap().is_empty());
}
