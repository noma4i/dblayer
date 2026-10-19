use super::fixtures::*;
use super::*;
use crate::commit::{OperationRecord, OperationStatus, OperationTransition};
use crate::syncstate::{FailureKind, OperationFailure};
use crate::upload::{UploadPart, UploadScenario, UploadStage, UploadState, UploadStatus};
use serde_json::json;

fn record() -> OperationRecord {
    OperationRecord {
        seq: None,
        action_key: "messages:update".into(),
        status: OperationStatus::Pending,
        row_ids: vec![id("m1"), id("m2")],
        temp_id: None,
        input: json!({"id": "m1", "body": "hi", "extra": {"list": [1, "two", null]}}),
        transport_started: false,
        base_revision: Some(3),
        created_at: 5,
        dedupe_key: None,
        rollback: Some(json!({"rows": [{"kind": "edited", "model": "messages", "id": "m1", "before": {"body": "old"}, "owned": {"body": true}}]})),
        upload: None,
    }
}

fn send(store: &Store, items: Vec<OperationTransition>) -> i64 {
    let mut commit = envelope(store, vec![], vec![]);
    commit.operation_transitions = items;
    *store.commit(commit).unwrap().operations.iter().next().unwrap()
}

fn begun() -> (Store, i64) {
    let (store, _) = booted_store();
    let seq = send(&store, vec![OperationTransition::Begin(record())]);
    (store, seq)
}

fn upload() -> UploadState {
    let mut state = UploadState::queued();
    state.phase = UploadStatus::Uploading;
    state.stage = UploadStage::Part;
    state.scenario = Some(UploadScenario::Multipart);
    state.total_bytes = 90;
    state.sent_bytes = 40;
    state.key = Some("k1".into());
    state.parts = vec![UploadPart { number: 1, etag: "a".into(), confirmed_bytes: 20 }, UploadPart { number: 2, etag: "b".into(), confirmed_bytes: 20 }];
    state
}

#[test]
fn the_outbox_tables_keep_no_blob_columns() {
    let (store, _) = booted_store();
    let blobs = "SELECT COUNT(*) FROM pragma_table_info('{}') WHERE name IN ({})";
    let op = blobs.replacen("{}", "op", 1).replacen("{}", "'row_ids', 'input', 'rollback', 'error', 'upload'", 1);
    let sync = blobs.replacen("{}", "row_sync", 1).replacen("{}", "'actions', 'dirty_fields', 'error', 'upload'", 1);
    assert_eq!((count(&store, &op), count(&store, &sync)), (0, 0));
}

#[test]
fn an_operation_keeps_its_rows_and_values_in_child_tables() {
    let (store, seq) = begun();
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_row WHERE seq = {seq} AND row_id IN ('m1', 'm2')")), 2);
    assert!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'input'")) >= 6);
    assert!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'prior'")) >= 6);
}

#[test]
fn a_failure_and_the_upload_progress_are_columns() {
    let (store, seq) = begun();
    let failure = OperationFailure::new(FailureKind::Server, "http 503");
    send(&store, vec![OperationTransition::Start { seq }]);
    send(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure }]);
    send(&store, vec![OperationTransition::Progress { seq, item_position: 0, state: upload() }]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM pragma_table_info('op') WHERE name LIKE 'up_%'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op JOIN op_item USING (seq) WHERE op.error_kind = 'server' AND op.error_message = 'http 503' AND position = 0 AND phase = 'uploading' AND sent_bytes = 40"), 1);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_part WHERE seq = {seq} AND etag IN ('a', 'b')")), 2);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_sync WHERE error_kind = 'server' AND error_message = 'http 503'"), 2);
}

#[test]
fn graphql_extensions_survive_stored_failures_and_retry_clears_them() {
    use crate::transport::{GraphqlExtensions, TransactionDisposition};
    for disposition in [TransactionDisposition::Finish, TransactionDisposition::Keep, TransactionDisposition::Unknown { value: "LATER".into() }] {
        let (store, seq) = begun();
        let failure = OperationFailure {
            kind: FailureKind::Rejected,
            message: "purchase refused".into(),
            extensions: Some(GraphqlExtensions { code: Some("DUPLICATE_CLIENT_KEY".into()), transaction_disposition: Some(disposition) }),
        };
        send(&store, vec![OperationTransition::Start { seq }]);
        send(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: failure.clone() }]);
        store.read(&[], |reader| {
            let stored = crate::operation::open_operations(reader.connection())?;
            assert_eq!(stored[0].error.as_ref(), Some(&failure));
            let rows = crate::syncstate::read_sync(reader.connection(), "messages", &["m1".into(), "m2".into()])?;
            assert_eq!(rows.len(), 2);
            assert!(rows.values().all(|row| row.error.as_ref() == Some(&failure)));
            Ok(())
        }).unwrap();
        send(&store, vec![OperationTransition::Retry { seq }]);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind IS NULL AND error_message IS NULL AND error_code IS NULL AND error_disposition IS NULL"), 1);
        assert_eq!(count(&store, "SELECT COUNT(*) FROM row_sync WHERE error_kind IS NULL AND error_message IS NULL AND error_code IS NULL AND error_disposition IS NULL"), 2);
    }
}

#[test]
fn row_sync_keeps_actions_and_dirty_fields_in_child_tables() {
    let (store, _) = begun();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_action WHERE row_id = 'm1' AND name = 'update'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_field WHERE row_id = 'm1' AND name = 'body'"), 1);
}

#[test]
fn closing_an_operation_removes_its_child_rows() {
    let (store, seq) = begun();
    send(&store, vec![OperationTransition::Progress { seq, item_position: 0, state: upload() }]);
    send(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Committed, once: false }]);
    for table in ["op", "op_row", "op_value", "op_item", "op_part", "row_sync", "sync_action", "sync_field"] {
        assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table}")), 0, "{table}");
    }
}

#[test]
fn rewriting_an_operation_replaces_its_rows_and_values() {
    let (store, seq) = begun();
    let mut next = record();
    next.seq = Some(seq);
    next.row_ids = vec![id("m3")];
    next.input = json!({"id": "m3"});
    next.rollback = None;
    send(&store, vec![OperationTransition::Begin(next)]);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_row WHERE seq = {seq}")), 1);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'prior'")), 0);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'input'")), 2);
}
