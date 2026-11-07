use super::fixtures::*;
use super::*;
use crate::store::{OperationRecord, OperationTransition};
use crate::owners::write::operation_status::OperationStatus;
use crate::value::sync::{FailureKind, OperationFailure};
use serde_json::json;

fn record() -> OperationRecord {
    OperationRecord {
        seq: None,
        action_key: "messages:update".into(),
        status: OperationStatus::Pending,
        row_ids: vec![id("m1"), id("m2")],
        temp_id: None,
        input: json!({"id": "m1", "body": "hi", "extra": {"list": [1, "two", null]}}),
        base_revision: Some(3),
        created_at: 5,
        dedupe_key: None,
        rollback: Some(json!({"rows": [{"kind": "edited", "model": "messages", "id": "m1", "before": {"body": "old"}, "owned": {"body": 1}}]})),
    }
}

fn send(store: &Store, items: Vec<OperationTransition>) -> i64 {
    let mut commit = envelope(store, vec![], vec![]);
    commit.operation_transitions = items;
    *store.commit(commit).unwrap().operations.iter().next().unwrap()
}

fn begun() -> (Store, i64) {
    let (store, _) = booted_store();
    let seq = send(&store, vec![OperationTransition::Begin(Box::new(record()))]);
    (store, seq)
}

#[test]
fn the_outbox_tables_keep_no_blob_columns() {
    let (store, _) = booted_store();
    let blobs = "SELECT COUNT(*) FROM pragma_table_info('{}') WHERE name IN ({})";
    let op = blobs.replacen("{}", "op", 1).replacen("{}", "'row_ids', 'input', 'rollback', 'error', 'upload', 'error_extensions'", 1);
    let sync = blobs.replacen("{}", "row_sync", 1).replacen("{}", "'actions', 'dirty_fields', 'error', 'upload', 'error_extensions'", 1);
    assert_eq!((count(&store, &op), count(&store, &sync)), (0, 0));
}

#[test]
fn an_operation_keeps_its_rows_and_values_in_child_tables() {
    let (store, seq) = begun();
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_row WHERE seq = {seq} AND row_id IN ('m1', 'm2')")), 2);
    assert!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'input'")) >= 6);
    assert!(count(&store, &format!("SELECT COUNT(*) FROM op_effect_value WHERE seq = {seq}")) >= 6);
}

#[test]
fn a_failure_is_columns() {
    let (store, seq) = begun();
    let failure = OperationFailure::new(FailureKind::Server, "http 503");
    send(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure }]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind = 'server' AND error_message = 'http 503'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_sync WHERE error_kind = 'server' AND error_message = 'http 503'"), 2);
}

#[test]
fn graphql_extensions_survive_reopen_in_every_error_owner_and_leave_with_it() {
    use crate::transport::GraphqlExtensions;
    let nodes = |store: &Store, table: &str| count(store, &format!("SELECT COUNT(*) FROM {table}"));
    let failure = |extensions: Option<serde_json::Value>, message: &str| OperationFailure {
        kind: FailureKind::Rejected,
        message: message.into(),
        extensions: extensions.map(|value| serde_json::from_value::<GraphqlExtensions>(value).unwrap()),
    };
    let cases = [
        Some(json!({"code":"REFUSED","transaction_disposition":"LATER","fields":{"detail":"\"refused\"","limit":"3","context":"{\"values\":[true,null,2.5,[1,{\"k\":null}]]}"}})),
        Some(json!({"code":null,"transaction_disposition":null,"fields":{}})),
        Some(json!({"code":null,"transaction_disposition":"KEEP","fields":{"nothing":"null"}})),
        None,
    ];
    for extensions in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.sqlite");
        let store = Store::open(&path).unwrap();
        store.boot(&[messages_spec(), items_spec()], None).unwrap();
        store.mark_ready();
        let seq = send(&store, vec![OperationTransition::Begin(Box::new(record()))]);
        let failure = failure(extensions.clone(), "purchase refused");
        send(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: failure.clone() }]);
        drop(store);
        let store = Store::open(&path).unwrap();
        store.boot(&[messages_spec(), items_spec()], None).unwrap();
        store.mark_ready();
        store.read(&[], |reader| {
            let stored = crate::store::operation::open_operations(reader.connection())?;
            assert_eq!(stored.len(), 1);
            assert!(stored.iter().all(|operation| operation.error.as_ref() == Some(&failure)), "op");
            let rows = crate::store::sync_state::read_sync(reader.connection(), "messages", &["m1".into(), "m2".into()])?;
            assert_eq!(rows.len(), 2);
            assert!(rows.values().all(|row| row.error.as_ref() == Some(&failure)), "row_sync");
            Ok(())
        }).unwrap();
        assert_eq!(nodes(&store, "op_error") > 0, extensions.is_some(), "op_error");
        for table in ["op_error", "sync_row_error"] {
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table} WHERE kind = 4 AND (text LIKE '[%' OR text LIKE '{{%' OR text LIKE '\"%')")), 0, "{table} keeps JSON text");
        }
        let fields = extensions.as_ref().map(|value| value["fields"].clone()).unwrap_or_default();
        if fields.get("context").is_some() {
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_error WHERE seq = {seq} AND name = 'detail' AND kind = 4 AND text = 'refused'")), 1, "a string field has no JSON quotes");
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_error WHERE seq = {seq} AND name = 'limit' AND kind = 2 AND whole = 3")), 1);
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_error AS context JOIN op_error AS values_node ON values_node.seq = context.seq AND values_node.parent = context.node WHERE context.seq = {seq} AND context.name = 'context' AND context.kind = 5 AND values_node.name = 'values' AND values_node.kind = 6")), 1, "an object field holds typed child nodes");
        }
        if fields.get("nothing").is_some() {
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_error WHERE seq = {seq} AND name = 'nothing' AND kind = 0 AND text IS NULL")), 1, "a null field stays null");
        }
        assert_eq!(nodes(&store, "sync_row_error") > 0, extensions.is_some(), "sync_row_error");
        send(&store, vec![OperationTransition::Retry { seq }]);
        assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_error WHERE seq = {seq}")), 0, "retry clears the operation tree");
        send(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Committed }]);
        for table in ["op_error", "sync_row_error"] {
            assert_eq!(nodes(&store, table), 0, "{table} after close");
        }
    }
}

#[test]
fn a_field_that_is_not_json_fails_the_commit_and_stores_nothing() {
    let (store, seq) = begun();
    let extensions = serde_json::from_value(json!({"code":null,"transaction_disposition":null,"fields":{"detail":"{bad"}})).unwrap();
    let failure = OperationFailure { kind: FailureKind::Rejected, message: "refused".into(), extensions: Some(extensions) };
    let mut commit = envelope(&store, vec![], vec![]);
    commit.operation_transitions = vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure }];
    assert!(store.commit(commit).is_err());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind IS NOT NULL"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op_error"), 0);
    let failure = OperationFailure { kind: FailureKind::Rejected, message: "refused".into(), extensions: None };
    send(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure }]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind IS NOT NULL"), 1, "the store still applies a valid commit");
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
    send(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Committed }]);
    for table in ["op", "op_row", "op_value", "op_error", "row_sync", "sync_row_error", "sync_action", "sync_field"] {
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
    send(&store, vec![OperationTransition::Begin(Box::new(next))]);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_row WHERE seq = {seq}")), 1);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_effect_value WHERE seq = {seq}")), 0);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM op_value WHERE seq = {seq} AND part = 'input'")), 2);
}
