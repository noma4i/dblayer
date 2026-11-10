use crate::identity::RowId;
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{StoreBatch, StoreOp, UploadKind, UploadRow};
use crate::owners::write::operation_queue::{queue_of, take_comparisons};
use crate::owners::write::operation_status::{Operation, OperationId, OperationStatus};
use crate::schema::OnFailure;
use crate::store::{fixtures::all_specs, Store};
use serde_json::json;

thread_local! { static SQL: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn count_sql(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") { SQL.with(|count| count.set(count.get() + 1)); }
    }
}

fn operation(seq: i64, row: &str, action: &str, status: OperationStatus) -> Operation {
    let mut value = Operation::new(&format!("moments:{action}"), vec![RowId::new(row).unwrap()], 0, seq);
    value.id = OperationId(seq);
    value.status = status;
    value
}

#[test]
fn dlv_operations_blocked_by_table() {
    for (input, expected) in [
        (vec![("a", "update", OperationStatus::Failed), ("a", "update", OperationStatus::Pending)], vec![None, Some(1)]),
        (vec![("a", "update", OperationStatus::Failed), ("a", "update", OperationStatus::Failed), ("a", "update", OperationStatus::Pending)], vec![None, Some(1), Some(2)]),
        (vec![("a", "update", OperationStatus::Failed), ("b", "update", OperationStatus::Pending)], vec![None, None]),
        (vec![("a", "upload:photo", OperationStatus::Failed), ("a", "update", OperationStatus::Pending)], vec![None, Some(1)]),
    ] {
        let operations: Vec<_> = input.into_iter().enumerate().map(|(i, (row, action, status))| operation(i as i64 + 1, row, action, status)).collect();
        let actual: Vec<_> = queue_of(&operations).unwrap().into_iter().map(|item| item.blocked_by).collect();
        assert_eq!(actual, expected);
    }
}

fn queue_work(n: usize, m: usize) -> (usize, usize) {
    let db = Store::open_models(None, all_specs(), None).unwrap();
    {
        let inner = db.lock();
        for index in 0..n {
            let mut op = operation(0, &format!("r{index}"), "update", OperationStatus::Pending);
            crate::store::operation::append_operation(&inner.connection, &mut op, &json!({})).unwrap();
        }
    }
    for index in 0..m {
        let row = UploadRow { key: format!("moments:t{index}:upload"), seq: 100 + index as u64, source: Some("/source".into()), parts: 1, batch: None, fields: None, upload_id: None, confirmed_parts: 0, attempts: 0, completed: false, failed: None, created_at: 1, inline: false, size: 9, sha256: "hash".into(), path: "/mutation".into(), body: "{}".into(), on_failure: OnFailure::Keep, steps_path: None, steps_body: None, transfer_id: 100 + index as u64, cycle_id: 1, signed_id: None, aborted: false, staged: false, failed_at: None, blob_created_at: None, op_id: None, etags: vec![] };
        let batch = StoreBatch { author: BatchAuthor::Transfer, id: index as u64 + 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Upload { row, kind: UploadKind::SyncUpload }] };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    }
    SQL.with(|count| count.set(0));
    take_comparisons();
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_sql));
    let (_, operations, transfers) = db.read_queue(0).unwrap();
    assert_eq!((operations.len(), transfers.len()), (n, m));
    queue_of(&operations).unwrap();
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
    (SQL.with(|count| count.get()), take_comparisons())
}

#[test]
fn dlv_operations_work_linear() {
    let small = queue_work(2, 1);
    let large = queue_work(40, 8);
    assert_eq!(small.0, large.0, "SQL query count must not grow with the queue");
    assert!(large.1 <= 4 * (40 + 8), "comparisons must be linear: {large:?}");
}

fn nested_work(size: usize) -> usize {
    use crate::schema::{FieldLink, FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let mut user = FieldSpec::object("user").nullable();
    user.link = Some(FieldLink { model: "User".into(), id: "userId".into() });
    user.fields = vec![FieldSpec::scalar("name", ScalarKind::Str).nullable()];
    let mut reactions = FieldSpec::array("reactions").optional();
    reactions.fields = vec![FieldSpec::scalar("userId", ScalarKind::Id).nullable(), user];
    let records = ModelSpec::new("Record", vec![FieldSpec::scalar("id", ScalarKind::Id), reactions]);
    let users = ModelSpec::new("User", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).nullable()]);
    let db = Store::open_models(None, vec![records, users], None).unwrap();
    for index in 0..size {
        crate::store::fixtures::put(&db, "User", json!({"id": format!("u{index}"), "name": "current"}));
        crate::store::fixtures::put(&db, "Record", json!({"id": format!("r{index}"), "reactions": [{"userId": format!("u{index}"), "user": {"name": "old"}}]}));
    }
    SQL.with(|count| count.set(0));
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_sql));
    let rows = db.query(&crate::store::query::Query::new("Record")).unwrap();
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
    assert_eq!(rows.len(), size);
    assert!(rows.iter().all(|row| row.data["reactions"][0]["user"]["name"] == "current"));
    SQL.with(|count| count.get())
}

#[test]
fn dlv_nested_refs_work_linear() {
    let small = nested_work(2);
    let large = nested_work(40);
    assert_eq!(small, large, "SQL query count must not grow with referenced rows");
}

fn edge_work(size: usize) -> usize {
    let db = Store::open_models(None, all_specs(), None).unwrap();
    {
        let inner = db.lock();
        for index in 0..size {
            let id = format!("r{index}");
            inner.connection.execute("INSERT INTO scope_member (model, scope_key, row_id, order_key) VALUES (?1, ?2, ?3, ?3)", rusqlite::params!["moments", "scope", id]).unwrap();
            inner.connection.execute("INSERT INTO scope_edge (model, scope_key, row_id, name, text) VALUES (?1, ?2, ?3, 'tag', 'value')", rusqlite::params!["moments", "scope", id]).unwrap();
        }
    }
    let ids: Vec<String> = (0..size).map(|index| format!("r{index}")).collect();
    let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
    SQL.with(|count| count.set(0));
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_sql));
    let edges = db.read(&["moments"], |reader| reader.scope_edges("moments", "scope", &wanted)).unwrap();
    db.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
    assert_eq!(edges.members.len(), size);
    assert!(edges.members.values().all(|fields| fields.len() == 1 && fields[0].0 == "tag"));
    SQL.with(|count| count.get())
}

#[test]
fn dlv_edges_work_linear() {
    let small = edge_work(2);
    let large = edge_work(40);
    assert_eq!(small, large, "SQL query count must not grow with requested edges");
}
