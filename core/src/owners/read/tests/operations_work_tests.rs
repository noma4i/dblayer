use crate::identity::RowId;
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{StoreBatch, StoreOp, UploadKind, UploadRow};
use crate::owners::write::operation_queue::{queue_of, take_comparisons};
use crate::owners::write::operation_status::{Operation, OperationId, OperationStatus};
use crate::schema::OnFailure;
use crate::store::{fixtures::all_specs, Store};
use serde_json::json;

thread_local! { static SQL: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn count_sql(sql: &str) {
    if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") { SQL.with(|count| count.set(count.get() + 1)); }
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
    db.lock().connection.trace(Some(count_sql));
    let (_, operations, transfers) = db.read_queue(0).unwrap();
    assert_eq!((operations.len(), transfers.len()), (n, m));
    queue_of(&operations).unwrap();
    db.lock().connection.trace(None);
    (SQL.with(|count| count.get()), take_comparisons())
}

#[test]
fn dlv_operations_work_linear() {
    let small = queue_work(2, 1);
    let large = queue_work(40, 8);
    assert_eq!(small.0, large.0, "SQL query count must not grow with the queue");
    assert!(large.1 <= 4 * (40 + 8), "comparisons must be linear: {large:?}");
}
