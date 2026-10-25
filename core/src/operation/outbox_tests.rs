use super::*;
use crate::identity::{ModelKey, RowId};
use OperationStatus::*;

fn id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn operation(seq: i64, model: &str, status: OperationStatus, rows: &[&str], temp: Option<&str>, started: bool) -> Operation {
    let mut operation = Operation::new(&Operation::action_key(model, "send"), rows.iter().map(|r| id(r)).collect(), 0, seq);
    operation.id = OperationId(seq);
    operation.status = status;
    operation.temp_id = temp.map(id);
    operation.transport_started = started;
    operation
}

#[test]
fn open_row_ids_include_pending_rows_and_temp_id() {
    let queue = OutboxQueue::new(vec![
        operation(1, "messages", Pending, &["1"], Some("2"), false),
        operation(2, "messages", Committed, &["3"], None, false),
    ]);
    assert_eq!(queue.open_row_ids(&ModelKey::new("messages")), [id("1"), id("2")].into_iter().collect());
}

#[test]
fn open_row_ids_for_other_model_is_empty() {
    let queue = OutboxQueue::new(vec![operation(1, "profiles", Pending, &["1"], None, false)]);
    assert!(queue.open_row_ids(&ModelKey::new("messages")).is_empty());
}
