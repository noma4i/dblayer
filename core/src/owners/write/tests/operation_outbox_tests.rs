use crate::owners::write::operation_status::{Operation, OperationId, OperationStatus};
use super::operation_outbox::*;
use crate::identity::{ModelKey, RowId};
use OperationStatus::*;

fn id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn operation(seq: i64, model: &str, status: OperationStatus, rows: &[&str], temp: Option<&str>) -> Operation {
    let mut operation = Operation::new(&Operation::action_key(model, "send"), rows.iter().map(|r| id(r)).collect(), 0, seq);
    operation.id = OperationId(seq);
    operation.status = status;
    operation.temp_id = temp.map(id);
    operation
}

#[test]
fn open_row_ids_of_a_model_include_its_pending_rows_and_temp_id() {
    let queue = OutboxQueue::new(vec![
        operation(1, "messages", Pending, &["1"], Some("2")),
        operation(2, "messages", Committed, &["3"], None),
        operation(3, "profiles", Pending, &["4"], None),
    ]);
    assert_eq!(queue.open_row_ids(&ModelKey::new("messages")), [id("1"), id("2")].into_iter().collect());
}
