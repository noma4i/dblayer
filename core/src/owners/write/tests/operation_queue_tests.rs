use crate::identity::RowId;
use crate::owners::write::operation_queue::{queue_of, QueueStatus};
use crate::owners::write::operation_status::{Operation, OperationId, OperationStatus};

fn operation(seq: i64, key: &str, status: OperationStatus, row: &str) -> Operation {
    let mut operation = Operation::new(key, vec![RowId::new(row).unwrap()], 0, seq);
    operation.id = OperationId(seq);
    operation.status = status;
    operation
}

/// P-DLV-24: the queue shows the open operations in `seq` order and a later operation of a row waits behind the latest earlier failed one of that row.
#[test]
fn the_queue_shows_open_operations_and_the_failed_one_each_waits_behind() {
    let queue = queue_of(&[
        operation(1, "Moment:send", OperationStatus::Failed, "a"),
        operation(2, "Moment:send", OperationStatus::Committed, "a"),
        operation(3, "Moment:send", OperationStatus::Pending, "a"),
        operation(4, "Moment:send", OperationStatus::Pending, "b"),
        operation(5, "Moment:upload:photo", OperationStatus::Sending, "a"),
    ])
    .unwrap();
    let shown: Vec<(i64, QueueStatus, Option<i64>)> = queue.iter().map(|item| (item.seq, item.status, item.blocked_by)).collect();
    assert_eq!(shown, [(1, QueueStatus::Failed, None), (3, QueueStatus::Pending, Some(1)), (4, QueueStatus::Pending, None), (5, QueueStatus::Sending, Some(1))]);
}

/// P-DLV-24: an open operation whose key the queue cannot split fails the read, it is not skipped.
#[test]
fn an_open_operation_with_a_malformed_key_fails_the_read() {
    let refused = queue_of(&[operation(1, "Moment:send", OperationStatus::Pending, "a"), operation(2, "broken", OperationStatus::Pending, "a")]);
    assert!(refused.is_err_and(|error| error.contains("malformed key broken")));
}
