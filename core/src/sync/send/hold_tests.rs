use super::{SendResult, SkipReason};

#[test]
fn an_operation_that_was_not_sent_keeps_the_later_operations_of_its_row_waiting() {
    assert!(SendResult::Deferred.keeps_row_waiting());
    assert!(SendResult::Discarded.keeps_row_waiting());
    for settled in [
        SendResult::Committed,
        SendResult::Rejected("no".to_string()),
        SendResult::DeliveryUnknown,
        SendResult::Quarantined,
        SendResult::Skipped(SkipReason::Offline),
    ] {
        assert!(!settled.keeps_row_waiting(), "{settled:?} is settled in the queue and blocks by its own state");
    }
}
