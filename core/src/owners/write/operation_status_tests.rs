use super::operation_status::*;

#[test]
fn action_key_joins_model_and_action() {
    let key = Operation::action_key("message", "create");
    assert_eq!(key, "message:create");
    assert_eq!(Operation::split_action_key(&key), Some(("message".to_string(), "create".to_string())));
    assert_eq!(Operation::split_action_key("messagecreate"), None);
    assert_eq!(Operation::split_action_key("message:create:extra"), None);
    assert_eq!(Operation::split_action_key(":create"), None);
    assert_eq!(Operation::split_action_key("message:"), None);
}

#[test]
fn ledger_removes_terminal_rows() {
    use OperationStatus::*;
    assert!(!removes_row(Pending));
    assert!(!removes_row(Failed));
    assert!(removes_row(Committed));
    assert!(removes_row(RolledBack));
}

#[test]
fn open_statuses_are_pending_and_failed() {
    use OperationStatus::*;
    let open: Vec<bool> = [Pending, Failed, Committed, RolledBack].iter().map(|s| is_open(*s)).collect();
    assert_eq!(open, [true, true, false, false]);
}

#[test]
fn status_text_round_trips() {
    for status in [OperationStatus::Pending, OperationStatus::Committed, OperationStatus::Failed, OperationStatus::RolledBack] {
        assert_eq!(OperationStatus::from_sql(status.as_sql()), Some(status));
    }
    assert_eq!(OperationStatus::from_sql("unknown"), None);
}
