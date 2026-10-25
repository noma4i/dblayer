use super::*;

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
fn ledger_removes_terminal_rows_recovers_by_transport_flag_and_namespaces_by_action_key() {
    use OperationStatus::*;
    assert!(!removes_row(Pending));
    assert!(!removes_row(DeliveryUnknown));
    assert!(!removes_row(Failed));
    assert!(removes_row(Committed));
    assert!(removes_row(RolledBack));
    let recovered = |transport_started, status| OperationFacts { status, transport_started }.recovered();
    assert_eq!(recovered(true, Pending), DeliveryUnknown);
    assert_eq!(recovered(false, Pending), Pending);
    assert_eq!(recovered(true, DeliveryUnknown), DeliveryUnknown);
    assert_eq!(recovered(false, Failed), Failed);
}

#[test]
fn open_statuses_are_pending_failed_and_delivery_unknown() {
    use OperationStatus::*;
    let open: Vec<bool> = [Pending, Failed, DeliveryUnknown, Committed, RolledBack].iter().map(|s| is_open(*s)).collect();
    assert_eq!(open, [true, true, true, false, false]);
}

#[test]
fn status_text_round_trips() {
    for status in [OperationStatus::Pending, OperationStatus::DeliveryUnknown, OperationStatus::Committed, OperationStatus::Failed, OperationStatus::RolledBack] {
        assert_eq!(OperationStatus::from_sql(status.as_sql()), Some(status));
    }
    assert_eq!(OperationStatus::DeliveryUnknown.as_sql(), "delivery_unknown");
    assert_eq!(OperationStatus::from_sql("unknown"), None);
}
