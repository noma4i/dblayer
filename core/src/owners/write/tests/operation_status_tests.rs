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
fn status_text_round_trips() {
    for status in [OperationStatus::Pending, OperationStatus::Committed, OperationStatus::Failed, OperationStatus::RolledBack] {
        assert_eq!(OperationStatus::from_sql(status.as_sql()), Some(status));
    }
    assert_eq!(OperationStatus::from_sql("unknown"), None);
}
