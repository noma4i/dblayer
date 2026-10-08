use super::*;
use crate::identity::RowId;
use crate::schema::ActionMode;

fn operation(dedupe_key: Option<&str>) -> Operation {
    let mut operation = Operation::new("message:create", ActionMode::Request, vec![], 1, 2);
    operation.dedupe_key = dedupe_key.map(String::from);
    operation
}

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
    assert_eq!(recovered_status(true, Pending), DeliveryUnknown);
    assert_eq!(recovered_status(false, Pending), Failed);
    assert_eq!(recovered_status(true, DeliveryUnknown), DeliveryUnknown);
    assert_eq!(recovered_status(false, Failed), Failed);
    assert_eq!(dedupe_namespace(&operation(Some("message-1"))), Some("message:create".to_string()));
    assert_eq!(dedupe_namespace(&operation(None)), None);
    assert_eq!(dedupe_namespace(&operation(Some(""))), None);
}

#[test]
fn open_statuses_are_pending_failed_and_delivery_unknown() {
    use OperationStatus::*;
    let open: Vec<bool> = [Pending, Failed, DeliveryUnknown, Committed, RolledBack].iter().map(|s| is_open(*s)).collect();
    assert_eq!(open, [true, true, true, false, false]);
}

#[test]
fn status_text_round_trips() {
    for status in ALL_STATUSES {
        assert_eq!(OperationStatus::from_sql(status.as_sql()), Some(status));
    }
    assert_eq!(OperationStatus::DeliveryUnknown.as_sql(), "delivery_unknown");
    assert_eq!(OperationStatus::from_sql("unknown"), None);
}

#[test]
fn operation_round_trips_through_json() {
    let mut operation = Operation::new("message:create", ActionMode::Durable, vec![RowId::new("row-1").unwrap()], 4, 5);
    operation.id = OperationId(3);
    operation.status = OperationStatus::DeliveryUnknown;
    operation.temp_id = Some(RowId::new("temp-1").unwrap());
    operation.transport_started = true;
    operation.dedupe_key = Some("message-3".into());
    let encoded = serde_json::to_string(&operation).unwrap();
    assert_eq!(serde_json::from_str::<Operation>(&encoded).unwrap(), operation);
    assert!(encoded.contains("\"delivery_unknown\""));
}
