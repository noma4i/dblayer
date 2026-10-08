use super::*;
use crate::identity::RowId;
use serde_json::{json, Map, Value};

fn fields(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn fields_round_trip_without_loss() {
    let input = fields(json!({"title": "hello", "count": 7}));
    assert_eq!(decode_input(&encode_input(&input).unwrap()).unwrap(), input);
}

#[test]
fn scalar_input_is_rejected_during_decode() {
    assert_eq!(decode_input(b"7"), Err(InputFault::NotAnObject));
    assert_eq!(decode_input(b"not json"), Err(InputFault::NotAnObject));
}

#[test]
fn round_trips_nested_values_and_rejects_empty_fields() {
    assert!(input_round_trips(&fields(json!({"payload": {"items": ["first", 2]}}))));
    assert!(!input_round_trips(&Map::new()));
    assert_eq!(encode_input(&Map::new()), Err(InputFault::EmptyInput));
}

#[test]
fn make_builds_pending_operation() {
    let request = EncodedOperation::make("message:create", &fields(json!({"body": "hello"})), None, 12).unwrap();
    assert_eq!(request.operation.status, OperationStatus::Pending);
    assert_eq!(request.operation.created_at, 12);
    assert_eq!(decode_input(&request.input).unwrap(), fields(json!({"body": "hello"})));
}

#[test]
fn temp_id_is_stored_in_operation_rows_and_field() {
    let temp = RowId::new("temp-1").unwrap();
    let request = EncodedOperation::make("message:create", &fields(json!({"body": "hello"})), Some(temp.clone()), 12).unwrap();
    assert_eq!(request.operation.row_ids, [temp.clone()]);
    assert_eq!(request.operation.temp_id, Some(temp));
}

#[test]
fn empty_fields_propagate_input_fault() {
    assert_eq!(EncodedOperation::make("message:create", &Map::new(), None, 12).unwrap_err(), InputFault::EmptyInput);
}

#[test]
fn resumable_statuses_are_pending_and_delivery_unknown() {
    let mut request = EncodedOperation::make("message:create", &fields(json!({"a": 1})), None, 12).unwrap();
    assert!(request.is_resumable());
    request.operation.status = OperationStatus::DeliveryUnknown;
    assert!(request.is_resumable());
    request.operation.status = OperationStatus::Committed;
    assert!(!request.is_resumable());
}
