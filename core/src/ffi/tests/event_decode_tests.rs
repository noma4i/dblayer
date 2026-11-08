use super::super::read::read_value;
use crate::ffi::{DbError, ReadValue};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

/// boundary_event_decode_failure (P-BND-06, delivery.md ModelEvent): a damaged payload of an event is a failure `Internal`, never an event with empty fields, while a payload that only lacks a field stays an event.
#[test]
fn boundary_event_decode_failure() {
    let wire = crate::ffi::wire::WireModel::of(&ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id)]));
    let damaged = [
        "not json",
        "[1]",
        r#"{"id":7,"area":"A","kind":"K"}"#,
        r#"{"id":"n1","area":["A"],"kind":"K"}"#,
        r#"{"id":"n1","area":"A","kind":"K","after":5}"#,
        r#"{"id":" ","area":"A","kind":"K","after":{"id":" "}}"#,
    ];
    for payload in damaged {
        let value = read_value(crate::owners::read::deliveries::Value::Event { position: 1, payload: payload.into() }, Some(&wire));
        assert!(matches!(value, Err(DbError::Internal { .. })), "{payload}: {value:?}");
    }
    let sparse = read_value(crate::owners::read::deliveries::Value::Event { position: 2, payload: r#"{"id":"n1"}"#.into() }, Some(&wire));
    assert!(matches!(sparse, Ok(ReadValue::Event { position: 2, ref area, ref kind, ref id, before: None, after: None }) if area.is_empty() && kind.is_empty() && id == "n1"), "{sparse:?}");
}
