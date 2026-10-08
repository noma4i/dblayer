use super::*;
use crate::identity::RowId;
use crate::schema::{Sort, SortField};
use crate::value::{OrderValue, SortDirection};
use serde_json::{json, Map, Value};

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn by_created_and_name() -> Sort {
    Sort::Fields(vec![
        SortField { column: "createdAt".into(), direction: SortDirection::Asc },
        SortField { column: "name".into(), direction: SortDirection::Desc },
    ])
}

#[test]
fn server_order_delegates_ordering() {
    let id = RowId::new("1").unwrap();
    let source = row(json!({"createdAt": 1}));
    assert!(order_values(&Sort::ServerOrder, &source).is_empty());
    assert!(directions(&Sort::ServerOrder).is_empty());
    assert_eq!(order_key(&Sort::ServerOrder, &source, &id), None);
}

#[test]
fn fields_keep_values_and_directions() {
    let source = row(json!({"createdAt": 5, "name": "a"}));
    assert_eq!(order_values(&by_created_and_name(), &source), vec![OrderValue::Number(5.0), OrderValue::Text("a".into())]);
    assert_eq!(directions(&by_created_and_name()), vec![SortDirection::Asc, SortDirection::Desc]);
}

#[test]
fn missing_null_and_container_values_sort_as_missing() {
    let sort = by_created_and_name();
    assert_eq!(order_values(&sort, &row(json!({"createdAt": null}))), vec![OrderValue::Missing, OrderValue::Missing]);
    assert_eq!(
        order_values(&sort, &row(json!({"createdAt": [1], "name": {}}))),
        vec![OrderValue::Missing, OrderValue::Missing]
    );
}

#[test]
fn booleans_sort_as_numbers() {
    let source = row(json!({"createdAt": true, "name": false}));
    assert_eq!(order_values(&by_created_and_name(), &source), vec![OrderValue::Number(1.0), OrderValue::Number(0.0)]);
}

#[test]
fn keys_follow_the_field_order_per_direction() {
    let id = RowId::new("1").unwrap();
    for (direction, ascending) in [(SortDirection::Asc, true), (SortDirection::Desc, false)] {
        let sort = Sort::Fields(vec![SortField { column: "createdAt".into(), direction }]);
        let first = order_key(&sort, &row(json!({"createdAt": 1})), &id).unwrap();
        let second = order_key(&sort, &row(json!({"createdAt": 2})), &id).unwrap();
        assert_eq!(first < second, ascending, "{direction:?}");
    }
}
