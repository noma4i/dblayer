use super::variables::*;
use crate::schema::RemoteShape;
use serde_json::{Map, Value};
use crate::schema::{Coverage, PageDirection};
use serde_json::json;

fn params() -> Map<String, Value> {
    json!({"userId": "u1"}).as_object().unwrap().clone()
}

fn connection(direction: PageDirection) -> RemoteShape {
    RemoteShape::Connection { direction, coverage: Coverage::Page, max_pages: None, cursor: None }
}

#[test]
fn forward_first_page_sends_first_without_after() {
    let variables = load_variables(&params(), &connection(PageDirection::Forward), 20, None, &Default::default());
    assert_eq!(variables, json!({"userId": "u1", "first": 20}));
}

#[test]
fn forward_next_page_sends_first_and_after() {
    let variables = load_variables(&params(), &connection(PageDirection::Forward), 20, Some("c1"), &Default::default());
    assert_eq!(variables, json!({"userId": "u1", "first": 20, "after": "c1"}));
}

#[test]
fn backward_page_sends_last_and_before() {
    let variables = load_variables(&params(), &connection(PageDirection::Backward), 10, Some("c9"), &Default::default());
    assert_eq!(variables, json!({"userId": "u1", "last": 10, "before": "c9"}));
}

#[test]
fn list_and_single_send_only_params() {
    assert_eq!(load_variables(&params(), &RemoteShape::List { coverage: Coverage::Complete }, 20, None, &Default::default()), json!({"userId": "u1"}));
    assert_eq!(load_variables(&Map::new(), &RemoteShape::Single, 20, None, &Default::default()), json!({}));
}

fn sequenced() -> RemoteShape {
    let cursor = crate::schema::CursorSpec { variable: "afterSequence".into(), from: "lastSequenceNumber".into(), numeric: true };
    RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: Some(cursor) }
}

#[test]
fn custom_cursor_names_its_variable_and_sends_no_page_size() {
    assert_eq!(load_variables(&params(), &sequenced(), 20, None, &Default::default()), json!({"userId": "u1"}));
    assert_eq!(load_variables(&params(), &sequenced(), 20, Some("7"), &Default::default()), json!({"userId": "u1", "afterSequence": 7}));
}

#[test]
fn data_scope_variables() {
    use crate::schema::VariableSource;
    use std::collections::BTreeMap;
    let mapping = BTreeMap::from([
        ("statusFilter".to_string(), VariableSource::Param { name: "userId".to_string() }),
        ("includeGifts".to_string(), VariableSource::Constant { value: json!(false) }),
    ]);
    let variables = load_variables(&params(), &connection(PageDirection::Forward), 20, None, &mapping);
    assert_eq!(variables, json!({"statusFilter": "u1", "includeGifts": false, "first": 20}));
}

#[test]
fn bindings_read_the_original_params_and_choose_whether_to_pass_the_rest() {
    use std::collections::BTreeMap;
    use crate::schema::VariableSource::{Constant, Param};
    let params = json!({"a": "first", "b": "second", "rest": 3}).as_object().unwrap().clone();
    for (mapping, declared, remaining) in [
        (BTreeMap::from([("a".into(), Param { name: "b".into() }), ("b".into(), Param { name: "a".into() })]), json!({"a": "second", "b": "first"}), json!({"a": "second", "b": "first", "rest": 3})),
        (BTreeMap::from([("x".into(), Param { name: "a".into() }), ("y".into(), Param { name: "a".into() })]), json!({"x": "first", "y": "first"}), json!({"x": "first", "y": "first", "b": "second", "rest": 3})),
        (BTreeMap::from([("a".into(), Constant { value: json!(false) }), ("b".into(), Param { name: "a".into() })]), json!({"a": false, "b": "first"}), json!({"a": false, "b": "first", "rest": 3})),
    ] {
        assert_eq!(Value::Object(declared_variables(&params, &mapping)), declared);
        assert_eq!(Value::Object(mapped_variables(&params, &mapping)), remaining);
    }
}
