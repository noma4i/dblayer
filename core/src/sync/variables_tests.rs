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
fn mapped_variables_rename_params_and_add_constants() {
    use crate::schema::VariableSource;
    use std::collections::BTreeMap;
    let mapping = BTreeMap::from([
        ("statusFilter".to_string(), VariableSource::Param { name: "userId".to_string() }),
        ("includeGifts".to_string(), VariableSource::Constant { value: json!(false) }),
    ]);
    let variables = load_variables(&params(), &connection(PageDirection::Forward), 20, None, &mapping);
    assert_eq!(variables, json!({"statusFilter": "u1", "includeGifts": false, "first": 20}));
}
