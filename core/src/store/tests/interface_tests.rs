use super::fixtures::*;
use super::*;
use crate::schema::screens_fixture::screens_spec;
use serde_json::{json, Value};

fn store_with_screens() -> Store {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&[screens_spec()], None).unwrap();
    store.mark_ready();
    store
}

fn put(store: &Store, value: Value) {
    store.commit(envelope(store, vec![upsert("screens", value)], vec![])).unwrap();
}

fn read(store: &Store, id: &str) -> Value {
    Value::Object(store.point("screens", id).unwrap().unwrap())
}

fn elements() -> Vec<Value> {
    vec![
        json!({"__typename": "SampleColumn", "id": "root", "children": ["b1", "h1"], "gap": 8.5}),
        json!({"__typename": "SampleButton", "id": "b1", "children": [], "press": {"__typename": "SampleNavigateAction", "slug": "home", "title": "Home"}, "label": {"__typename": "SampleStateRef", "path": "user.name"}, "disabled": true}),
        json!({"__typename": "SampleHeading", "id": "h1", "children": [], "text": "Hi", "tags": ["a", "b"]}),
        json!({"__typename": "SamplePressable", "id": "p1", "children": ["h1"], "press": {"__typename": "SampleOpenRecordAction", "recordId": "c9"}}),
    ]
}

fn screen(elements: Vec<Value>) -> Value {
    json!({"id": "s1", "slug": "main", "spec": {"root": "root", "elements": elements}})
}

#[test]
fn a_list_of_interface_values_reads_back_in_order_with_nested_unions_and_child_lists() {
    let store = store_with_screens();
    put(&store, screen(elements()));
    assert_eq!(read(&store, "s1"), screen(elements()));
}

#[test]
fn interface_values_live_in_one_element_table_with_the_shared_columns_declared_once() {
    let store = store_with_screens();
    put(&store, screen(elements()));
    let table = "row_screens__spec__elements";
    let column = |name: &str| count(&store, &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = '{name}'"));
    assert_eq!((column("u"), column("u__shared__id"), column("u__shared__press"), column("u__button__disabled"), column("u__heading__text")), (1, 1, 1, 1, 1));
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name GLOB 'u__*__id'")), 1);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table} WHERE row_id = 's1'")), 4);
    assert_eq!(text(&store, &format!("SELECT u FROM {table} WHERE row_id = 's1' AND pos = 1")).as_deref(), Some("SampleButton"));
}

#[test]
fn an_element_that_changes_variant_clears_the_columns_and_child_rows_of_the_old_variant() {
    let store = store_with_screens();
    put(&store, screen(elements()));
    let changed = vec![json!({"__typename": "SampleColumn", "id": "b1", "children": ["x"], "gap": 1.5})];
    put(&store, screen(changed.clone()));
    let table = "row_screens__spec__elements";
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table} WHERE row_id = 's1'")), 1);
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table} WHERE u__button__disabled IS NOT NULL OR u__heading__text IS NOT NULL OR u__shared__press IS NOT NULL")), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_screens__spec__elements__heading__tags WHERE row_id = 's1'"), 0);
    assert_eq!(read(&store, "s1"), screen(changed));
}

#[test]
fn an_element_with_an_unknown_typename_keeps_only_its_name_and_a_bad_element_is_dropped() {
    let store = store_with_screens();
    let mixed = vec![elements().remove(0), json!({"__typename": "SampleFuture", "id": "f1", "children": ["x"], "size": 3}), json!({"id": "n1"}), json!("x")];
    put(&store, screen(mixed));
    assert_eq!(read(&store, "s1")["spec"]["elements"], json!([elements()[0].clone(), {"__typename": "SampleFuture"}]));
}

#[test]
fn an_empty_list_and_an_absent_list_differ() {
    let store = store_with_screens();
    put(&store, screen(vec![]));
    put(&store, json!({"id": "s2", "spec": {"root": "r"}}));
    assert_eq!(read(&store, "s1")["spec"]["elements"], json!([]));
    assert_eq!(read(&store, "s2")["spec"].get("elements"), None);
}
