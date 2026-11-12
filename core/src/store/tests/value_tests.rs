use super::fixtures::*;
use super::*;
use crate::store::DestroyReason;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use serde_json::json;

fn blobs_spec() -> ModelSpec {
    ModelSpec::new(
        "blobs",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            FieldSpec::raw("data").nullable(),
            FieldSpec::object("meta").nullable(),
            FieldSpec::array("steps").nullable(),
        ],
    )
}

fn store_with_blobs() -> Store {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&[blobs_spec()], None).unwrap();
    store.mark_ready();
    store
}

fn put(store: &Store, value: serde_json::Value) {
    store.commit(envelope(store, vec![upsert("blobs", value)], vec![])).unwrap();
}

fn full() -> serde_json::Value {
    json!({
        "id": "b1",
        "title": "t",
        "data": {"a": [1, 2.5, "x", true, null, {"deep": [[], {}]}], "b": "text", "c": 9007199254740993_i64},
        "meta": {"k": "v", "n": {"m": [1]}},
        "steps": [{"s": 1}, {"s": 2}]
    })
}

#[test]
fn free_shaped_values_read_back_as_written() {
    let store = store_with_blobs();
    put(&store, full());
    assert_eq!(store.point("blobs", "b1").unwrap().unwrap(), row(full()));
}

#[test]
fn removing_a_field_or_a_row_removes_its_nodes() {
    let store = store_with_blobs();
    put(&store, full());
    put(&store, json!({"id": "b1", "title": "t", "data": null, "meta": {"k": "v"}}));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_blobs__value WHERE field IN ('data', 'steps')"), 0);
    store.commit(envelope(&store, vec![destroy("blobs", "b1", DestroyReason::Local)], vec![])).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_blobs__value"), 0);
}
