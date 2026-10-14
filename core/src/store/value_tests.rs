use super::fixtures::*;
use super::*;
use crate::commit::DestroyReason;
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
    let store = Store::open_in_memory().unwrap();
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
fn no_column_holds_the_free_shaped_value_as_json() {
    let store = store_with_blobs();
    put(&store, full());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM pragma_table_info('row_blobs') WHERE name = 'rest'"), 0);
    assert!(count(&store, "SELECT COUNT(*) FROM row_blobs__value WHERE row_id = 'b1' AND field = 'data'") > 5);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_blobs__value WHERE text LIKE '{%' OR text LIKE '[%'"), 0);
}

#[test]
fn changing_one_free_field_leaves_the_others_untouched() {
    let store = store_with_blobs();
    put(&store, full());
    exec(&store, "CREATE TEMP TABLE writes (field TEXT); CREATE TEMP TRIGGER node_written AFTER INSERT ON row_blobs__value BEGIN INSERT INTO writes VALUES (NEW.field); END;");
    let mut next = full();
    next["meta"] = json!({"k": "w"});
    put(&store, next.clone());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM writes WHERE field != 'meta'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM writes WHERE field = 'meta'"), 2);
    assert_eq!(store.point("blobs", "b1").unwrap().unwrap(), row(next));
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
