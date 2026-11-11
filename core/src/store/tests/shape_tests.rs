use super::fixtures::*;
use super::*;
use crate::schema::{resolve_types, FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use serde_json::json;

fn typed(mut field: FieldSpec, name: &str, children: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.to_string());
    field.fields = children;
    field
}

fn gifts_spec() -> ModelSpec {
    let item = vec![
        FieldSpec::scalar("title", ScalarKind::Str),
        FieldSpec::scalar("price", ScalarKind::Num).nullable(),
        FieldSpec::scalar("owner", ScalarKind::Id).nullable(),
        FieldSpec::list("tags", ScalarKind::Str),
        typed(FieldSpec::object("size"), "GiftSize", vec![FieldSpec::scalar("w", ScalarKind::Int), FieldSpec::scalar("h", ScalarKind::Int)]),
    ];
    let node = vec![FieldSpec::scalar("name", ScalarKind::Str), typed(FieldSpec::array("children"), "GiftNode", vec![])];
    let mut owner = FieldSpec::scalar("ownerId", ScalarKind::Id);
    owner.from = Some("owner_id".to_string());
    let mut spec = ModelSpec::new(
        "gifts",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::list("labels", ScalarKind::Str),
            FieldSpec::list("counts", ScalarKind::Int).nullable(),
            typed(FieldSpec::array("items"), "GiftItem", item),
            typed(FieldSpec::object("tree"), "GiftNode", node).nullable(),
            typed(FieldSpec::object("meta"), "GiftMeta", vec![owner, FieldSpec::enumeration("state", &["ready", "done"])]).nullable(),
        ],
    );
    resolve_types(std::slice::from_mut(&mut spec));
    spec
}

fn store_with_gifts() -> Store {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&[gifts_spec()], None).unwrap();
    store.mark_ready();
    store
}

#[test]
fn nullable_defaults_preserve_absence_null_and_values_after_reopen() {
    let variants = vec![FieldSpec::variant("item", "Item", vec![FieldSpec::scalar("text", ScalarKind::Str)])];
    let cases = [
        (FieldSpec::scalar("value", ScalarKind::Str), json!("text")),
        (FieldSpec::enumeration("value", &["text"]), json!("text")),
        (FieldSpec::list("value", ScalarKind::Str), json!(["text"])),
        (typed(FieldSpec::object("value"), "Value", vec![FieldSpec::scalar("text", ScalarKind::Str)]), json!({"text": "text"})),
        (typed(FieldSpec::array("value"), "Value", vec![FieldSpec::scalar("text", ScalarKind::Str)]), json!([{"text": "text"}])),
        (FieldSpec::raw("value"), json!({"text": "text"})),
        (FieldSpec::union("value", "Choice", variants.clone()), json!({"__typename": "Item", "text": "text"})),
        (FieldSpec::union_array("value", "Choice", variants), json!([{"__typename": "Item", "text": "text"}])),
    ];
    for (field, value) in cases {
        for location in 0..5 {
            let field = field.clone().nullable().with_default(value.clone());
            let field = match location {
                0 => field,
                1 => typed(FieldSpec::object("meta"), "Meta", vec![field]),
                2 => typed(FieldSpec::array("meta"), "Meta", vec![field]),
                3 => FieldSpec::union("meta", "Meta", vec![FieldSpec::variant("item", "Item", vec![field])]),
                _ => FieldSpec::union("meta", "Meta", vec![FieldSpec::variant("item", "Item", vec![])]).with_shared(vec![field]),
            };
            let spec = ModelSpec::new("gifts", vec![FieldSpec::scalar("id", ScalarKind::Id), field]);
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("values.sqlite");
            for input in [None, Some(serde_json::Value::Null), Some(value.clone()), Some(serde_json::Value::Null)] {
                let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
                store.boot(std::slice::from_ref(&spec), None).unwrap();
                store.mark_ready();
                let mut fields = serde_json::Map::new();
                if let Some(value) = &input { fields.insert("value".into(), value.clone()); }
                let data = match location {
                    0 => { fields.insert("id".into(), json!("g1")); json!(fields) }
                    1 => json!({"id": "g1", "meta": fields}),
                    2 => json!({"id": "g1", "meta": [fields]}),
                    _ => { fields.insert("__typename".into(), json!("Item")); json!({"id": "g1", "meta": fields}) }
                };
                put(&store, data);
                drop(store);
                let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
                store.boot(std::slice::from_ref(&spec), None).unwrap();
                store.mark_ready();
                let row = store.point("gifts", "g1").unwrap().unwrap();
                let actual = match location {
                    0 => row.get("value"),
                    2 => row.get("meta").and_then(|value| value.get(0)).and_then(|value| value.get("value")),
                    _ => row.get("meta").and_then(|value| value.get("value")),
                };
                assert_eq!(actual, input.as_ref(), "location={location}, field={:?}", spec.fields[1]);
            }
        }
    }
}

fn put(store: &Store, value: serde_json::Value) {
    store.commit(envelope(store, vec![upsert("gifts", value)], vec![])).unwrap();
}

fn tree() -> serde_json::Value {
    json!({"name": "root", "children": [{"name": "a", "children": [{"name": "a1", "children": []}]}, {"name": "b", "children": []}]})
}

#[test]
fn typed_siblings_stay_in_columns_when_a_member_has_unrestricted_values() {
    for free in [true, false] {
        let mut fields = vec![FieldSpec::scalar("title", ScalarKind::Str)];
        let mut value = json!({"title": "first"});
        if free {
            fields.push(FieldSpec::raw("extra"));
            value["extra"] = json!({"arbitrary": [null, 3, {"text": "kept"}]});
        }
        let spec = ModelSpec::new("gifts", vec![FieldSpec::scalar("id", ScalarKind::Id), typed(FieldSpec::object("meta"), "Meta", fields)]);
        let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        store.boot(&[spec], None).unwrap();
        store.mark_ready();
        put(&store, json!({"id": "g1", "meta": value}));
        assert_eq!(count(&store, "SELECT COUNT(*) FROM pragma_table_info('row_gifts') WHERE name = 'meta__title' AND type = 'TEXT'"), 1);
        assert_eq!(store.point("gifts", "g1").unwrap().unwrap().get("meta"), Some(&value), "raw member: {free}");
        value["title"] = json!("second");
        put(&store, json!({"id": "g1", "meta": value}));
        assert_eq!(store.point("gifts", "g1").unwrap().unwrap().get("meta"), Some(&value), "raw member: {free}");
    }
}

#[test]
fn lists_arrays_objects_and_recursive_trees_read_back_as_written() {
    let store = store_with_gifts();
    let full = json!({
        "id": "g1",
        "labels": ["x", "y"],
        "counts": [1, 2, 3],
        "items": [
            {"title": "one", "price": 1.5, "owner": "u1", "tags": ["p", "q"], "size": {"w": 2, "h": 3}},
            {"title": "two", "tags": [], "size": {"w": 4, "h": 5}}
        ],
        "tree": tree(),
        "meta": {"owner_id": "u9", "state": "done"}
    });
    put(&store, full.clone());
    assert_eq!(store.point("gifts", "g1").unwrap().unwrap(), row(full));
}

#[test]
fn an_empty_list_is_present_and_an_absent_list_is_absent() {
    let store = store_with_gifts();
    put(&store, json!({"id": "g1", "labels": [], "items": []}));
    put(&store, json!({"id": "g2"}));
    let first = store.point("gifts", "g1").unwrap().unwrap();
    let second = store.point("gifts", "g2").unwrap().unwrap();
    assert_eq!((first.get("labels"), first.get("items")), (Some(&json!([])), Some(&json!([]))));
    assert_eq!((second.get("labels"), second.get("items"), second.get("tree")), (None, None, None));
}

#[test]
fn unknown_keys_wrong_types_and_nulls_do_not_reach_storage() {
    let store = store_with_gifts();
    put(&store, json!({"id": "g1", "meta": {"owner_id": 42, "state": "nope", "extra": true}, "items": [{"title": "t", "tags": ["a"], "size": {"w": 1, "h": 2}, "extra": 1}, 7]}));
    let stored = store.point("gifts", "g1").unwrap().unwrap();
    assert_eq!(stored["meta"], json!({"owner_id": "42", "state": "nope"}));
    assert_eq!(stored["items"], json!([{"title": "t", "tags": ["a"], "size": {"w": 1, "h": 2}}]));
}

#[test]
fn nested_dates_keep_their_text_in_objects_and_lists() {
    for free in [false, true] {
        let mut fields = vec![FieldSpec::scalar("at", ScalarKind::Date), FieldSpec::list("dates", ScalarKind::Date)];
        if free {
            fields.push(FieldSpec::raw("extra"));
        }
        let spec = ModelSpec::new("gifts", vec![FieldSpec::scalar("id", ScalarKind::Id), typed(FieldSpec::object("meta"), "Meta", fields)]);
        let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        store.boot(&[spec], None).unwrap();
        store.mark_ready();
        put(&store, json!({"id": "g1", "meta": {"at": "2026-10-14T00:00:00Z", "dates": ["2026-10-14T10:00:00+10:00"]}}));
        let stored = store.point("gifts", "g1").unwrap().unwrap();
        assert_eq!(stored["meta"]["at"], "2026-10-14T00:00:00Z");
        assert_eq!(stored["meta"]["dates"], json!(["2026-10-14T10:00:00+10:00"]));
    }
}

#[test]
fn a_recursive_array_keeps_all_levels_in_one_table() {
    let store = store_with_gifts();
    put(&store, json!({"id": "g1", "tree": tree()}));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_gifts__tree__children"), 3);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_gifts__tree__children WHERE owner = 0"), 2);
}

#[test]
fn changing_one_list_rewrites_only_that_list() {
    let store = store_with_gifts();
    put(&store, json!({"id": "g1", "labels": ["a"], "counts": [1]}));
    exec(
        &store,
        "CREATE TEMP TABLE writes (name TEXT);
         CREATE TEMP TRIGGER labels_written AFTER INSERT ON row_gifts__labels BEGIN INSERT INTO writes VALUES ('labels'); END;
         CREATE TEMP TRIGGER counts_written AFTER INSERT ON row_gifts__counts BEGIN INSERT INTO writes VALUES ('counts'); END;",
    );
    put(&store, json!({"id": "g1", "labels": ["a", "b"], "counts": [1]}));
    assert_eq!(text(&store, "SELECT group_concat(name) FROM writes"), Some("labels,labels".into()));
    assert_eq!(store.point("gifts", "g1").unwrap().unwrap()["labels"], json!(["a", "b"]));
}

#[test]
fn destroying_a_row_removes_its_child_rows() {
    let store = store_with_gifts();
    put(&store, json!({"id": "g1", "labels": ["a"], "items": [{"title": "t", "tags": ["z"], "size": {"w": 1, "h": 1}}], "tree": tree()}));
    store.commit(envelope(&store, vec![destroy("gifts", "g1", crate::store::DestroyReason::Local)], vec![])).unwrap();
    for table in ["row_gifts__labels", "row_gifts__items", "row_gifts__items__tags", "row_gifts__tree__children"] {
        assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM {table}")), 0, "{table}");
    }
}

#[test]
fn an_identical_typed_upsert_is_a_noop() {
    let store = store_with_gifts();
    let value = json!({"id": "g1", "labels": ["a"], "items": [{"title": "t", "tags": [], "size": {"w": 1, "h": 1}}], "tree": tree(), "meta": {"owner_id": 5}});
    put(&store, value.clone());
    let repeat = store.commit(envelope(&store, vec![upsert("gifts", value)], vec![])).unwrap();
    assert!(repeat.is_noop());
}

#[test]
fn named_types_resolve_across_models_and_a_recursive_name_keeps_no_members() {
    let declared = typed(FieldSpec::object("a"), "Shared", vec![FieldSpec::scalar("n", ScalarKind::Int)]);
    let reused = typed(FieldSpec::array("b"), "Shared", vec![]);
    let unknown = typed(FieldSpec::object("c"), "Nowhere", vec![]);
    let mut models = vec![
        ModelSpec::new("one", vec![FieldSpec::scalar("id", ScalarKind::Id), declared]),
        ModelSpec::new("two", vec![FieldSpec::scalar("id", ScalarKind::Id), reused, unknown]),
    ];
    resolve_types(&mut models);
    assert_eq!(models[1].fields[1].fields.len(), 1);
    assert_eq!(models[1].fields[2].app_type, None);
    let children = &gifts_spec().fields[4].fields[1];
    let inner = &children.fields[1];
    assert_eq!((children.fields.len(), children.app_type.as_deref()), (2, Some("GiftNode")));
    assert_eq!((inner.fields.len(), inner.app_type.as_deref()), (0, Some("GiftNode")));
}
