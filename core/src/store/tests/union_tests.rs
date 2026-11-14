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

fn messages_spec() -> ModelSpec {
    let mut tier = FieldSpec::scalar("tier", ScalarKind::Str);
    tier.from = Some("giftTier".to_string());
    let variants = vec![
        FieldSpec::variant("gift", "GiftAttachment", vec![FieldSpec::scalar("id", ScalarKind::Id), tier, FieldSpec::list("tags", ScalarKind::Str), FieldSpec::scalar("status", ScalarKind::Str).nullable()]),
        FieldSpec::variant("moment", "MomentAttachment", vec![FieldSpec::scalar("momentId", ScalarKind::Id), FieldSpec::scalar("caption", ScalarKind::Str).nullable()]),
        FieldSpec::variant(
            "profile",
            "ProfileAttachment",
            vec![typed(FieldSpec::object("user"), "UserRef", vec![FieldSpec::scalar("id", ScalarKind::Id).nullable(), FieldSpec::scalar("name", ScalarKind::Str).nullable()]).nullable()],
        ),
        FieldSpec::variant("reaction", "ReactionAttachment", vec![FieldSpec::scalar("body", ScalarKind::Str)]),
    ];
    let mut spec = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional(), FieldSpec::union("attachment", "MessageAttachment", variants).nullable()]);
    resolve_types(std::slice::from_mut(&mut spec));
    spec
}

fn store_with_messages() -> Store {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&[messages_spec()]).unwrap();
    store.mark_ready();
    store
}

fn put(store: &Store, value: serde_json::Value) {
    store.commit(envelope(vec![upsert("messages", value)], vec![])).unwrap();
}

fn read(store: &Store, id: &str) -> serde_json::Value {
    serde_json::Value::Object(store.point("messages", id).unwrap().unwrap())
}

fn variants() -> Vec<serde_json::Value> {
    vec![
        json!({"__typename": "GiftAttachment", "id": "g1", "giftTier": "gold", "tags": ["a", "b"], "status": "sent"}),
        json!({"__typename": "MomentAttachment", "momentId": "m1", "caption": "hi"}),
        json!({"__typename": "ProfileAttachment", "user": {"id": "u1", "name": "Ada"}}),
        json!({"__typename": "ReactionAttachment", "body": "+1"}),
    ]
}

#[test]
fn data_union_variant() {
    let store = store_with_messages();
    for (at, attachment) in variants().into_iter().enumerate() {
        let id = format!("m{at}");
        put(&store, json!({"id": id, "text": "t", "attachment": attachment}));
        assert_eq!(read(&store, &id)["attachment"], attachment);
    }
}

#[test]
fn an_unknown_typename_keeps_only_its_name() {
    let store = store_with_messages();
    put(&store, json!({"id": "m1", "attachment": {"__typename": "FutureAttachment", "size": 3}}));
    assert_eq!(read(&store, "m1")["attachment"], json!({"__typename": "FutureAttachment"}));
    assert_eq!(text(&store, "SELECT attachment FROM row_messages WHERE id = 'm1'").as_deref(), Some("FutureAttachment"));
    let stored = read(&store, "m1");
    put(&store, json!({"id": "m1", "attachment": stored["attachment"]}));
    assert_eq!(read(&store, "m1")["attachment"], json!({"__typename": "FutureAttachment"}));
}

#[test]
fn null_and_absent_are_the_same_state() {
    let store = store_with_messages();
    put(&store, json!({"id": "m1", "attachment": null}));
    put(&store, json!({"id": "m2"}));
    assert_eq!((read(&store, "m1").get("attachment").cloned(), read(&store, "m2").get("attachment").cloned()), (None, None));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages WHERE attachment IS NULL"), 2);
}

#[test]
fn a_value_without_a_text_typename_is_refused() {
    let store = store_with_messages();
    for bad in [json!({"body": "x"}), json!({"__typename": 7, "body": "x"}), json!("x")] {
        assert!(store.commit(envelope(vec![upsert("messages", json!({"id": "m1", "attachment": bad}))], vec![])).is_err());
        assert!(store.point("messages", "m1").unwrap().is_none());
    }
}

#[test]
fn a_new_variant_clears_the_columns_and_child_rows_of_the_old_one() {
    let store = store_with_messages();
    put(&store, json!({"id": "m1", "attachment": variants()[0]}));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages__attachment__gift__tags WHERE row_id = 'm1'"), 2);
    put(&store, json!({"id": "m1", "attachment": variants()[3]}));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages WHERE attachment__gift__tier IS NOT NULL OR attachment__gift__id IS NOT NULL"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages__attachment__gift__tags WHERE row_id = 'm1'"), 0);
    assert_eq!(read(&store, "m1")["attachment"], variants()[3]);
}

#[test]
fn an_answer_for_the_same_variant_replaces_its_members() {
    let store = store_with_messages();
    put(&store, json!({"id": "m1", "attachment": variants()[0]}));
    put(&store, json!({"id": "m1", "attachment": {"__typename": "GiftAttachment", "id": "g1", "giftTier": "silver", "tags": []}}));
    assert_eq!(read(&store, "m1")["attachment"], json!({"__typename": "GiftAttachment", "id": "g1", "giftTier": "silver", "tags": []}));
}

#[test]
fn a_union_inside_a_nested_object_and_an_array_element_is_stored_in_columns() {
    let action = |name: &str| {
        FieldSpec::union(
            name,
            "Action",
            vec![
                FieldSpec::variant("link", "LinkAction", vec![FieldSpec::scalar("url", ScalarKind::Str), FieldSpec::list("hints", ScalarKind::Str)]),
                FieldSpec::variant("close", "CloseAction", vec![FieldSpec::scalar("reason", ScalarKind::Str).nullable()]),
            ],
        )
        .nullable()
    };
    let screen = typed(FieldSpec::object("screen"), "Screen", vec![FieldSpec::scalar("title", ScalarKind::Str), action("press")]).nullable();
    let items = typed(FieldSpec::array("items"), "Item", vec![FieldSpec::scalar("key", ScalarKind::Id), action("press")]);
    let mut spec = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), screen, items]);
    resolve_types(std::slice::from_mut(&mut spec));
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&[spec]).unwrap();
    store.mark_ready();
    let value = json!({
        "id": "s1",
        "screen": {"title": "Home", "press": {"__typename": "LinkAction", "url": "u", "hints": ["a", "b"]}},
        "items": [
            {"key": "k1", "press": {"__typename": "CloseAction", "reason": "done"}},
            {"key": "k2", "press": {"__typename": "FutureAction"}},
            {"key": "k3", "press": {"__typename": "LinkAction", "url": "v", "hints": []}},
            {"key": "k4"}
        ]
    });
    store.commit(envelope(vec![upsert("screens", value.clone())], vec![])).unwrap();
    assert_eq!(serde_json::Value::Object(store.point("screens", "s1").unwrap().unwrap()), value);
}
