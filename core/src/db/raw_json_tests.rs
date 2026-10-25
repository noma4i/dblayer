use super::fixtures::{obj, put, rid};
use super::*;
use crate::commit::ScopeRef;
use crate::identity::{ModelKey, ListKey};
use crate::schema::{Coverage, EphemeralSpec, FieldSpec, MergeRule, ModelSpec, RelationSpec, Storage};
use crate::value::ScalarKind;
use serde_json::{json, Value};

fn parsed(name: &str) -> FieldSpec {
    serde_json::from_value(json!({"name": name, "kind": {"type": "raw"}, "parse_json": true, "nullable": true})).unwrap()
}

#[test]
fn edge_fields_reject_json_parsing_with_an_explicit_error() {
    let mut spec = ModelSpec::new("samples", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut relation = RelationSpec::new("all", &[]);
    relation.is_global = true;
    relation.edge_fields.push(parsed("data"));
    spec.relations.push(relation);
    assert_eq!(spec.validate(&[]).unwrap_err().to_string(), "edge field data cannot parse JSON");
}

#[test]
fn json_text_fields_accept_only_valid_json_strings() {
    let field = parsed("data");
    for (input, expected) in [
        (json!("{\"a\":[1,true,null,{\"b\":[]}]}"), Some(json!({"a":[1,true,null,{"b":[]}]}))),
        (json!("[1, {\"a\":false}]"), Some(json!([1,{"a":false}]))),
        (json!("\"text\""), Some(json!("text"))),
        (json!("\"{\\\"n\\\":1}\""), Some(json!("{\"n\":1}"))),
        (json!("false"), Some(json!(false))),
        (json!("null"), Some(Value::Null)),
        (json!(""), None), (json!("{"), None), (json!("invalid"), None),
        (json!({"a": 1}), None), (json!([1]), None), (json!(12), None), (json!(true), None), (Value::Null, None),
    ] {
        assert_eq!(field.decode(&input), expected, "{input}");
    }
    let plain = FieldSpec::raw("data").nullable();
    let sample = |field| ModelSpec::new("samples", vec![FieldSpec::scalar("id", ScalarKind::Id), field]);
    assert_ne!(crate::ddl::fingerprint(&sample(field.clone())), crate::ddl::fingerprint(&sample(plain.clone())));
    assert_ne!(crate::ffi::wire::wire_layout(&sample(field)), crate::ffi::wire::wire_layout(&sample(plain)));
}

#[test]
fn parsed_values_survive_storage_canonical_normalization_and_other_field_patches() {
    let mut meta = FieldSpec::object("meta");
    meta.app_type = Some("Meta".into());
    meta.fields = vec![parsed("data")];
    let mut items = FieldSpec::array("items");
    items.app_type = Some("Item".into());
    items.fields = vec![parsed("data")];
    let spec = ModelSpec::new("samples", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("label", ScalarKind::Str), parsed("data"), meta, items]);
    let db = Db::open_in_memory(vec![spec], None).unwrap();
    let request = ReplaceRequest::new(ScopeRef::new(ModelKey::new("samples"), ListKey::from_raw("all")), Coverage::Complete);
    for (i, (input, expected)) in [
        (json!("{\"values\":[1,null,{\"nested\":[true]}]}"), Some(json!({"values":[1,null,{"nested":[true]}]}))),
        (json!("\"text\""), Some(json!("text"))),
        (json!("\"{\\\"n\\\":1}\""), Some(json!("{\"n\":1}"))),
        (json!("invalid"), None), (json!(""), None), (json!({"v": 1}), None), (json!(false), None), (Value::Null, None),
    ].into_iter().enumerate() {
        let id = format!("s{i}");
        db.replace(&request, vec![obj(json!({"id":id,"label":"first","data":input,"meta":{"data":input},"items":[{"data":input}]}))]).unwrap();
        let check = || {
            let row = db.find("samples", &rid(&id)).unwrap().unwrap();
            assert_eq!(row.get("data").filter(|v| !v.is_null()), expected.as_ref(), "top {input}");
            assert_eq!(row.get("meta").unwrap().get("data").filter(|v| !v.is_null()), expected.as_ref(), "object {input}");
            assert_eq!(row.get("items").unwrap()[0].get("data").filter(|v| !v.is_null()), expected.as_ref(), "list {input}");
        };
        check();
        db.replace(&request, vec![obj(json!({"id":id,"label":"second"}))]).unwrap();
        check();
        let saved = db.shared.store.point("samples", &id).unwrap().unwrap();
        put(&db, "samples", Value::Object(saved));
        check();
    }
}

#[test]
fn parsed_defaults_ephemeral_and_local_values_keep_their_decoded_objects() {
    let mut local = parsed("local");
    local.merge = Some(MergeRule::Local);
    let mut spec = ModelSpec::new("samples", vec![FieldSpec::scalar("id", ScalarKind::Id), parsed("data").with_default(json!({"enabled":true})), local]);
    spec.ephemeral.push(EphemeralSpec::new(parsed("live").with_default(json!({"enabled":false}))));
    let db = Db::open_in_memory(vec![spec], None).unwrap();
    let request = ReplaceRequest::new(ScopeRef::new(ModelKey::new("samples"), ListKey::from_raw("all")), Coverage::Complete);
    db.replace(&request, vec![obj(json!({"id":"s1"}))]).unwrap();
    assert_eq!(db.find("samples", &rid("s1")).unwrap().unwrap().get("data"), Some(&json!({"enabled":true})));
    assert_eq!(db.find("samples", &rid("s1")).unwrap().unwrap().get("live"), Some(&json!({"enabled":false})));
    db.set_ephemeral("samples", &rid("s1"), obj(json!({"live":"{\"enabled\":true}"}))).unwrap();
    db.set_local("samples", &rid("s1"), "local", json!("{\"nested\":[false,null]}" )).unwrap();
    let row = db.find("samples", &rid("s1")).unwrap().unwrap();
    assert_eq!(row.get("live"), Some(&json!({"enabled":true})));
    assert_eq!(row.get("local"), Some(&json!({"nested":[false,null]})));
    assert!(db.set_ephemeral("samples", &rid("s1"), obj(json!({"live":"invalid"}))).is_err());
    assert!(db.set_local("samples", &rid("s1"), "local", json!({"nested":[]})).is_err());
}

#[test]
fn local_creates_and_updates_decode_nested_json_text() {
    let mut meta = FieldSpec::object("meta");
    meta.app_type = Some("Meta".into());
    meta.fields = vec![parsed("data")];
    let mut spec = ModelSpec::new("samples", vec![FieldSpec::scalar("id", ScalarKind::Id), meta]);
    spec.storage = Storage::Local;
    let db = Db::open_in_memory(vec![spec], None).unwrap();
    let row = db.create("samples", obj(json!({"meta":{"data":"{\"n\":1}"}}))).unwrap();
    assert_eq!(row.get("meta"), Some(&json!({"data":{"n":1}})));
    let changed = db.update("samples", &row.id, obj(json!({"meta":{"data":"[true,null]"}}))).unwrap();
    assert_eq!(changed.get("meta"), Some(&json!({"data":[true,null]})));
}
