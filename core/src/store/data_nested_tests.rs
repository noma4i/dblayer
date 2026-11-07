//! Cells of `data.md` for nested values: a local write checks them by the declared type before it plans anything.

use serde_json::{json, Value};

use crate::schema::{ActionDeclaration, FieldSpec, MergeRule, ModelSpec, RootShape, SchemaError};
use crate::store::fixtures::{id, row};
use crate::store::{CoreError, Store};
use crate::value::ScalarKind;

fn typed(mut field: FieldSpec, name: &str, members: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.into());
    field.fields = members;
    field.optional()
}

fn notes() -> Store {
    let user = typed(FieldSpec::object("user"), "Person", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut draft = typed(FieldSpec::object("draft"), "Draft", vec![FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    draft.merge = Some(MergeRule::Local);
    let mut spec = ModelSpec::new(
        "notes",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            typed(FieldSpec::object("media"), "Media", vec![FieldSpec::scalar("kind", ScalarKind::Str).optional(), FieldSpec::scalar("width", ScalarKind::Int).optional()]),
            typed(FieldSpec::array("replies"), "Reply", vec![FieldSpec::scalar("body", ScalarKind::Str).optional(), user]),
            FieldSpec::list("tags", ScalarKind::Str).optional(),
            FieldSpec::raw("meta").optional(),
            draft,
        ],
    );
    spec.actions.push(ActionDeclaration::new("touch", RootShape::Upsert));
    let db = Store::open_models(None, vec![spec], None).unwrap();
    crate::store::fixtures::put(&db, "notes", json!({"id": "n1", "title": "kept"}));
    db
}

fn verdict<T>(result: Result<T, CoreError>) -> String {
    match result {
        Ok(_) => "ok".into(),
        Err(CoreError::UnknownField { field, .. }) => format!("unknown {field}"),
        Err(CoreError::Schema(SchemaError::InvalidValue(path))) => format!("invalid {path}"),
        Err(other) => format!("other {other}"),
    }
}

#[test]
fn data_nested_write_rejects_path() {
    let db = notes();
    let cases: Vec<(Value, &str)> = vec![
        (json!({"media": {"kind": "IMAGE", "id": "x"}}), "unknown media.id"),
        (json!({"media": {"width": "wide"}}), "invalid media.width"),
        (json!({"replies": [{"body": "a"}, {"body": "b", "nope": 1}]}), "unknown replies[1].nope"),
        (json!({"replies": [{"body": "a", "user": {"id": "u1", "extra": true}}]}), "unknown replies[0].user.extra"),
        (json!({"replies": [{"body": 7}]}), "invalid replies[0].body"),
        (json!({"tags": ["a", {"b": 1}]}), "invalid tags[1]"),
        (json!({"meta": {"anything": {"goes": [1, "x"]}}}), "ok"),
        (json!({"media": {"kind": "IMAGE", "width": 3}, "replies": [{"body": "a", "user": {"id": "u1"}}]}), "ok"),
    ];
    for (values, expected) in cases {
        let created = verdict(db.plan_create("notes", row(values.clone()), Default::default()));
        let updated = verdict(db.plan_update("notes", &id("n1"), row(values.clone()), Default::default()));
        let acted = verdict(db.plan_act("notes", "touch", &id("n1"), row(values.clone()), Default::default()));
        assert_eq!((created.as_str(), updated.as_str(), acted.as_str()), (expected, expected, expected), "{values}");
    }
}

#[test]
fn data_nested_write_rejects_path_on_set_local() {
    let db = notes();
    for (value, expected) in [(json!({"text": "hi", "nope": 1}), "unknown draft.nope"), (json!({"text": 5}), "other value for local field draft of notes is invalid"), (json!({"text": "hi"}), "ok")] {
        assert_eq!(verdict(db.plan_set_local("notes", &id("n1"), "draft", value.clone())), expected, "{value}");
    }
}

fn parsed(name: &str) -> FieldSpec {
    serde_json::from_value(json!({"name": name, "kind": {"type": "raw"}, "parse_json": true, "nullable": true})).unwrap()
}

#[test]
fn data_parse_json() {
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
    assert_ne!(crate::store::ddl::fingerprint(&sample(field.clone())).unwrap(), crate::store::ddl::fingerprint(&sample(plain.clone())).unwrap());
    assert_ne!(crate::ffi::wire::wire_layout(&sample(field)), crate::ffi::wire::wire_layout(&sample(plain)));
}
