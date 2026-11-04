use super::fixtures::{obj, rid};
use super::*;
use crate::schema::{ActionDeclaration, FieldSpec, MergeRule, ModelSpec, RootShape, SchemaError};
use crate::value::ScalarKind;
use serde_json::{json, Value};

fn typed(mut field: FieldSpec, name: &str, members: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.into());
    field.fields = members;
    field.optional()
}

fn notes() -> Db {
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
    Db::open_in_memory(vec![spec], None).unwrap()
}

fn verdict(result: Result<(), DbError>) -> String {
    match result {
        Ok(()) => "ok".into(),
        Err(DbError::UnknownField { field, .. }) => format!("unknown {field}"),
        Err(DbError::Schema(SchemaError::InvalidValue(path))) => format!("invalid {path}"),
        Err(other) => format!("other {other}"),
    }
}

fn cases() -> Vec<(Value, &'static str)> {
    vec![
        (json!({"media": {"kind": "IMAGE", "id": "x"}}), "unknown media.id"),
        (json!({"media": {"width": "wide"}}), "invalid media.width"),
        (json!({"replies": [{"body": "a"}, {"body": "b", "nope": 1}]}), "unknown replies[1].nope"),
        (json!({"replies": [{"body": "a", "user": {"id": "u1", "extra": true}}]}), "unknown replies[0].user.extra"),
        (json!({"replies": [{"body": 7}]}), "invalid replies[0].body"),
        (json!({"tags": ["a", {"b": 1}]}), "invalid tags[1]"),
        (json!({"meta": {"anything": {"goes": [1, "x"]}}}), "ok"),
        (json!({"media": {"kind": "IMAGE", "width": 3}, "replies": [{"body": "a", "user": {"id": "u1"}}]}), "ok"),
    ]
}

#[test]
fn nested_values_are_checked_on_create_update_and_act() {
    let db = notes();
    let kept = db.as_app().create("notes", obj(json!({"title": "kept"}))).unwrap();
    for (values, expected) in cases() {
        let revision = db.revision_seq().unwrap();
        let created = verdict(db.as_app().create("notes", obj(values.clone())).map(|_| ()));
        let updated = verdict(db.as_app().update("notes", &kept.id, obj(values.clone())).map(|_| ()));
        let acted = verdict(db.as_app().act("notes", "touch", &kept.id, obj(values.clone()), Default::default()).map(|_| ()));
        assert_eq!((created.as_str(), updated.as_str(), acted.as_str()), (expected, expected, expected), "{values}");
        if expected != "ok" {
            assert_eq!(db.revision_seq().unwrap(), revision, "{values}");
        }
    }
}

#[test]
fn nested_values_are_checked_on_set_local() {
    let db = notes();
    let kept = db.as_app().create("notes", obj(json!({"title": "kept"}))).unwrap();
    for (value, expected) in [(json!({"text": "hi", "nope": 1}), "unknown draft.nope"), (json!({"text": 5}), "other value for local field draft of notes is invalid"), (json!({"text": "hi"}), "ok")] {
        assert_eq!(verdict(db.as_app().set_local("notes", &kept.id, "draft", value.clone())), expected, "{value}");
    }
    assert_eq!(db.find("notes", &rid(kept.id.as_str())).unwrap().unwrap().get("draft"), Some(&json!({"text": "hi"})));
}
