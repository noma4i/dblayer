use serde_json::{json, Value};

use super::MergeRule;
use super::MergeRule::*;

fn ladder() -> MergeRule {
    Ladder { tiers: vec![vec!["PENDING".into(), "PROGRESSING".into()], vec!["COMPLETED".into(), "FAILED".into()]] }
}

type Case = (&'static str, Option<Value>, Value, Value);

fn run(rule: &MergeRule, cases: Vec<Case>) {
    for (name, stored, incoming, expected) in cases {
        assert_eq!(rule.apply(stored.as_ref(), incoming), expected, "{name}");
    }
}

#[test]
fn data_merge_present() {
    run(&Present, vec![
        ("keeps a value against null", Some(json!("a")), Value::Null, json!("a")),
        ("takes any other value", Some(json!("a")), json!(""), json!("")),
        ("takes a new value", Some(json!("a")), json!("b"), json!("b")),
        ("takes null when nothing is stored", None, Value::Null, Value::Null),
    ]);
}

#[test]
fn data_merge_non_empty() {
    run(&NonEmpty, vec![
        ("keeps a value against an empty string", Some(json!("u")), json!(""), json!("u")),
        ("keeps a list against an empty list", Some(json!([1])), json!([]), json!([1])),
        ("keeps an object against an empty object", Some(json!({"a": 1})), json!({}), json!({"a": 1})),
        ("keeps a value against null", Some(json!("u")), Value::Null, json!("u")),
        ("takes a value when the stored one is empty", Some(json!("")), json!("u"), json!("u")),
        ("writes an empty value over an empty one", Some(json!([])), json!({}), json!({})),
    ]);
}

#[test]
fn data_merge_positive() {
    run(&Positive, vec![
        ("keeps a size against zero", Some(json!(640)), json!(0), json!(640)),
        ("keeps a size against a negative number", Some(json!(640)), json!(-1), json!(640)),
        ("keeps a size against null", Some(json!(640)), Value::Null, json!(640)),
        ("takes a smaller positive size", Some(json!(640)), json!(320), json!(320)),
        ("takes any value over zero", Some(json!(0)), json!(0), json!(0)),
        ("takes a size over a negative one", Some(json!(-5)), json!(7), json!(7)),
    ]);
}

#[test]
fn data_merge_monotonic() {
    run(&Monotonic, vec![
        ("keeps a higher number", Some(json!(80)), json!(40), json!(80)),
        ("takes a higher number", Some(json!(40)), json!(80), json!(80)),
        ("takes an equal number", Some(json!(40)), json!(40), json!(40)),
        ("keeps a value against null", Some(json!(80)), Value::Null, json!(80)),
        ("keeps an exact large integer", Some(json!(9_007_199_254_740_993_i64)), json!(9_007_199_254_740_992_i64), json!(9_007_199_254_740_993_i64)),
        ("takes an exact large integer", Some(json!(9_007_199_254_740_992_i64)), json!(9_007_199_254_740_993_i64), json!(9_007_199_254_740_993_i64)),
        ("keeps a later date", Some(json!("2026-01-02T00:00:00.000Z")), json!("2026-01-01T00:00:00.000Z"), json!("2026-01-02T00:00:00.000Z")),
        ("takes a later date", Some(json!("2026-01-01T00:00:00.000Z")), json!("2026-01-02T00:00:00.000Z"), json!("2026-01-02T00:00:00.000Z")),
    ]);
}

#[test]
fn data_merge_tiers() {
    run(&ladder(), vec![
        ("never steps down a tier", Some(json!("COMPLETED")), json!("PROGRESSING"), json!("COMPLETED")),
        ("moves inside a tier", Some(json!("COMPLETED")), json!("FAILED"), json!("FAILED")),
        ("moves inside the lowest tier", Some(json!("PROGRESSING")), json!("PENDING"), json!("PENDING")),
        ("steps up", Some(json!("PENDING")), json!("COMPLETED"), json!("COMPLETED")),
        ("ignores null", Some(json!("COMPLETED")), Value::Null, json!("COMPLETED")),
        ("takes a value outside all tiers", Some(json!("COMPLETED")), json!("SKIPPED"), json!("SKIPPED")),
        ("replaces a stored value outside all tiers", Some(json!("SKIPPED")), json!("PENDING"), json!("PENDING")),
    ]);
}

#[test]
fn data_merge_rule_fits_type() {
    use crate::schema::{DeclarationError, FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let declared = |field: FieldSpec| ModelSpec::new("things", vec![FieldSpec::scalar("id", ScalarKind::Id), field]).validate(&[]);
    let with = |field: FieldSpec, rule: MergeRule| FieldSpec { merge: Some(rule), ..field };
    assert!(declared(with(FieldSpec::scalar("n", ScalarKind::Int).optional(), Positive)).is_ok());
    assert!(declared(with(FieldSpec::scalar("t", ScalarKind::Date).optional(), Monotonic)).is_ok());
    for (field, rule) in [
        (FieldSpec::scalar("s", ScalarKind::Str).optional(), Positive),
        (FieldSpec::scalar("s", ScalarKind::Str).optional(), Monotonic),
        (FieldSpec::file("f").optional(), Present),
        (FieldSpec::file("f").optional(), NonEmpty),
        (FieldSpec::scalar("s", ScalarKind::Str).optional(), StableList),
        (FieldSpec::scalar("s", ScalarKind::Str).optional(), Keys { keys: Default::default() }),
        (FieldSpec::scalar("s", ScalarKind::Int).optional(), ladder()),
    ] {
        let name = field.name.clone();
        assert!(matches!(declared(with(field, rule.clone())), Err(DeclarationError::MergeRuleMismatch(found)) if found == name), "{rule:?} on {name}");
    }
}

#[test]
fn data_merge_keys() {
    let rule = Keys { keys: [("fileUrl".to_string(), NonEmpty), ("width".to_string(), Positive), ("transcodeStatus".to_string(), ladder()), ("transcodeProgress".to_string(), Monotonic)].into() };
    let stored = json!({"fileUrl": "f", "width": 640, "transcodeStatus": "COMPLETED", "transcodeProgress": 100, "kind": "video"});
    assert_eq!(rule.apply(Some(&stored), Value::Null), stored);
    let late = json!({"fileUrl": "", "width": 0, "transcodeStatus": "PROGRESSING", "transcodeProgress": 40, "kind": null, "thumbUrl": "t"});
    assert_eq!(rule.apply(Some(&stored), late), json!({"fileUrl": "f", "width": 640, "transcodeStatus": "COMPLETED", "transcodeProgress": 100, "kind": "video", "thumbUrl": "t"}));
}

#[test]
fn a_server_row_merges_by_the_rule_of_each_field() {
    use crate::schema::{FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let mut media = FieldSpec::object("media").nullable().optional();
    media.merge = Some(Keys { keys: [("transcodeProgress".to_string(), Monotonic)].into() });
    let mut avatar = FieldSpec::scalar("avatarUrl", ScalarKind::Str).nullable().optional();
    avatar.merge = Some(NonEmpty);
    let spec = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str).nullable().optional(), media, avatar]);
    let stored = json!({"id": "1", "body": "hi", "media": {"transcodeProgress": 80}, "avatarUrl": "a"}).as_object().unwrap().clone();
    let patch = spec.patch(json!({"id": "1", "body": null, "media": {"transcodeProgress": 40}, "avatarUrl": ""}).as_object().unwrap());
    let merged = spec.merged(Some(&stored), &patch).unwrap();
    assert_eq!(Value::Object(merged), json!({"id": "1", "body": null, "media": {"transcodeProgress": 80}, "avatarUrl": "a"}));
}

#[test]
fn data_merge_stable_list() {
    use crate::schema::{FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let mut ids = FieldSpec::list("momentIds", ScalarKind::Id).nullable().optional();
    ids.merge = Some(StableList);
    let spec = ModelSpec::new("results", vec![FieldSpec::scalar("id", ScalarKind::Id), ids]);
    assert!(spec.validate(&[]).is_ok());
    let stored = json!({"id": "r", "momentIds": ["a", "b", "c"]}).as_object().unwrap().clone();
    let cases = [
        ("reordered and new", json!(["d", "c", "a", "e"]), json!(["a", "c", "d", "e"])),
        ("vanished", json!(["c"]), json!(["c"])),
        ("duplicates", json!(["b", "b", "x", "x"]), json!(["b", "x"])),
        ("null keeps the stored list", Value::Null, json!(["a", "b", "c"])),
    ];
    for (name, incoming, expected) in cases {
        let patch = spec.patch(json!({"id": "r", "momentIds": incoming}).as_object().unwrap());
        let merged = spec.merged(Some(&stored), &patch).unwrap();
        assert_eq!(merged["momentIds"], expected, "{name}");
    }
}
