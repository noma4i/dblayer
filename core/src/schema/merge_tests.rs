use serde_json::{json, Value};

use super::MergeRule;
use super::MergeRule::*;

fn ladder() -> MergeRule {
    Ladder { tiers: vec![vec!["PENDING".into(), "PROGRESSING".into()], vec!["COMPLETED".into(), "FAILED".into()]] }
}

#[test]
fn a_rule_decides_what_an_arriving_value_may_replace() {
    let cases: Vec<(&str, MergeRule, Option<Value>, Value, Value)> = vec![
        ("present keeps a value against null", Present, Some(json!("a")), Value::Null, json!("a")),
        ("present takes a new value", Present, Some(json!("a")), json!("b"), json!("b")),
        ("present takes null when nothing is stored", Present, None, Value::Null, Value::Null),
        ("non_empty keeps a value against an empty string", NonEmpty, Some(json!("u")), json!(""), json!("u")),
        ("non_empty keeps a list against an empty list", NonEmpty, Some(json!([1])), json!([]), json!([1])),
        ("non_empty takes a value when the stored one is empty", NonEmpty, Some(json!("")), json!("u"), json!("u")),
        ("positive keeps a size against zero", Positive, Some(json!(640)), json!(0), json!(640)),
        ("positive takes a bigger size", Positive, Some(json!(640)), json!(720), json!(720)),
        ("monotonic keeps a higher number", Monotonic, Some(json!(80)), json!(40), json!(80)),
        ("monotonic takes a higher number", Monotonic, Some(json!(40)), json!(80), json!(80)),
        ("monotonic keeps a later date", Monotonic, Some(json!("2026-01-02T00:00:00.000Z")), json!("2026-01-01T00:00:00.000Z"), json!("2026-01-02T00:00:00.000Z")),
        ("monotonic keeps a value against null", Monotonic, Some(json!(80)), Value::Null, json!(80)),
        ("ladder never steps down a tier", ladder(), Some(json!("COMPLETED")), json!("PROGRESSING"), json!("COMPLETED")),
        ("ladder moves inside a tier", ladder(), Some(json!("COMPLETED")), json!("FAILED"), json!("FAILED")),
        ("ladder steps up", ladder(), Some(json!("PENDING")), json!("COMPLETED"), json!("COMPLETED")),
        ("ladder accepts a value it does not know", ladder(), Some(json!("COMPLETED")), json!("SKIPPED"), json!("SKIPPED")),
    ];
    for (name, rule, stored, incoming, expected) in cases {
        assert_eq!(rule.apply(stored.as_ref(), incoming), expected, "{name}");
    }
}

#[test]
fn keys_merge_an_object_key_by_key_and_keep_the_object_against_null() {
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
