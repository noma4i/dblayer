use super::*;
use serde_json::json;

#[test]
fn v3_selected_null_paths_clear_and_missing_paths_preserve_fields() {
    let models = [
        (ModelSpec::new("notes", vec![
            FieldSpec::scalar("replyId", ScalarKind::Str).nullable().read_from("replyTo.id"),
            FieldSpec::scalar("sourceId", ScalarKind::Str).nullable().read_from("forwardedFrom.id"),
            FieldSpec::scalar("attachmentKind", ScalarKind::Str).nullable().read_from("media.kind"),
        ]), json!({"replyId": "note-2", "sourceId": "writer-1", "attachmentKind": "photo"})),
        (ModelSpec::new("readers", vec![
            FieldSpec::scalar("town", ScalarKind::Str).nullable().read_from("profile.location.town"),
            FieldSpec::scalar("latitude", ScalarKind::Num).nullable().read_from("profile.location.latitude"),
            FieldSpec::scalar("quiet", ScalarKind::Bool).nullable().read_from("settings.quiet"),
            FieldSpec::scalar("radius", ScalarKind::Num).with_default(json!(25)).read_from("settings.search.radius"),
        ]), json!({"town": "Harbor", "latitude": 12.5, "quiet": true, "radius": 50})),
    ];
    let mut failures = Vec::new();
    for (model, previous) in models {
        for field in model.fields.iter().filter(|field| field.from.is_some()) {
            let path: Vec<_> = field.from.as_ref().unwrap().split('.').collect();
            let stored = Map::from_iter([(field.name.clone(), previous[&field.name].clone())]);
            for depth in 0..path.len() {
                for (tail, expected) in [(Value::Null, field.default.clone().filter(|_| !field.nullable).unwrap_or(Value::Null)), (json!({}), previous[&field.name].clone())] {
                    let selected = tail.is_null();
                    let mut input = tail;
                    for segment in path[..=depth].iter().rev() {
                        input = json!({*segment: input});
                    }
                    if !selected && depth + 1 == path.len() {
                        continue;
                    }
                    let patch = model.patch(input.as_object().unwrap());
                    let row = model.merged(Some(&stored), &patch).unwrap();
                    if row.get(&field.name) != Some(&expected) {
                        failures.push(format!("{}.{} {input}: {:?}, expected {expected}", model.key, field.name, row.get(&field.name)));
                    }
                }
            }
            assert!(!model.patch(&Map::new()).contains_key(&field.name));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
