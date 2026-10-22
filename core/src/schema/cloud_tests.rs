use serde_json::json;

use crate::cloud::fixtures::{notes_spec, profiles_spec};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

#[test]
fn inv_cloud_12_the_cloud_list_accepts_only_declared_syncable_attributes() {
    let mut files = notes_spec(&["title"]);
    files.fields.push(FieldSpec::file("photo").optional());
    files.cloud = vec!["photo".into()];
    let mut memory = notes_spec(&["title"]);
    memory.storage = Storage::Memory;
    let mut server_field = profiles_spec();
    server_field.cloud = vec!["name".into()];
    let mut ephemeral = notes_spec(&["title"]);
    ephemeral.ephemeral.push(crate::schema::EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))));
    ephemeral.cloud = vec!["typing".into()];
    let cases: Vec<(&str, ModelSpec, Option<&str>)> = vec![
        ("local model", notes_spec(&["title", "rank", "tags"]), None),
        ("server model local field", profiles_spec(), None),
        ("empty list", notes_spec(&[]), None),
        ("unknown attribute", notes_spec(&["missing"]), Some("is not declared")),
        ("id", notes_spec(&["id"]), Some("cannot be id or ephemeral")),
        ("duplicate", notes_spec(&["title", "title"]), Some("listed twice")),
        ("file", files, Some("6.4.0")),
        ("memory model", memory, Some("memory model cannot sync")),
        ("server field of a disk model", server_field, Some("must be local")),
        ("ephemeral", ephemeral, Some("cannot be id or ephemeral")),
    ];
    for (name, spec, expected) in cases {
        let outcome = spec.validate(&[]).err().map(|error| error.to_string());
        match expected {
            None => assert_eq!(outcome, None, "{name}"),
            Some(text) => assert!(outcome.as_deref().is_some_and(|message| message.contains(text)), "{name}: {outcome:?}"),
        }
    }
}

#[test]
fn inv_cloud_12_the_cloud_key_reads_from_the_model_json_and_an_empty_list_is_omitted() {
    let mut spec = notes_spec(&["title", "rank"]);
    let text = serde_json::to_string(&spec).unwrap();
    assert!(text.contains("\"cloud\":[\"title\",\"rank\"]"));
    assert_eq!(serde_json::from_str::<ModelSpec>(&text).unwrap().cloud, vec!["title", "rank"]);
    spec.cloud.clear();
    let bare = serde_json::to_string(&spec).unwrap();
    assert!(!bare.contains("cloud"));
    assert!(serde_json::from_str::<ModelSpec>(&bare).unwrap().cloud.is_empty());
}
