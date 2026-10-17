use serde_json::{json, Map, Value};

use super::normalize::normalize_nodes;
use crate::db::Db;
use crate::schema::{AssociationKind, AssociationSpec, FieldSpec, ModelSpec};
use crate::sync::rig::specs;
use crate::value::ScalarKind;

fn db() -> Db {
    let mut specs = specs();
    specs[0].associations.push(AssociationSpec::new("pinned", AssociationKind::HasOne, "moments", "userId"));
    specs[0].associations.push(AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId"));
    let mut mentions = AssociationSpec::new("mentions", AssociationKind::References, "users", "mentionIds");
    mentions.from = Some("likes.nodes".to_string());
    let mut editor = AssociationSpec::new("editor", AssociationKind::BelongsTo, "users", "editorId");
    editor.from = Some("meta.editor".to_string());
    specs[1].fields.push(FieldSpec::list("mentionIds", ScalarKind::Id).optional());
    specs[1].fields.push(FieldSpec::scalar("editorId", ScalarKind::Id).nullable().optional().indexed());
    specs[1].associations.extend([mentions, editor]);
    let mut hits = ModelSpec::new("hits", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("momentId", ScalarKind::Id).optional().indexed()]);
    hits.associations.push(AssociationSpec::new("moment", AssociationKind::BelongsTo, "moments", "momentId"));
    specs.push(hits);
    Db::open_in_memory(specs, None).unwrap()
}

fn root(model: &str, node: Value) -> Map<String, Value> {
    let node = node.as_object().unwrap().clone();
    let mut normalized = normalize_nodes(&db(), model, &[node]).unwrap();
    assert!(normalized.related.is_empty(), "a null object lands no related row: {:?}", normalized.related);
    normalized.roots.remove(0)
}

#[test]
fn explicit_null_of_a_belongs_to_object_to_a_nullable_key_resets_it() {
    let row = root("moments", json!({"id": "m1", "author": null}));
    assert_eq!(row.get("userId"), Some(&Value::Null));
    assert!(!row.contains_key("author"));
}

#[test]
fn an_absent_belongs_to_object_leaves_the_key_out_of_the_patch() {
    assert!(!root("moments", json!({"id": "m1"})).contains_key("userId"));
}

#[test]
fn explicit_null_of_a_belongs_to_object_to_a_required_key_changes_nothing() {
    assert!(!root("hits", json!({"id": "h1", "moment": null})).contains_key("momentId"));
}

#[test]
fn a_null_on_the_path_of_a_belongs_to_resets_the_key_and_an_absent_leaf_does_not() {
    for (node, expected) in [
        (json!({"id": "m1", "meta": null}), Some(Value::Null)),
        (json!({"id": "m1", "meta": {"editor": null}}), Some(Value::Null)),
        (json!({"id": "m1", "meta": {}}), None),
        (json!({"id": "m1"}), None),
    ] {
        assert_eq!(root("moments", node.clone()).get("editorId").cloned(), expected, "{node}");
    }
}

#[test]
fn explicit_null_of_a_references_path_empties_the_list_and_an_absent_path_does_not() {
    assert_eq!(root("moments", json!({"id": "m1", "likes": null})).get("mentionIds"), Some(&json!([])));
    assert_eq!(root("moments", json!({"id": "m1", "likes": {"nodes": []}})).get("mentionIds"), Some(&json!([])));
    assert!(!root("moments", json!({"id": "m1"})).contains_key("mentionIds"));
}

#[test]
fn explicit_null_of_a_has_one_or_has_many_holds_no_key_and_lands_no_row() {
    let row = root("users", json!({"id": "u1", "pinned": null, "moments": null}));
    assert_eq!(row.keys().collect::<Vec<_>>(), ["id"]);
}
