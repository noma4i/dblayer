use super::error::SyncError;
use super::normalize::*;
use serde_json::{Map, Value};
use crate::sync::rig::{rig, Rig};
use serde_json::json;

fn nodes(value: Value) -> Vec<Map<String, Value>> {
    value.as_array().unwrap().iter().map(|node| node.as_object().unwrap().clone()).collect()
}

#[test]
fn nested_author_becomes_user_row_and_sets_foreign_key() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"id": "m1", "text": "hi", "author": {"id": "u1", "name": "Ann", "ignored": 1}}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 1);
    assert_eq!(normalized.roots[0]["userId"], json!("u1"));
    assert_eq!(normalized.related.len(), 1);
    assert_eq!(normalized.related[0].0.as_str(), "users");
    assert_eq!(normalized.related[0].1["name"], json!("Ann"));
    assert!(!normalized.roots[0].contains_key("author"));
}

#[test]
fn shared_author_is_reported_once() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([
        {"id": "m1", "author": {"id": "u1", "name": "Ann"}},
        {"id": "m2", "author": {"id": "u1", "name": "Ann B"}}
    ]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 2);
    assert_eq!(normalized.related.len(), 1);
    assert_eq!(normalized.related[0].1["name"], json!("Ann B"));
}

#[test]
fn row_without_id_is_dropped_and_counted() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"text": "no id"}, {"id": "m2", "text": "ok"}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 1);
    assert_eq!(normalized.dropped, 1);
}

#[test]
fn invalid_author_is_skipped_but_moment_stays() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"id": "m1", "userId": "u9", "author": {"name": "no id"}}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots[0]["userId"], json!("u9"));
    assert!(normalized.related.is_empty());
}

#[test]
fn unknown_model_is_an_error() {
    let Rig { db, .. } = rig();
    assert_eq!(normalize_nodes(&db, "ghosts", &[]), Err(SyncError::UnknownModel("ghosts".to_string())));
}

fn nesting_db() -> crate::db::Db {
    use crate::schema::{AssociationKind, AssociationSpec, FieldSpec};
    use crate::value::ScalarKind;
    let mut specs = crate::sync::rig::specs();
    specs[0].associations.push(AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId"));
    let mut mentions = AssociationSpec::new("mentions", AssociationKind::References, "users", "mentionIds");
    mentions.from = Some("likes.nodes".to_string());
    let mut similar = AssociationSpec::new("similar", AssociationKind::References, "moments", "similarIds");
    similar.from = Some("compass.similarMoment".to_string());
    specs[1].fields.push(FieldSpec::list("mentionIds", ScalarKind::Id).optional());
    specs[1].fields.push(FieldSpec::list("similarIds", ScalarKind::Id).optional());
    specs[1].associations.extend([mentions, similar]);
    crate::db::Db::open_in_memory(specs, None).unwrap()
}

#[test]
fn nested_children_of_has_many_become_rows_that_point_at_the_parent() {
    let db = nesting_db();
    let input = nodes(json!([{"id": "u1", "name": "Ann", "moments": [{"id": "m1", "text": "a"}, {"id": "m2", "text": "b"}]}]));
    let normalized = normalize_nodes(&db, "users", &input).unwrap();
    let mut related: Vec<_> = normalized.related.iter().map(|(model, row)| (model.as_str().to_string(), row["id"].clone(), row["userId"].clone())).collect();
    related.sort_by_key(|entry| entry.1.to_string());
    assert_eq!(related, vec![("moments".to_string(), json!("m1"), json!("u1")), ("moments".to_string(), json!("m2"), json!("u1"))]);
    assert!(!normalized.roots[0].contains_key("moments"));
}

#[test]
fn references_read_their_rows_from_a_path_and_keep_the_ids_on_the_parent() {
    let db = nesting_db();
    let input = nodes(json!([{"id": "m1", "likes": {"nodes": [{"id": "u2", "name": "B"}, {"id": "u3", "name": "C"}]}}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots[0]["mentionIds"], json!(["u2", "u3"]));
    let mut users: Vec<_> = normalized.related.iter().map(|(model, row)| (model.as_str(), row["id"].as_str().unwrap().to_string())).collect();
    users.sort();
    assert_eq!(users, vec![("users", "u2".to_string()), ("users", "u3".to_string())]);
}

#[test]
fn a_path_walks_through_lists() {
    let db = nesting_db();
    let input = nodes(json!([{"id": "m1", "compass": [{"similarMoment": {"id": "m7", "text": "x"}}, {"similarMoment": null}, {"similarMoment": {"id": "m8"}}]}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots[0]["similarIds"], json!(["m7", "m8"]));
    assert_eq!(normalized.related.len(), 2);
}

#[test]
fn absent_nested_data_leaves_the_parent_untouched() {
    let db = nesting_db();
    let normalized = normalize_nodes(&db, "moments", &nodes(json!([{"id": "m1"}]))).unwrap();
    assert!(!normalized.roots[0].contains_key("mentionIds"));
}
