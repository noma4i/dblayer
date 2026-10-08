use super::support::*;
use super::*;
use serde_json::json;

#[test]
fn belongs_to_returns_the_parent_row() {
    let db = booted();
    seed_tree(&db);
    let moment = db.find("moments", &rid("m1")).unwrap().unwrap();
    let author = db.belongs_to("moments", &moment, "author").unwrap().unwrap();
    assert_eq!(author.id.as_str(), "u1");
}

#[test]
fn belongs_to_with_null_foreign_key_is_none() {
    let db = booted();
    put(&db, "moments", json!({"id": "m9", "userId": null, "text": "x"}));
    let moment = db.find("moments", &rid("m9")).unwrap().unwrap();
    assert!(db.belongs_to("moments", &moment, "author").unwrap().is_none());
}

#[test]
fn has_many_returns_only_the_children_of_the_row() {
    let db = booted();
    seed_tree(&db);
    let moment = db.find("moments", &rid("m1")).unwrap().unwrap();
    let mut ids: Vec<String> = db.has_many("moments", &moment, "vibes").unwrap().iter().map(|row| row.id.to_string()).collect();
    ids.sort();
    assert_eq!(ids, vec!["v1", "v2"]);
    let other = db.find("moments", &rid("m2")).unwrap().unwrap();
    assert!(db.has_many("moments", &other, "vibes").unwrap().is_empty());
}

#[test]
fn has_many_count_is_a_count_query() {
    let db = booted();
    seed_tree(&db);
    let moment = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!(db.has_many_count("moments", &moment, "vibes").unwrap(), 2);
    assert_eq!(db.cache_stats().misses, 1);
}

#[test]
fn association_of_the_wrong_kind_fails() {
    let db = booted();
    seed_tree(&db);
    let moment = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert!(matches!(db.has_many("moments", &moment, "author"), Err(DbError::WrongAssociation { .. })));
    assert!(matches!(db.belongs_to("moments", &moment, "vibes"), Err(DbError::WrongAssociation { .. })));
}

#[test]
fn unknown_association_fails() {
    let db = booted();
    seed_tree(&db);
    let moment = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert!(matches!(db.has_many("moments", &moment, "ghosts"), Err(DbError::UnknownAssociation { .. })));
}

#[test]
fn has_many_query_can_be_observed() {
    let db = booted();
    seed_tree(&db);
    let query = db.has_many_query("moments", &rid("m1"), "vibes").unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    put(&db, "vibes", json!({"id": "v3", "momentId": "m1", "kind": "fire"}));
    put(&db, "vibes", json!({"id": "v4", "momentId": "m2", "kind": "fire"}));
    assert_eq!(calls(&log), vec![2, 3]);
}

#[test]
fn has_one_returns_the_single_child_or_none() {
    let db = booted();
    seed_tree(&db);
    let ann = db.find("users", &rid("u1")).unwrap().unwrap();
    assert_eq!(db.has_one("users", &ann, "latest").unwrap().unwrap().id.as_str(), "m1");
    put(&db, "users", json!({"id": "u3", "name": "Cy"}));
    let cy = db.find("users", &rid("u3")).unwrap().unwrap();
    assert!(db.has_one("users", &cy, "latest").unwrap().is_none());
}

#[test]
fn has_one_with_several_children_returns_one_of_them() {
    let db = booted();
    seed_tree(&db);
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    let ann = db.find("users", &rid("u1")).unwrap().unwrap();
    let found = db.has_one("users", &ann, "latest").unwrap().unwrap();
    assert!(["m1", "m3"].contains(&found.id.as_str()));
}

#[test]
fn references_follow_the_id_array_in_order_and_skip_missing_rows() {
    let db = booted();
    seed_tree(&db);
    put(&db, "users", json!({"id": "u3", "name": "Cy", "friendIds": ["u2", "ghost", "u1", 7]}));
    let cy = db.find("users", &rid("u3")).unwrap().unwrap();
    let ids: Vec<String> = db.references("users", &cy, "friends").unwrap().iter().map(|row| row.id.to_string()).collect();
    assert_eq!(ids, vec!["u2", "u1"]);
}

#[test]
fn references_without_the_array_are_empty() {
    let db = booted();
    seed_tree(&db);
    let ann = db.find("users", &rid("u1")).unwrap().unwrap();
    assert!(db.references("users", &ann, "friends").unwrap().is_empty());
    put(&db, "users", json!({"id": "u4", "friendIds": []}));
    let empty = db.find("users", &rid("u4")).unwrap().unwrap();
    assert!(db.references("users", &empty, "friends").unwrap().is_empty());
}

#[test]
fn has_one_and_references_reject_other_association_kinds() {
    let db = booted();
    seed_tree(&db);
    let ann = db.find("users", &rid("u1")).unwrap().unwrap();
    assert!(matches!(db.has_one("users", &ann, "moments"), Err(DbError::WrongAssociation { .. })));
    assert!(matches!(db.references("users", &ann, "latest"), Err(DbError::WrongAssociation { .. })));
    assert!(matches!(db.has_many("users", &ann, "latest"), Err(DbError::WrongAssociation { .. })));
    assert!(matches!(db.has_one("users", &ann, "ghost"), Err(DbError::UnknownAssociation { .. })));
}
