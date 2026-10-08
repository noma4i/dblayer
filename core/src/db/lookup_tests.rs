use super::fixtures::*;
use super::*;
use crate::pacing::Counters;
use crate::query::{Order, Query, Where};
use crate::value::SortDirection;
use serde_json::json;
use std::sync::Arc;

#[test]
fn find_returns_the_stored_row_and_the_second_find_hits_the_cache() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let first = db.find("users", &rid("u1")).unwrap().unwrap();
    assert_eq!(first.text("name"), Some("Ann"));
    assert_eq!(db.counters(), Counters { cache_hits: 0, cache_misses: 1, cache_evictions: 0, ..Counters::default() });
    let second = db.find("users", &rid("u1")).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(db.counters(), Counters { cache_hits: 1, cache_misses: 1, cache_evictions: 0, ..Counters::default() });
}

#[test]
fn find_of_a_missing_row_is_none() {
    let db = booted();
    assert!(db.find("users", &rid("nope")).unwrap().is_none());
}

#[test]
fn find_on_an_unknown_model_fails() {
    let db = booted();
    assert!(matches!(db.find("ghosts", &rid("g1")), Err(DbError::UnknownModel(_))));
}

#[test]
fn find_many_keeps_request_order_and_skips_missing_ids() {
    let db = booted();
    seed_tree(&db);
    let rows = db.find_many("moments", &[rid("m2"), rid("zz"), rid("m1")]).unwrap();
    let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(ids, vec!["m2", "m1"]);
}

#[test]
fn query_parses_each_row_once_while_unchanged() {
    let db = booted();
    seed_tree(&db);
    let query = Query::new("vibes").ordered(vec![Order::new("id", SortDirection::Asc)]);
    let first = db.query(&query).unwrap();
    assert_eq!(db.counters().cache_misses, 2);
    let second = db.query(&query).unwrap();
    assert_eq!(db.counters(), Counters { cache_hits: 2, cache_misses: 2, cache_evictions: 0, ..Counters::default() });
    assert!(first.iter().zip(&second).all(|(a, b)| Arc::ptr_eq(a, b)));
}

#[test]
fn query_reparses_only_the_row_that_changed() {
    let db = booted();
    seed_tree(&db);
    let query = Query::new("vibes").ordered(vec![Order::new("id", SortDirection::Asc)]);
    db.query(&query).unwrap();
    put(&db, "vibes", json!({"id": "v1", "momentId": "m1", "kind": "love"}));
    let rows = db.query(&query).unwrap();
    assert_eq!(rows[0].text("kind"), Some("love"));
    assert_eq!(db.counters().cache_misses, 3);
    assert_eq!(db.counters().cache_hits, 1);
}

#[test]
fn find_sees_a_row_changed_after_it_was_cached_by_a_landing_and_by_a_local_update() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("Ann"));
    put(&db, "users", json!({"id": "u1", "name": "Anna"}));
    assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("Anna"));
    assert_eq!(db.counters().cache_evictions, 1);
    let created = db.create("moments", obj(json!({"userId": "u1", "text": "a"}))).unwrap();
    db.find("moments", &created.id).unwrap();
    db.update("moments", &created.id, obj(json!({"text": "b"}))).unwrap();
    assert_eq!(db.find("moments", &created.id).unwrap().unwrap().text("text"), Some("b"));
}

#[test]
fn find_does_not_return_a_destroyed_row_from_the_cache() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    db.find("users", &rid("u1")).unwrap();
    db.destroy("users", &rid("u1")).unwrap();
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
}

#[test]
fn count_exists_and_first_follow_the_query() {
    let db = booted();
    seed_tree(&db);
    let all = Query::new("moments").ordered(vec![Order::new("id", SortDirection::Desc)]);
    assert_eq!(db.count(&all).unwrap(), 2);
    assert_eq!(db.count(&all.clone().limited(1)).unwrap(), 2);
    assert_eq!(db.first(&all).unwrap().unwrap().id.as_str(), "m2");
    let none = Query::new("moments").filtered(Where::equal("text", json!("zzz")));
    assert!(!db.exists(&none).unwrap());
    assert!(db.first(&none).unwrap().is_none());
    assert!(db.exists(&all).unwrap());
}

#[test]
fn exists_reads_no_payload() {
    let db = booted();
    seed_tree(&db);
    assert!(db.exists(&Query::new("moments")).unwrap());
    assert_eq!(db.counters().cache_misses, 0);
}

#[test]
fn cache_is_bounded_by_the_configured_row_limit() {
    let db = Db::open_with(None, all_specs(), None, DbConfig { cache_rows: 2 }).unwrap();
    seed_tree(&db);
    db.query(&Query::new("vibes")).unwrap();
    db.query(&Query::new("moments")).unwrap();
    assert!(db.counters().cache_evictions >= 2);
}
