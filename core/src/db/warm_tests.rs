use super::fixtures::*;
use super::*;
use crate::query::Query;
use serde_json::json;

fn names(peeked: &Peeked) -> Vec<&str> {
    peeked.rows.iter().filter_map(|row| row.text("name")).collect()
}

#[test]
fn a_warm_query_is_peeked_from_memory_and_follows_commits() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let query = Query::new("users");
    assert!(db.peek_query(&query).is_none());
    db.warm(&query).unwrap();
    let first = db.peek_query(&query).unwrap();
    assert_eq!(names(&first), ["Ann"]);
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    let second = db.peek_query(&query).unwrap();
    assert_eq!(names(&second), ["Ann", "Bob"]);
    assert!(second.revision > first.revision);
    let before = db.counters();
    db.warm(&query).unwrap();
    db.peek_query(&query).unwrap();
    let after = db.counters();
    assert_eq!((after.observation_reads, after.rows_loaded), (before.observation_reads, before.rows_loaded), "peek and a second warm read nothing");
    assert_eq!((after.peek_hits - before.peek_hits, after.peek_misses - before.peek_misses), (1, 0));
}

#[test]
fn a_warm_query_is_not_a_subscription_and_the_warm_set_is_bounded() {
    let db = booted();
    db.warm(&Query::new("users")).unwrap();
    assert_eq!((db.observer_census().subscriptions, db.observer_census().observed_queries), (0, 0));
    for limit in 1..=20 {
        db.warm(&Query { limit: Some(limit), ..Query::new("users") }).unwrap();
    }
    let warm = (1..=20).filter(|limit| db.peek_query(&Query { limit: Some(*limit), ..Query::new("users") }).is_some()).count();
    assert_eq!(warm, 16);
    assert!(db.peek_query(&Query::new("users")).is_none());
}

#[test]
fn peek_row_answers_from_the_row_cache_only() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    db.find("users", &rid("u1")).unwrap();
    let before = db.counters();
    assert_eq!(db.peek_row("users", &rid("u1")).unwrap().text("name"), Some("Ann"));
    assert!(db.peek_row("users", &rid("ghost")).is_none());
    let after = db.counters();
    assert_eq!((after.peek_hits - before.peek_hits, after.peek_misses - before.peek_misses, after.rows_loaded - before.rows_loaded), (1, 1, 0));
}
