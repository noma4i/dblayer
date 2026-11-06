use super::fixtures::*;
use super::*;
use serde_json::json;

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
