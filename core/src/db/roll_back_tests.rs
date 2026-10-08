use super::support::*;
use super::*;
use crate::query::Query;
use serde_json::json;

fn only_seq(db: &Db) -> i64 {
    db.outbox().unwrap()[0].operation.id.0
}

#[test]
fn rolling_back_an_update_restores_the_old_values_for_observers() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    db.settle();
    db.roll_back(only_seq(&db)).unwrap();
    db.settle();
    assert_eq!(texts(changes(&log).last().unwrap()), ["one"]);
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("one"));
    assert!(db.outbox().unwrap().is_empty());
}

#[test]
fn rolling_back_a_create_removes_the_row() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.settle();
    let row = db.create("moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
    db.settle();
    db.roll_back(only_seq(&db)).unwrap();
    db.settle();
    assert_eq!(calls(&log), vec![0, 1, 0]);
    assert_eq!(changes(&log)[2].diff.removed, vec![row.id.clone()]);
    assert!(db.outbox().unwrap().is_empty());
}

#[test]
fn rolling_back_a_destroy_restores_the_cascade() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("vibes"), callback).unwrap();
    db.settle();
    db.destroy("moments", &rid("m1")).unwrap();
    db.settle();
    assert_eq!(calls(&log), vec![2, 0]);
    db.roll_back(only_seq(&db)).unwrap();
    db.settle();
    assert_eq!(calls(&log), vec![2, 0, 2]);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
}

#[test]
fn rolling_back_an_unknown_operation_fails() {
    let db = booted();
    assert!(matches!(db.roll_back(99), Err(DbError::UnknownOperation(99))));
}
