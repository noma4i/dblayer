use serde_json::json;

use crate::db::fixtures::*;
use crate::db::*;
use crate::query::Query;

#[test]
fn inv_row_04_every_write_is_visible_to_a_read_and_to_a_new_observer_as_soon_as_it_returns() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let observed = |db: &Db| {
        let (log, callback) = recorder();
        let _subscription = db.observe(Query::new("moments"), callback).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        let seen = changes(&log).last().map(texts).unwrap_or_default();
        seen
    };
    let created = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "a"}))).unwrap();
    assert_eq!(db.find("moments", &created.id).unwrap().unwrap().text("text"), Some("a"));
    assert_eq!(observed(&db), vec!["a"]);
    db.update(&db.admit().unwrap(), "moments", &created.id, obj(json!({"text": "b"}))).unwrap();
    assert_eq!(db.find("moments", &created.id).unwrap().unwrap().text("text"), Some("b"));
    assert_eq!(observed(&db), vec!["b"]);
    db.destroy(&db.admit().unwrap(), "moments", &created.id).unwrap();
    assert!(db.find("moments", &created.id).unwrap().is_none());
    assert!(observed(&db).is_empty());
}

#[test]
fn inv_row_10_an_unknown_model_field_or_value_type_fails_the_call_and_writes_nothing() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    assert!(matches!(db.create(&db.admit().unwrap(), "ghosts", obj(json!({"text": "x"}))), Err(DbError::UnknownModel(_))));
    assert!(matches!(db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "x", "nope": 1}))), Err(DbError::UnknownField { .. })));
    let kept = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "kept"}))).unwrap();
    let revision = db.revision_seq().unwrap();
    assert!(matches!(db.update(&db.admit().unwrap(), "moments", &kept.id, obj(json!({"nope": 1}))), Err(DbError::UnknownField { .. })));
    assert_eq!(db.revision_seq().unwrap(), revision);
    assert!(db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": 5}))).is_err());
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 1);
    assert_eq!(db.outbox().unwrap().len(), 1);
}
