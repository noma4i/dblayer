use serde_json::json;

use crate::db::fixtures::*;
use crate::db::*;
use crate::query::Query;


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
