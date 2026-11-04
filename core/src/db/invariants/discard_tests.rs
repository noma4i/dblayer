use serde_json::json;

use super::fixtures::*;
use crate::db::fixtures::*;
use crate::db::DbError;
use crate::syncstate::SyncState;

#[test]
fn inv_op_09_discard_takes_back_a_pending_operation_and_refuses_a_sending_one() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.as_app().update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    assert_eq!(db.as_app().discard("moments", &rid("m1"), None).unwrap(), 1);
    assert_eq!(snapshot(&db).1, 0);
    assert_eq!(snapshot(&db).2.as_deref(), Some("one"));
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Synced);

    let created = db.as_app().create("moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
    assert_eq!(db.as_app().discard("moments", &created.id, None).unwrap(), 1);
    assert_eq!(db.find("moments", &created.id).unwrap(), None);

    db.as_app().update("moments", &rid("m1"), obj(json!({"text": "three"}))).unwrap();
    start(&db, only_seq(&db));
    let before = snapshot(&db);
    assert!(matches!(db.as_app().discard("moments", &rid("m1"), None), Err(DbError::InFlight { .. })));
    assert_eq!(snapshot(&db), before);
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Sending);
}

