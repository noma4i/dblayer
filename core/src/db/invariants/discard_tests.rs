use serde_json::json;

use super::fixtures::*;
use crate::db::fixtures::*;
use crate::db::DbError;
use crate::syncstate::SyncState;

#[test]
fn inv_op_09_discard_takes_back_a_pending_operation_and_refuses_a_sending_one() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    assert_eq!(db.discard("moments", &rid("m1")).unwrap(), 1);
    assert_eq!(snapshot(&db).1, 0);
    assert_eq!(snapshot(&db).2.as_deref(), Some("one"));
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Synced);

    let created = db.create("moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
    assert_eq!(db.discard("moments", &created.id).unwrap(), 1);
    assert_eq!(db.find("moments", &created.id).unwrap(), None);

    db.update("moments", &rid("m1"), obj(json!({"text": "three"}))).unwrap();
    start(&db, only_seq(&db));
    let before = snapshot(&db);
    assert!(matches!(db.discard("moments", &rid("m1")), Err(DbError::InFlight { .. })));
    assert_eq!(snapshot(&db), before);
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Sending);
}

