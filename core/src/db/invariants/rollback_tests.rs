use serde_json::json;

use super::fixtures::*;
use crate::db::fixtures::*;

fn text(db: &crate::db::Db) -> Option<String> {
    db.find("moments", &rid("m1")).unwrap().and_then(|row| row.text("text").map(str::to_string))
}

#[test]
fn inv_rollback_08_a_field_the_server_overwrote_after_the_operation_is_not_restored_by_its_rollback() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let seq = only_seq(&db);
    start(&db, seq);
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "three"}));
    let landed = text(&db);
    assert_eq!(landed.as_deref(), Some("three"));
    db.roll_back(seq).unwrap();
    assert_eq!(text(&db), landed, "landed {landed:?}");
}

#[test]
fn inv_rollback_09_a_rollback_after_a_restart_gives_the_same_result_as_before_it() {
    for started in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("1.sqlite");
        let first = crate::db::Db::open(&path, all_specs(), None).unwrap();
        put(&first, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        first.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
        first.update("moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
        if started {
            start(&first, first.outbox().unwrap()[0].operation.id.0);
        }
        drop(first);
        let reopened = crate::db::Db::open(&path, all_specs(), None).unwrap();
        let oldest = reopened.outbox().unwrap()[0].operation.id.0;
        reopened.roll_back(oldest).unwrap();
        let row = reopened.find("moments", &rid("m1")).unwrap().unwrap();
        assert_eq!((row.text("text"), row.text("userId")), (Some("one"), Some("u2")), "started {started}");
        assert_eq!(reopened.outbox().unwrap().len(), 1);
    }
}
