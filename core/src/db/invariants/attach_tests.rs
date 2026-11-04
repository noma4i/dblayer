use serde_json::json;

use super::fixtures::*;
use crate::db::fixtures::*;
use crate::syncstate::SyncState;

#[test]
fn inv_file_09_attach_and_detach_make_no_operation_and_leave_the_sync_state() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.attach(&db.admit().unwrap(), "moments", &rid("m1"), "media", b"bytes", "a.bin", "application/octet-stream").unwrap();
    assert!(db.outbox().unwrap().is_empty());
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Synced);
    assert!(db.detach(&db.admit().unwrap(), "moments", &rid("m1"), "media").unwrap());
    assert!(db.outbox().unwrap().is_empty());
    assert_eq!(state_of(&db, "moments", "m1"), SyncState::Synced);

    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let queued = db.outbox().unwrap().len();
    db.attach(&db.admit().unwrap(), "moments", &rid("m1"), "media", b"again", "b.bin", "application/octet-stream").unwrap();
    assert_eq!(db.outbox().unwrap().len(), queued);
}
