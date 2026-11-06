use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ListKey};
use crate::query::Query;
use serde_json::json;

fn only_seq(db: &Db) -> i64 {
    db.outbox().unwrap()[0].operation.id.0
}




#[test]
fn inv_rollback_03_rolling_back_an_older_update_keeps_a_newer_edit() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "A"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "B"}))).unwrap();
    let older = only_seq(&db);
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "C"}))).unwrap();
    db.roll_back(&db.admit().unwrap(), older).unwrap();
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("C"));
}

#[test]
fn inv_rollback_02_rolling_back_every_update_restores_the_original_in_either_order() {
    for oldest_first in [true, false] {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "A"}));
        db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "B"}))).unwrap();
        db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "C"}))).unwrap();
        let mut seqs: Vec<i64> = db.outbox().unwrap().iter().map(|entry| entry.operation.id.0).collect();
        if !oldest_first {
            seqs.reverse();
        }
        for seq in seqs {
            db.roll_back(&db.admit().unwrap(), seq).unwrap();
        }
        assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("A"), "oldest_first={oldest_first}");
    }
}

#[test]
fn rolling_back_a_create_with_an_explicit_id_removes_the_row_and_leaves_its_queued_edits_to_the_outbox() {
    let db = booted();
    db.create(&db.admit().unwrap(), "moments", obj(json!({"id": "m9", "userId": "u1", "text": "new"}))).unwrap();
    let create = only_seq(&db);
    db.update(&db.admit().unwrap(), "moments", &rid("m9"), obj(json!({"text": "edited"}))).unwrap();
    db.roll_back(&db.admit().unwrap(), create).unwrap();
    assert!(db.find("moments", &rid("m9")).unwrap().is_none());
    assert_eq!(db.outbox().unwrap().len(), 1);
}

#[test]
fn rolling_back_an_unknown_operation_fails() {
    let db = booted();
    assert!(matches!(db.roll_back(&db.admit().unwrap(), 99), Err(DbError::UnknownOperation(99))));
}

#[test]
fn inv_order_04_restored_member_takes_a_free_key_when_its_old_key_was_reused() {
    let db = booted();
    let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
    for id in ["a", "b"] { put(&db, "moments", json!({"id": id, "userId": "u1", "text": id})); }
    let first = crate::value::fractional_between(None, None).unwrap();
    let second = crate::value::fractional_between(Some(&first), None).unwrap();
    let mut initial = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for (id, key) in [("a", &first), ("b", &second)] { initial.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: rid(id), order_key: key.clone() }); }
    db.land(&db.admit().unwrap(), initial).unwrap();
    db.destroy(&db.admit().unwrap(), "moments", &rid("b")).unwrap();
    put(&db, "moments", json!({"id": "x", "userId": "u1", "text": "x"}));
    let mut replacement = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    replacement.scope_ops.push(ScopeOp::Append { scope, row: rid("x"), order_key: second.clone() });
    db.land(&db.admit().unwrap(), replacement).unwrap();
    db.roll_back(&db.admit().unwrap(), only_seq(&db)).unwrap();
    let members: Vec<String> = db.shared.store.scope_members("moments", "feed").unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(members.len(), 3);
    assert!(members.contains(&"b".to_string()));
}
