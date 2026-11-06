use serde_json::json;

use super::fixtures::*;
use crate::commit::{CommitEnvelope, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::fixtures::*;
use crate::identity::{ModelKey, ListKey};
use crate::query::Query;
use crate::syncstate::SyncState;

fn text(db: &crate::db::Db) -> Option<String> {
    db.find("moments", &rid("m1")).unwrap().and_then(|row| row.text("text").map(str::to_string))
}

#[test]
fn inv_rollback_08_a_field_the_server_overwrote_after_the_operation_is_not_restored_by_its_rollback() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let seq = only_seq(&db);
    start(&db, seq);
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "three"}));
    let landed = text(&db);
    assert_eq!(landed.as_deref(), Some("three"));
    db.roll_back(&db.admit().unwrap(), seq).unwrap();
    assert_eq!(text(&db), landed, "landed {landed:?}");
}

#[test]
fn inv_rollback_09_a_rollback_after_a_restart_gives_the_same_result_as_before_it() {
    for started in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("1.sqlite");
        let first = crate::db::Db::open(&path, all_specs(), None).unwrap();
        put(&first, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        first.update(&first.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
        first.update(&first.admit().unwrap(), "moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
        if started {
            start(&first, first.outbox().unwrap()[0].operation.id.0);
        }
        first.close().unwrap();
        drop(first);
        let reopened = crate::db::Db::open(&path, all_specs(), None).unwrap();
        let oldest = reopened.outbox().unwrap()[0].operation.id.0;
        reopened.roll_back(&reopened.admit().unwrap(), oldest).unwrap();
        let row = reopened.find("moments", &rid("m1")).unwrap().unwrap();
        assert_eq!((row.text("text"), row.text("userId")), (Some("one"), Some("u2")), "started {started}");
        assert_eq!(reopened.outbox().unwrap().len(), 1);
    }
}

fn permutations(items: Vec<usize>) -> Vec<Vec<usize>> {
    if items.len() <= 1 {
        return vec![items];
    }
    let mut all = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.clone();
        let head = rest.remove(index);
        all.extend(permutations(rest).into_iter().map(|mut tail| {
            tail.insert(0, head);
            tail
        }));
    }
    all
}

#[test]
fn inv_rollback_01_any_cancellation_order_leaves_the_value_of_the_newest_edit_that_is_still_live() {
    let edits = ["B", "C", "D", "E"];
    for order in permutations((0..edits.len()).collect()) {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "A"}));
        for edit in edits {
            db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": edit}))).unwrap();
        }
        let seqs: Vec<i64> = db.outbox().unwrap().iter().map(|entry| entry.operation.id.0).collect();
        let mut cancelled = Vec::new();
        for index in order {
            db.roll_back(&db.admit().unwrap(), seqs[index]).unwrap();
            cancelled.push(index);
            let live = (0..edits.len()).rev().find(|edit| !cancelled.contains(edit)).map_or("A", |edit| edits[edit]);
            assert_eq!(text(&db).as_deref(), Some(live), "cancelled {cancelled:?}");
        }
    }
}

#[test]
fn inv_rollback_04_rolling_back_a_create_removes_the_row_with_its_edits_file_and_membership_for_either_kind_of_id() {
    for explicit in [true, false] {
        let db = booted();
        let row = if explicit { obj(json!({"id": "m9", "userId": "u1", "text": "new"})) } else { obj(json!({"userId": "u1", "text": "new"})) };
        let id = db.create(&db.admit().unwrap(), "moments", row).unwrap().id.clone();
        let create = only_seq(&db);
        db.attach(&db.admit().unwrap(), "moments", &id, "media", b"bytes", "a.bin", "a/b").unwrap();
        db.update(&db.admit().unwrap(), "moments", &id, obj(json!({"text": "edited"}))).unwrap();
        let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.scope_ops.push(ScopeOp::Append { scope, row: id.clone(), order_key: "a".into() });
        db.land(&db.admit().unwrap(), envelope).unwrap();
        assert_eq!(db.shared.store.scope_members("moments", "feed").unwrap().len(), 1);
        db.roll_back(&db.admit().unwrap(), create).unwrap();
        assert!(db.find("moments", &id).unwrap().is_none(), "explicit {explicit}");
        assert_eq!(db.read("moments", &id, "media").unwrap(), None, "explicit {explicit}");
        assert_eq!(db.outbox().unwrap().len(), 1, "the edit of the row stays queued, the owner Outbox runs it next: explicit {explicit}");
        assert!(db.shared.store.scope_members("moments", "feed").unwrap().is_empty(), "explicit {explicit}");
        assert_eq!(db.sync_info("moments", &id).unwrap().state, SyncState::Pending, "the queued edit still shows: explicit {explicit}");
    }
}


#[test]
fn inv_rollback_10_rolling_back_an_action_patch_gives_way_to_a_newer_edit_of_the_same_field() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "A"}));
    db.act(&db.admit().unwrap(), "moments", "react", &rid("m1"), obj(json!({"text": "P"})), obj(json!({}))).unwrap();
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "N"}))).unwrap();
    let seqs: Vec<i64> = db.outbox().unwrap().iter().map(|entry| entry.operation.id.0).collect();
    db.roll_back(&db.admit().unwrap(), seqs[0]).unwrap();
    assert_eq!(text(&db).as_deref(), Some("N"));
    db.roll_back(&db.admit().unwrap(), seqs[1]).unwrap();
    assert_eq!(text(&db).as_deref(), Some("A"));
}
