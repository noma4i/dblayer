use serde_json::json;

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::db::fixtures::*;
use crate::db::queue_write_tests::{page, paged_moments};
use crate::db::Db;
use crate::identity::ModelKey;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::StoreOp;

fn land_as(db: &Db, origin: WriteOrigin, model: &str, row: serde_json::Value) -> crate::commit::CommitResult {
    let row = obj(row);
    let id = rid(row["id"].as_str().unwrap());
    let mut envelope = CommitEnvelope::new(origin, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
    db.land(&db.admit().unwrap(), envelope).unwrap()
}

fn event(db: &Db, model: &str, row: serde_json::Value) {
    db.land_events(&[(model.to_string(), json!({"op": "upsert", "id": row["id"], "row": row}).to_string())]).unwrap();
}

fn members(db: &Db, user: &str) -> Vec<String> {
    let scope = db.spec("moments").unwrap().selection_key("mine", &obj(json!({"userId": user}))).unwrap();
    db.shared.store.read(&["moments"], |reader| Ok(reader.scope_members("moments", scope.as_str())?)).unwrap().into_iter().map(|(id, _)| id).collect()
}

fn key(user: &str) -> WindowKey {
    WindowKey::relation("moments", "mine", &obj(json!({"userId": user})))
}

fn fresh(db: &Db, mut op: StoreOp) -> StoreOp {
    if let StoreOp::Window { base_revision, .. } = &mut op {
        *base_revision = db.revision_seq().unwrap();
    }
    op
}

fn replacing(mut op: StoreOp) -> StoreOp {
    if let StoreOp::Window { replace_all, .. } = &mut op {
        *replace_all = true;
    }
    op
}

#[test]
fn inv_merge_01_inv_merge_02_every_path_applies_the_sent_fields_and_leaves_the_rest() {
    let seed = || {
        let db = paged_moments();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one", "createdAt": "2026-01-01T00:00:00.000Z"}));
        db
    };
    let kept = |db: &Db| db.find("moments", &rid("m1")).unwrap().unwrap().get("createdAt").cloned();
    let before = kept(&seed());
    assert!(before.is_some());
    let db = seed();
    event(&db, "moments", json!({"id": "m1", "text": "event"}));
    assert_eq!((db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), kept(&db)), (Some("event"), before.clone()), "event");
    event(&db, "moments", json!({"id": "m1", "text": null}));
    assert!(db.find("moments", &rid("m1")).unwrap().unwrap().get("text").is_none_or(serde_json::Value::is_null), "a null erases the value");
    let db = seed();
    let row = crate::owners::store::RowValue { row_id: "m1".into(), values: json!({"id": "m1", "text": "answer"}).to_string() };
    db.write_queue_ops(&[StoreOp::Rows { model: "moments".into(), rows: vec![row] }]).unwrap();
    assert_eq!((db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), kept(&db)), (Some("answer"), before.clone()), "mutation answer");
    let db = seed();
    db.write_queue_ops(&[fresh(&db, page(&key("u1"), 1, &["m1"], None))]).unwrap();
    assert_eq!((db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), kept(&db)), (Some("m1"), before), "page of a list");
}

#[test]
fn inv_merge_03_a_server_row_does_not_undo_an_unconfirmed_local_edit() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "mine"}))).unwrap();
    event(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "server"}));
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("mine"));
}

#[test]
fn inv_merge_04_a_row_that_did_not_change_is_not_written_again() {
    let db = booted();
    let first = land_as(&db, WriteOrigin::Response, "users", json!({"id": "u1", "name": "Ann"}));
    assert!(!first.is_noop());
    let revision = db.revision_seq().unwrap();
    let second = land_as(&db, WriteOrigin::Response, "users", json!({"id": "u1", "name": "Ann"}));
    assert!(second.is_noop());
    assert_eq!(db.revision_seq().unwrap(), revision);
}

#[test]
fn inv_merge_05_an_answer_with_an_older_base_does_not_overwrite_newer_data() {
    let db = paged_moments();
    let base = db.revision_seq().unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "new"}));
    let mut stale = page(&key("u1"), 1, &["m1"], None);
    if let StoreOp::Window { base_revision, .. } = &mut stale {
        *base_revision = base;
    }
    db.write_queue_ops(&[stale]).unwrap();
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("new"));
}

#[test]
fn inv_merge_07_a_page_keeps_the_tail_after_a_found_anchor_and_drops_it_without_one() {
    let db = paged_moments();
    db.write_queue_ops(&[page(&key("u1"), 1, &["m1", "m2", "m3"], None)]).unwrap();
    db.write_queue_ops(&[fresh(&db, page(&key("u1"), 1, &["m1", "m2"], Some("c1")))]).unwrap();
    assert_eq!(members(&db, "u1"), ["m1", "m2", "m3"], "the anchor m2 is found: the tail stays");
    db.write_queue_ops(&[fresh(&db, replacing(page(&key("u1"), 1, &["n1", "n2"], None)))]).unwrap();
    assert_eq!(members(&db, "u1"), ["n1", "n2"], "the anchor n2 is not in the list: the tail goes");
}

#[test]
fn inv_merge_08_a_landing_changes_only_the_list_of_its_own_selection() {
    let db = paged_moments();
    db.write_queue_ops(&[page(&key("u1"), 1, &["m1"], None)]).unwrap();
    db.write_queue_ops(&[page(&key("u2"), 1, &["m2"], None)]).unwrap();
    db.write_queue_ops(&[fresh(&db, replacing(page(&key("u1"), 1, &["m3"], None)))]).unwrap();
    assert_eq!(members(&db, "u1"), ["m3"]);
    assert_eq!(members(&db, "u2"), ["m2"]);
}

#[test]
fn inv_merge_09_rows_membership_and_window_land_in_one_commit() {
    let db = paged_moments();
    let before = db.revision_seq().unwrap();
    db.write_queue_ops(&[page(&key("u1"), 1, &["m1", "m2"], Some("c1"))]).unwrap();
    assert_eq!(db.revision_seq().unwrap(), before + 1, "rows, membership and the window record are one commit");
    assert_eq!(members(&db, "u1"), ["m1", "m2"]);
}

#[test]
fn inv_merge_10_a_landing_that_fails_changes_no_row_no_member_and_no_window() {
    let db = paged_moments();
    db.write_queue_ops(&[page(&key("u1"), 1, &["m1"], Some("c1"))]).unwrap();
    let revision = db.revision_seq().unwrap();
    let broken = StoreOp::Window { key: "mine".into(), model: "ghost".into(), page: 2, replace_all: false, cursor: None, roots: Vec::new(), related: Vec::new(), total: None, base_revision: 0 };
    assert!(db.write_queue_ops(&[broken]).is_err());
    assert_eq!(db.revision_seq().unwrap(), revision);
    assert_eq!(members(&db, "u1"), ["m1"]);
}

#[test]
fn inv_merge_12_no_landing_of_server_data_changes_the_value_written_by_set_local() {
    let db = Db::open_in_memory(crate::db::open_reset_tests::specs(false), None).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.set_local(&db.admit().unwrap(), "moments", &rid("m1"), "pinned", json!("yes")).unwrap();
    let pinned = || db.find("moments", &rid("m1")).unwrap().unwrap().text("pinned").map(str::to_string);
    event(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "two", "pinned": "server"}));
    assert_eq!(pinned().as_deref(), Some("yes"), "event");
    let answer = crate::owners::store::RowValue { row_id: "m1".into(), values: json!({"id": "m1", "userId": "u1", "text": "three", "pinned": "server"}).to_string() };
    db.write_queue_ops(&[StoreOp::Rows { model: "moments".into(), rows: vec![answer] }]).unwrap();
    assert_eq!(pinned().as_deref(), Some("yes"), "mutation answer");
}
