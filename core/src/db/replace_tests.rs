use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ListKey};
use crate::query::Query;
use crate::schema::Coverage;
use serde_json::{json, Value};

fn scope(name: &str) -> ScopeRef {
    ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw(name))
}

fn rows(items: &[(&str, &str)]) -> Vec<serde_json::Map<String, Value>> {
    items.iter().map(|(id, text)| obj(json!({"id": id, "userId": "u1", "text": text}))).collect()
}

fn request(name: &str, coverage: Coverage) -> ReplaceRequest {
    ReplaceRequest::new(scope(name), coverage)
}

fn members(db: &Db, name: &str) -> Vec<String> {
    db.shared.store.scope_members("moments", name).unwrap().into_iter().map(|(id, _)| id).collect()
}

fn observe_scope(db: &Db, name: &str) -> (ChangeLog, ObserverHandle) {
    let (log, callback) = recorder();
    let subscription = db.observe(Query::new("moments").in_scope(scope(name).key), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    (log, subscription)
}

fn start(db: &Db, name: &str, items: &[(&str, &str)]) {
    db.replace(&request(name, Coverage::Complete), rows(items)).unwrap();
    db.await_revision(db.revision_seq().unwrap());
}

#[test]
fn inv_identity_01_foreign_pending_rows_do_not_survive_replacement() {
    for coverage in [Coverage::Page, Coverage::Complete] {
        for on_absent in [OnAbsent::Detach, OnAbsent::Destroy] {
            let db = booted();
            start(&db, "feed", &[("same", "server"), ("own", "local")]);
            db.create("users", obj(json!({"id": "same", "name": "pending"}))).unwrap();
            db.update("moments", &rid("own"), obj(json!({"text": "edited"}))).unwrap();
            let mut request = request("feed", coverage);
            request.on_absent = on_absent;
            request.replace_all = true;
            db.replace(&request, vec![]).unwrap();
            assert_eq!(members(&db, "feed"), ["own"], "{coverage:?} {on_absent:?}");
            assert_eq!(db.find("moments", &rid("same")).unwrap().is_some(), on_absent == OnAbsent::Detach);
            assert!(db.find("users", &rid("same")).unwrap().is_some());
        }
    }
}

#[test]
fn inv_merge_04_inv_observe_05_three_merges_in_a_row_give_one_delivery_each_and_unchanged_rows_stay_out_of_updated() {
    let db = booted();
    start(&db, "feed", &[("m1", "one"), ("m2", "two")]);
    let (log, _subscription) = observe_scope(&db, "feed");
    let steps = [rows(&[("m1", "one"), ("m2", "two!"), ("m3", "three")]), rows(&[("m1", "uno"), ("m2", "two!"), ("m3", "three")]), rows(&[("m3", "three"), ("m1", "uno"), ("m2", "two!")])];
    for step in steps {
        let before = db.revision_seq().unwrap();
        db.replace(&request("feed", Coverage::Complete), step).unwrap();
        assert_eq!(db.revision_seq().unwrap(), before + 1);
        db.await_revision(db.revision_seq().unwrap());
    }
    let all = changes(&log);
    assert_eq!(all.len(), 4);
    assert!(all.iter().all(|change| !change.rows.is_empty()));
    assert_eq!((all[1].diff.updated.clone(), all[1].diff.inserted.clone()), (vec![rid("m2")], vec![rid("m3")]));
    assert_eq!(all[2].diff.updated, vec![rid("m1")]);
    assert_eq!(all[3].diff.moved.len(), 1);
    assert!(all[3].diff.updated.is_empty());
}

#[test]
fn identical_rows_commit_nothing() {
    let db = booted();
    start(&db, "feed", &[("m1", "one"), ("m2", "two")]);
    let revision = db.revision_seq().unwrap();
    let result = db.replace(&request("feed", Coverage::Complete), rows(&[("m1", "one"), ("m2", "two")])).unwrap();
    assert!(result.is_noop());
    assert_eq!(db.revision_seq().unwrap(), revision);
}

#[test]
fn replace_merges_repeated_ids_before_storing_unique_members() {
    let db = booted();
    db.replace(&request("feed", Coverage::Complete), vec![
        obj(json!({"id": "m2", "userId": "u1", "text": "first"})),
        obj(json!({"id": "m1", "userId": "u1", "text": "middle"})),
        obj(json!({"id": "m2", "text": "last"})),
    ]).unwrap();
    assert_eq!(members(&db, "feed"), ["m2", "m1"]);
    let row = db.find("moments", &rid("m2")).unwrap().unwrap();
    assert_eq!((row.text("text"), row.text("userId")), (Some("last"), Some("u1")));
}

#[test]
fn inv_merge_02_fields_of_a_stored_row_that_the_response_omits_are_kept() {
    let db = booted();
    start(&db, "feed", &[("m1", "one")]);
    db.replace(&request("feed", Coverage::Complete), vec![obj(json!({"id": "m1", "text": "changed"}))]).unwrap();
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!((row.text("text"), row.text("userId")), (Some("changed"), Some("u1")));
}

#[test]
fn inv_merge_07_a_found_anchor_keeps_the_tail_and_drops_the_vanished_row_before_it() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2"), ("m3", "3"), ("m4", "4"), ("m5", "5")]);
    let (log, _subscription) = observe_scope(&db, "feed");
    db.replace(&request("feed", Coverage::Page), rows(&[("m1", "1"), ("m3", "3")])).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids(changes(&log).last().unwrap()), ["m1", "m3", "m4", "m5"]);
    assert_eq!(changes(&log).len(), 2);
    assert!(db.find("moments", &rid("m2")).unwrap().is_some());
}

#[test]
fn inv_merge_07_a_missing_anchor_drops_the_tail() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2"), ("m3", "3")]);
    db.replace(&request("feed", Coverage::Page), rows(&[("m9", "9"), ("m8", "8")])).unwrap();
    assert_eq!(members(&db, "feed"), ["m9", "m8"]);
}

#[test]
fn inv_merge_07_local_rows_stay_in_the_list_on_their_places() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2")]);
    let local = db.create("moments", obj(json!({"userId": "u1", "text": "mine"}))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: scope("feed"), row: local.id.clone(), order_key: "Zz".to_string() });
    db.land(envelope).unwrap();
    let pending = db.create("moments", obj(json!({"id": "m5", "userId": "u1", "text": "pending"}))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: scope("feed"), row: pending.id.clone(), order_key: "b00".to_string() });
    db.land(envelope).unwrap();
    db.replace(&request("feed", Coverage::Complete), rows(&[("m2", "2"), ("m3", "3")])).unwrap();
    let after = members(&db, "feed");
    assert_eq!(after.first().map(String::as_str), Some(local.id.as_str()));
    assert!(after.contains(&"m5".to_string()) && after.contains(&"m2".to_string()) && after.contains(&"m3".to_string()));
    assert!(!after.contains(&"m1".to_string()));
}

#[test]
fn inv_merge_08_a_merge_changes_only_its_own_scope() {
    let db = booted();
    start(&db, "byUser1", &[("m1", "1"), ("m2", "2")]);
    start(&db, "byUser2", &[("m3", "3")]);
    start(&db, "feed", &[("m1", "1"), ("m3", "3")]);
    let (other, _subscription) = observe_scope(&db, "byUser2");
    let (feed, _feed_subscription) = observe_scope(&db, "feed");
    db.replace(&request("byUser1", Coverage::Complete), rows(&[("m2", "2")])).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(members(&db, "byUser2"), ["m3"]);
    assert_eq!(calls(&other), vec![1]);
    assert_eq!(changes(&feed).len(), 1);
}

#[test]
fn detach_keeps_the_row_and_destroy_removes_it_with_its_cascade() {
    let db = booted();
    seed_tree(&db);
    start(&db, "feed", &[("m1", "one"), ("m2", "two")]);
    db.replace(&request("feed", Coverage::Complete), rows(&[("m2", "two")])).unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    start(&db, "feed", &[("m1", "one"), ("m2", "two")]);
    let mut destroying = request("feed", Coverage::Complete);
    destroying.on_absent = OnAbsent::Destroy;
    let before = db.revision_seq().unwrap();
    db.replace(&destroying, rows(&[("m2", "two")])).unwrap();
    assert_eq!(db.revision_seq().unwrap(), before + 1);
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert_eq!(db.count(&Query::new("vibes")).unwrap(), 0);
}

#[test]
fn destroy_spares_local_rows() {
    let db = booted();
    start(&db, "feed", &[("m1", "1")]);
    let local = db.create("moments", obj(json!({"userId": "u1", "text": "mine"}))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: scope("feed"), row: local.id.clone(), order_key: "b00".to_string() });
    db.land(envelope).unwrap();
    let mut destroying = request("feed", Coverage::Complete);
    destroying.on_absent = OnAbsent::Destroy;
    db.replace(&destroying, vec![]).unwrap();
    assert!(db.find("moments", &local.id).unwrap().is_some());
    assert_eq!(members(&db, "feed"), [local.id.to_string()]);
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
}

#[test]
fn replace_all_ignores_the_anchor() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2"), ("m3", "3")]);
    let mut all = request("feed", Coverage::Page);
    all.replace_all = true;
    db.replace(&all, rows(&[("m1", "1")])).unwrap();
    assert_eq!(members(&db, "feed"), ["m1"]);
}

#[test]
fn replace_on_an_unknown_model_fails() {
    let db = booted();
    let ghost = ScopeRef::new(ModelKey::new("ghosts"), ListKey::from_raw("x"));
    assert!(matches!(db.replace(&ReplaceRequest::new(ghost, Coverage::Complete), vec![]), Err(DbError::UnknownModel(_))));
}

const OLD: &str = "2026-01-01T00:00:00.000Z";
const NEW: &str = "2026-01-02T00:00:00.000Z";
const NEWER: &str = "2026-01-03T00:00:00.000Z";

#[test]
fn a_server_row_moves_the_parent_pointer_only_forward() {
    let cases = [
        ("an older pointer from the server is kept out", "m1", OLD, ("m9", NEW)),
        ("a newer pointer from the server is taken", "m10", NEWER, ("m10", NEWER)),
        ("a pending pointer holds against a confirmed one", "m10", NEWER, ("temp-1", NEW)),
    ];
    for (name, id, at, (held, held_at)) in cases {
        let db = booted();
        put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": held, "lastMessageAt": held_at}));
        let chats = ScopeRef::new(ModelKey::new("chats"), ListKey::from_raw("all"));
        db.replace(&ReplaceRequest::new(chats, Coverage::Complete), vec![obj(json!({"id": "c1", "title": "t", "lastMessageId": id, "lastMessageAt": at}))]).unwrap();
        let chat = db.find("chats", &rid("c1")).unwrap().unwrap();
        let expected = if name.starts_with("a newer") { (id, at) } else { (held, held_at) };
        assert_eq!((chat.text("lastMessageId"), chat.text("lastMessageAt")), (Some(expected.0), Some(expected.1)), "{name}");
    }
}

fn touches_to_replace_with_new_rows(size: usize) -> usize {
    let db = booted();
    let items: Vec<(String, String)> = (0..size).map(|index| (format!("m{index:05}"), "x".to_string())).collect();
    let borrowed: Vec<(&str, &str)> = items.iter().map(|(id, text)| (id.as_str(), text.as_str())).collect();
    let before = crate::identity::touches::total();
    db.replace(&request("feed", Coverage::Complete), rows(&borrowed)).unwrap();
    crate::identity::touches::total() - before
}

#[test]
fn ordering_all_new_rows_scales_linearly_with_the_row_count() {
    let (small, large) = (touches_to_replace_with_new_rows(500), touches_to_replace_with_new_rows(2000));
    assert!(large <= small * 6, "{small} touches for 500 rows, {large} for 2000");
}

#[test]
fn replacing_a_list_on_a_disk_database_plans_off_the_writer() {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("db.sqlite"), all_specs(), None).unwrap();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let before = db.shared.store.counters();
    db.replace(&request("recent", Coverage::Complete), rows(&[("m1", "one"), ("m2", "two")])).unwrap();
    let after = db.shared.store.counters();
    assert_eq!((after.plans_off_writer - before.plans_off_writer, after.plans_under_writer - before.plans_under_writer), (1, 0));
}
