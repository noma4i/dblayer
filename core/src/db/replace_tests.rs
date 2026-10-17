use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ScopeKey};
use crate::query::Query;
use crate::schema::Coverage;
use serde_json::{json, Value};

fn scope(name: &str) -> ScopeRef {
    ScopeRef::new(ModelKey::new("moments"), ScopeKey::from_raw(name))
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
    db.settle();
    (log, subscription)
}

fn start(db: &Db, name: &str, items: &[(&str, &str)]) {
    db.replace(&request(name, Coverage::Complete), rows(items)).unwrap();
    db.settle();
}

#[test]
fn three_merges_in_a_row_give_one_delivery_each_and_unchanged_rows_stay_out_of_updated() {
    let db = booted();
    start(&db, "feed", &[("m1", "one"), ("m2", "two")]);
    let (log, _subscription) = observe_scope(&db, "feed");
    let steps = [rows(&[("m1", "one"), ("m2", "two!"), ("m3", "three")]), rows(&[("m1", "uno"), ("m2", "two!"), ("m3", "three")]), rows(&[("m3", "three"), ("m1", "uno"), ("m2", "two!")])];
    for step in steps {
        let before = db.revision_seq().unwrap();
        db.replace(&request("feed", Coverage::Complete), step).unwrap();
        assert_eq!(db.revision_seq().unwrap(), before + 1);
        db.settle();
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
fn v3_replace_merges_repeated_ids_before_storing_unique_members() {
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
fn fields_of_a_stored_row_that_the_response_omits_are_kept() {
    let db = booted();
    start(&db, "feed", &[("m1", "one")]);
    db.replace(&request("feed", Coverage::Complete), vec![obj(json!({"id": "m1", "text": "changed"}))]).unwrap();
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!((row.text("text"), row.text("userId")), (Some("changed"), Some("u1")));
}

#[test]
fn a_found_anchor_keeps_the_tail_and_drops_the_vanished_row_before_it() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2"), ("m3", "3"), ("m4", "4"), ("m5", "5")]);
    let (log, _subscription) = observe_scope(&db, "feed");
    db.replace(&request("feed", Coverage::Page), rows(&[("m1", "1"), ("m3", "3")])).unwrap();
    db.settle();
    assert_eq!(ids(changes(&log).last().unwrap()), ["m1", "m3", "m4", "m5"]);
    assert_eq!(changes(&log).len(), 2);
    assert!(db.find("moments", &rid("m2")).unwrap().is_some());
}

#[test]
fn a_missing_anchor_drops_the_tail() {
    let db = booted();
    start(&db, "feed", &[("m1", "1"), ("m2", "2"), ("m3", "3")]);
    db.replace(&request("feed", Coverage::Page), rows(&[("m9", "9"), ("m8", "8")])).unwrap();
    assert_eq!(members(&db, "feed"), ["m9", "m8"]);
}

#[test]
fn local_rows_stay_in_the_list_on_their_places() {
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
fn a_merge_changes_only_its_own_scope() {
    let db = booted();
    start(&db, "byUser1", &[("m1", "1"), ("m2", "2")]);
    start(&db, "byUser2", &[("m3", "3")]);
    start(&db, "feed", &[("m1", "1"), ("m3", "3")]);
    let (other, _subscription) = observe_scope(&db, "byUser2");
    let (feed, _feed_subscription) = observe_scope(&db, "feed");
    db.replace(&request("byUser1", Coverage::Complete), rows(&[("m2", "2")])).unwrap();
    db.settle();
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
    let ghost = ScopeRef::new(ModelKey::new("ghosts"), ScopeKey::from_raw("x"));
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
        let chats = ScopeRef::new(ModelKey::new("chats"), ScopeKey::from_raw("all"));
        db.replace(&ReplaceRequest::new(chats, Coverage::Complete), vec![obj(json!({"id": "c1", "title": "t", "lastMessageId": id, "lastMessageAt": at}))]).unwrap();
        let chat = db.find("chats", &rid("c1")).unwrap().unwrap();
        let expected = if name.starts_with("a newer") { (id, at) } else { (held, held_at) };
        assert_eq!((chat.text("lastMessageId"), chat.text("lastMessageAt")), (Some(expected.0), Some(expected.1)), "{name}");
    }
}
