use serde_json::json;

use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};

fn named(db: &Db, id: &str, name: &str) {
    put(db, "users", json!({"id": id, "name": name}));
}

fn users(range: std::ops::Range<usize>) -> Vec<RowId> {
    range.map(|n| rid(&format!("u{n}"))).collect()
}

fn rig() -> (Db, EntityLog, EntityObserver) {
    rig_of(10)
}

fn rig_of(count: usize) -> (Db, EntityLog, EntityObserver) {
    let db = booted();
    for n in 0..count {
        named(&db, &format!("u{n}"), "first");
    }
    named(&db, "outsider", "first");
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&users(0..count)).unwrap();
    db.settle();
    (db, log, observer)
}

fn work(db: &Db, run: impl FnOnce()) -> (usize, usize) {
    let before = db.counters();
    run();
    db.settle();
    let after = db.counters();
    (after.observation_reads - before.observation_reads, after.observer_calls - before.observer_calls)
}

fn delivered(log: &EntityLog) -> Vec<EntityChange> {
    log.lock().unwrap().clone()
}

#[test]
fn retaining_delivers_the_rows_once_and_a_commit_delivers_only_the_changed_row() {
    let (db, log, _observer) = rig();
    let first = delivered(&log);
    assert_eq!((first.len(), first[0].rows.len()), (1, 10));
    assert_eq!(work(&db, || named(&db, "u3", "second")), (1, 1));
    let last = delivered(&log).pop().unwrap();
    assert_eq!((last.rows.len(), last.rows[0].id.as_str(), last.removed.len()), (1, "u3", 0));
    assert_eq!(work(&db, || named(&db, "outsider", "second")), (0, 0), "a row nobody retained neither reads nor delivers");
}

#[test]
fn a_released_id_costs_no_read_and_no_delivery() {
    let (db, log, observer) = rig();
    observer.release(&users(0..5));
    assert_eq!(db.keyed_rows(), 5, "a released id leaves the reverse index");
    assert_eq!(work(&db, || named(&db, "u2", "second")), (0, 0));
    assert_eq!(work(&db, || named(&db, "u7", "second")), (1, 1));
    assert_eq!(delivered(&log).len(), 2);
}

#[test]
fn delivery_order_by_revision() {
    let (db, log, _observer) = rig();
    for n in 0..20 {
        named(&db, "u1", &format!("name{n}"));
    }
    db.settle();
    let seen = delivered(&log);
    let revisions: Vec<i64> = seen.iter().map(|change| change.revision).collect();
    assert!(revisions.windows(2).all(|pair| pair[0] <= pair[1]), "revisions {revisions:?}");
    let last = seen.last().unwrap().rows.last().unwrap();
    assert_eq!(last.get("name"), Some(&json!("name19")));
}

#[test]
fn dropping_the_observer_empties_the_index_and_stops_delivery() {
    let (db, log, observer) = rig();
    assert_eq!((db.keyed_rows(), db.observer_census().subscriptions), (10, 1));
    drop(observer);
    assert_eq!((db.keyed_rows(), db.observer_census().subscriptions), (0, 0));
    assert_eq!(work(&db, || named(&db, "u1", "second")), (0, 0));
    assert_eq!(delivered(&log).len(), 1);
}

#[test]
fn a_row_that_appears_later_and_one_that_is_destroyed_are_delivered() {
    let (db, log, observer) = rig();
    observer.retain(&[rid("late")]).unwrap();
    named(&db, "late", "here");
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("users"), id: rid("u4"), reason: DestroyReason::Server });
    db.land(envelope).unwrap();
    db.settle();
    let seen = delivered(&log);
    assert_eq!(seen[1].removed, vec![rid("late")], "an absent id is told as removed once, so the reader knows it is not there");
    assert_eq!(seen[2].rows[0].id.as_str(), "late");
    assert_eq!(seen[3].removed, vec![rid("u4")]);
}

#[test]
fn a_reset_of_the_model_delivers_the_removed_ids() {
    let (db, log, _observer) = rig();
    db.reset(&["users"]).unwrap();
    db.settle();
    let mut removed: Vec<String> = delivered(&log).iter().flat_map(|change| change.removed.iter().map(ToString::to_string)).collect();
    removed.sort();
    assert_eq!(removed, (0..10).map(|n| format!("u{n}")).collect::<Vec<_>>());
}

#[test]
fn one_changed_row_delivers_and_decodes_one_row() {
    for count in [10, 100, 1000] {
        let (db, log, _observer) = rig_of(count);
        assert_eq!(work(&db, || named(&db, "u3", "second")), (1, 1), "{count} retained ids: 1 read, 1 delivery");
        let last = delivered(&log).pop().unwrap();
        assert_eq!(last.rows.len(), 1, "{count} retained ids: 1 row reaches the platform to decode");
    }
}
