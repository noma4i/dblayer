use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::fixtures::*;
use super::open::locked;
use super::*;

fn gated() -> (EntityLog, impl Fn(&EntityChange) + Send + Sync + 'static, mpsc::Sender<()>, mpsc::Receiver<()>) {
    let log: EntityLog = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let (open, gate) = mpsc::channel::<()>();
    let (entered_tx, entered) = mpsc::channel::<()>();
    let gate = Mutex::new(gate);
    let listener = move |change: &EntityChange| {
        let first = sink.lock().unwrap().is_empty();
        sink.lock().unwrap().push(change.clone());
        if first {
            entered_tx.send(()).unwrap();
            gate.lock().unwrap().recv().unwrap();
        }
    };
    (log, listener, open, entered)
}

#[test]
fn inv_observe_19_a_retained_row_destroyed_while_the_listener_is_busy_is_told_as_removed() {
    let db = booted();
    put(&db, "users", json!({"id": "a", "name": "one"}));
    put(&db, "users", json!({"id": "b", "name": "two"}));
    let (log, listener, open, entered) = gated();
    let observer = db.observe_entities("users", listener).unwrap();
    observer.retain(&[rid("a"), rid("b")]).unwrap();
    entered.recv().unwrap();
    db.update("users", &rid("a"), obj(json!({"name": "edited"}))).unwrap();
    db.update("users", &rid("b"), obj(json!({"name": "seen"}))).unwrap();
    db.destroy("users", &rid("b")).unwrap();
    open.send(()).unwrap();
    db.settle();
    let seen = log.lock().unwrap().clone();
    let removed: Vec<_> = seen.iter().flat_map(|change| change.removed.iter().cloned()).collect();
    assert_eq!(removed, vec![rid("b")], "deliveries {seen:?}");
}

#[test]
fn inv_observe_19_a_destroy_right_after_a_delivered_update_is_told_as_removed() {
    for temporary in [false, true] {
        let db = booted();
        let (a, b) = if temporary {
            {
            let one = db.create("moments", obj(json!({"userId": "u1", "text": "one"}))).unwrap().id.clone();
            let two = db.create("moments", obj(json!({"userId": "u1", "text": "two"}))).unwrap().id.clone();
            db.settle();
            (one, two)
        }
        } else {
            put(&db, "moments", json!({"id": "a", "userId": "u1", "text": "one"}));
            put(&db, "moments", json!({"id": "b", "userId": "u1", "text": "two"}));
            (rid("a"), rid("b"))
        };
        let (log, callback) = entity_recorder();
        let observer = db.observe_entities("moments", callback).unwrap();
        observer.retain(&[a.clone(), b.clone()]).unwrap();
        db.settle();
        db.update("moments", &a, obj(json!({"text": "edited"}))).unwrap();
        db.settle();
        observer.release(&[a.clone()]);
        db.update("moments", &a, obj(json!({"text": "unseen"}))).unwrap();
        db.update("moments", &b, obj(json!({"text": "seen"}))).unwrap();
        db.settle();
        db.destroy("moments", &b).unwrap();
        db.settle();
        let seen = log.lock().unwrap().clone();
        assert_eq!(seen.len(), 4, "temporary {temporary}: {seen:?}");
        assert_eq!(seen[3].removed, vec![b.clone()], "temporary {temporary}");
        let counters = db.counters();
        assert_eq!((counters.entity_facts_told, counters.entity_position_skips, counters.entity_reads_failed), (5, 0, 0), "temporary {temporary}");
    }
}

#[test]
fn inv_observe_19_a_refresh_that_cannot_read_is_counted_and_a_reset_tells_every_retained_row_as_removed() {
    let db = booted();
    for id in ["u0", "u1", "u2"] {
        put(&db, "users", json!({"id": id, "name": "one"}));
    }
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("u0"), rid("u1"), rid("u2")]).unwrap();
    db.settle();
    db.shared.store.lock().poison = Some("the store cannot read".to_string());
    let key = locked(&db.shared.observers).entities.keys().next().cloned().unwrap();
    db.shared.refresh_entities(&key, &crate::identity::ModelKey::new("users"), [rid("u1")].into_iter().collect());
    db.settle();
    assert_eq!((db.counters().entity_reads_failed, log.lock().unwrap().len()), (1, 1), "the failed read is counted and tells nothing");
    db.reset(&["users"]).unwrap();
    db.settle();
    let mut removed: Vec<String> = log.lock().unwrap().iter().flat_map(|change| change.removed.iter().map(ToString::to_string)).collect();
    removed.sort();
    assert_eq!(removed, ["u0", "u1", "u2"], "the reset that ends the poison tells every retained row");
}
