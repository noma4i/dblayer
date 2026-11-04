use serde_json::json;

use super::dispatch::Event;
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use super::fixtures::*;
use super::watch::Subject;
use super::window::WindowSpec;
use crate::identity::{ModelKey, RowRef};
use crate::query::Query;
use crate::test_wait::join_within;

#[test]
fn inv_observe_29_saturation_bounds_reads_until_the_owner_applies_the_results() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (_initial, initial_callback) = recorder();
    let _initial_observer = db.observe(Query::new("users"), initial_callback).unwrap();
    let (window_log, window_callback) = recorder();
    let query = Query::new("users").ordered(vec![crate::query::Order::new("id", crate::value::SortDirection::Asc)]);
    let window = db.observe_window(query, WindowSpec::Tail { size: 0, max: None, paged: false }, window_callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let paused = db.shared.dispatch.pause_owner();
    db.shared.reads.inline.store(true, std::sync::atomic::Ordering::SeqCst);
    let before = db.counters().observation_reads;
    let mut observers = Vec::new();
    let mut logs = Vec::new();
    for index in 0..32 {
        let (log, callback) = recorder();
        observers.push(db.observe(Query::new("users").limited(index + 1), callback).unwrap());
        logs.push(log);
    }
    let (joined, joined_callback) = recorder();
    let _joined_observer = db.observe(Query::new("users"), joined_callback).unwrap();
    window.grow_before(1);
    let cancelled = logs.pop().unwrap();
    drop(observers.pop());
    let issued = db.counters().observation_reads - before;
    db.as_app().update("users", &rid("u1"), obj(json!({"name": "latest"}))).unwrap();
    drop(paused);
    db.await_revision(db.revision_seq().unwrap());
    assert!(issued <= 2, "ready results retain their read slots: {issued} reads before any result was applied");
    assert!(logs.iter().all(|log| log.lock().unwrap().last().is_some_and(|change| change.rows.len() == 1 && change.rows[0].text("name") == Some("latest"))), "every deferred opening reaches the last committed row");
    assert_eq!(calls(&joined).last(), Some(&1), "deferred join retains its subscriber");
    assert_eq!(window_log.lock().unwrap().last().unwrap().rows.len(), 1, "deferred growth retains its requested size");
    assert!(cancelled.lock().unwrap().is_empty(), "cancelled deferred opening stays silent");
}

#[test]
fn inv_observe_29_repeated_window_growth_keeps_one_pending_read() {
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..64 {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&format!("u{index:02}")), row: obj(json!({"id": format!("u{index:02}"), "name": "Ann"})) });
    }
    db.as_app().land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (log, callback) = recorder();
    let query = Query::new("users").ordered(vec![crate::query::Order::new("id", crate::value::SortDirection::Asc)]);
    let window = db.observe_window(query, WindowSpec::Tail { size: 1, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let paused = db.shared.dispatch.pause_owner();
    db.shared.reads.inline.store(true, std::sync::atomic::Ordering::SeqCst);
    let before = db.counters().observation_reads;
    for _ in 0..64 {
        window.grow_before(1);
    }
    drop(paused);
    db.await_revision(db.revision_seq().unwrap());
    let issued = db.counters().observation_reads - before;
    assert!(issued <= 2, "one in-flight growth and one combined growth: {issued} reads");
    assert_eq!(log.lock().unwrap().last().unwrap().rows.len(), 64);
}

fn hit(model: &str, id: &str) -> Event {
    let model = ModelKey::new(model);
    Event { revision: i64::MAX / 2, models: [model.clone()].into(), rows: [RowRef::new(model, rid(id))].into(), ..Event::default() }
}

#[test]
fn inv_observe_21_a_subscription_and_its_refreshes_read_outside_the_owner() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&log), [1, 2]);
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_21_a_second_subscription_to_a_group_joins_after_a_read_outside_the_owner() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (first, first_callback) = recorder();
    let (second, second_callback) = recorder();
    let _one = db.observe(Query::new("users"), first_callback).unwrap();
    let _two = db.observe(Query::new("users"), second_callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!((calls(&first), calls(&second)), (vec![1, 2], vec![1, 2]));
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_21_a_window_grows_by_a_read_outside_the_owner() {
    let db = booted();
    for index in 0..6 {
        put(&db, "users", json!({"id": format!("u{index}"), "name": format!("n{index}")}));
    }
    let (log, callback) = recorder();
    let window = db.observe_window(Query::new("users").ordered(vec![crate::query::Order::new("name", crate::value::SortDirection::Asc)]), WindowSpec::Tail { size: 2, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    window.grow_before(2);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(log.lock().unwrap().last().map(|change| change.size), Some(4));
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_22_a_group_that_cannot_be_read_is_counted_and_read_again_by_the_next_commit() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    let at = db.revision_seq().unwrap();
    db.await_revision(at);
    *db.shared.store.lock().poison.lock().unwrap() = Some("the store cannot read".to_string());
    db.shared.refresh_groups(&[hit("users", "u1")]);
    db.await_revision(at);
    assert_eq!((db.counters().query_reads_failed, calls(&log)), (1, vec![1]));
    *db.shared.store.lock().poison.lock().unwrap() = None;
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(db.counters().observation_reads, reads + 1);
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&log), [1, 2]);
    put(&db, "users", json!({"id": "u3", "name": "Cy"}));
    assert_eq!((db.counters().query_reads_failed, calls(&log)), (1, vec![1, 2, 3]));
}

#[test]
fn inv_observe_23_a_panic_while_applying_one_group_fails_that_group_and_leaves_the_others() {
    let db = booted();
    let (users, users_callback) = recorder();
    let (moments, moments_callback) = recorder();
    let _users = db.observe(Query::new("users"), users_callback).unwrap();
    let _moments = db.observe(Query::new("moments"), moments_callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    *db.shared.panic_key.lock().unwrap() = Some(Subject::Rows(Query::new("users")).key());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for (model, row) in [("users", json!({"id": "u1", "name": "Ann"})), ("moments", json!({"id": "m1", "userId": "u1", "text": "one"}))] {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: rid(row["id"].as_str().unwrap()), row: obj(row) });
    }
    db.as_app().land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!((calls(&users), calls(&moments)), (vec![0], vec![0, 1]));
    assert_eq!(db.counters().owner_panics, 1);
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    assert_eq!(calls(&users), [0, 1], "a batch of another model reads the failed group again");
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&users), [0, 1, 2]);
}

fn returns_while_the_registry_is_held(call: impl FnOnce() + Send + 'static, db: &crate::db::Db) -> bool {
    let (held, release) = (std::sync::Arc::new(crate::test_wait::Rendezvous::new(2)), std::sync::Arc::new(crate::test_wait::Rendezvous::new(2)));
    let holder = {
        let (shared, held, release) = (db.shared.clone(), held.clone(), release.clone());
        std::thread::spawn(move || {
            let _registry = shared.observers();
            held.meet();
            release.meet();
        })
    };
    held.meet();
    let (done, finished) = std::sync::mpsc::channel();
    let caller = std::thread::spawn(move || {
        call();
        let _ = done.send(());
    });
    let returned = finished.recv_timeout(std::time::Duration::from_secs(2)).is_ok();
    release.meet();
    join_within(holder);
    join_within(caller);
    returned
}

#[test]
fn nb1_cancelling_and_growing_an_observer_do_not_wait_for_the_registry() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let query = Query::new("users").ordered(vec![crate::query::Order::new("id", crate::value::SortDirection::Asc)]);
    let (_log, callback) = recorder();
    let window = db.observe_window(query, WindowSpec::Tail { size: 1, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert!(returns_while_the_registry_is_held(move || window.grow_before(1), &db), "grow waited for the observers lock");
    let (_log, callback) = recorder();
    let observer = db.observe(Query::new("users"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert!(returns_while_the_registry_is_held(move || drop(observer), &db), "cancel waited for the observers lock");
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(db.observer_census().subscriptions, 0, "deferred cancellations and the dropped window reach the registry");
}

#[test]
fn d3_a_commit_that_touches_no_group_notifies_no_feed_subscriber() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let mut keep = Vec::new();
    for index in 0..300 {
        keep.push(db.observe_query_feed(Query::new("users").limited(index + 1), false, false, |_| {}).unwrap());
    }
    db.await_revision(db.revision_seq().unwrap());
    let before = db.counters().reach_notices;
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "x"}));
    let notices = db.counters().reach_notices - before;
    assert_eq!(notices, 0, "feed subscribers told about a commit that touched none of their groups: {notices}");
}

#[test]
fn the_first_delivery_of_a_warm_query_reads_sqlite_off_the_calling_thread() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let query = Query::new("users");
    db.warm(&query).unwrap();
    db.shared.cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
    let revision = db.revision_seq().unwrap();
    store_reads(&db).clear();
    let caller = std::thread::current().id();
    let (log, callback) = recorder();
    let _observer = db.observe(query, callback).unwrap();
    db.await_revision(revision);
    assert_eq!(calls(&log), [1], "the first delivery carries the stored row");
    let read_on = store_reads(&db).clone();
    assert!(!read_on.is_empty(), "the first delivery read nothing from the store");
    assert!(read_on.iter().all(|thread| *thread != caller), "the first delivery read the store on the calling thread");
}

fn store_reads(db: &super::Db) -> std::sync::MutexGuard<'_, Vec<std::thread::ThreadId>> {
    db.shared.store.read_threads.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
