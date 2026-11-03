use serde_json::json;

use super::dispatch::Event;
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use super::fixtures::*;
use super::watch::Subject;
use super::window::WindowSpec;
use crate::identity::{ModelKey, RowRef};
use crate::query::Query;
use crate::test_wait::{join_within, recv_or_closed, spin_until};

#[test]
fn inv_observe_29_saturation_bounds_reads_until_the_owner_applies_the_results() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (_initial, initial_callback) = recorder();
    let _initial_observer = db.observe(Query::new("users"), initial_callback).unwrap();
    let (window_log, window_callback) = recorder();
    let query = Query::new("users").ordered(vec![crate::query::Order::new("id", crate::value::SortDirection::Asc)]);
    let window = db.observe_window(query, WindowSpec::Tail { size: 0, max: None, paged: false }, window_callback).unwrap();
    let (entity_log, entity_callback) = entity_recorder();
    let entities = db.observe_entities("users", entity_callback).unwrap();
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
    entities.retain(&[rid("u1")]).unwrap();
    let cancelled = logs.pop().unwrap();
    drop(observers.pop());
    let issued = db.counters().observation_reads - before;
    db.update("users", &rid("u1"), obj(json!({"name": "latest"}))).unwrap();
    drop(paused);
    db.await_revision(db.revision_seq().unwrap());
    assert!(issued <= 2, "ready results retain their read slots: {issued} reads before any result was applied");
    assert!(logs.iter().all(|log| log.lock().unwrap().last().is_some_and(|change| change.rows.len() == 1 && change.rows[0].text("name") == Some("latest"))), "every deferred opening reaches the last committed row");
    assert_eq!(calls(&joined).last(), Some(&1), "deferred join retains its subscriber");
    assert_eq!(window_log.lock().unwrap().last().unwrap().rows.len(), 1, "deferred growth retains its requested size");
    assert_eq!(entity_log.lock().unwrap().last().unwrap().rows[0].text("name"), Some("latest"), "deferred retain reaches the row");
    assert!(cancelled.lock().unwrap().is_empty(), "cancelled deferred opening stays silent");
}

#[test]
fn inv_observe_29_entity_refreshes_merge_until_the_in_flight_read_is_applied() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("u1")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let key = db.shared.observers().entities.keys().next().cloned().unwrap();
    let paused = db.shared.dispatch.pause_owner();
    db.shared.reads.inline.store(true, std::sync::atomic::Ordering::SeqCst);
    let before = db.counters().observation_reads;
    for _ in 0..64 {
        db.shared.refresh_entities(&key, &ModelKey::new("users"), [rid("u1")].into());
    }
    let issued = db.counters().observation_reads - before;
    db.update("users", &rid("u1"), obj(json!({"name": "latest"}))).unwrap();
    drop(paused);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(issued, 1, "one retained group has one read in flight");
    assert_eq!(log.lock().unwrap().last().unwrap().rows[0].text("name"), Some("latest"));
    assert!(db.counters().observation_reads - before <= 3, "repeated refreshes become one following read plus the committed update");
}

#[test]
fn inv_observe_29_repeated_window_growth_keeps_one_pending_read() {
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..64 {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&format!("u{index:02}")), row: obj(json!({"id": format!("u{index:02}"), "name": "Ann"})) });
    }
    db.land(envelope).unwrap();
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
fn inv_observe_21_a_retain_reads_outside_the_owner_and_delivers_the_row() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("u1")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let rows: Vec<_> = log.lock().unwrap().iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["u1"]);
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_22_a_retain_whose_read_failed_keeps_the_id_and_reports_the_failure_by_event() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    observer.retain(&[rid("late")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(db.counters().entity_reads_failed, 1);
    assert_eq!(log.lock().unwrap().iter().filter(|change| change.failure.is_some()).count(), 1, "the failure reaches the subscriber of the ids");
    put(&db, "users", json!({"id": "late", "name": "arrived"}));
    let rows: Vec<_> = log.lock().unwrap().iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["late"]);
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
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!((calls(&users), calls(&moments)), (vec![0], vec![0, 1]));
    assert_eq!(db.counters().owner_panics, 1);
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    assert_eq!(calls(&users), [0, 1], "a batch of another model reads the failed group again");
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&users), [0, 1, 2]);
}

#[test]
fn inv_observe_29_a_refresh_deferred_by_saturation_still_reads_only_the_touched_rows() {
    use super::reads::{ReadJob, ReadRows, ReadTarget};
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..500 {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&format!("u{index:03}")), row: obj(json!({"id": format!("u{index:03}"), "name": "Ann"})) });
    }
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let gate = std::sync::Arc::new(std::sync::Mutex::new(gate));
    for _ in 0..2 {
        let gate = gate.clone();
        let run = Box::new(move |_: &super::open::Shared| {
            let _ = recv_or_closed(&gate.lock().unwrap());
            Ok(ReadRows::Skipped { revision: 0, epoch: 0 })
        });
        let job = ReadJob { target: ReadTarget::Entities { key: "saturating".into(), ids: Default::default(), leases: Default::default() }, run };
        assert!(db.shared.reads.submit(&db.shared, job));
    }
    let before = db.counters().rows_loaded;
    db.update("users", &rid("u001"), obj(json!({"name": "latest"}))).unwrap();
    spin_until("an observer read was deferred", || db.shared.observers().deferred_reads() > 0);
    drop(release);
    db.await_revision(db.revision_seq().unwrap());
    let loaded = db.counters().rows_loaded - before;
    assert_eq!(log.lock().unwrap().last().unwrap().rows.iter().find(|row| row.id.as_str() == "u001").unwrap().text("name"), Some("latest"));
    assert!(loaded <= 1, "a deferred refresh of one touched row loaded {loaded} rows");
}

#[test]
fn inv_observe_29_refreshes_deferred_by_saturation_merge_their_touched_rows() {
    use super::reads::{ReadJob, ReadRows, ReadTarget};
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..500 {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&format!("u{index:03}")), row: obj(json!({"id": format!("u{index:03}"), "name": "Ann"})) });
    }
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let gate = std::sync::Arc::new(std::sync::Mutex::new(gate));
    for _ in 0..2 {
        let gate = gate.clone();
        let run = Box::new(move |_: &super::open::Shared| {
            let _ = recv_or_closed(&gate.lock().unwrap());
            Ok(ReadRows::Skipped { revision: 0, epoch: 0 })
        });
        let job = ReadJob { target: ReadTarget::Entities { key: "saturating".into(), ids: Default::default(), leases: Default::default() }, run };
        assert!(db.shared.reads.submit(&db.shared, job));
    }
    let before = db.counters().rows_loaded;
    db.update("users", &rid("u001"), obj(json!({"name": "latest"}))).unwrap();
    spin_until("an observer read was deferred", || db.shared.observers().deferred_reads() > 0);
    db.update("users", &rid("u002"), obj(json!({"name": "second"}))).unwrap();
    let revision = db.revision_seq().unwrap();
    spin_until("the groups saw the revision", || db.shared.observers().groups_seen_through(revision));
    drop(release);
    db.await_revision(db.revision_seq().unwrap());
    let loaded = db.counters().rows_loaded - before;
    assert_eq!(log.lock().unwrap().last().unwrap().rows.iter().find(|row| row.id.as_str() == "u001").unwrap().text("name"), Some("latest"));
    assert!(loaded <= 2, "two deferred refreshes of one touched row each loaded {loaded} rows");
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
