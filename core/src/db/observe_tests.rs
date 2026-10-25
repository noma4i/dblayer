use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use super::fixtures::*;
use super::*;
use crate::query::{Query, Where};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn observe_delivers_the_first_value_with_everything_inserted() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let first = changes(&log);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].diff.inserted.len(), 2);
    assert!(first[0].diff.removed.is_empty() && first[0].diff.updated.is_empty());
}

#[test]
fn observe_returns_without_calling_the_listener_on_the_caller_thread() {
    let db = booted();
    let caller = std::thread::current().id();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _subscription = db.observe(Query::new("moments"), move |_| sink.lock().unwrap().push(std::thread::current().id())).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let threads = seen.lock().unwrap().clone();
    assert_eq!(threads.len(), 1);
    assert_ne!(threads[0], caller);
}

#[test]
fn observe_on_an_unknown_model_fails() {
    let db = booted();
    let (_, callback) = recorder();
    assert!(matches!(db.observe(Query::new("ghosts"), callback), Err(DbError::UnknownModel(_))));
}

#[test]
fn observe_with_an_unknown_include_fails() {
    let db = booted();
    let (_, callback) = recorder();
    assert!(matches!(db.observe(Query::new("moments").including(&["ghosts"]), callback), Err(DbError::UnknownAssociation { .. })));
}

#[test]
fn observer_is_called_when_a_matching_row_lands() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(calls(&log), vec![0, 1]);
    assert_eq!(changes(&log)[1].diff.inserted, vec![rid("m1")]);
}

#[test]
fn observer_is_not_called_for_a_commit_on_another_model() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    assert_eq!(calls(&log), vec![0]);
    assert_eq!(db.counters().observation_reads, reads);
}

#[test]
fn observer_is_not_called_when_the_result_set_is_unchanged() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "other"}));
    assert_eq!(calls(&log), vec![1]);
    assert_eq!(db.counters().observation_reads, reads, "a row that is outside the list and fails its filter costs no read of the list");
}

#[test]
fn a_commit_with_the_same_content_is_silent() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let revision = db.revision_seq().unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    assert_eq!(db.revision_seq().unwrap(), revision);
    assert_eq!(calls(&log), vec![1]);
}

#[test]
fn observer_is_called_when_a_member_changes_revision() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.update("moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![1, 1]);
    assert_eq!(changes(&log)[1].diff.updated, vec![rid("m1")]);
}

#[test]
fn observer_is_called_when_a_member_leaves_the_result() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.update("moments", &rid("m1"), obj(json!({"text": "gone"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![1, 0]);
    assert_eq!(changes(&log)[1].diff.removed, vec![rid("m1")]);
}

#[test]
fn touching_a_parent_notifies_observers_of_the_parent() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("chats"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.create("messages", obj(json!({"chatId": "c1", "body": "yo"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![1, 1]);
    assert_eq!(changes(&log)[1].diff.updated, vec![rid("c1")]);
}

#[test]
fn revisions_of_deliveries_grow() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    for number in 0..5 {
        put(&db, "moments", json!({"id": format!("m{number}"), "userId": "u1", "text": "t"}));
    }
    let revisions: Vec<i64> = changes(&log).iter().map(|change| change.revision).collect();
    assert_eq!(revisions.len(), 6);
    assert!(revisions.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn dropping_the_subscription_stops_delivery() {
    let db = booted();
    let (log, callback) = recorder();
    let subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(db.observer_census().subscriptions, 1);
    drop(subscription);
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(calls(&log), vec![0]);
    assert_eq!(db.observer_census().subscriptions, 0);
    assert_eq!(db.observer_census().observed_queries, 0);
}

#[test]
fn many_observers_of_one_query_share_one_read_per_commit() {
    let db = booted();
    seed_tree(&db);
    let logs: Vec<(ChangeLog, ObserverHandle)> = (0..5)
        .map(|_| {
            let (log, callback) = recorder();
            (log, db.observe(Query::new("moments"), callback).unwrap())
        })
        .collect();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(db.counters().observation_reads, 1);
    assert_eq!(db.observer_census().observed_queries, 1);
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    assert_eq!(db.counters().observation_reads, 2);
    assert!(logs.iter().all(|(log, _)| calls(log) == vec![2, 3]));
    assert_eq!(db.counters().observer_calls, 10);
}

#[test]
fn different_queries_are_read_separately() {
    let db = booted();
    let (_, first) = recorder();
    let (_, second) = recorder();
    let _a = db.observe(Query::new("moments"), first).unwrap();
    let _b = db.observe(Query::new("moments").limited(1), second).unwrap();
    assert_eq!(db.observer_census().observed_queries, 2);
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(db.counters().observation_reads, reads + 2);
}

#[test]
fn late_subscriber_joins_the_shared_value_without_a_read() {
    let db = booted();
    seed_tree(&db);
    let (_, first) = recorder();
    let _a = db.observe(Query::new("moments"), first).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    let (log, second) = recorder();
    let _b = db.observe(Query::new("moments"), second).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(db.counters().observation_reads, reads);
    assert_eq!(calls(&log), vec![2]);
    assert_eq!(changes(&log)[0].diff.inserted.len(), 2);
}

#[test]
fn inv_observe_09_observer_may_read_and_write_the_database_from_its_callback() {
    let db = booted();
    let seen = Arc::new(AtomicUsize::new(0));
    let handle = db.clone();
    let counter = seen.clone();
    let _subscription = db
        .observe(Query::new("moments"), move |change| {
            counter.store(handle.count(&Query::new("moments")).unwrap() as usize, Ordering::SeqCst);
            if change.rows.len() == 1 {
                handle.create("chats", obj(json!({"title": "from listener"}))).unwrap();
            }
        })
        .unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(db.count(&Query::new("chats")).unwrap(), 1);
}

#[test]
fn inv_observe_09_a_panicking_listener_does_not_stop_the_others() {
    let db = booted();
    let _bad = db.observe(Query::new("moments"), |_| panic!("listener failed")).unwrap();
    let (log, callback) = recorder();
    let _good = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(calls(&log), vec![0, 1]);
}

#[test]
fn late_observer_of_a_shared_query_sees_the_write_that_preceded_it() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "old"}));
    let (_, first) = recorder();
    let _early = db.observe(Query::new("moments"), first).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    for round in 0..50 {
        let text = format!("v{round}");
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m1"), row: obj(json!({"id": "m1", "userId": "u1", "text": text})) });
        db.land(envelope).unwrap();
        let (log, second) = recorder();
        let _late = db.observe(Query::new("moments"), second).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        assert_eq!(texts(&changes(&log)[0]), vec![text], "round {round}");
    }
}

#[test]
fn attach_cancelled_at_any_moment_leaves_no_subscription() {
    let db = booted();
    drop(db.observe(Query::new("users"), |_: &Change| {}).unwrap());
    let settled = db.observe(Query::new("users"), |_: &Change| {}).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    drop(settled);
    let (log, callback) = recorder();
    let after_first = db.observe(Query::new("users"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log).len(), 1);
    drop(after_first);
    let entities = db.observe_entities("users", |_| {}).unwrap();
    entities.retain(&[rid("u1")]).unwrap();
    drop(entities);
    db.await_revision(db.revision_seq().unwrap());
    let census = db.observer_census();
    assert_eq!((census.observed_queries, census.subscriptions), (0, 0));
}
