use super::support::*;
use super::*;
use crate::query::{Query, Where};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn observe_delivers_the_first_value_immediately() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    assert_eq!(calls(&log), vec![2]);
}

#[test]
fn observe_on_an_unknown_model_fails() {
    let db = booted();
    let (_, callback) = recorder();
    assert!(matches!(db.observe(Query::new("ghosts"), callback), Err(DbError::UnknownModel(_))));
}

#[test]
fn observer_is_called_when_a_matching_row_lands() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(calls(&log), vec![0, 1]);
}

#[test]
fn observer_is_not_called_for_a_commit_on_another_model() {
    let db = booted();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    let reads = db.diagnostics().observation_reads;
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    assert_eq!(calls(&log), vec![0]);
    assert_eq!(db.diagnostics().observation_reads, reads);
}

#[test]
fn observer_is_not_called_when_the_result_set_is_unchanged() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    let reads = db.diagnostics().observation_reads;
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "other"}));
    assert_eq!(calls(&log), vec![1]);
    assert_eq!(db.diagnostics().observation_reads, reads + 1);
}

#[test]
fn observer_is_called_when_a_member_changes_revision() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    db.update("moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
    assert_eq!(calls(&log), vec![1, 1]);
}

#[test]
fn observer_is_called_when_a_member_leaves_the_result() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "keep"}));
    let query = Query::new("moments").filtered(Where::equal("text", json!("keep")));
    let (log, callback) = recorder();
    let _subscription = db.observe(query, callback).unwrap();
    db.update("moments", &rid("m1"), obj(json!({"text": "gone"}))).unwrap();
    assert_eq!(calls(&log), vec![1, 0]);
}

#[test]
fn touching_a_parent_notifies_observers_of_the_parent() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("chats"), callback).unwrap();
    db.create("messages", obj(json!({"chatId": "c1", "body": "yo"}))).unwrap();
    assert_eq!(calls(&log), vec![1, 1]);
}

#[test]
fn dropping_the_subscription_stops_delivery() {
    let db = booted();
    let (log, callback) = recorder();
    let subscription = db.observe(Query::new("moments"), callback).unwrap();
    assert_eq!(db.diagnostics().subscriptions, 1);
    drop(subscription);
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(calls(&log), vec![0]);
    assert_eq!(db.diagnostics().subscriptions, 0);
    assert_eq!(db.diagnostics().observed_queries, 0);
}

#[test]
fn many_observers_of_one_query_share_one_read_per_commit() {
    let db = booted();
    seed_tree(&db);
    let logs: Vec<(CallLog, Subscription)> = (0..5)
        .map(|_| {
            let (log, callback) = recorder();
            (log, db.observe(Query::new("moments"), callback).unwrap())
        })
        .collect();
    assert_eq!(db.diagnostics().observation_reads, 1);
    assert_eq!(db.diagnostics().observed_queries, 1);
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    assert_eq!(db.diagnostics().observation_reads, 2);
    assert!(logs.iter().all(|(log, _)| calls(log) == vec![2, 3]));
    assert_eq!(db.diagnostics().observer_calls, 10);
}

#[test]
fn different_queries_are_read_separately() {
    let db = booted();
    let (_, first) = recorder();
    let (_, second) = recorder();
    let _a = db.observe(Query::new("moments"), first).unwrap();
    let _b = db.observe(Query::new("moments").limited(1), second).unwrap();
    assert_eq!(db.diagnostics().observed_queries, 2);
    let reads = db.diagnostics().observation_reads;
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(db.diagnostics().observation_reads, reads + 2);
}

#[test]
fn late_subscriber_joins_the_shared_value_without_a_read() {
    let db = booted();
    seed_tree(&db);
    let (_, first) = recorder();
    let _a = db.observe(Query::new("moments"), first).unwrap();
    let reads = db.diagnostics().observation_reads;
    let (log, second) = recorder();
    let _b = db.observe(Query::new("moments"), second).unwrap();
    assert_eq!(db.diagnostics().observation_reads, reads);
    assert_eq!(calls(&log), vec![2]);
}

#[test]
fn observer_may_read_the_database_from_its_callback() {
    let db = booted();
    let seen = Arc::new(AtomicUsize::new(0));
    let reader = db.clone();
    let counter = seen.clone();
    let _subscription = db
        .observe(Query::new("moments"), move |_| {
            counter.store(reader.count(&Query::new("moments")).unwrap() as usize, Ordering::SeqCst);
        })
        .unwrap();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}
