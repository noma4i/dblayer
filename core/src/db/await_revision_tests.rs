use std::sync::{Arc, Mutex};

use serde_json::json;

use super::fixtures::*;
use super::listeners::Feed;
use crate::query::Query;

type Tape = Arc<Mutex<Vec<i64>>>;

fn reached_tape() -> (Tape, impl Fn(Feed) + Send + Sync + 'static) {
    let log: Tape = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    (log, move |feed: Feed| {
        if let Feed::Reached(revision) = feed {
            sink.lock().unwrap().push(revision);
        }
    })
}

#[test]
fn inv_observe_28_await_revision_returns_after_every_subscriber_was_told_that_revision() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = reached_tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, feed).unwrap();
    for number in 0..20 {
        let id = format!("m{number}");
        db.create("moments", obj(json!({"id": id, "userId": "u1", "text": "t"}))).unwrap();
        let revision = db.revision_seq().unwrap();
        db.await_revision(revision);
        assert_eq!(log.lock().unwrap().last().copied(), Some(revision), "commit {number} of a model that the group does not read");
    }
}

#[test]
fn inv_observe_28_await_revision_of_a_past_revision_does_not_wait_for_later_commits() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = reached_tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, feed).unwrap();
    let past = db.revision_seq().unwrap();
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    db.await_revision(past);
    assert!(log.lock().unwrap().last().copied().unwrap() >= past);
}
