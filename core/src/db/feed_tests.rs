use std::sync::{Arc, Mutex};

use serde_json::json;

use super::fixtures::*;
use super::listeners::Feed;
use super::watch::Subject;
use super::window::WindowSpec;
use crate::query::{Order, Query, Where};
use crate::value::SortDirection;
use crate::test_wait::join_within;

type Tape = Arc<Mutex<Vec<String>>>;

fn tape() -> (Tape, impl Fn(Feed) + Send + Sync + 'static) {
    let log: Tape = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    (log, move |feed: Feed| {
        let tag = match feed {
            Feed::Rows(change) => format!("rows:{}:{}", change.revision, change.size),
            Feed::Count(change) => format!("count:{}", change.count),
            Feed::Reached(revision) => format!("reached:{revision}"),
            Feed::Failed(_) => "failed".to_string(),
        };
        sink.lock().unwrap().push(tag);
    })
}

fn told(log: &Tape) -> Vec<String> {
    log.lock().unwrap().clone()
}

fn last_reached(log: &Tape) -> Option<i64> {
    told(log).iter().rev().find_map(|tag| tag.strip_prefix("reached:").and_then(|revision| revision.parse().ok()))
}

#[test]
fn inv_observe_25_a_subscriber_reaches_the_revision_of_a_commit_that_changed_none_of_its_rows() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let observer = db.observe_query_feed(Query::new("users"), false, false, feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let at = db.revision_seq().unwrap();
    assert_ne!(last_reached(&log), Some(at), "a commit that touched none of its rows is not announced unasked: {:?}", told(&log));
    observer.control().want_reach(at);
    db.await_revision(at);
    assert_eq!(last_reached(&log), Some(at), "told {:?}", told(&log));
    assert_eq!(told(&log).iter().filter(|tag| tag.starts_with("rows:")).count(), 1);
}

#[test]
fn inv_observe_25_a_commit_that_reaches_a_group_but_leaves_its_rows_is_reached_too() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let query = Query::new("users").filtered(Where::equal("name", json!("Ann")));
    let _observer = db.observe_query_feed(query, false, false, feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    let at = db.revision_seq().unwrap();
    assert_eq!(last_reached(&log), Some(at), "told {:?}", told(&log));
    assert_eq!(told(&log).iter().filter(|tag| tag.starts_with("rows:")).count(), 1);
}

#[test]
fn inv_observe_25_a_subscriber_that_joins_a_group_is_told_how_far_the_group_has_read() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (first, first_feed) = tape();
    let _one = db.observe_query_feed(Query::new("users"), false, false, first_feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let (second, second_feed) = tape();
    let _two = db.observe_query_feed(Query::new("users"), false, false, second_feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let at = db.revision_seq().unwrap();
    assert_eq!((last_reached(&first), last_reached(&second)), (Some(at), Some(at)), "told {:?} and {:?}", told(&first), told(&second));
}

#[test]
fn inv_observe_25_a_count_that_asks_is_told_how_far_it_has_read_and_a_window_that_did_not_is_not() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (windowed, window_feed) = tape();
    let (counted, count_feed) = tape();
    let ordered = Query::new("users").ordered(vec![Order::new("name", SortDirection::Asc)]);
    let _window = db.observe_window_feed(ordered, WindowSpec::Tail { size: 2, max: None, paged: false }, false, window_feed).unwrap();
    let count = db.observe_query_feed(Query::new("users"), true, false, count_feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let at = db.revision_seq().unwrap();
    count.control().want_reach(at);
    db.await_revision(at);
    assert_eq!((last_reached(&windowed).is_some_and(|reached| reached >= at), last_reached(&counted)), (false, Some(at)), "told {:?} and {:?}", told(&windowed), told(&counted));
    assert!(told(&counted).contains(&"count:1".to_string()));
}

#[test]
fn inv_observe_26_a_read_that_failed_reaches_the_subscriber_and_the_next_read_heals_it() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, false, feed).unwrap();
    let at = db.revision_seq().unwrap();
    db.await_revision(at);
    *db.shared.store.lock().poison.lock().unwrap() = Some("the store cannot read".to_string());
    db.shared.refresh_groups(&[super::dispatch::Event { revision: i64::MAX / 2, models: [crate::identity::ModelKey::new("users")].into(), ..Default::default() }]);
    db.await_revision(at);
    assert_eq!(told(&log).iter().filter(|tag| *tag == "failed").count(), 1, "told {:?}", told(&log));
    *db.shared.store.lock().poison.lock().unwrap() = None;
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    let after = told(&log);
    assert!(after.last().map(String::as_str).filter(|tag| tag.starts_with("reached:")).is_some(), "told {after:?}");
    assert!(after.iter().any(|tag| tag.starts_with("rows:") && tag.ends_with(":2")), "told {after:?}");
}

#[test]
fn inv_observe_26_an_open_that_failed_reaches_the_subscriber_before_any_rows() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let at = db.revision_seq().unwrap();
    *db.shared.store.lock().poison.lock().unwrap() = Some("the store cannot read".to_string());
    let (log, feed) = tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, false, feed).unwrap();
    db.await_revision(at);
    assert_eq!(told(&log), vec!["failed".to_string()]);
    *db.shared.store.lock().poison.lock().unwrap() = None;
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert!(told(&log).iter().any(|tag| tag.starts_with("rows:") && tag.ends_with(":2")), "told {:?}", told(&log));
}

#[test]
fn inv_observe_26_a_panic_while_applying_a_group_reaches_its_subscriber() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, false, feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    *db.shared.panic_key.lock().unwrap() = Some(Subject::Rows(Query::new("users")).key());
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert!(told(&log).contains(&"failed".to_string()), "told {:?}", told(&log));
}

fn land_user(db: &crate::db::Db, id: &str, name: &str) {
    let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: crate::identity::ModelKey::new("users"), id: rid(id), row: obj(json!({"id": id, "name": name})) });
    db.as_app().land(envelope).unwrap();
}

fn wait_until(mut done: impl FnMut() -> bool) {
    for _ in 0..5_000_000 {
        if done() {
            return;
        }
        std::thread::yield_now();
    }
    panic!("condition did not hold within the bound");
}

#[test]
fn a_paused_feed_delivers_nothing_then_one_snapshot_of_the_latest_rows_on_resume() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let observer = db.observe_feed(Subject::Rows(Query::new("users")), true, feed).unwrap();
    for index in 0..20 {
        land_user(&db, &format!("n{index}"), "x");
    }
    let revision = db.revision_seq().unwrap();
    wait_until(|| db.shared.observers().groups_read_through(revision) && observer.owes_delivery());
    assert!(told(&log).is_empty(), "nothing is delivered while paused: {:?}", told(&log));
    observer.control().resume();
    db.await_revision(db.revision_seq().unwrap());
    let tags = told(&log);
    let rows: Vec<&String> = tags.iter().filter(|tag| tag.starts_with("rows:")).collect();
    assert_eq!(rows.len(), 1, "one coalesced change: {tags:?}");
    assert!(rows[0].ends_with(":21"), "the full snapshot of the latest rows: {tags:?}");
    assert_eq!(last_reached(&log), Some(db.revision_seq().unwrap()));
}

#[test]
fn cancelling_a_paused_feed_leaves_nothing_owed() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, feed) = tape();
    let observer = db.observe_feed(Subject::Rows(Query::new("users")), true, feed).unwrap();
    land_user(&db, "u2", "Bob");
    drop(observer);
    db.await_revision(db.revision_seq().unwrap());
    assert!(told(&log).is_empty(), "a cancelled feed stays silent: {:?}", told(&log));
    assert_eq!(db.observer_census().subscriptions, 0);
}

#[test]
fn a_feed_resumed_with_nothing_owed_delivers_nothing_extra() {
    let db = booted();
    let (log, feed) = tape();
    let observer = db.observe_feed(Subject::Rows(Query::new("users")), true, feed).unwrap();
    observer.control().resume();
    observer.control().resume();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    db.await_revision(db.revision_seq().unwrap());
    let rows: Vec<String> = told(&log).into_iter().filter(|tag| tag.starts_with("rows:")).collect();
    assert!(rows.len() <= 2 && rows.last().map(String::as_str) == Some("rows:1:1"), "at most the empty first snapshot, then the commit: {rows:?}");
}

#[test]
fn resume_racing_with_commits_never_loses_the_wakeup() {
    for round in 0..100 {
        let db = booted();
        put(&db, "users", json!({"id": "u0", "name": "Ann"}));
        let (log, feed) = tape();
        let observer = db.observe_feed(Subject::Rows(Query::new("users")), true, feed).unwrap();
        let writer = {
            let db = db.clone();
            std::thread::spawn(move || {
                for index in 0..5 {
                    land_user(&db, &format!("r{round}-{index}"), "x");
                }
            })
        };
        observer.control().resume();
        join_within(writer);
        db.await_revision(db.revision_seq().unwrap());
        let last = told(&log).iter().rev().find(|tag| tag.starts_with("rows:")).cloned().unwrap_or_default();
        assert!(last.ends_with(":6"), "round {round}: the last delivery is the latest rows: {:?}", told(&log));
    }
}

#[test]
fn inv_observe_30_a_group_with_an_unread_commit_is_not_read_through_that_revision() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (_, feed) = tape();
    let _observer = db.observe_query_feed(Query::new("users"), false, false, feed).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let ahead = i64::MAX / 2;
    let commit = || super::dispatch::Event { revision: ahead, models: [crate::identity::ModelKey::new("users")].into(), ..Default::default() };
    let planned = db.shared.plan_refresh(&[commit()]);
    assert!(!db.shared.observers().groups_read_through(ahead), "the group has not read the commit yet");
    db.shared.run_refresh(&[commit()], planned);
    db.await_revision(ahead);
}
