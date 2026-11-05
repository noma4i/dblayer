use serde_json::json;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::rig::*;
use super::subscribe_tests::change_feed;
use super::{EventReader, SyncConfig};
use crate::db::Db;
use crate::schema::ChangeRead;
use crate::transport::{MockTransport, Reply, Response, SubscriptionEvent};

fn event(topic: &str, action: &str, id: &str) -> serde_json::Value {
    json!({"topic": topic, "action": action, "recordId": id})
}

fn emit(rig: &Rig, changes: Vec<serde_json::Value>) {
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents": {"changes": changes}}))));
}

fn chat_rig() -> Rig {
    let original = super::node_tests::node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    let chats = specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap();
    chats.changes.push(ChangeRead { area: "CHAT".into(), relation: "node".into(), parameter: None, removes_on: vec!["DELETED".into()], observed: false, payload: None, id_of: None });
    chats.change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    rig
}

#[derive(Debug)]
pub(super) enum HeardItem {
    Event(super::ModelEvent),
    Lost(u64),
}

/// Keeps what a reader of model events hears, the way the journal of a stream takes it: an event of another session is dropped, one of an older generation and one that could not be read are counted as missed.
struct Log {
    model: String,
    life: Arc<crate::ffi::legacy::Leases>,
    items: Mutex<VecDeque<HeardItem>>,
    ready: tokio::sync::Notify,
}

impl Log {
    fn gap(&self, lost: u64) {
        let mut items = self.items.lock().unwrap();
        match items.back_mut() {
            Some(HeardItem::Lost(count)) => *count += lost,
            _ => items.push_back(HeardItem::Lost(lost)),
        }
        drop(items);
        self.ready.notify_one();
    }
}

impl EventReader for Log {
    fn model(&self) -> &str { &self.model }

    fn heard(&self, event: &super::ModelEvent, stamp: crate::ffi::legacy::Stamp) {
        let now = self.life.snapshot().stamp();
        if now.session_epoch != stamp.session_epoch { return; }
        if now.generation != stamp.generation { return self.gap(1); }
        self.items.lock().unwrap().push_back(HeardItem::Event(event.clone()));
        self.ready.notify_one();
    }

    fn unread(&self, count: u64) { self.gap(count); }
}

/// A test reader of the events of a model with the sources it holds.
pub(super) struct Heard {
    events: super::ModelEvents,
    log: Arc<Log>,
}

impl Heard {
    pub(super) async fn next(&self) -> Option<HeardItem> {
        loop {
            let notified = self.log.ready.notified();
            if let Some(item) = self.log.items.lock().unwrap().pop_front() { return Some(item); }
            notified.await;
        }
    }

    pub(super) fn close(&self) { self.events.close(); }
}

pub(super) async fn open_heard(sync: &super::Sync, model: &str) -> Result<Heard, super::SyncError> {
    let log = Arc::new(Log { model: model.to_string(), life: sync.inner.db.shared.store.lifecycle.clone(), items: Mutex::default(), ready: tokio::sync::Notify::new() });
    let reader: Arc<dyn EventReader> = log.clone();
    let events = sync.open_model_events(model, Arc::downgrade(&reader)).await?;
    Ok(Heard { events, log })
}

async fn within(events: &Heard) -> Option<HeardItem> {
    crate::test_wait::timeout(std::time::Duration::from_secs(5), events.next()).await.expect("the next event did not arrive")
}

fn expect_event(item: Option<HeardItem>) -> super::ModelEvent {
    match item {
        Some(HeardItem::Event(event)) => event,
        other => panic!("expected an event, got {other:?}"),
    }
}

fn title(row: &Option<crate::db::Row>) -> Option<&str> {
    row.as_ref().and_then(|row| row.text("title"))
}

#[tokio::test]
async fn model_events_open_again_after_a_data_reset() {
    let rig = chat_rig();
    let _old = crate::test_wait::within_secs(5, open_heard(&rig.sync, "chats")).await.unwrap();
    rig.db.clear_account().unwrap();
    let events = crate::test_wait::within_secs(5, open_heard(&rig.sync, "chats")).await.unwrap();
    crate::test_wait::within_secs(5, rig.transport.await_calls("Changes", 2)).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    let seen = expect_event(crate::test_wait::within_secs(5, events.next()).await);
    assert_eq!((seen.area.as_str(), seen.kind.as_str(), seen.id.as_str()), ("CHAT", "DELETED", "c1"));
}

#[test]
fn inv_api_06_opening_events_establishes_the_subscription_or_returns_its_failure() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    assert_eq!(rig.transport.call_count("ChatNode"), 0, "idle");
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "after"}})));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.area.as_str(), seen.kind.as_str(), seen.id.as_str()), ("CHAT", "UPDATED", "c1"));
    assert_eq!((title(&seen.before), title(&seen.after)), (Some("before"), Some("after")));
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    events.close();
    crate::test_wait::pass(rig.sync.inner.config.live_linger).await;
    rig.transport.await_cancellations("Changes", 1).await;

    let failed = chat_rig();
    failed.sync.inner.operations.lock().unwrap().remove("Changes");
    let opened = crate::test_wait::timeout(std::time::Duration::from_secs(5), open_heard(&failed.sync, "chats")).await.expect("opening must report a failed subscription");
    assert!(matches!(opened, Err(super::SyncError::UnknownOperation(operation)) if operation == "Changes"));
    assert!(!failed.sync.inner.has_event_readers("chats"));
}) }

#[test]
fn offline_open_waits_for_connectivity_and_delivers_without_another_open() { crate::test_wait::paused(async {
    let rig = chat_rig();
    rig.sync.set_online(false);
    let mut opening = Box::pin(open_heard(&rig.sync, "chats"));
    assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), &mut opening).await.is_err());
    assert_eq!(rig.transport.call_count("Changes"), 0);
    rig.sync.set_online(true);
    let events = crate::test_wait::timeout(std::time::Duration::from_secs(5), opening).await.unwrap().unwrap();
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    assert_eq!(expect_event(within(&events).await).id, "c1");
    assert_eq!(rig.transport.call_count("Changes"), 1);
}) }

#[test]
fn opening_waits_for_server_ready_and_reports_a_refusal_before_ready() { crate::test_wait::paused(async {
    for refused in [false, true] {
        let rig = chat_rig();
        rig.transport.script_subscription("Changes", vec![]);
        let mut opening = Box::pin(open_heard(&rig.sync, "chats"));
        assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), &mut opening).await.is_err());
        rig.transport.await_consumed("Changes").await;
        if refused {
            let error = crate::transport::TransportError::Http { status: 403, body: "refused".into() };
            rig.transport.emit("Changes", SubscriptionEvent::Failed(crate::transport::SubscriptionFailure::from_error(error.clone())));
            assert!(matches!(opening.await, Err(super::SyncError::Transport(seen)) if seen == error));
            assert!(!rig.sync.inner.has_event_readers("chats"));
        } else {
            rig.transport.emit("Changes", SubscriptionEvent::Ready);
            let events = opening.await.unwrap();
            emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
            assert_eq!(expect_event(within(&events).await).id, "c1");
        }
    }
}) }

#[test]
fn abandoning_a_prepared_or_pending_feed_releases_its_subscription() { crate::test_wait::paused(async {
    for pending in [false, true] {
        let rig = chat_rig();
        if pending { rig.transport.script_subscription("Changes", vec![]); }
        let mut opening = Box::pin(open_heard(&rig.sync, "chats"));
        if pending {
            assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), &mut opening).await.is_err());
            drop(opening);
        } else {
            drop(opening.await.unwrap());
        }
        assert!(!rig.sync.inner.has_event_readers("chats"));
        crate::test_wait::pass(rig.sync.inner.config.live_linger).await;
        rig.transport.await_cancellations("Changes", 1).await;
        assert_eq!(rig.sync.inner.live_status("Changes|{}"), None);
    }
}) }

#[test]
fn removal_event_has_no_after_row_and_makes_no_request() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((title(&seen.before), seen.after.is_none()), (Some("before"), true));
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
}) }

#[test]
fn two_events_of_one_row_arrive_in_order_and_chain_before_to_after() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "v1"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    for next in ["v2", "v3"] { rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": next}}))); }
    emit(&rig, vec![event("CHAT", "UPDATED", "c1"), event("CHAT", "UPDATED", "c1")]);
    let (first, second) = (expect_event(within(&events).await), expect_event(within(&events).await));
    assert_eq!([title(&first.before), title(&first.after), title(&second.before), title(&second.after)], [Some("v1"), Some("v2"), Some("v2"), Some("v3")]);
    assert_eq!(rig.transport.call_count("ChatNode"), 2);
}) }

#[test]
fn observed_event_rereads_every_watched_parameterized_read_once_and_no_other() { crate::test_wait::paused(async {
    let mut moments = moments_spec();
    moments.changes.push(ChangeRead { area: "WALLET".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true, payload: None, id_of: None });
    moments.change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    let _wild = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "wild"}))).unwrap();
    let events = open_heard(&rig.sync, "moments").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    assert_eq!(rig.transport.call_count("MomentsByMood"), 0, "idle");
    for _ in 0..2 { rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]}))); }
    emit(&rig, vec![event("WALLET", "CREATED", "t1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.area.as_str(), seen.id.as_str()), ("WALLET", "t1"));
    assert_eq!(rig.transport.call_count("MomentsByMood"), 2);
    assert_eq!(rig.transport.call_count("MomentsByUser"), 0, "unwatched read");
    for _ in 0..2 { rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": []}))); }
    emit(&rig, vec![event("WALLET", "CREATED", "t2")]);
    expect_event(within(&events).await);
    assert_eq!(rig.transport.call_count("MomentsByMood"), 4);
}) }

#[test]
fn a_failed_read_leaves_a_gap_in_the_queue_at_the_place_of_the_event() { crate::test_wait::paused(async {
    let rig = chat_rig();
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    let failure = || crate::db::DbError::InvalidUpload("read failed".into());
    let inner = &rig.sync.inner;
    assert!(inner.publish_rows(super::model_event::Heard { stamp: inner.db.shared.store.lifecycle.snapshot().stamp(), area: "CHAT", kind: "UPDATED" }, "chats", "c1", Ok(None), Ok(None)));
    assert!(!inner.publish_rows(super::model_event::Heard { stamp: inner.db.shared.store.lifecycle.snapshot().stamp(), area: "CHAT", kind: "UPDATED" }, "chats", "c2", Err(failure()), Ok(None)));
    assert!(!inner.publish_rows(super::model_event::Heard { stamp: inner.db.shared.store.lifecycle.snapshot().stamp(), area: "CHAT", kind: "UPDATED" }, "chats", "c3", Ok(None), Err(failure())));
    assert!(inner.publish_rows(super::model_event::Heard { stamp: inner.db.shared.store.lifecycle.snapshot().stamp(), area: "CHAT", kind: "UPDATED" }, "chats", "c4", Ok(None), Ok(None)));
    assert_eq!(expect_event(within(&events).await).id, "c1");
    assert!(matches!(within(&events).await, Some(HeardItem::Lost(2))));
    assert_eq!(expect_event(within(&events).await).id, "c4");
}) }

#[test]
fn a_failed_reread_leaves_a_gap_for_the_reader() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    rig.transport.enqueue("ChatNode", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "rejected".into() }));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    assert!(matches!(within(&events).await, Some(HeardItem::Lost(1))));
}) }

#[test]
fn rows_are_read_for_an_event_only_while_the_model_has_a_reader() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "v1"}));
    let opened = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    drop(opened);
    let finds = || crate::db::FINDS.with(std::cell::Cell::get);
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "v2"}})));
    let start = finds();
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    rig.transport.await_consumed("Changes").await;
    let unwatched = finds() - start;
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "v3"}})));
    let start = finds();
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    expect_event(within(&events).await);
    assert_eq!(finds() - start, unwatched + 2);
}) }

#[test]
fn a_model_gets_the_events_of_a_changes_source_that_another_model_declares() { crate::test_wait::paused(async {
    for (change, area) in [
        (ChangeRead { area: "WALLET".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true, payload: None, id_of: None }, "WALLET"),
        (ChangeRead { area: "MOOD".into(), relation: "by_mood".into(), parameter: Some("mood".into()), removes_on: vec![], observed: false, payload: None, id_of: None }, "MOOD"),
    ] {
        let (mut users, mut moments) = (users_spec(), moments_spec());
        users.change_feed = Some(change_feed());
        moments.changes.push(change);
        let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
        let events = open_heard(&rig.sync, "moments").await.unwrap();
        rig.transport.await_calls("Changes", 1).await;
        rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]})));
        emit(&rig, vec![event(area, "CREATED", "calm")]);
        let seen = expect_event(within(&events).await);
        assert_eq!((seen.area.as_str(), seen.id.as_str()), (area, "calm"), "{area}");
        assert_eq!(rig.transport.call_count("MomentsByMood"), 1, "{area}");
        assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "{area}: one event only");
    }
}) }

#[test]
fn an_event_that_arrived_before_the_reader_is_not_replayed_and_the_next_one_is_delivered() { crate::test_wait::paused(async {
    let (mut users, mut moments) = (users_spec(), moments_spec());
    users.change_feed = Some(change_feed());
    moments.changes.push(ChangeRead { area: "MOOD".into(), relation: "by_mood".into(), parameter: Some("mood".into()), removes_on: vec![], observed: false, payload: None, id_of: None });
    let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    rig.transport.await_calls("Changes", 1).await;
    for _ in 0..2 { rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]}))); }
    emit(&rig, vec![event("MOOD", "CREATED", "calm")]);
    rig.transport.await_calls("MomentsByMood", 1).await;
    let events = open_heard(&rig.sync, "moments").await.unwrap();
    assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "the earlier event is not replayed");
    emit(&rig, vec![event("MOOD", "CREATED", "wild")]);
    assert_eq!(expect_event(within(&events).await).id, "wild");
}) }

#[test]
fn one_event_of_an_area_shared_by_two_models_rereads_both_and_reaches_both_event_streams() { crate::test_wait::paused(async {
    let (mut users, mut moments) = (users_spec(), moments_spec());
    for spec in [&mut users, &mut moments] {
        spec.changes.push(ChangeRead { area: "WALLET".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true, payload: None, id_of: None });
    }
    users.change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _me = rig.sync.watch("users", "current", &params(json!({}))).unwrap();
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    let (user_events, moment_events) = (open_heard(&rig.sync, "users").await.unwrap(), open_heard(&rig.sync, "moments").await.unwrap());
    rig.transport.await_calls("Changes", 1).await;
    assert_eq!((rig.transport.call_count("CurrentUser"), rig.transport.call_count("MomentsByMood")), (0, 0), "idle");
    rig.transport.enqueue("CurrentUser", Reply::data(json!({"me": {"id": "u1", "name": "a"}})));
    rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]})));
    emit(&rig, vec![event("WALLET", "CREATED", "t1")]);
    for events in [&user_events, &moment_events] {
        let seen = expect_event(within(events).await);
        assert_eq!((seen.area.as_str(), seen.id.as_str()), ("WALLET", "t1"));
        assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "one item per event");
    }
    assert_eq!((rig.transport.call_count("CurrentUser"), rig.transport.call_count("MomentsByMood")), (1, 1));
}) }

#[test]
fn two_reads_on_one_area_reread_once_each_and_deliver_one_event() { crate::test_wait::paused(async {
    let (mut users, mut moments) = (users_spec(), moments_spec());
    users.change_feed = Some(change_feed());
    moments.changes.push(ChangeRead { area: "MOOD".into(), relation: "by_mood".into(), parameter: Some("mood".into()), removes_on: vec![], observed: false, payload: None, id_of: None });
    moments.changes.push(ChangeRead { area: "MOOD".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true, payload: None, id_of: None });
    let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    let events = open_heard(&rig.sync, "moments").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    for _ in 0..2 { rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]}))); }
    emit(&rig, vec![event("MOOD", "CREATED", "calm")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.area.as_str(), seen.id.as_str()), ("MOOD", "calm"));
    assert_eq!(rig.transport.call_count("MomentsByMood"), 2);
    assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "one event only");
}) }

#[test]
fn an_events_collector_keeps_receiving_across_sign_out_and_the_next_sign_in() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "t"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.set_session(Some("first"));
    let reads = rig.transport.waits("Changes");
    rig.sync.commit_session_for_test();
    rig.transport.await_waits("Changes", reads + 1).await;
    rig.transport.await_calls("Changes", 1).await;
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "one"}})));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    assert_eq!(expect_event(within(&events).await).id, "c1");
    rig.sync.lose_session_for_test();
    crate::test_wait::pass(std::time::Duration::from_secs(31)).await;
    rig.transport.set_session(Some("second"));
    rig.sync.commit_session_for_test();
    crate::test_wait::pass(std::time::Duration::from_secs(1)).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.kind.as_str(), seen.id.as_str()), ("DELETED", "c1"));
}) }

#[test]
fn an_events_collector_keeps_one_stream_across_a_session_start_and_hears_only_the_new_connection() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "t"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.transport.await_calls("Changes", 1).await;
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.start_session_in_tests(Some("another-account".into()), life.admit_passive(crate::ffi::legacy::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    rig.transport.await_calls("Changes", 2).await;
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "new"}})));
    let change = |kind: &str| SubscriptionEvent::Next(Response::data(json!({"recordEvents": {"changes": [event("CHAT", kind, "c1")]}})));
    rig.transport.emit_on("Changes", 0, change("DELETED"));
    rig.transport.emit_on("Changes", 1, change("UPDATED"));
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.kind.as_str(), seen.id.as_str(), title(&seen.after)), ("UPDATED", "c1", Some("new")));
    assert!(crate::test_wait::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "the old connection delivered nothing");
}) }

#[test]
fn an_events_collector_keeps_receiving_across_a_denied_and_allowed_listening() { crate::test_wait::paused(async {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "t"}));
    let events = open_heard(&rig.sync, "chats").await.unwrap();
    rig.transport.await_calls("Changes", 1).await;
    rig.sync.set_listening(false);
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    crate::test_wait::pass(std::time::Duration::from_secs(1)).await;
    rig.sync.set_listening(true);
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "one"}})));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    assert_eq!(expect_event(within(&events).await).id, "c1");
    rig.sync.set_listening(false);
    crate::test_wait::pass(std::time::Duration::from_secs(31)).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    assert_eq!(expect_event(within(&events).await).kind, "DELETED");
}) }

#[tokio::test(flavor = "multi_thread")]
async fn an_event_received_before_the_session_ended_is_not_published_after_it() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "account A"}));
    put(&rig, "chats", json!({"id": "c2", "title": "account B"}));
    let events = crate::test_wait::within_secs(5, open_heard(&rig.sync, "chats")).await.unwrap();
    crate::test_wait::within_secs(5, rig.transport.await_calls("Changes", 1)).await;
    let (entered, entered_receiver) = std::sync::mpsc::channel();
    let (release, release_receiver) = std::sync::mpsc::channel::<()>();
    let held = std::sync::Mutex::new(Some(release_receiver));
    *rig.db.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |_| {
        if let Some(release) = held.lock().unwrap().take() {
            entered.send(()).unwrap();
            crate::test_wait::recv_within(&release);
        }
    }));
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    crate::test_wait::recv_within(&entered_receiver);
    rig.sync.lose_session_for_test();
    rig.transport.set_session(Some("second"));
    rig.sync.commit_session_for_test();
    release.send(()).unwrap();
    *rig.db.shared.store.after_commit.lock().unwrap() = None;
    crate::test_wait::within_secs(5, rig.transport.await_calls("Changes", 2)).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c2")]);
    let seen = expect_event(crate::test_wait::within_secs(5, events.next()).await);
    assert_eq!(seen.id, "c2", "an event of the ended session reached the queue of the next one");
}
