use serde_json::json;

use super::model_event::EVENT_QUEUE_LIMIT;
use super::rig::*;
use super::subscribe_tests::change_feed;
use super::{ModelEventItem, SyncConfig};
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
    chats.changes.push(ChangeRead { area: "CHAT".into(), relation: "node".into(), parameter: None, removes_on: vec!["DELETED".into()], observed: false });
    chats.change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    rig
}

async fn within(events: &super::ModelEvents) -> Option<ModelEventItem> {
    tokio::time::timeout(std::time::Duration::from_secs(5), events.next()).await.expect("the next event did not arrive")
}

fn expect_event(item: Option<ModelEventItem>) -> super::ModelEvent {
    match item {
        Some(ModelEventItem::Event(event)) => event,
        other => panic!("expected an event, got {other:?}"),
    }
}

fn title(row: &Option<crate::db::Row>) -> Option<&str> {
    row.as_ref().and_then(|row| row.text("title"))
}

#[tokio::test(start_paused = true)]
async fn node_event_carries_the_row_before_and_after_with_one_request() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    assert_eq!(rig.transport.call_count("ChatNode"), 0, "idle");
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "after"}})));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((seen.area.as_str(), seen.kind.as_str(), seen.id.as_str()), ("CHAT", "UPDATED", "c1"));
    assert_eq!((title(&seen.before), title(&seen.after)), (Some("before"), Some("after")));
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn removal_event_has_no_after_row_and_makes_no_request() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    emit(&rig, vec![event("CHAT", "DELETED", "c1")]);
    let seen = expect_event(within(&events).await);
    assert_eq!((title(&seen.before), seen.after.is_none()), (Some("before"), true));
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
}

#[tokio::test(start_paused = true)]
async fn two_events_of_one_row_arrive_in_order_and_chain_before_to_after() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "v1"}));
    let events = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    for next in ["v2", "v3"] { rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": next}}))); }
    emit(&rig, vec![event("CHAT", "UPDATED", "c1"), event("CHAT", "UPDATED", "c1")]);
    let (first, second) = (expect_event(within(&events).await), expect_event(within(&events).await));
    assert_eq!([title(&first.before), title(&first.after), title(&second.before), title(&second.after)], [Some("v1"), Some("v2"), Some("v2"), Some("v3")]);
    assert_eq!(rig.transport.call_count("ChatNode"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_full_queue_reports_the_dropped_count_and_never_holds_back_the_next_event() {
    let rig = chat_rig();
    let slow = rig.sync.model_events("chats").unwrap();
    let fast = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    let total = EVENT_QUEUE_LIMIT + 44;
    emit(&rig, (0..total).map(|index| event("CHAT", "DELETED", &format!("c{index}"))).collect());
    rig.transport.await_consumed("Changes").await;
    for index in 0..EVENT_QUEUE_LIMIT { assert_eq!(expect_event(within(&fast).await).id, format!("c{index}")); }
    assert!(matches!(within(&fast).await, Some(ModelEventItem::Missed(44))));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "z", "title": "late"}})));
    emit(&rig, vec![event("CHAT", "UPDATED", "z")]);
    assert_eq!(expect_event(within(&fast).await).id, "z");
    assert_eq!(expect_event(within(&slow).await).id, "c0");
}

#[tokio::test(start_paused = true)]
async fn observed_event_rereads_every_watched_parameterized_read_once_and_no_other() {
    let mut moments = moments_spec();
    moments.changes.push(ChangeRead { area: "WALLET".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true });
    moments.change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    let _wild = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "wild"}))).unwrap();
    let events = rig.sync.model_events("moments").unwrap();
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
}

#[tokio::test(start_paused = true)]
async fn a_failed_read_leaves_a_gap_in_the_queue_at_the_place_of_the_event() {
    let rig = chat_rig();
    let events = rig.sync.model_events("chats").unwrap();
    let failure = || crate::db::DbError::InvalidUpload("read failed".into());
    let inner = &rig.sync.inner;
    assert!(inner.publish_rows("chats", "CHAT", "UPDATED", "c1", Ok(None), Ok(None)));
    assert!(!inner.publish_rows("chats", "CHAT", "UPDATED", "c2", Err(failure()), Ok(None)));
    assert!(!inner.publish_rows("chats", "CHAT", "UPDATED", "c3", Ok(None), Err(failure())));
    assert!(inner.publish_rows("chats", "CHAT", "UPDATED", "c4", Ok(None), Ok(None)));
    assert_eq!(expect_event(within(&events).await).id, "c1");
    assert!(matches!(within(&events).await, Some(ModelEventItem::Missed(2))));
    assert_eq!(expect_event(within(&events).await).id, "c4");
}

#[tokio::test(start_paused = true)]
async fn a_failed_reread_leaves_a_gap_for_the_reader() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "before"}));
    let events = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    rig.transport.enqueue("ChatNode", Reply::fail(crate::transport::TransportError::Http { status: 422, body: "rejected".into() }));
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    assert!(matches!(within(&events).await, Some(ModelEventItem::Missed(1))));
}

#[tokio::test(start_paused = true)]
async fn rows_are_read_for_an_event_only_while_the_model_has_a_reader() {
    let rig = chat_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "v1"}));
    let opened = rig.sync.model_events("chats").unwrap();
    rig.transport.await_calls("Changes", 1).await;
    drop(opened);
    let finds = || crate::db::FINDS.with(std::cell::Cell::get);
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "v2"}})));
    let start = finds();
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    rig.transport.await_consumed("Changes").await;
    let unwatched = finds() - start;
    let events = rig.sync.model_events("chats").unwrap();
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node": {"id": "c1", "title": "v3"}})));
    let start = finds();
    emit(&rig, vec![event("CHAT", "UPDATED", "c1")]);
    expect_event(within(&events).await);
    assert_eq!(finds() - start, unwatched + 2);
}

#[tokio::test(start_paused = true)]
async fn a_model_gets_the_events_of_a_changes_source_that_another_model_declares() {
    for (change, area) in [
        (ChangeRead { area: "WALLET".into(), relation: "observed".into(), parameter: None, removes_on: vec![], observed: true }, "WALLET"),
        (ChangeRead { area: "MOOD".into(), relation: "by_mood".into(), parameter: Some("mood".into()), removes_on: vec![], observed: false }, "MOOD"),
    ] {
        let (mut users, mut moments) = (users_spec(), moments_spec());
        users.change_feed = Some(change_feed());
        moments.changes.push(change);
        let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
        let events = rig.sync.model_events("moments").unwrap();
        rig.transport.await_calls("Changes", 1).await;
        rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]})));
        emit(&rig, vec![event(area, "CREATED", "calm")]);
        let seen = expect_event(within(&events).await);
        assert_eq!((seen.area.as_str(), seen.id.as_str()), (area, "calm"), "{area}");
        assert_eq!(rig.transport.call_count("MomentsByMood"), 1, "{area}");
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "{area}: one event only");
    }
}

#[tokio::test(start_paused = true)]
async fn an_event_that_arrived_before_the_reader_is_not_replayed_and_the_next_one_is_delivered() {
    let (mut users, mut moments) = (users_spec(), moments_spec());
    users.change_feed = Some(change_feed());
    moments.changes.push(ChangeRead { area: "MOOD".into(), relation: "by_mood".into(), parameter: Some("mood".into()), removes_on: vec![], observed: false });
    let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let _calm = rig.sync.watch("moments", "by_mood", &params(json!({"mood": "calm"}))).unwrap();
    rig.transport.await_calls("Changes", 1).await;
    for _ in 0..2 { rig.transport.enqueue("MomentsByMood", Reply::data(json!({"moments": [{"id": "m1", "text": "a"}]}))); }
    emit(&rig, vec![event("MOOD", "CREATED", "calm")]);
    rig.transport.await_calls("MomentsByMood", 1).await;
    let events = rig.sync.model_events("moments").unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(50), events.next()).await.is_err(), "the earlier event is not replayed");
    emit(&rig, vec![event("MOOD", "CREATED", "wild")]);
    assert_eq!(expect_event(within(&events).await).id, "wild");
}
