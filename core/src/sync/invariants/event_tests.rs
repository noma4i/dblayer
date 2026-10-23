use std::time::Duration;

use serde_json::{json, Value};

use super::super::rig::*;
use super::super::subscribe::{EventEffect, SubscriptionDef};
use super::fixtures::*;
use crate::query::Query;
use crate::sync::LoadOptions;
use crate::transport::{EventSpec, Response, SubscriptionEvent};

fn rig_with_events() -> Rig {
    let rig = rig();
    rig.sync.register(crate::sync::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    rig
}

fn event(node: Value) -> SubscriptionEvent {
    SubscriptionEvent::Next(Response::data(json!({"momentChanged": node})))
}

fn upserts(spec: EventSpec) -> SubscriptionDef {
    SubscriptionDef::new(spec, "MomentChanged", "moments", EventEffect::Upsert)
}

fn column(rig: &Rig, field: &str) -> Option<String> {
    rig.db.find("moments", &rid("m1")).unwrap().and_then(|row| row.get(field).and_then(Value::as_str).map(str::to_string))
}

#[tokio::test(start_paused = true)]
async fn inv_event_07_an_event_held_in_the_debounce_window_does_not_create_a_row_after_a_reset() {
    let rig = rig_with_events();
    let spec = EventSpec::new("moment_changed").debounced(Duration::from_millis(500)).merged_by_row();
    let _subscription = rig.sync.subscribe(upserts(spec)).await.unwrap();
    rig.transport.emit("MomentChanged", event(json!({"id": "m9", "text": "before the reset"})));
    tokio::time::sleep(Duration::from_millis(100)).await;
    rig.db.reset(&["moments"]).unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(text_of(&rig, "moments", "m9"), None);
    assert_eq!(rig.db.count(&Query::new("moments")).unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn inv_live_05_unversioned_events_apply_in_arrival_order_and_the_last_one_wins() {
    let rig = rig_with_events();
    put(&rig, "moments", moment("m1", "start"));
    let _subscription = rig.sync.subscribe(upserts(EventSpec::new("moment_changed"))).await.unwrap();
    for text in ["a", "b", "c"] {
        rig.transport.emit("MomentChanged", event(json!({"id": "m1", "text": text})));
    }
    rig.transport.await_consumed("MomentChanged").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("c".to_string()));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load("moments", "feed", &params(json!({})), LoadOptions { force: true }).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("m1".to_string()));
    rig.transport.emit("MomentChanged", event(json!({"id": "m1", "text": "after the load"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("after the load".to_string()));
}

#[tokio::test(start_paused = true)]
async fn inv_event_03_an_event_and_a_local_edit_of_one_row_end_as_one_of_the_two_orders_without_losing_the_edit() {
    for (local_field, event_field) in [("userId", "text"), ("text", "text")] {
        for event_first in [true, false] {
            let rig = rig_with_events();
            put(&rig, "moments", json!({"id": "m1", "text": "start", "userId": "u0"}));
            let _subscription = rig.sync.subscribe(upserts(EventSpec::new("moment_changed"))).await.unwrap();
            let edit = || rig.db.update("moments", &rid("m1"), params(json!({ local_field: "local" }))).unwrap();
            let push = || rig.transport.emit("MomentChanged", event(json!({"id": "m1", event_field: "server"})));
            if event_first {
                push();
                rig.transport.await_consumed("MomentChanged").await;
                edit();
            } else {
                edit();
                push();
                rig.transport.await_consumed("MomentChanged").await;
            }
            let case = format!("local {local_field}, event {event_field}, event first {event_first}");
            if local_field != event_field {
                assert_eq!(column(&rig, local_field).as_deref(), Some("local"), "{case}");
                assert_eq!(column(&rig, event_field).as_deref(), Some("server"), "{case}");
            } else {
                let landed = column(&rig, "text");
                assert!(landed.as_deref() == Some("local") || landed.as_deref() == Some("server"), "{case}: {landed:?}");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn inv_event_03_an_edit_before_during_or_after_a_held_event_is_never_lost() {
    for schedule in ["before the event", "while the event is held", "after the event"] {
        let rig = rig_with_events();
        put(&rig, "moments", json!({"id": "m1", "text": "start", "userId": "u0"}));
        let spec = EventSpec::new("moment_changed").debounced(Duration::from_millis(500)).merged_by_row();
        let _subscription = rig.sync.subscribe(upserts(spec)).await.unwrap();
        let edit = || rig.db.update("moments", &rid("m1"), params(json!({"userId": "local"}))).unwrap();
        if schedule == "before the event" { edit(); }
        rig.transport.emit("MomentChanged", event(json!({"id": "m1", "text": "server"})));
        rig.transport.await_consumed("MomentChanged").await;
        if schedule == "while the event is held" { edit(); }
        assert_eq!(column(&rig, "text").as_deref(), Some("start"), "{schedule}: the event is still held");
        tokio::time::advance(Duration::from_millis(500)).await;
        rig.db.await_revision(rig.db.revision_seq().unwrap());
        while column(&rig, "text").as_deref() != Some("server") { tokio::task::yield_now().await; }
        if schedule == "after the event" { edit(); }
        assert_eq!((column(&rig, "userId").as_deref(), column(&rig, "text").as_deref()), (Some("local"), Some("server")), "{schedule}");
    }
}
