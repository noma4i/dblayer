use std::time::Duration;

use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;
use crate::transport::SubscriptionEvent;
use crate::transport::Response;

const KEY: &str = "moments|MomentEvents";

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn event(node: serde_json::Value) -> SubscriptionEvent {
    SubscriptionEvent::Next(Response::data(json!({"momentEvent": node})))
}

#[tokio::test(start_paused = true)]
async fn first_reader_starts_the_subscription_and_the_second_does_not_duplicate_it() {
    let rig = rig();
    let first = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    let second = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.sync.inner.live_state(KEY), Some((2, true)));
    drop((first, second));
}

#[tokio::test(start_paused = true)]
async fn watching_a_relation_without_updates_starts_nothing() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 0);
    assert_eq!(rig.sync.inner.live_state(KEY), None);
}

#[tokio::test(start_paused = true)]
async fn subscription_closes_after_the_last_reader_leaves_and_the_linger_passes() {
    let rig = rig();
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    drop(watch);
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), Some((0, true)));
    assert_eq!(rig.transport.cancellations("MomentEvents"), 0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), None);
    assert_eq!(rig.transport.cancellations("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn reader_returning_within_the_linger_keeps_the_same_subscription() {
    let rig = rig();
    drop(rig.sync.watch("moments", "pulse", &none()).unwrap());
    settle().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _back = rig.sync.watch("moments", "pulse", &none()).unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), Some((1, true)));
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn dropped_connection_rereads_the_watched_relation_after_it_reconnects() {
    let rig = rig();
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1", "m2"], false, "c2"));
    let _watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.sync.load("moments", "pulse", &none(), LoadOptions::default()).await.unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentsPulse"), 1);
    rig.transport.disconnect("MomentEvents");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 2);
    assert_eq!(rig.transport.call_count("MomentsPulse"), 2);
    assert_eq!(member_ids(&rig, "moments", "pulse", &none()), ["m1", "m2"]);
}

#[tokio::test(start_paused = true)]
async fn event_with_the_deleted_by_field_set_removes_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let _watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": false})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("edited".to_string()));
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": true})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn a_relation_with_two_subscriptions_starts_both_and_a_removing_one_deletes_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.transport.call_count("MomentRemoved"), 1);
    rig.transport.emit("MomentRemoved", SubscriptionEvent::Next(Response::data(json!({"momentRemoved": {"id": "m1"}}))));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    drop(watch);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentRemoved"), 1);
}
