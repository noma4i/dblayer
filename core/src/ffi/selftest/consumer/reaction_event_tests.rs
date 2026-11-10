//! message.dbl and moment.dbl of the app against feed.md rows feed_payload_lands (l.104), feed_payload_incomplete (l.105), feed_changed_by_node (l.127) and delivery.md l.10 (`Entity` with `fetch: never` has no window).
use serde_json::json;

use super::stand::{message, moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{ReadFetch, ReadValue};
use crate::test_wait::settled;

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_reaction_rereads_the_observed_node() {
    let stand = Stand::open().await;
    stand.answer("MessageNode", json!({"node": message("m1", "c1", "before")}));
    let (events, token) = stand.events("Message").await;
    let rows = stand.entity("Message", "m1", ReadFetch::Stale);
    settled(&stand, "the message node answered", || stand.text_of(&rows, "Message", "m1", "body").as_deref() == Some("before")).await;
    let asked = stand.requests("MessageNode");
    stand.answer("MessageNode", json!({"node": message("m1", "c1", "reacted")}));
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "m1", "message": null}));
    let event = bound(events.pull_next()).await;
    assert!(matches!(&event, Some(ReadValue::Event { area, kind, id, .. }) if area == "REACTION" && kind == "UPDATED" && id == "m1"), "{event:?}");
    stand.settle().await;
    assert_eq!(stand.requests("MessageNode"), asked + 1);
    assert_eq!(stand.text_of(&rows, "Message", "m1", "body").as_deref(), Some("reacted"));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_reaction_without_window_is_an_event() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Message").await;
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "m1"}));
    let value = bound(events.pull_next()).await;
    assert!(matches!(&value, Some(ReadValue::Event { area, id, .. }) if area == "REACTION" && id == "m1"), "{value:?}");
    stand.settle().await;
    assert_eq!(stand.requests("MessageNode"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_reaction_rereads_the_watched_node() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node": moment("mo1", 0)}));
    let (events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "mo1", ReadFetch::Stale);
    settled(&stand, "the node answered", || stand.int_of(&rows, "Moment", "mo1", "visitsCount") == Some(0)).await;
    let asked = stand.requests("MomentNode");
    stand.answer("MomentNode", json!({"node": moment("mo1", 5)}));
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    settled(&stand, "the watched node is read again", || stand.requests("MomentNode") == asked + 1).await;
    settled(&stand, "the reread row shows the reaction", || stand.int_of(&rows, "Moment", "mo1", "visitsCount") == Some(5)).await;
    let event = bound(events.pull_next()).await;
    assert!(matches!(&event, Some(ReadValue::Event { area, kind, id, .. }) if area == "REACTION" && kind == "UPDATED" && id == "mo1"), "{event:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_reaction_without_a_window_asks_for_nothing() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node": moment("mo1", 5)}));
    let (_events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "mo1", ReadFetch::Never);
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    stand.settle().await;
    assert_eq!(stand.requests("MomentNode"), 0);
    assert!(rows.0.lock().unwrap().is_empty(), "a `never` reader holds no window and loads nothing");
}
