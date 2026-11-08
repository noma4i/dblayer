//! chat.dbl of the app against feed.md rows feed_payload_lands (l.104), feed_changed_by_two_forms (l.100), feed_changed_by_observed (l.130), feed_live_event_unwatched (l.123).
use serde_json::json;

use super::stand::{chat, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{ReadFetch, ReadValue};
use crate::test_wait::yield_until;

/// l.104 + l.100/130: `CHAT` with a `chat` payload lands its row first, then the `observed` form rereads the node with a reader.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_chat_updated_lands_payload_then_rereads_the_watched_node() {
    let stand = Stand::open().await;
    stand.answer("ChatNode", json!({"node": chat("c1", "before")}));
    let (events, token) = stand.events("Chat").await;
    let rows = stand.entity("Chat", "c1", ReadFetch::Stale);
    yield_until("the node answered", || stand.text_of(&rows, "Chat", "c1", "name").as_deref() == Some("before")).await;
    let asked = stand.requests("ChatNode");
    stand.answer("ChatNode", json!({"node": chat("c1", "reread")}));
    stand.push(&token, json!({"area": "CHAT", "kind": "UPDATED", "entityId": "c1", "chat": chat("c1", "payload")}));
    let event = bound(events.pull()).await;
    assert!(matches!(&event, Some(ReadValue::Event { area, kind, id, after: Some(row), .. }) if area == "CHAT" && kind == "UPDATED" && id == "c1" && crate::ffi::tests::lifecycle_tests::named(row, "payload")), "the event carries the payload row: {event:?}");
    yield_until("the watched node is read again", || stand.requests("ChatNode") == asked + 1).await;
    yield_until("the reread answer replaces the payload", || stand.text_of(&rows, "Chat", "c1", "name").as_deref() == Some("reread")).await;
}

/// l.104 + l.130: the same event with no reader lands the payload row and asks the server for nothing.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_chat_updated_without_a_reader_asks_for_nothing() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Chat").await;
    stand.push(&token, json!({"area": "CHAT", "kind": "UPDATED", "entityId": "c1", "chat": chat("c1", "payload")}));
    let event = bound(events.pull()).await;
    assert!(matches!(&event, Some(ReadValue::Event { after: Some(_), .. })), "{event:?}");
    stand.settle().await;
    assert_eq!(stand.requests("ChatNode"), 0);
}

/// l.123: an entry whose area `Chat` does not declare gives `events(Chat)` no value, the next declared entry is position 1.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_events_of_chat_give_nothing_for_an_area_it_does_not_declare() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Chat").await;
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "c1"}));
    stand.push(&token, json!({"area": "CHAT", "kind": "UPDATED", "entityId": "c1", "chat": chat("c1", "payload")}));
    let event = bound(events.pull()).await;
    assert!(matches!(&event, Some(ReadValue::Event { position: 1, area, .. }) if area == "CHAT"), "{event:?}");
}
