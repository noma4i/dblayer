//! message.dbl and moment.dbl of the app against feed.md rows feed_payload_lands (l.104), feed_payload_incomplete (l.105), feed_changed_by_node (l.127) and delivery.md l.10 (`Entity` with `fetch: never` has no window).
use serde_json::json;

use super::stand::{message, moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{ReadFetch, ReadValue};
use crate::test_wait::yield_until;

/// l.104: `REACTION` on a message carries the `message` payload, its row lands and the server is not asked.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_message_reaction_lands_the_payload_row() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Message").await;
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "m1", "message": message("m1", "c1", "reacted")}));
    let event = bound(events.pull()).await;
    assert!(matches!(&event, Some(ReadValue::Event { area, id, after: Some(row), .. }) if area == "REACTION" && id == "m1" && crate::ffi::tests::lifecycle_tests::named(row, "reacted")), "{event:?}");
    stand.settle().await;
    assert_eq!(stand.requests("MessageNode"), 0);
}

/// l.105: `REACTION` on a message without the `message` object loses the position and asks the server for nothing.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_message_reaction_without_payload_is_lost() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Message").await;
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "m1"}));
    let value = bound(events.pull()).await;
    assert!(matches!(value, Some(ReadValue::Lost { first: 1, last: 1 })), "{value:?}");
    stand.settle().await;
    assert_eq!(stand.requests("MessageNode"), 0);
}

/// l.127: `REACTION` on a moment is `via: :node` without payload, the node with a reader is read again and the row changes.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_moment_reaction_rereads_the_watched_node() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node": moment("mo1", 0)}));
    let (events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "mo1", ReadFetch::Stale);
    yield_until("the node answered", || stand.int_of(&rows, "Moment", "mo1", "visitsCount") == Some(0)).await;
    let asked = stand.requests("MomentNode");
    stand.answer("MomentNode", json!({"node": moment("mo1", 5)}));
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    yield_until("the watched node is read again", || stand.requests("MomentNode") == asked + 1).await;
    yield_until("the reread row shows the reaction", || stand.int_of(&rows, "Moment", "mo1", "visitsCount") == Some(5)).await;
    let event = bound(events.pull()).await;
    assert!(matches!(&event, Some(ReadValue::Event { area, kind, id, .. }) if area == "REACTION" && kind == "UPDATED" && id == "mo1"), "{event:?}");
}

/// l.127 + delivery.md l.10: without a window (no reader, or `fetch: never`) the same event asks for no node.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_moment_reaction_without_a_window_asks_for_nothing() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node": moment("mo1", 5)}));
    let (_events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "mo1", ReadFetch::Never);
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    stand.settle().await;
    assert_eq!(stand.requests("MomentNode"), 0);
    assert!(rows.0.lock().unwrap().is_empty(), "a `never` reader holds no window and loads nothing");
}
