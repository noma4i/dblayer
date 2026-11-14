use serde_json::json;

use super::delivery_reactivity_tests::feed_database_joining;
use super::rig_tests::{bound, World};
use crate::ffi::{ReadRequest, ReadValue};

/// A fresh window asks nothing on a second load, and a `changed_by` event that names its relation makes the next load ask again -> `Database.load` under an open `events` stream, then a change-feed event of the area -> the count of the window's query stays 1, then becomes 2 -> a window that ignores the event (served stale) or a load that always asks.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_by_event_makes_the_next_load_of_a_fresh_window_ask_again() {
    let world = World::new("server", "A", true);
    let changes = vec![json!({"area":"CHAT","relation":"list","observed":true})];
    let (database, transport) = feed_database_joining(world.clone(), None, None, false, changes, (true, true)).await;
    let epoch = database.session_epoch();
    let events = database.subscribe_values(epoch, ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
    let load = || bound(database.load(epoch, "Chat".into(), "list".into(), json!({"list": "PRIMARY"}).to_string()));
    load().await.unwrap();
    load().await.unwrap();
    assert_eq!(world.count("ChatList"), 1, "the fresh window asks nothing");
    transport.subscribed(&database).await;
    let token = transport.token.lock().unwrap().clone().unwrap();
    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"CHAT","action":"UPDATED","recordId":"c1"}]}}})));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { .. })));
    crate::test_wait::settled(&database, "the event is applied", || true).await;
    load().await.unwrap();
    assert_eq!(world.count("ChatList"), 2, "the event marked the window stale");
    bound(database.close_database()).await.unwrap();
}
