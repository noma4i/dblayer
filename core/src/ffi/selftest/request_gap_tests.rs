//! D-478: identical requests share one, the same window key waits 1 s between requests, events of one batch reread a window once.

use std::sync::Arc;

use serde_json::json;

use super::node_event_other_load_tests::{node_answer, online_of, open};
use super::rig_tests::{bound, World};
use crate::ffi::tape::Rows;
use crate::ffi::{ReadFetch, ReadRequest};

/// P-WIN-36: `nodes` calls with the same ids in another order share the request in flight, each call keeps its own order.
#[tokio::test(flavor = "multi_thread")]
async fn request_single_flight_per_window_key() {
    let world = World::new("server", "A", true);
    let (database, _, _) = open(world.clone()).await;
    let epoch = database.session_epoch();
    let row = |id: &str| json!({"__typename": "Presence", "id": id, "online": true});
    world.answer_with("PresenceNodes", json!({"data": {"nodes": [row("p1"), row("p2")]}}));
    let gate = world.hold_next("PresenceNodes");
    let first = { let database = database.clone(); tokio::spawn(async move { database.nodes(epoch, "Presence".into(), "nodes".into(), vec!["p1".into(), "p2".into()]).await }) };
    gate.entered().await;
    let second = { let database = database.clone(); tokio::spawn(async move { database.nodes(epoch, "Presence".into(), "nodes".into(), vec!["p2".into(), "p1".into()]).await }) };
    crate::test_wait::settled(&database, "the second call waits", || true).await;
    gate.release();
    let ids = |rows: Vec<crate::ffi::WireRow>| rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
    let answered = (ids(bound(first).await.unwrap().unwrap()), ids(bound(second).await.unwrap().unwrap()), world.count("PresenceNodes"));
    assert_eq!(answered, (vec!["p1".to_string(), "p2".into()], vec!["p2".to_string(), "p1".into()], 1), "(first call, second call, requests)");
    bound(database.close_database()).await.unwrap();
}

/// P-WIN-37: a forced read inside 1 s of the last answer of its key gets that answer without a request; after 1 s it asks the server.
#[tokio::test(flavor = "multi_thread")]
async fn request_gap_answers_calls_from_the_last_answer() {
    let world = World::new("server", "A", true);
    let (database, _, clock) = open(world.clone()).await;
    let epoch = database.session_epoch();
    world.answer_with("PresenceNode", node_answer(true));
    let mut seen = Vec::new();
    for advance in [0u64, 0, 999, 1] {
        clock.advance(advance);
        let row = bound(database.node(epoch, "Presence".into(), "node".into(), "p1".into(), true)).await.unwrap();
        seen.push((advance, row.map(|row| row.id), world.count("PresenceNode")));
    }
    let p1 = || Some("p1".to_string());
    assert_eq!(seen, vec![(0, p1(), 1), (0, p1(), 1), (999, p1(), 1), (1, p1(), 2)], "(ms advanced, row, requests)");
    bound(database.close_database()).await.unwrap();
}

/// P-WIN-37, P-WIN-38: an event inside the gap rereads once at the end of the gap and the reader shows the new value; two events of one batch reread the row once.
#[tokio::test(flavor = "multi_thread")]
async fn request_gap_defers_event_rereads_and_one_batch_rereads_once() {
    let world = World::new("server", "A", true);
    let (database, cable, clock) = open(world.clone()).await;
    let epoch = database.session_epoch();
    let rows = Arc::new(Rows::default());
    let reader = database.watch(ReadRequest::Entity { model: "Presence".into(), id: "p1".into(), fetch: ReadFetch::Never }, rows.clone());
    world.answer_with("PresenceNode", node_answer(true));
    bound(database.node(epoch, "Presence".into(), "node".into(), "p1".into(), true)).await.unwrap();
    bound(reader.wait_for_initial_load()).await.unwrap();
    crate::test_wait::signalled(&cable.opened, "the subscription opens", || cable.token.lock().unwrap().is_some()).await;
    let token = cable.token.lock().unwrap().clone().unwrap();
    let asked = world.count("PresenceNode");
    world.answer_with("PresenceNode", node_answer(false));
    let updated = json!({"topic": "PRESENCE", "action": "UPDATED", "recordId": "p1"});
    assert!(cable.peer.event(&token, json!({"data": {"recordEvents": {"entries": [updated.clone(), updated]}}})));
    crate::test_wait::settled(&database, "the event batch is applied", || true).await;
    let in_gap = (world.count("PresenceNode") - asked, online_of(&rows));
    clock.advance(1_000);
    crate::test_wait::settled(&database, "the deferred reread lands", || online_of(&rows) == Some(false)).await;
    assert_eq!((in_gap, world.count("PresenceNode") - asked), ((0, Some(true)), 1), "((requests inside the gap, value inside the gap), requests after the gap)");
    drop(reader);
    bound(database.close_database()).await.unwrap();
}
