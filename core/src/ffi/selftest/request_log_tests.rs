//! D-477: the `requests` stream gives each request that leaves for the network with its operation, variables and reason.

use serde_json::json;

use super::node_event_other_load_tests::{node_answer, open};
use super::rig_tests::{bound, Fault, World};
use crate::ffi::{ReadClaim, ReadRequest, ReadValue, RequestReason};

/// D-477: a first load, a forced read, an event reread and a retry after a network failure each show once, in order, with their reason and variables.
#[tokio::test(flavor = "multi_thread")]
async fn each_request_shows_once_with_its_reason() {
    let world = World::new("server", "A", true);
    let (database, cable, clock) = open(world.clone()).await;
    let epoch = database.session_epoch();
    let log = database.subscribe_values(epoch, ReadRequest::Requests);
    let ReadClaim::Claimed { reader } = log.claim_reader().await else { panic!("the requests reader claim ended") };
    let details = |id: &str| json!({"data": {"presence": {"__typename": "Presence", "id": id, "online": true}}});
    world.answer_with("PresenceDetails", details("p1"));
    bound(database.load(epoch, "Presence".into(), "details".into(), json!({"key": "p1"}).to_string())).await.unwrap();
    world.answer_with("PresenceNode", node_answer(true));
    bound(database.node(epoch, "Presence".into(), "node".into(), "p1".into(), true)).await.unwrap();
    let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
    let row_reader = database.watch(ReadRequest::Entity { model: "Presence".into(), id: "p1".into(), fetch: crate::ffi::ReadFetch::Never }, rows);
    bound(row_reader.wait_for_initial_load()).await.unwrap();
    let events = database.subscribe_values(epoch, ReadRequest::Events { model: "Presence".into() });
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
    crate::test_wait::signalled(&cable.opened, "the subscription opens", || cable.token.lock().unwrap().is_some()).await;
    let token = cable.token.lock().unwrap().clone().unwrap();
    clock.advance(1_000);
    world.answer_with("PresenceNode", node_answer(false));
    assert!(cable.peer.event(&token, json!({"data": {"recordEvents": {"entries": [{"topic": "PRESENCE", "action": "UPDATED", "recordId": "p1"}]}}})));
    crate::test_wait::settled(&database, "the event reread lands", || world.count("PresenceNode") == 2).await;
    world.fail_next("PresenceDetails", Fault::Down);
    world.answer_with("PresenceDetails", details("p2"));
    let armed = clock.armed();
    let loading = { let database = database.clone(); tokio::spawn(async move { database.load(epoch, "Presence".into(), "details".into(), json!({"key": "p2"}).to_string()).await }) };
    clock.await_armed(armed).await;
    clock.advance(crate::runtime::RetryPolicy::QueryNetwork.delay(1).as_nanos().div_ceil(1_000_000) as u64);
    bound(loading).await.unwrap().unwrap();
    let mut seen = Vec::new();
    while seen.len() < 5 {
        match bound(log.pull(reader)).await {
            Some(ReadValue::Request { operation, variables_json, reason }) => seen.push((operation, reason, variables_json.contains("p2"))),
            Some(ReadValue::Ready) => {}
            other => panic!("the requests stream gave {other:?}"),
        }
    }
    assert_eq!(seen, vec![
        ("PresenceDetails".to_string(), RequestReason::Load, false),
        ("PresenceNode".to_string(), RequestReason::Refresh, false),
        ("PresenceNode".to_string(), RequestReason::Event, false),
        ("PresenceDetails".to_string(), RequestReason::Load, true),
        ("PresenceDetails".to_string(), RequestReason::Retry, true),
    ], "(operation, reason, variables name p2)");
    drop(events);
    drop(row_reader);
    bound(database.close_database()).await.unwrap();
}
