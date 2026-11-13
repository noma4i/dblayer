use std::sync::Arc;

use serde_json::json;

use super::delivery_reactivity_tests::feed_database_in;
use super::rig_tests::{bound, World};
use crate::ffi::tape::Rows;
use crate::ffi::{ReadRequest, ReadValue};
use crate::test_wait::settled;

fn list_requests(world: &World) -> usize { world.log().iter().filter(|entry| entry.operation == "ChatList").count() }

#[tokio::test(flavor = "multi_thread")]
async fn a_list_with_an_events_subscription_loads_once_when_the_network_returns() {
    let world = World::new("server", "A", true);
    world.answer_with("ChatList", json!({"data":{"chats":[{"id":"c1","title":"one","revision":1,"lists":["PRIMARY"]}]}}));
    let (database, _transport) = feed_database_in(world.clone(), Some(json!({"subscription":"RecordEvents", "root":"recordEvents.item", "apply":{"effect":"upsert"}})), false, false).await;
    let events = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
    let rows = Arc::new(Rows::default());
    let query = json!({"model":"Chat","relation":"list","params":{"list":"PRIMARY"},"refreshOnConnect":true}).to_string();
    drop(database.watch(ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, rows.clone()));
    settled(&database, "the list shows its first load", || rows.len() == 1).await;
    assert_eq!(list_requests(&world), 1);
    database.set_online(false).await.unwrap();
    database.set_online(true).await.unwrap();
    settled(&database, "the list loaded again", || list_requests(&world) >= 2).await;
    loop {
        if matches!(bound(events.pull_next()).await, Some(ReadValue::Gap)) { break; }
    }
    let life = database.life_handle();
    crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert_eq!(list_requests(&world), 2, "the return of the network and the gap of the subscription read the list twice");
    bound(database.close_database()).await.unwrap();
}
