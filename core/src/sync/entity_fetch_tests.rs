use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::node_tests::{data, node_rig};
use super::{RefreshError, RefreshSink};
use crate::test_wait::within;

type Told = Arc<Mutex<Vec<(String, Option<RefreshError>)>>>;

fn sink() -> (RefreshSink, Told) {
    let told: Told = Arc::default();
    let into = told.clone();
    (Arc::new(move |id, error| into.lock().unwrap().push((id.to_string(), error))), told)
}

async fn told(told: &Told, count: usize) -> Vec<Option<RefreshError>> {
    within(async { while told.lock().unwrap().len() < count { tokio::time::sleep(Duration::from_millis(10)).await; } }).await;
    told.lock().unwrap().iter().map(|(_, error)| error.clone()).collect()
}

fn ids(id: &str) -> Vec<String> {
    vec![id.to_string()]
}

#[tokio::test(start_paused = true)]
async fn two_readers_of_one_entity_send_one_request_and_a_stale_read_of_a_fresh_row_sends_none() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "one"}})).after(Duration::from_secs(1)));
    let (sink, got) = sink();
    rig.sync.refresh_entities("chats", &ids("c1"), true, sink.clone()).unwrap();
    rig.sync.refresh_entities("chats", &ids("c1"), true, sink.clone()).unwrap();
    assert_eq!(told(&got, 2).await, [None, None]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    rig.sync.refresh_entities("chats", &ids("c1"), false, sink.clone()).unwrap();
    assert_eq!(told(&got, 3).await.len(), 3);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "two"}})));
    rig.sync.refresh_entities("chats", &ids("c1"), true, sink).unwrap();
    told(&got, 4).await;
    assert_eq!(rig.transport.call_count("ChatNode"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_stale_read_of_a_missing_row_sends_one_request() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c2", "title": "x"}})));
    let (sink, got) = sink();
    rig.sync.refresh_entities("chats", &ids("c2"), false, sink).unwrap();
    told(&got, 1).await;
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn an_offline_refresh_reports_offline_and_repeats_once_when_the_network_returns() {
    let rig = node_rig();
    rig.sync.set_online(false);
    let (sink, got) = sink();
    rig.sync.refresh_entities("chats", &ids("c1"), true, sink).unwrap();
    assert_eq!(told(&got, 1).await, [Some(RefreshError::Offline)]);
    assert_eq!(rig.transport.call_count("ChatNode"), 0);
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "back"}})));
    rig.sync.set_online(true);
    assert_eq!(told(&got, 2).await, [Some(RefreshError::Offline), None]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_model_without_a_node_read_is_refused() {
    let rig = super::rig::rig();
    let (sink, _) = sink();
    assert!(rig.sync.refresh_entities("moments", &ids("m1"), true, sink).is_err());
}
