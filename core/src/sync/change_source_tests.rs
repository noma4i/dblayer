use serde_json::json;

use super::node_tests::{node_rig, stored};
use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::SyncConfig;
use crate::db::Db;
use crate::schema::ChangeIdOf;
use crate::transport::{EventSpec, MockTransport, Reply, SubscriptionEvent};

async fn chat_of_message_rig() -> (Rig, super::subscribe::Updates) {
    let original = node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    let id_of = Some(ChangeIdOf { model: "messages".into(), field: "chatId".into() });
    specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap().changes.push(crate::schema::ChangeRead { area: "MESSAGE".into(), relation: "node".into(), parameter: None, removes_on: vec![], observed: false, id_of });
    specs.iter_mut().find(|spec| spec.key.as_str() == "messages").unwrap().changes.push(crate::schema::ChangeRead { area: "MESSAGE".into(), relation: "transcode_status".into(), parameter: None, removes_on: vec!["DELETED".into()], observed: false, id_of: None });
    specs[0].change_feed = Some(super::subscribe_tests::change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "chats", EventEffect::Upsert)).await.unwrap();
    (rig, subscription)
}

fn message_event(rig: &Rig, kind: &str, id: &str) {
    rig.transport.emit("Changes", SubscriptionEvent::Next(crate::transport::Response::data(json!({"recordEvents":{"changes":[{"topic":"MESSAGE", "action":kind, "recordId":id}]}}))));
}

fn chat_node_ids(rig: &Rig) -> Vec<serde_json::Value> {
    rig.transport.calls().iter().filter(|call| call.operation_name == "ChatNode").map(|call| call.variables["id"].clone()).collect()
}

#[tokio::test(start_paused = true)]
async fn a_message_event_rereads_the_chat_of_the_message_it_loaded() {
    let (rig, subscription) = chat_of_message_rig().await;
    rig.transport.enqueue("MessageTranscodeStatus", Reply::data(json!({"node":{"id":"m1", "chatId":"c1"}})));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":{"id":"c1", "title":"fresh"}})));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert_eq!(chat_node_ids(&rig), [json!("c1")]);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("fresh"));
    assert_eq!(rig.transport.call_count("MessageTranscodeStatus"), 1);
    assert_eq!(subscription.status().rejected, 0);
}

#[tokio::test(start_paused = true)]
async fn a_missing_message_skips_the_chat_read() {
    let (rig, subscription) = chat_of_message_rig().await;
    rig.transport.enqueue("MessageTranscodeStatus", Reply::data(json!({"node":null})));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(chat_node_ids(&rig).is_empty());
    assert_eq!(subscription.status().rejected, 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_message_read_skips_the_chat_read_even_when_the_row_is_stored() {
    let (rig, subscription) = chat_of_message_rig().await;
    put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"old"}));
    rig.transport.enqueue("MessageTranscodeStatus", Reply::graphql_error("boom", "INTERNAL"));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(chat_node_ids(&rig).is_empty());
    assert_eq!(subscription.status().rejected, 2);
}

#[tokio::test(start_paused = true)]
async fn a_deleted_message_still_rereads_the_chat_it_belonged_to() {
    let (rig, subscription) = chat_of_message_rig().await;
    put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"old"}));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":{"id":"c1", "title":"after delete"}})));
    message_event(&rig, "DELETED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(stored(&rig, "messages", "m1").is_none());
    assert_eq!(chat_node_ids(&rig), [json!("c1")]);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("after delete"));
    assert_eq!(subscription.status().rejected, 0);
}
