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
    specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap().changes.push(crate::schema::ChangeRead { area: "MESSAGE".into(), relation: "node".into(), parameter: None, removes_on: vec![], observed: false, payload: None, id_of });
    specs.iter_mut().find(|spec| spec.key.as_str() == "messages").unwrap().changes.push(crate::schema::ChangeRead { area: "MESSAGE".into(), relation: "transcode_status".into(), parameter: None, removes_on: vec!["DELETED".into()], observed: false, payload: None, id_of: None });
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

#[test]
fn a_message_event_rereads_the_chat_of_the_message_it_loaded() { crate::test_wait::paused(async {
    let (rig, subscription) = chat_of_message_rig().await;
    rig.transport.enqueue("MessageTranscodeStatus", Reply::data(json!({"node":{"id":"m1", "chatId":"c1"}})));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":{"id":"c1", "title":"fresh"}})));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert_eq!(chat_node_ids(&rig), [json!("c1")]);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("fresh"));
    assert_eq!(rig.transport.call_count("MessageTranscodeStatus"), 1);
    assert_eq!(subscription.status().rejected, 0);
}) }

#[test]
fn a_missing_message_skips_the_chat_read() { crate::test_wait::paused(async {
    let (rig, subscription) = chat_of_message_rig().await;
    rig.transport.enqueue("MessageTranscodeStatus", Reply::data(json!({"node":null})));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(chat_node_ids(&rig).is_empty());
    assert_eq!(subscription.status().rejected, 1);
}) }

#[test]
fn a_failed_message_read_skips_the_chat_read_even_when_the_row_is_stored() { crate::test_wait::paused(async {
    let (rig, subscription) = chat_of_message_rig().await;
    put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"old"}));
    rig.transport.enqueue("MessageTranscodeStatus", Reply::graphql_error("boom", "INTERNAL"));
    message_event(&rig, "CREATED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(chat_node_ids(&rig).is_empty());
    assert_eq!(subscription.status().rejected, 2);
}) }

#[test]
fn a_deleted_message_still_rereads_the_chat_it_belonged_to() { crate::test_wait::paused(async {
    let (rig, subscription) = chat_of_message_rig().await;
    put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"old"}));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":{"id":"c1", "title":"after delete"}})));
    message_event(&rig, "DELETED", "m1");
    rig.transport.await_consumed("Changes").await;
    assert!(stored(&rig, "messages", "m1").is_none());
    assert_eq!(chat_node_ids(&rig), [json!("c1")]);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("after delete"));
    assert_eq!(subscription.status().rejected, 0);
}) }

fn activity_model() -> crate::schema::ModelSpec {
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let mut model = crate::schema::ModelSpec::new("activity", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("chatId", ScalarKind::Id).optional(),
        FieldSpec::scalar("userId", ScalarKind::Id).optional(),
        FieldSpec::scalar("kind", ScalarKind::Str).optional(),
        FieldSpec::scalar("active", ScalarKind::Bool).optional(),
    ]);
    model.storage = crate::schema::Storage::Memory;
    model.expire_after = Some("8.seconds".into());
    model.listens.push(serde_json::from_value(json!({"subscription":"ActionSignal", "root":"actionSignal", "id":["chatId", "userId"], "deleted_unless":"active"})).unwrap());
    model
}

#[test]
fn events_with_one_chat_and_user_are_one_row_and_an_inactive_event_deletes_it() { crate::test_wait::paused(async {
    let rig = rig_on(Db::open_in_memory(vec![activity_model()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("ActionSignal", "subscription ActionSignal { actionSignal { chatId userId kind active } }", "actionSignal"));
    rig.sync.inner.listen_all();
    rig.transport.await_calls("ActionSignal", 1).await;
    let send = |chat: &str, user: &str, kind: &str, active: bool| rig.transport.emit("ActionSignal", SubscriptionEvent::Next(crate::transport::Response::data(json!({"actionSignal":{"chatId":chat, "userId":user, "kind":kind, "active":active}}))));
    let rows = || rig.db.query(&crate::query::Query::new("activity")).unwrap();
    send("c1", "u1", "typing", true);
    send("c1", "u1", "recording", true);
    send("c1", "u2", "typing", true);
    rig.transport.await_consumed("ActionSignal").await;
    let mut kinds: Vec<_> = rows().iter().map(|row| (row.text("userId").unwrap().to_string(), row.text("kind").unwrap().to_string())).collect();
    kinds.sort();
    assert_eq!(kinds, [("u1".to_string(), "recording".to_string()), ("u2".to_string(), "typing".to_string())]);
    send("c1", "u1", "recording", false);
    rig.transport.await_consumed("ActionSignal").await;
    let left: Vec<_> = rows().iter().map(|row| row.text("userId").unwrap().to_string()).collect();
    assert_eq!(left, ["u2"]);
}) }
