use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;

use super::encrypted_cable_tests::Peer;
use super::rig_tests::{bound};
use super::rig_models::plain_models;
use crate::ffi::tests::encrypted_tests::ThroughServer;
use crate::ffi::tests::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::ffi::{Config, Database, ReadRequest, ReadValue};
use crate::schema::{FieldSpec, ModelSpec, Storage, UpdatesSpec};
use crate::value::ScalarKind;

fn typing_models() -> String {
    let mut models: Vec<ModelSpec> = serde_json::from_str(&plain_models()).unwrap();
    let mut typing = ModelSpec::new("Typing", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("userId", ScalarKind::Id)]);
    typing.storage = Storage::Memory;
    typing.listens.push(UpdatesSpec { apply: None, subscription: "UserTyping".into(), deleted_by: None, deleted_unless: None, set: serde_json::Map::new(), removes: false, debounce: None, root: None, id: Vec::new() });
    models.push(typing);
    serde_json::to_string(&models).unwrap()
}

/// D-69: the `subscribe` of a memory model is a channel of its own: the events stream of the model opens it and receives the frames that arrive on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_memory_model_with_subscribe_receives_the_frames_of_its_channel() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::tests::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, _closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(typing_models()), crate::ffi::OpenPorts { transport: Some(peer.clone()), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(json!([{"name":"UserTyping","document":"subscription UserTyping { userTyping { id userId } }","rootPath":"userTyping"}]).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Typing".into() });
    let token = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    let opened = bound(stream.pull_next()).await;
    assert!(matches!(opened, Some(ReadValue::Ready)), "the first value is the open source: {opened:?}");
    assert!(peer.cable.event(&token, json!({"data":{"userTyping":{"id":"t1","userId":"u1"}}})), "the subscription is open");
    let event = bound(stream.pull_next()).await;
    assert!(matches!(event, Some(ReadValue::Event { .. })), "the frame of the channel did not reach the events stream: {event:?}");
    database.close_now();
}

async fn until<F: Fn(&ReadValue) -> bool>(stream: &crate::ffi::ReadStream, what: &str, wanted: F) -> ReadValue {
    for _ in 0..20 {
        let value = bound(stream.pull_next()).await.unwrap_or_else(|| panic!("{what}: the stream ended"));
        if wanted(&value) { return value; }
    }
    panic!("{what}: no such value")
}

/// One signal subscription feeds a relation of one memory model and the events of another: the typing row and the notice both arrive from the same frames.
#[tokio::test(flavor = "multi_thread")]
async fn one_signal_routes_typed_actions_and_notices() {
    let rig = Rig::new();
    let config = crate::transport::encrypted::tests::protocol_tests::config();
    let (subscribed, mut subscriptions) = mpsc::unbounded_channel();
    let (closed, _closes) = mpsc::unbounded_channel();
    let peer = Arc::new(Peer::new(config.clone(), subscribed, closed));
    let models = include_str!("wrapper_models.inc").to_string();
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(models), crate::ffi::OpenPorts { transport: Some(peer.clone()), encrypted: Some(config), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    database.register_operations(json!([{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"}]).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let epoch = database.session_epoch();
    let notices = database.subscribe_values(epoch, ReadRequest::Events { model: "ChatActivityNotice".into() });
    let actions = database.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(json!({"model":"ChatAction","relation":"by_chat","params":{"chatId":"c"}}).to_string()) });
    let token = crate::test_wait::within(subscriptions.recv()).await.unwrap();
    until(&notices, "the notices open", |value| matches!(value, ReadValue::Ready)).await;
    until(&actions, "the list opens", |value| matches!(value, ReadValue::Snapshot { rows, .. } if rows.is_empty())).await;
    let frame = |kind: &str, active: bool| json!({"data":{"chatActionSignal":{"chatId":"c","userId":"u","kind":kind,"active":active}}});
    assert!(peer.cable.event(&token, frame("TYPING", true)));
    until(&actions, "the typing row", |value| matches!(value, ReadValue::Delta { edits, .. } if edits.len() == 1) || matches!(value, ReadValue::Snapshot { rows, .. } if rows.len() == 1)).await;
    assert!(peer.cable.event(&token, frame("SCREENSHOT", true)));
    until(&notices, "the screenshot notice", |value| matches!(value, ReadValue::Event { .. })).await;
    assert!(peer.cable.event(&token, frame("TYPING", false)));
    until(&actions, "the typing stops", |value| matches!(value, ReadValue::Delta { edits, .. } if matches!(edits.as_slice(), [crate::ffi::RowEdit::Remove { .. }])) || matches!(value, ReadValue::Snapshot { rows, .. } if rows.is_empty())).await;
    database.close_now();
}
