//! `SchemaFeedCodec` on frames of the `.dbl` declarations. Requirement -> observed value -> assert:
//! feed_codec_payload_upsert: a change with a full payload -> an `upsert` event of the declared model with the normalized row.
//! feed_codec_removes_on: a change of a `removes_on` kind -> a `delete` event of the event id.
//! feed_codec_direct_reread: a direct read -> the `reread` key of the relation with the event id as its parameter.
//! feed_codec_listens: `listens` -> `apply`, `deleted_by`, `deleted_unless`, `id`, `matches` decide the op of the event.
//! feed_codec_invalid: a change that misses its kind -> an `invalid` event, never a silent drop.
//! feed_codec_joins: an upserted row joins the relations `joins_events` by its columns.
//! feed_codec_request: the request is the document of the declared subscription, an unregistered operation fails the codec.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::feed::FramedEvent;
use super::windows::WindowKey;
use super::{FeedCodec, SchemaFeedCodec};
use crate::db::fixtures::{chats_spec, messages_spec};
use crate::db::Db;
use crate::schema::{ChangeFeed, Coverage, ModelSpec, RelationSpec, RemoteShape, UpdatesSpec};
use crate::sync::OperationDef;
use crate::transport::Response;

fn read(value: Value) -> crate::schema::ChangeRead { serde_json::from_value(value).unwrap() }

fn updates(value: Value) -> UpdatesSpec { serde_json::from_value(value).unwrap() }

fn operations() -> super::Operations {
    Arc::new(|name| match name {
        "Changes" => Some(Arc::new(OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId message { id chatId body } } } }", "recordEvents"))),
        "MessageChanged" => Some(Arc::new(OperationDef::new("MessageChanged", "subscription MessageChanged { messageChanged { id chatId body deleted } }", "messageChanged"))),
        _ => None,
    })
}

fn specs(with_feed: bool) -> Vec<ModelSpec> {
    let mut chats = chats_spec();
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    chats.relations.push(list);
    chats.changes.push(read(json!({"area":"CHAT", "relation":"list", "removes_on":["DELETED"]})));
    let mut messages = messages_spec();
    messages.changes.push(read(json!({"area":"MESSAGE", "relation":"x", "payload":"message", "removes_on":["DELETED"]})));
    let mut by_chat = RelationSpec::new("by_chat", &["chatId"]);
    by_chat.joins_events = true;
    by_chat.remote = Some(crate::schema::RemoteSpec { operation: "Chat".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None });
    messages.relations.push(by_chat);
    if with_feed {
        chats.change_feed = Some(ChangeFeed { subscription: "Changes".into(), root: "recordEvents.changes".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    } else {
        messages.listens.push(updates(json!({"subscription":"MessageChanged", "deleted_by":"deleted"})));
    }
    vec![chats, messages]
}

fn codec(with_feed: bool) -> SchemaFeedCodec { SchemaFeedCodec::new(Db::open_in_memory(specs(with_feed), None).unwrap(), operations()).unwrap() }

fn frame(data: Value) -> Response { Response::data(data) }

fn payloads(events: &[FramedEvent]) -> Vec<Value> { events.iter().map(|event| serde_json::from_str(&event.payload).unwrap()).collect() }

fn change(topic: &str, action: &str, id: &str, message: Value) -> Value { json!({"recordEvents":{"changes":[{"topic":topic, "action":action, "recordId":id, "message":message}]}}) }

#[test]
fn feed_codec_payload_upsert() {
    let events = codec(true).events(frame(change("MESSAGE", "UPDATED", "m1", json!({"id":"m1", "chatId":"c1", "body":"hi"}))));
    assert_eq!(events.len(), 1);
    let upsert = payloads(&events).remove(0);
    assert_eq!((upsert["op"].as_str(), upsert["model"].as_str(), upsert["id"].as_str(), upsert["row"]["body"].as_str()), (Some("upsert"), Some("messages"), Some("m1"), Some("hi")));
    assert_eq!(events[0].model, "messages");
}

#[test]
fn feed_codec_removes_on() {
    let events = codec(true).events(frame(change("MESSAGE", "DELETED", "m1", Value::Null)));
    assert_eq!(payloads(&events), [json!({"op":"delete", "model":"messages", "id":"m1"})]);
}

#[test]
fn feed_codec_direct_reread() {
    let events = codec(true).events(frame(change("CHAT", "UPDATED", "c1", Value::Null)));
    let expected = BTreeSet::from([WindowKey::relation("chats", "list", &Map::from_iter([("list".to_string(), json!("c1"))]))]);
    assert_eq!((events.len(), events[0].model.as_str(), &events[0].reread), (1, "chats", &expected));
    assert_eq!(payloads(&events)[0]["op"], "reread");
    assert_eq!(payloads(&codec(true).events(frame(change("CHAT", "DELETED", "c1", Value::Null)))), [json!({"op":"delete", "model":"chats", "id":"c1"})]);
}

#[test]
fn feed_codec_listens() {
    let codec = codec(false);
    let live = payloads(&codec.events(frame(json!({"messageChanged":{"id":"m2", "chatId":"c1", "body":"x", "deleted":false}}))));
    let gone = payloads(&codec.events(frame(json!({"messageChanged":{"id":"m2", "chatId":"c1", "body":"x", "deleted":true}}))));
    assert_eq!((live[0]["op"].as_str(), live[0]["id"].as_str()), (Some("upsert"), Some("m2")));
    assert_eq!(gone, [json!({"op":"delete", "model":"messages", "id":"m2"})]);
    assert!(codec.events(frame(json!({"other":1}))).is_empty(), "a frame of another root has no event");
}

#[test]
fn feed_codec_invalid() {
    let events = codec(true).events(frame(json!({"recordEvents":{"changes":[{"topic":"MESSAGE", "recordId":"m1"}]}})));
    assert_eq!(payloads(&events)[0]["op"], "invalid");
}

#[test]
fn feed_codec_joins() {
    let codec = codec(true);
    let row = json!({"id":"m1", "chatId":"c1"}).as_object().unwrap().clone();
    assert_eq!(codec.joins_of("messages", &row), [json!({"relation":"by_chat", "params":{"chatId":"c1"}})]);
    assert!(codec.joins_of("messages", &json!({"id":"m1"}).as_object().unwrap().clone()).is_empty(), "a row without the join column joins nothing");
}

#[test]
fn feed_codec_request() {
    let request = codec(true).request(&["messages".to_string()]);
    assert_eq!(request.operation_name.as_str(), "Changes");
    assert!(request.document.starts_with("subscription Changes"));
    assert!(SchemaFeedCodec::new(Db::open_in_memory(vec![ModelSpec::new("things", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)])], None).unwrap(), operations()).is_err(), "no declared subscription");
}
