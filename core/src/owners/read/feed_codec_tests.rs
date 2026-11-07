//! `SchemaFeedCodec` on frames of the `.dbl` declarations. Requirement -> observed value -> assert:
//! feed_codec_payload_upsert: a change with a full payload -> an `upsert` event of the declared model with the normalized row.
//! feed_codec_removes_on: a change of a `removes_on` kind -> a `delete` event of the event id.
//! feed_codec_direct_reread: a direct read -> the `reread` key of the relation with the event id as its parameter.
//! feed_codec_observed_reread: an `observed` read -> `StaleKeys::All` of the model, so Windows decides which windows are live; the codec reads no registry.
//! feed_codec_listens: `listens` -> `apply`, `deleted_by`, `deleted_unless`, `id`, `matches` decide the op of the event.
//! feed_codec_invalid: a change that misses its kind -> an `invalid` event, never a silent drop.
//! feed_codec_joins: an upserted row joins the relations `joins_events` by its columns.
//! feed_codec_request: the request of a channel is the document of the declared subscription.
//! feed_codec_relation_updates_channel: the `updates` of a relation read the frames of their own channel.
//! feed_codec_channels_of_model: a model reads the change feed when it declares changes, and the subscription of each `listens` and relation `updates`.

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use super::feed::FramedEvent;
use super::windows::{StaleKeys, WindowKey};
use super::{FeedCodec, SchemaFeedCodec};
use crate::store::fixtures::{chat_messages_spec, chats_spec};
use crate::store::Store;
use crate::schema::{ChangeFeed, Coverage, ModelSpec, RelationSpec, RemoteShape, UpdatesSpec};
use super::ReadOperation;
use crate::transport::Response;

fn read(value: Value) -> crate::schema::ChangeRead { serde_json::from_value(value).unwrap() }

fn updates(value: Value) -> UpdatesSpec { serde_json::from_value(value).unwrap() }

fn operations() -> super::feed_codec::Operations {
    let known = super::ReadOperations::default();
    let declare = |name: &str, document: &str, root: &str| ReadOperation { name: name.into(), document: document.into(), root_path: root.into(), page_size: 0, returns: None };
    known.declare([
        declare("Changes", "subscription Changes { recordEvents { changes { topic action recordId message { id chatId body } } } }", "recordEvents"),
        declare("MessageChanged", "subscription MessageChanged { messageChanged { id chatId body deleted } }", "messageChanged"),
        declare("RelationChanged", "subscription RelationChanged { relationChanged { id chatId body deleted } }", "relationChanged"),
    ]);
    known
}

fn specs(with_feed: bool) -> Vec<ModelSpec> {
    let mut chats = chats_spec();
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    chats.relations.push(list);
    chats.changes.push(read(json!({"area":"CHAT", "relation":"list", "removes_on":["DELETED"]})));
    chats.changes.push(read(json!({"area":"CHATSEEN", "relation":"list", "observed":true})));
    let mut messages = chat_messages_spec();
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

fn codec(with_feed: bool) -> SchemaFeedCodec { SchemaFeedCodec::new(Store::open_models(None, specs(with_feed), None).map(std::sync::Arc::new).unwrap(), operations()) }

fn codec_for_apply(apply: Value) -> SchemaFeedCodec {
    let mut messages = chat_messages_spec();
    messages.listens.push(updates(json!({"subscription":"MessageChanged", "apply":apply})));
    SchemaFeedCodec::new(Store::open_models(None, vec![chats_spec(), messages], None).map(std::sync::Arc::new).unwrap(), operations())
}

fn frame(data: Value) -> Response { Response::data(data) }

fn payloads(events: &[FramedEvent]) -> Vec<Value> { events.iter().map(|event| serde_json::from_str(&event.payload).unwrap()).collect() }

fn change(topic: &str, action: &str, id: &str, message: Value) -> Value { json!({"recordEvents":{"changes":[{"topic":topic, "action":action, "recordId":id, "message":message}]}}) }

#[test]
fn feed_codec_payload_upsert() {
    let events = codec(true).events("Changes", frame(change("MESSAGE", "UPDATED", "m1", json!({"id":"m1", "chatId":"c1", "body":"hi"}))));
    assert_eq!(events.len(), 1);
    let upsert = payloads(&events).remove(0);
    assert_eq!((upsert["op"].as_str(), upsert["model"].as_str(), upsert["id"].as_str(), upsert["row"]["body"].as_str()), (Some("upsert"), Some("messages"), Some("m1"), Some("hi")));
    assert_eq!(events[0].model, "messages");
}

#[test]
fn feed_codec_removes_on() {
    let events = codec(true).events("Changes", frame(change("MESSAGE", "DELETED", "m1", Value::Null)));
    assert_eq!(payloads(&events), [json!({"op":"delete", "model":"messages", "id":"m1"})]);
}

#[test]
fn feed_codec_direct_reread() {
    let events = codec(true).events("Changes", frame(change("CHAT", "UPDATED", "c1", Value::Null)));
    let expected = StaleKeys::Set(BTreeSet::from([WindowKey::relation("chats", "list", &Map::from_iter([("list".to_string(), json!("c1"))]))]));
    assert_eq!((events.len(), events[0].model.as_str(), &events[0].reread), (1, "chats", &expected));
    assert_eq!(payloads(&events)[0]["op"], "reread");
    assert_eq!(payloads(&codec(true).events("Changes", frame(change("CHAT", "DELETED", "c1", Value::Null)))), [json!({"op":"delete", "model":"chats", "id":"c1"})]);
}

#[test]
fn feed_codec_observed_reread() {
    let events = codec(true).events("Changes", frame(change("CHATSEEN", "UPDATED", "c1", Value::Null)));
    assert_eq!((events.len(), events[0].model.as_str(), &events[0].reread), (1, "chats", &StaleKeys::All));
}

#[test]
fn feed_codec_listens() {
    let codec = codec(false);
    let live = payloads(&codec.events("MessageChanged", frame(json!({"messageChanged":{"id":"m2", "chatId":"c1", "body":"x", "deleted":false}}))));
    let gone = payloads(&codec.events("MessageChanged", frame(json!({"messageChanged":{"id":"m2", "chatId":"c1", "body":"x", "deleted":true}}))));
    assert_eq!((live[0]["op"].as_str(), live[0]["id"].as_str()), (Some("upsert"), Some("m2")));
    assert_eq!(gone, [json!({"op":"delete", "model":"messages", "id":"m2"})]);
    assert!(codec.events("MessageChanged", frame(json!({"other":1}))).is_empty(), "a frame of another root has no event");
}

#[test]
fn feed_apply_ignore() {
    let codec = codec_for_apply(json!({"effect":"ignore"}));
    assert!(codec.events("MessageChanged", frame(json!({"messageChanged":{"id":"m1", "body":"hello"}}))).is_empty());
}

#[test]
fn feed_apply_matches_skip() {
    let codec = codec_for_apply(json!({"effect":"upsert", "matches":{"kind":["created"], "source":{"not":["hidden"]}}}));
    for value in [json!({}), json!({"kind":null}), json!({"kind":1}), json!({"kind":"updated"}), json!({"kind":"created", "source":"hidden"})] {
        let mut event = json!({"id":"m1", "chatId":"c1", "body":"hello", "source":"public"});
        event.as_object_mut().unwrap().extend(value.as_object().unwrap().clone());
        assert!(codec.events("MessageChanged", frame(json!({"messageChanged":event}))).is_empty());
    }
    let accepted = codec.events("MessageChanged", frame(json!({"messageChanged":{"id":"m1", "chatId":"c1", "body":"hello", "kind":"created", "source":"public"}})));
    assert_eq!(payloads(&accepted)[0]["op"], "upsert");
}

#[test]
fn feed_codec_invalid() {
    let events = codec(true).events("Changes", frame(json!({"recordEvents":{"changes":[{"topic":"MESSAGE", "recordId":"m1"}]}})));
    assert_eq!(payloads(&events)[0]["op"], "invalid");
}

#[test]
fn feed_changes_area_unknown() {
    let events = codec(true).events("Changes", frame(change("UNKNOWN", "UPDATED", "m1", Value::Null)));
    assert!(events.is_empty());
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
    let request = codec(true).request("Changes");
    assert_eq!(request.operation_name.as_str(), "Changes");
    assert!(request.document.starts_with("subscription Changes"));
}

#[test]
fn feed_codec_relation_updates_channel() {
    let mut messages = chat_messages_spec();
    let mut by_chat = RelationSpec::new("by_chat", &["chatId"]);
    by_chat.updates.push(updates(json!({"subscription":"RelationChanged", "deleted_by":"deleted"})));
    messages.relations.push(by_chat);
    messages.listens.push(updates(json!({"subscription":"MessageChanged"})));
    let codec = SchemaFeedCodec::new(Store::open_models(None, vec![chats_spec(), messages], None).map(std::sync::Arc::new).unwrap(), operations());
    let live = payloads(&codec.events("RelationChanged", frame(json!({"relationChanged":{"id":"m3", "chatId":"c1", "body":"y", "deleted":false}}))));
    assert_eq!((live[0]["op"].as_str(), live[0]["model"].as_str(), live[0]["id"].as_str()), (Some("upsert"), Some("messages"), Some("m3")));
    assert!(codec.events("MessageChanged", frame(json!({"relationChanged":{"id":"m3", "chatId":"c1"}}))).is_empty(), "an event is read by the channel it came on");
}

#[test]
fn feed_codec_channels_of_model() {
    let channels = super::channels_of(&Store::open_models(None, specs(true), None).map(std::sync::Arc::new).unwrap());
    assert_eq!(channels.get("chats"), Some(&vec!["Changes".to_string()]));
    assert_eq!(channels.get("messages"), Some(&vec!["Changes".to_string()]));
}

#[test]
fn feed_codec_memory_event_without_id_gets_a_uuid() {
    let mut notices = ModelSpec::new("notices", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::scalar("body", crate::value::ScalarKind::Str)]);
    notices.storage = crate::schema::Storage::Memory;
    notices.listens.push(updates(json!({"subscription":"MessageChanged"})));
    let codec = SchemaFeedCodec::new(Store::open_models(None, vec![notices], None).map(std::sync::Arc::new).unwrap(), operations());
    let first = payloads(&codec.events("MessageChanged", frame(json!({"messageChanged":{"body":"a"}}))));
    let second = payloads(&codec.events("MessageChanged", frame(json!({"messageChanged":{"body":"a"}}))));
    assert_eq!(first[0]["op"], "upsert");
    assert_ne!(first[0]["id"], second[0]["id"], "each event without an id is a new row");
    assert!(uuid::Uuid::parse_str(first[0]["id"].as_str().unwrap()).is_ok());
}
