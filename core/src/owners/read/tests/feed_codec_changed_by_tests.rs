//! `changed_by` and `effect: :reread` on frames: the keys of the windows a frame turns stale, by form (`via`).

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use super::feed::FramedEvent;
use super::windows::{StaleKeys, WindowKey};
use super::{FeedCodec, ReadOperation, SchemaFeedCodec};
use crate::schema::{ChangeFeed, ModelSpec, NodeRead, RelationSpec};
use crate::store::fixtures::{chat_messages_spec, chats_spec, put};
use crate::store::Store;
use crate::transport::Response;

fn read(value: Value) -> crate::schema::ChangeRead { serde_json::from_value(value).unwrap() }

fn relation(name: &str, params: &[&str]) -> RelationSpec {
    let mut relation = RelationSpec::new(name, &[]);
    relation.text_params = params.iter().map(|param| param.to_string()).collect();
    relation
}

fn specs(reads: Vec<Value>) -> Vec<ModelSpec> {
    let mut chats = chats_spec();
    let mut node = relation("node", &[crate::schema::NODE_PARAM]);
    node.node = Some(NodeRead::One);
    chats.relations.extend([node, relation("list", &["list"]), relation("named", &["key", "other"]), RelationSpec::global("current")]);
    chats.changes = reads.into_iter().map(read).collect();
    chats.change_feed = Some(ChangeFeed { subscription: "Changes".into(), root: "changes".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let mut messages = chat_messages_spec();
    messages.changes.push(read(json!({"area":"MESSAGE", "relation":"x", "removes_on":["DELETED"]})));
    vec![chats, messages]
}

fn codec(reads: Vec<Value>) -> SchemaFeedCodec { codec_with_store(reads).0 }

fn codec_with_store(reads: Vec<Value>) -> (SchemaFeedCodec, std::sync::Arc<Store>) {
    let known = super::ReadOperations::default();
    known.declare([ReadOperation { name: "Changes".into(), document: "subscription Changes { changes { topic action recordId } }".into(), root_path: "changes".into(), page_size: 0 }]);
    let store = Store::open_models(None, specs(reads), None, std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    (SchemaFeedCodec::new(store.clone(), known), store)
}

fn frame(topic: &str, action: &str, id: &str) -> Response { Response::data(json!({"changes":[{"topic":topic, "action":action, "recordId":id}]})) }

fn key(relation: &str, params: Value) -> WindowKey { WindowKey::relation("chats", relation, params.as_object().unwrap_or(&Map::new())) }

fn keys(list: &[WindowKey]) -> StaleKeys { StaleKeys::Set(list.iter().cloned().collect::<BTreeSet<_>>()) }

fn of_model<'a>(events: &'a [FramedEvent], model: &str) -> Vec<&'a FramedEvent> { events.iter().filter(|event| event.model == model).collect() }

#[test]
fn feed_changed_by_two_forms() {
    let events = codec(vec![json!({"area":"CHAT", "relation":"node"}), json!({"area":"CHAT", "relation":"named", "parameter":"other"})]).events("Changes", frame("CHAT", "UPDATED", "c1"));
    assert_eq!(events.len(), 1, "one event of the model for two forms");
    assert_eq!(events[0].reread, keys(&[key("node", json!({crate::schema::NODE_PARAM: "c1"})), key("named", json!({"other":"c1"}))]));
    let observed = codec(vec![json!({"area":"CHAT", "relation":"node"}), json!({"area":"CHAT", "relation":"list", "observed":true})]).events("Changes", frame("CHAT", "UPDATED", "c1"));
    assert_eq!((observed.len(), &observed[0].reread), (1, &StaleKeys::All), "an observed form widens the union to the model");
}

#[test]
fn feed_payload_missing_uses_node() {
    for (relation, payload, accepted) in [("node", Some(Value::Null), true), ("node", None, true), ("node", Some(json!({})), false), ("", Some(Value::Null), false)] {
        let codec = codec(vec![json!({"area":"CHAT", "relation":relation, "payload":"chat"})]);
        let mut change = json!({"topic":"CHAT", "action":"UPDATED", "recordId":"c1"});
        if let Some(payload) = payload { change["chat"] = payload; }
        let response = Response::data(json!({"changes":[change]}));
        let events = codec.events("Changes", response);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].refused, !accepted);
        if accepted { assert_eq!(events[0].reread, keys(&[key("node", json!({crate::schema::NODE_PARAM: "c1"}))])); }
    }
}

#[test]
fn feed_changed_by_key_shapes() {
    let named = codec(vec![json!({"area":"CHAT", "relation":"named"})]).events("Changes", frame("CHAT", "UPDATED", "c1"));
    assert_eq!(named[0].reread, keys(&[key("named", json!({"key":"c1"}))]), "the first parameter of the relation takes the id");
    let current = codec(vec![json!({"area":"CHAT", "relation":"current"})]).events("Changes", frame("CHAT", "UPDATED", "c1"));
    assert_eq!(current[0].reread, keys(&[key("current", json!({}))]), "a singleton has no id in its key");
}

fn derived() -> (SchemaFeedCodec, std::sync::Arc<Store>) { codec_with_store(vec![json!({"area":"MESSAGE", "relation":"list", "id_of":{"model":"messages", "field":"chatId"}})]) }

#[test]
fn feed_id_of_reads_field() {
    let (codec, store) = derived();
    put(&store, "messages", json!({"id":"m1", "chatId":"c7"}));
    let events = of_model(&codec.events("Changes", frame("MESSAGE", "UPDATED", "m1")), "chats").into_iter().cloned().collect::<Vec<_>>();
    assert_eq!((events.len(), &events[0].reread), (1, &keys(&[key("list", json!({"list":"c7"}))])));
}

#[test]
fn feed_id_of_missing_source() {
    let (codec, store) = derived();
    let events = codec.events("Changes", frame("MESSAGE", "UPDATED", "m1"));
    let chats = of_model(&events, "chats");
    assert_eq!((chats.len(), chats[0].reread.is_empty(), chats[0].refused), (1, true, false), "no source row: an event with no keys");
    put(&store, "messages", json!({"id":"m2", "chatId":null}));
    let empty = codec.events("Changes", frame("MESSAGE", "UPDATED", "m2"));
    assert!(of_model(&empty, "chats")[0].reread.is_empty(), "an empty field gives no keys");
}

#[test]
fn feed_id_of_removed_source() {
    let (codec, store) = derived();
    put(&store, "messages", json!({"id":"m1", "chatId":"c7"}));
    let events = codec.events("Changes", frame("MESSAGE", "DELETED", "m1"));
    let payload: Value = serde_json::from_str(&of_model(&events, "messages")[0].payload).unwrap();
    assert_eq!(payload["op"], "delete", "the frame removes the source row");
    assert_eq!(of_model(&events, "chats")[0].reread, keys(&[key("list", json!({"list":"c7"}))]), "the key comes from the field before the removal");
}

#[test]
fn feed_apply_reread() {
    let known = super::ReadOperations::default();
    known.declare([ReadOperation { name: "ChatChanged".into(), document: "subscription ChatChanged { chatChanged { id } }".into(), root_path: "chatChanged".into(), page_size: 0 }]);
    let mut with = specs(Vec::new());
    with[0].change_feed = None;
    with[0].listens.push(serde_json::from_value(json!({"subscription":"ChatChanged", "apply":{"effect":"reread", "via":"list"}})).unwrap());
    let codec = SchemaFeedCodec::new(Store::open_models(None, with, None, std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap(), known);
    let events = codec.events("ChatChanged", Response::data(json!({"chatChanged":{"id":"c1"}})));
    assert_eq!((events.len(), &events[0].reread), (1, &keys(&[key("list", json!({"list":"c1"}))])));
    let payload: Value = serde_json::from_str(&events[0].payload).unwrap();
    assert_eq!((payload["op"].as_str(), payload["id"].as_str()), (Some("reread"), Some("c1")));
}
