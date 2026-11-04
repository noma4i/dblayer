use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::rig::*;
use super::{OperationDef, SyncConfig};
use crate::db::Db;
use crate::identity::RowId;
use crate::schema::{Coverage, NodeRead, NODE_PARAM, FieldSpec, MergeRule, ModelSpec, PageDirection, RelationSpec, RemoteShape, VariableSource};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;

fn chats_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "chats",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            FieldSpec::scalar("shareUrl", ScalarKind::Str).optional(),
            FieldSpec::scalar("isPublic", ScalarKind::Bool).optional(),
        ],
    );
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.to_string()];
    node.node = Some(NodeRead::One);
    let mut node_remote = remote("ChatNode", RemoteShape::Single, "5.minutes");
    node_remote.variables = BTreeMap::from([("id".to_string(), VariableSource::Param { name: NODE_PARAM.to_string() })]);
    node.remote = Some(node_remote);
    let mut batch = RelationSpec::global("batch");
    batch.node = Some(NodeRead::Many);
    batch.remote = Some(remote("ChatsByIds", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    spec.relations.extend([node, batch]);
    spec
}

fn messages_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).optional().indexed(),
            FieldSpec::scalar("text", ScalarKind::Str).optional(),
            FieldSpec { merge: Some(MergeRule::Keys { keys: BTreeMap::from([("fileUrl".to_string(), MergeRule::NonEmpty)]) }), ..FieldSpec::object("media").optional() },
        ],
    );
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    let mut thread_remote = remote("ChatThread", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    thread_remote.root = Some("node.messages".to_string());
    thread.remote = Some(thread_remote);
    let mut status = RelationSpec::new("transcode_status", &[]);
    status.text_params = vec![NODE_PARAM.to_string()];
    status.node = Some(NodeRead::One);
    let mut status_remote = remote("MessageTranscodeStatus", RemoteShape::Single, "30.seconds");
    status_remote.variables = BTreeMap::from([("messageId".to_string(), VariableSource::Param { name: NODE_PARAM.to_string() })]);
    status.remote = Some(status_remote);
    spec.relations.extend([thread, status]);
    spec
}

pub(super) fn node_rig() -> Rig {
    let rig = rig_on(Db::open_in_memory(vec![chats_spec(), messages_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    for (name, document) in [
        ("ChatNode", "query chat($id: ID!) { node(id: $id) { ... on Chat { id title shareUrl isPublic } } }"),
        ("ChatsByIds", "query chats($ids: [ID!]!) { nodes(ids: $ids) { ... on Chat { id title } } }"),
        ("ChatThread", "query chatThread($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after) { edges { node { id chatId text } } pageInfo { hasNextPage endCursor } } } } }"),
        ("MessageTranscodeStatus", "query status($messageId: ID!) { node(id: $messageId) { ... on Message { id chatId media { kind fileUrl } } } }"),
    ] {
        rig.sync.register(OperationDef::from_document(name, document).unwrap());
    }
    rig
}

pub(super) fn stored(rig: &Rig, model: &str, id: &str) -> Option<crate::db::Row> {
    rig.db.find(model, &RowId::new(id).unwrap()).unwrap()
}

pub(super) fn data(value: Value) -> Reply {
    Reply::data(value)
}

#[tokio::test(start_paused = true)]
async fn node_reads_one_entity_and_lands_its_row() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "Hi", "shareUrl": "u", "isPublic": true}})));
    let row = rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", false).await.unwrap().unwrap();
    assert_eq!((row.text("title"), row.get("isPublic")), (Some("Hi"), Some(&json!(true))));
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("shareUrl"), Some("u"));
    assert_eq!(rig.transport.calls()[0].variables, json!({"id": "c1"}));
}

#[tokio::test(start_paused = true)]
async fn an_event_joins_a_manual_node_flight_and_keeps_its_membership_intent() {
    use std::time::Duration;
    use crate::schema::Membership;
    use crate::transport::{Response, SubscriptionEvent};
    let mut chat = chats_spec();
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("name".into());
    list.membership.insert("name".into(), Membership::Equals("title".into()));
    list.joins_node = true;
    list.remote = Some(remote("Chats", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    chat.relations.push(list);
    chat.change_feed = Some(super::subscribe_tests::change_feed());
    chat.changes.push(serde_json::from_value(json!({"area":"CHAT", "relation":"node"})).unwrap());
    let rig = rig_on(Db::open_in_memory(vec![chat], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("Chats", "query Chats { chats { id title } }", "chats"));
    rig.sync.register(OperationDef::from_document("ChatNode", "query chat($id: ID!) { node(id: $id) { ... on Chat { id title } } }").unwrap());
    rig.sync.register(OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let values = params(json!({"name":"same"}));
    rig.transport.enqueue("Chats", data(json!({"chats":[]})));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &values, Default::default()).await.unwrap();
    put(&rig, "chats", json!({"id":"c1", "title":"before"}));
    let events = rig.sync.open_model_events("chats").await.unwrap();
    rig.transport.enqueue("ChatNode", data(json!({"node":{"id":"c1", "title":"same"}})).after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let manual = tokio::spawn(async move { sync.node(&sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await });
    rig.transport.await_calls("ChatNode", 1).await;
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"CHAT", "action":"UPDATED", "recordId":"c1"}]}}))));
    tokio::time::timeout(Duration::from_secs(1), rig.sync.wait_joined(1)).await.expect("the event must join the manual request");
    let row = tokio::time::timeout(Duration::from_secs(10), manual).await.expect("the manual request must complete").unwrap().unwrap().unwrap();
    assert_eq!(row.text("title"), Some("same"));
    let item = tokio::time::timeout(Duration::from_secs(5), events.next()).await.expect("the event must be served by the shared request");
    let Some(super::ModelEventItem::Event(event)) = item else { panic!("expected a model event: {item:?}") };
    assert_eq!(event.before.unwrap().text("title"), Some("before"));
    assert_eq!(event.after.unwrap().text("title"), Some("same"));
    assert_eq!(member_ids(&rig, "chats", "list", &values), ["c1"]);
    assert_eq!(rig.transport.call_count("ChatNode"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_relation_under_node_lands_the_connection_rows() {
    let rig = node_rig();
    let edges = json!([{"node": {"id": "m1", "chatId": "c1", "text": "a"}}, {"node": {"id": "m2", "chatId": "c1", "text": "b"}}]);
    rig.transport.enqueue("ChatThread", data(json!({"node": {"messages": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": "e"}}}})));
    let outcome = rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "messages", "thread", &params(json!({"chatId": "c1"})), Default::default()).await.unwrap();
    assert_eq!(outcome.rows, 2);
    assert_eq!(member_ids(&rig, "messages", "thread", &params(json!({"chatId": "c1"}))), ["m1", "m2"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"chatId": "c1", "first": 20}));
}

#[tokio::test(start_paused = true)]
async fn a_subset_read_changes_only_the_selected_fields() {
    let rig = node_rig();
    put(&rig, "messages", json!({"id": "m1", "chatId": "c1", "text": "keep", "media": {"fileUrl": "http://f", "extra": 1}}));
    rig.transport.enqueue("MessageTranscodeStatus", data(json!({"node": {"id": "m1", "chatId": "c1", "media": {"kind": "VIDEO", "fileUrl": ""}}})));
    let row = rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "messages", "transcode_status", "m1", false).await.unwrap().unwrap();
    assert_eq!(row.text("text"), Some("keep"));
    assert_eq!(row.get("media"), Some(&json!({"fileUrl": "http://f", "extra": 1, "kind": "VIDEO"})));
    assert_eq!(rig.transport.calls()[0].variables, json!({"messageId": "m1"}));
}

#[tokio::test(start_paused = true)]
async fn an_event_rereads_with_force_and_a_plain_call_inside_fresh_for_does_not() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "one"}})));
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "two"}})));
    rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", false).await.unwrap();
    let fresh = rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", false).await.unwrap().unwrap();
    assert_eq!((fresh.text("title"), rig.transport.call_count("ChatNode")), (Some("one"), 1));
    let forced = rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await.unwrap().unwrap();
    assert_eq!((forced.text("title"), rig.transport.call_count("ChatNode")), (Some("two"), 2));
}

#[tokio::test(start_paused = true)]
async fn an_answer_without_a_matching_entity_is_none_and_writes_nothing() {
    let rig = node_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "old"}));
    let before = rig.db.revision_seq().unwrap();
    for answer in [json!({"node": null}), json!({"node": {}}), json!({"node": {"__typename": "Gadget"}})] {
        rig.transport.enqueue("ChatNode", data(answer));
        assert_eq!(rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await.unwrap(), None);
    }
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("old"));
    assert_eq!(rig.db.revision_seq().unwrap(), before);
}

#[tokio::test(start_paused = true)]
async fn nodes_reads_many_in_one_request_keeps_order_and_skips_missing() {
    let rig = node_rig();
    rig.transport.enqueue("ChatsByIds", data(json!({"nodes": [{"id": "c2", "title": "b"}, null, {}, {"id": "c1", "title": "a"}]})));
    let rows = crate::sync::rig::app(&rig.sync, rig.sync.nodes(&rig.sync.inner.db.admit().unwrap(), "chats", "batch", &["c2", "x", "y", "c1"])).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.text("title").unwrap()).collect::<Vec<_>>(), ["b", "a"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"ids": ["c2", "x", "y", "c1"]}));
    assert_eq!(rig.transport.call_count("ChatsByIds"), 1);
}

#[tokio::test(start_paused = true)]
async fn nodes_accepts_duplicate_ids_and_keeps_response_order() {
    let rig = node_rig();
    rig.transport.enqueue("ChatsByIds", data(json!({"nodes": [
        {"id": "c2", "title": "old", "shareUrl": "kept"}, null,
        {"id": "c1", "title": "middle"}, {"id": "c2", "title": "last"}
    ]})));
    let rows = crate::sync::rig::app(&rig.sync, rig.sync.nodes(&rig.sync.inner.db.admit().unwrap(), "chats", "batch", &["c2", "missing", "c1", "c2"])).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["c2", "c1", "c2"]);
    assert_eq!(rows.iter().map(|row| row.text("title")).collect::<Vec<_>>(), [Some("last"), Some("middle"), Some("last")]);
    assert_eq!(stored(&rig, "chats", "c2").unwrap().text("shareUrl"), Some("kept"));
}
