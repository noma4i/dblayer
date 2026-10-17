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
    let row = rig.sync.node("chats", "node", "c1", false).await.unwrap().unwrap();
    assert_eq!((row.text("title"), row.get("isPublic")), (Some("Hi"), Some(&json!(true))));
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("shareUrl"), Some("u"));
    assert_eq!(rig.transport.calls()[0].variables, json!({"id": "c1"}));
}

#[tokio::test(start_paused = true)]
async fn a_relation_under_node_lands_the_connection_rows() {
    let rig = node_rig();
    let edges = json!([{"node": {"id": "m1", "chatId": "c1", "text": "a"}}, {"node": {"id": "m2", "chatId": "c1", "text": "b"}}]);
    rig.transport.enqueue("ChatThread", data(json!({"node": {"messages": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": "e"}}}})));
    let outcome = rig.sync.load("messages", "thread", &params(json!({"chatId": "c1"})), Default::default()).await.unwrap();
    assert_eq!(outcome.rows, 2);
    assert_eq!(member_ids(&rig, "messages", "thread", &params(json!({"chatId": "c1"}))), ["m1", "m2"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"chatId": "c1", "first": 20}));
}

#[tokio::test(start_paused = true)]
async fn a_subset_read_changes_only_the_selected_fields() {
    let rig = node_rig();
    put(&rig, "messages", json!({"id": "m1", "chatId": "c1", "text": "keep", "media": {"fileUrl": "http://f", "extra": 1}}));
    rig.transport.enqueue("MessageTranscodeStatus", data(json!({"node": {"id": "m1", "chatId": "c1", "media": {"kind": "VIDEO", "fileUrl": ""}}})));
    let row = rig.sync.node("messages", "transcode_status", "m1", false).await.unwrap().unwrap();
    assert_eq!(row.text("text"), Some("keep"));
    assert_eq!(row.get("media"), Some(&json!({"fileUrl": "http://f", "extra": 1, "kind": "VIDEO"})));
    assert_eq!(rig.transport.calls()[0].variables, json!({"messageId": "m1"}));
}

#[tokio::test(start_paused = true)]
async fn an_event_rereads_with_force_and_a_plain_call_inside_fresh_for_does_not() {
    let rig = node_rig();
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "one"}})));
    rig.transport.enqueue("ChatNode", data(json!({"node": {"id": "c1", "title": "two"}})));
    rig.sync.node("chats", "node", "c1", false).await.unwrap();
    let fresh = rig.sync.node("chats", "node", "c1", false).await.unwrap().unwrap();
    assert_eq!((fresh.text("title"), rig.transport.call_count("ChatNode")), (Some("one"), 1));
    let forced = rig.sync.node("chats", "node", "c1", true).await.unwrap().unwrap();
    assert_eq!((forced.text("title"), rig.transport.call_count("ChatNode")), (Some("two"), 2));
}

#[tokio::test(start_paused = true)]
async fn an_answer_without_a_matching_entity_is_none_and_writes_nothing() {
    let rig = node_rig();
    put(&rig, "chats", json!({"id": "c1", "title": "old"}));
    let before = rig.db.revision_seq().unwrap();
    for answer in [json!({"node": null}), json!({"node": {}}), json!({"node": {"__typename": "Gadget"}})] {
        rig.transport.enqueue("ChatNode", data(answer));
        assert_eq!(rig.sync.node("chats", "node", "c1", true).await.unwrap(), None);
    }
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("old"));
    assert_eq!(rig.db.revision_seq().unwrap(), before);
}

#[tokio::test(start_paused = true)]
async fn nodes_reads_many_in_one_request_keeps_order_and_skips_missing() {
    let rig = node_rig();
    rig.transport.enqueue("ChatsByIds", data(json!({"nodes": [{"id": "c2", "title": "b"}, null, {}, {"id": "c1", "title": "a"}]})));
    let rows = rig.sync.nodes("chats", "batch", &["c2", "x", "y", "c1"]).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.text("title").unwrap()).collect::<Vec<_>>(), ["b", "a"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"ids": ["c2", "x", "y", "c1"]}));
    assert_eq!(rig.transport.call_count("ChatsByIds"), 1);
}
