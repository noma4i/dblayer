//! D-476: an authoritative "no such row" answer to a node read deletes the local row like a server delete event.

use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_lists};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::schema::{AssociationKind, AssociationSpec, Coverage, Dependent, NodeRead, RelationSpec, RemoteShape};

/// A chat owns its messages (`dependent: :destroy`) and is read by `node` and `nodes`.
fn owned_messages(models: &mut Value) {
    add_chat(models);
    add_lists(models);
    let chat = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
    let mut messages = AssociationSpec::new("messages", AssociationKind::HasMany, "Message", "chatId");
    messages.dependent = Some(Dependent::Destroy);
    chat["associations"].as_array_mut().unwrap().push(serde_json::to_value(messages).unwrap());
    let mut nodes = RelationSpec::global("nodes");
    nodes.remote = Some(crate::schema_fixtures::remote("ChatNodes", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    nodes.node = Some(NodeRead::Many);
    chat["relations"].as_array_mut().unwrap().push(serde_json::to_value(nodes).unwrap());
}

fn chat(id: &str, name: &str) -> Value { json!({"__typename": "Chat", "id": id, "name": name, "lists": ["SECONDARY"]}) }

struct Seeded {
    rig: Rig,
    rows: Arc<Rows>,
}

/// Chats c1 and c9 are stored and shown, c9 owns message m9.
async fn seeded() -> Seeded {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(owned_messages)).await;
    let mut operations: Vec<Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "ChatNodes", "document": "query ChatNodes($ids: [ID!]!) { nodes(ids: $ids) { ... on Chat { id name lists } } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    rig.world.answer_with("ChatList", json!({"data": {"chats": []}}));
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rig.land("Chat", chat("c1", "one")).await;
    rig.land("Chat", chat("c9", "nine")).await;
    rig.land("Message", json!({"id": "m9", "chatId": "c9", "text": "hi", "at": 1})).await;
    Seeded { rig, rows }
}

impl Seeded {
    fn stored(&self, model: &str, id: &str) -> bool {
        self.rig.database.peek_row_now(self.rig.database.session_epoch(), model.into(), id.into()).unwrap().is_some_and(|row| row.id == id)
    }

    fn shown(&self) -> Vec<String> {
        let frames = self.rows.1.lock().unwrap();
        frames.last().map(|frame| frame.rows.iter().map(|row| row.id.clone()).collect()).unwrap_or_default()
    }

    /// The operations the server saw after `from` log entries.
    fn sent_after(&self, from: usize) -> Vec<String> { self.rig.world.log().into_iter().skip(from).map(|entry| entry.operation).collect() }

    /// What the rows look like after the answer: chat c9, its message m9, chat c9 in the shown list, the requests sent.
    fn outcome(&self, from: usize) -> (bool, bool, bool, Vec<String>) {
        (self.stored("Chat", "c9"), self.stored("Message", "m9"), self.shown().contains(&"c9".to_string()), self.sent_after(from))
    }
}

/// D-476: null and NOT_FOUND on `node(id, force: true)`, and a null entry of `nodes`, delete the row and its dependent rows, show the deletion and send no mutation. A foreign type writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn authoritative_absent_node_answer_deletes_the_row_and_its_dependents() {
    let deleted = |read: &str| (false, false, false, vec![read.to_string()]);
    let kept = |read: &str| (true, true, true, vec![read.to_string()]);
    let not_found = json!({"errors": [{"message": "not found", "path": ["node"], "extensions": {"code": "NOT_FOUND"}}], "data": {"node": null}});
    let cases: [(&str, &str, Value, (bool, bool, bool, Vec<String>)); 4] = [
        ("null node", "ChatNode", json!({"data": {"node": null}}), deleted("ChatNode")),
        ("NOT_FOUND node", "ChatNode", not_found, deleted("ChatNode")),
        ("null entry of nodes", "ChatNodes", json!({"data": {"nodes": [chat("c1", "one"), null]}}), deleted("ChatNodes")),
        ("foreign type", "ChatNode", json!({"data": {"node": {}}}), kept("ChatNode")),
    ];
    let mut missed = Vec::new();
    for (case, operation, answer, expected) in cases {
        let seeded = seeded().await;
        let rig = &seeded.rig;
        let epoch = rig.database.session_epoch();
        rig.world.answer_with(operation, answer);
        let from = rig.world.log().len();
        let result = match operation {
            "ChatNode" => bound(rig.database.node(epoch, "Chat".into(), "node".into(), "c9".into(), true)).await.map(|row| row.map(|row| row.id).into_iter().collect::<Vec<_>>()),
            _ => bound(rig.database.nodes(epoch, "Chat".into(), "nodes".into(), vec!["c1".into(), "c9".into()])).await.map(|rows| rows.into_iter().map(|row| row.id).collect()),
        };
        rig.settle().await;
        let answered = result.unwrap_or_else(|error| vec![format!("error {error:?}")]);
        let wanted = if operation == "ChatNode" { Vec::<String>::new() } else { vec!["c1".to_string()] };
        if answered != wanted { missed.push(format!("{case}: the call returned {answered:?}, wanted {wanted:?}")); }
        let outcome = seeded.outcome(from);
        if outcome != expected { missed.push(format!("{case}: (chat stored, message stored, chat shown, sent) = {outcome:?}, wanted {expected:?}")); }
        if !seeded.stored("Chat", "c1") { missed.push(format!("{case}: chat c1 of the answer was deleted")); }
        rig.database.close_now();
    }
    assert!(missed.is_empty(), "{missed:#?}");
}

/// D-476 (R14): a row written after the base revision of the read (an event landed while the read was in flight) is not deleted by its null answer.
#[tokio::test(flavor = "multi_thread")]
async fn absent_node_answer_keeps_a_row_written_during_the_read() {
    let seeded = seeded().await;
    let rig = &seeded.rig;
    let epoch = rig.database.session_epoch();
    rig.world.answer_with("ChatNode", json!({"data": {"node": null}}));
    let gate = rig.world.hold_next("ChatNode");
    let reading = { let database = rig.database.clone(); rig.spawn(async move { database.node(epoch, "Chat".into(), "node".into(), "c9".into(), true).await }) };
    gate.entered().await;
    rig.land_unsettled("Chat", chat("c9", "renamed")).await;
    gate.release();
    bound(reading).await.unwrap().unwrap();
    rig.settle().await;
    let row = rig.database.peek_row_now(epoch, "Chat".into(), "c9".into()).unwrap();
    assert!(row.is_some(), "the chat written by the event during the read stays");
    assert!(seeded.stored("Message", "m9"), "its message stays");
    rig.database.close_now();
}
