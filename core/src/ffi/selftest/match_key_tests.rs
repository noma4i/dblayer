use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_lists, cell, text};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::schema::{ActionDeclaration, FieldSpec, RootShape};
use crate::test_wait::settled;
use crate::value::ScalarKind;

const KEY: &str = "k1";

/// A chat that is created with a client key: the key is the `match_by` of the create, and a message event carries the chat as a related row.
fn add_chat_key(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(FieldSpec::scalar("clientKey", ScalarKind::Str).nullable().optional()).unwrap());
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("ChatCreate".into());
    create.root = Some("chatCreate".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.correlate = vec!["clientKey".into()];
    for name in ["name", "clientKey"] { create.variables.insert(name.into(), name.into()); }
    chat["actions"].as_array_mut().unwrap().push(serde_json::to_value(create).unwrap());
}

fn server_chat() -> Value { json!({"__typename": "Chat", "id": "c9", "name": "n", "clientKey": KEY, "lists": ["SECONDARY"]}) }

/// The row of the server holds the key.
fn keyed_in_store(rig: &Rig) -> bool {
    rig.database.db.read(&["Chat"], |reader| Ok(reader.stored_row("Chat", "c9")?.is_some_and(|row| row.row.get("clientKey") == Some(&json!(KEY))))).unwrap()
}

/// Which of the named rows stand in the store.
fn stored(rig: &Rig, ids: &[&str]) -> Vec<String> {
    rig.database.db.read(&["Chat"], |reader| Ok(ids.iter().filter(|id| reader.point("Chat", id).ok().flatten().is_some()).map(|id| id.to_string()).collect::<Vec<_>>())).unwrap()
}

/// The ids of the rows of the last frame that carry the key.
fn keyed(rig: &Rig, rows: &Rows) -> Vec<String> {
    let rows = rows.0.lock().unwrap();
    rows.iter().filter(|row| cell(rig, "Chat", row, "clientKey") == text(KEY)).map(|row| row.id.clone()).collect()
}

struct Held {
    rig: Rig,
    rows: Arc<Rows>,
    gate: Arc<super::rig_tests::Gate>,
    receipt: crate::ffi::WireOperationReceipt,
    temp: String,
}

/// The create with the key is in flight (its answer is held) and its temporary row is shown.
async fn held_create() -> Held {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| { add_chat(models); add_lists(models); add_chat_key(models); })).await;
    rig.world.answer_with("ChatCreate", json!({"data": {"chatCreate": server_chat()}}));
    rig.world.answer_with("ChatList", json!({"data": {"chats": [server_chat()]}}));
    rig.world.answer_with("ChatNode", json!({"data": {"node": server_chat()}}));
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let gate = rig.world.hold_next("ChatCreate");
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.create(epoch, "Chat".into(), json!({"name": "n", "clientKey": KEY}).to_string(), "{}".into())).await.unwrap();
    gate.entered().await;
    let temp = receipt.optimistic_value.clone().unwrap().id;
    settled(&rig, "the temporary row is shown", || keyed(&rig, &rows) == vec![temp.clone()]).await;
    Held { rig, rows, gate, receipt, temp }
}

impl Held {
    /// Once settled the store holds the row of the server only, and no frame showed both rows. The misses of the source come back.
    async fn replaced_by(self, source: &str) -> Vec<String> {
        let Held { rig, rows, gate, receipt, temp } = self;
        let mut missed = Vec::new();
        settled(&rig, &format!("{source}: the row of the server is stored"), || stored(&rig, &["c9"]) == vec!["c9".to_string()]).await;
        if stored(&rig, &[temp.as_str(), "c9"]) != vec!["c9".to_string()] { missed.push(format!("{source}: the temporary row stands beside the row of the server")); }
        let frames = rows.1.lock().unwrap().clone();
        if frames.iter().any(|frame| frame.rows.iter().any(|row| row.id == "c9") && frame.rows.iter().any(|row| row.id == temp)) { missed.push(format!("{source}: a frame shows the row of the server beside the temporary row")); }
        gate.release();
        bound(rig.database.confirmed(receipt, false)).await.unwrap();
        rig.settle().await;
        if stored(&rig, &[temp.as_str(), "c9"]) != vec!["c9".to_string()] { missed.push(format!("{source}: after the answer of the mutation the temporary row still stands")); }
        rig.database.close_now();
        missed
    }
}

/// P-DATA-23 (D-410): a server row with the key of an open create replaces the temporary row whatever brought the row.
#[tokio::test(flavor = "multi_thread")]
async fn server_row_from_any_source_replaces_the_temp_row_by_the_match_key() {
    let mut missed = Vec::new();
    for source in ["event row", "related row of an event", "window page row", "node answer row"] {
        let held = held_create().await;
        let rig = &held.rig;
        let epoch = rig.database.session_epoch();
        match source {
            "event row" => { rig.land_unsettled("Chat", server_chat()).await; }
            "related row of an event" => { rig.land_unsettled("Message", json!({"id": "m1", "text": "hi", "at": 1, "chat": server_chat()})).await; }
            "window page row" => { bound(rig.database.load(epoch, "Chat".into(), "list".into(), json!({"list": "SECONDARY"}).to_string())).await.unwrap(); }
            _ => { bound(rig.database.node(epoch, "Chat".into(), "node".into(), "c9".into(), true)).await.unwrap(); }
        }
        missed.extend(held.replaced_by(source).await);
    }
    assert!(missed.is_empty(), "{missed:#?}");
}

/// P-DATA-23 (D-410): a row that sat without the key replaces the temporary row when the key comes.
#[tokio::test(flavor = "multi_thread")]
async fn row_that_gets_its_match_key_later_replaces_the_temp_row() {
    let held = held_create().await;
    held.rig.land_unsettled("Chat", json!({"__typename": "Chat", "id": "c9", "name": "n", "lists": ["SECONDARY"]})).await;
    settled(&held.rig, "the row without the key stands beside the temporary row", || stored(&held.rig, &[held.temp.as_str(), "c9"]).len() == 2).await;
    held.rig.land_unsettled("Chat", server_chat()).await;
    let Held { rig, temp, .. } = &held;
    settled(rig, "the key reached the row", || keyed_in_store(rig)).await;
    assert_eq!(stored(rig, &[temp.as_str(), "c9"]), vec!["c9".to_string()], "the row with the key replaced the temporary row");
    held.rig.database.close_now();
}
