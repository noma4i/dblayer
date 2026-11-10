use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::message;
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig, World};
use super::flow_fixtures::{one_revision, add_chat, cell, chat_row, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{ReadFetch, ReadRequest, WireOperationReceipt};
use crate::test_wait::settled;

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"},"order":[{"field":"at","direction":"asc"}]}"#;
const CHATS: &str = r#"{"model":"Chat","relation":"all","params":{}}"#;

async fn rig_with_messages(messages: &[(&str, &str, i64)]) -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    for (id, body, at) in messages {
        rig.world.seed_message("A", id, body, *at);
        rig.land("Message", json!({"id": id, "chatId": "c1", "text": body, "at": at})).await;
    }
    rig
}

async fn watch(rig: &Rig, query: &str, shown: usize) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the list shows its rows", || rows.len() == shown).await;
    rows
}

fn entity(rig: &Rig, id: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: "Message".into(), id: id.into(), fetch: ReadFetch::Never }, rows.clone()));
    rows
}

fn field(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, model, row, name))
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

fn first_seen_at(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str, value: Cell) -> i64 {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == id && cell(rig, model, row, name) == value)).map(|seen| seen.revision).unwrap_or(-1)
}

async fn confirmed(rig: &Rig, receipt: WireOperationReceipt) {
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    rig.settle().await;
}

/// P-APP-11: an edit changes the text in the thread and in the entity of the last message at the revision of the receipt, the answer and an event of the server keep it.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_edit() {
    let rig = rig_with_messages(&[("m1", "hello", 1)]).await;
    let thread = watch(&rig, THREAD, 1).await;
    let last = entity(&rig, "m1");
    settled(&rig, "the entity shows m1", || last.len() == 1).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("MessageEdit");

    let receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "update".into(), json!({"text": "edited"}).to_string(), json!({"text": "edited"}).to_string())).await.unwrap();
    hold.entered().await;
    settled(&rig, "both places show the edit", || field(&rig, "Message", &thread, "m1", "text") == Some(text("edited")) && field(&rig, "Message", &last, "m1", "text") == Some(text("edited"))).await;
    for rows in [&thread, &last] {
        assert!(first_seen_at(&rig, "Message", rows, "m1", "text", text("edited")) >= receipt.revision, "a place showed the text before the receipt or never");
    }
    hold.release();
    confirmed(&rig, receipt).await;
    for rows in [&thread, &last] { assert_eq!(field(&rig, "Message", rows, "m1", "text"), Some(text("edited"))); }

    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "from event", "at": 1})).await;
    settled(&rig, "both places show the event", || field(&rig, "Message", &thread, "m1", "text") == Some(text("from event")) && field(&rig, "Message", &last, "m1", "text") == Some(text("from event"))).await;
    rig.database.close_now();
}

/// P-APP-12: a delete takes the message out of the thread and moves the pointer of the chat to the previous message at the revision of the receipt, a refusal brings both back, an event of the server deletes it the same way.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_delete() {
    let rig = rig_with_messages(&[("m1", "first", 1), ("m2", "second", 2)]).await;
    let thread = watch(&rig, THREAD, 2).await;
    let chats = watch(&rig, CHATS, 1).await;
    settled(&rig, "the chat points at m2", || field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(text("m2"))).await;
    let epoch = rig.database.session_epoch();

    let refused = rig.world.hold_then("MessageDelete", Fault::Business);
    let receipt = bound(rig.database.destroy(epoch, "Message".into(), "m2".into())).await.unwrap();
    refused.entered().await;
    settled(&rig, "the thread and the chat row moved", || ids(&thread) == ["m1"] && field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(text("m1"))).await;
    refused.release();
    assert!(matches!(bound(rig.database.confirmed(receipt, false)).await, Err(crate::ffi::DbError::Rejected { kept: false, .. })));
    settled(&rig, "the refusal brings the message and the pointer back", || ids(&thread) == ["m1", "m2"] && field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(text("m2"))).await;

    let hold = rig.world.hold_next("MessageDelete");
    let receipt = bound(rig.database.destroy(epoch, "Message".into(), "m2".into())).await.unwrap();
    hold.entered().await;
    settled(&rig, "the delete shows", || ids(&thread) == ["m1"] && field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(text("m1"))).await;
    let revision = chats.1.lock().unwrap().iter().rev().find(|seen| seen.rows.iter().any(|row| cell(&rig, "Chat", row, "lastMessageId") == text("m1"))).map(|seen| seen.revision);
    assert!(revision.is_some_and(|revision| revision >= receipt.revision), "the chat row changed before the receipt: {revision:?}");
    assert!(thread.1.lock().unwrap().iter().any(|seen| seen.revision >= receipt.revision && seen.removed.iter().any(|id| id == "m2")), "the thread did not drop m2 at or after the receipt");
    one_revision(&rig, &[&thread, &chats]).await;
    hold.release();
    confirmed(&rig, receipt).await;
    assert_eq!(ids(&thread), ["m1"]);

    rig.land_removed("Message", "m1").await;
    settled(&rig, "the event empties the thread and the pointer", || ids(&thread).is_empty() && field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(Cell::Null)).await;
    rig.database.close_now();
}

/// P-APP-13: a sent message is one bubble before and after the answer, never two. An event of the server with the status changes the bubble.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_status() {
    let rig = rig_with_messages(&[]).await;
    let thread = watch(&rig, THREAD, 0).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("SendMessage");

    let receipt = bound(rig.database.send(message(epoch, "hi", 10))).await.unwrap();
    hold.entered().await;
    settled(&rig, "the bubble shows", || thread.len() == 1).await;
    let temporary = ids(&thread).remove(0);
    assert_eq!(temporary, receipt.optimistic_value.clone().unwrap().id);
    hold.release();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    settled(&rig, "the bubble has the id of the server", || ids(&thread) == ["m1"]).await;
    let doubled = thread.1.lock().unwrap().iter().any(|seen| seen.size > 1);
    assert!(!doubled, "the thread showed two bubbles for one message");

    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hi", "at": 10, "status": "READ"})).await;
    settled(&rig, "the status shows", || field(&rig, "Message", &thread, "m1", "status") == Some(text("READ"))).await;
    assert_eq!(ids(&thread), ["m1"]);
    rig.database.close_now();
}
