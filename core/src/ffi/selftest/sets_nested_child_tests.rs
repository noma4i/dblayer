use serde_json::json;

use super::flow_fixtures::{add_chat, cell, int, text};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::wire::Cell;

async fn chat_pointer(rig: &Rig) -> (Cell, Cell, Cell) {
    let epoch = rig.database.session_epoch();
    let rows = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Chat"}"#))).await.unwrap();
    let row = rows.iter().find(|row| row.id == "c1").expect("the chat is stored");
    (cell(rig, "Chat", row, "lastMessageId"), cell(rig, "Chat", row, "lastMessageAt"), cell(rig, "Chat", row, "lastSequenceNumber"))
}

async fn message_ids(rig: &Rig) -> Vec<String> {
    let epoch = rig.database.session_epoch();
    let rows = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Message"}"#))).await.unwrap();
    let mut ids: Vec<String> = rows.into_iter().map(|row| row.id).collect();
    ids.sort();
    ids
}

fn parent_answer(nested: serde_json::Value) -> serde_json::Value {
    json!({"id": "c1", "name": "c1", "lastMessageId": "from-the-answer", "lastMessage": nested})
}

/// P-DATA-58: the object of a child inside the answer of its parent becomes the row of the child, the key of the answer does not set the pointer, and an object that cannot become a row writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn data_sets_nested_child_of_a_parent_answer_is_a_row() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", json!({"id": "c1", "name": "c1"})).await;

    rig.land("Chat", parent_answer(json!({"id": "m7"}))).await;
    assert_eq!(message_ids(&rig).await, Vec::<String>::new(), "an object with an id only is no row");
    assert_eq!(chat_pointer(&rig).await, (Cell::Null, Cell::Null, Cell::Null), "it moves no pointer");

    let child = |id: &str, at: i64, seq: i64| json!({"id": id, "chatId": "c1", "text": id, "at": at, "seq": seq});
    rig.land("Chat", parent_answer(child("m3", 30, 3))).await;
    assert_eq!(message_ids(&rig).await, ["m3"], "the child of the answer is a row");
    assert_eq!(chat_pointer(&rig).await, (text("m3"), int(30), int(3)), "the fields come from the child, not from the key of the answer");

    rig.land("Chat", parent_answer(child("m1", 10, 1))).await;
    assert_eq!(message_ids(&rig).await, ["m1", "m3"], "an older child of an old answer is a row too");
    assert_eq!(chat_pointer(&rig).await, (text("m3"), int(30), int(3)), "and does not move the pointer back");
    rig.database.close_now();
}
