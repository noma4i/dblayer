use std::sync::Arc;

use super::bridge_support_tests::message;
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use super::flow_fixtures::{add_chat, cell, chat_row, int, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::test_wait::settled;

fn chat_cell(rig: &Rig, rows: &Rows, field: &str) -> Option<Cell> {
    let rows = rows.0.lock().unwrap();
    rows.iter().find(|row| row.id == "c1").map(|row| cell(rig, "Chat", row, field))
}

async fn rig_with_chat() -> (Rig, Arc<Rows>) {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the chat list shows c1", || chat_cell(&rig, &rows, "name").is_some()).await;
    (rig, rows)
}

async fn send(rig: &Rig, at: i64) -> crate::ffi::WireOperationReceipt {
    let epoch = rig.database.session_epoch();
    bound(rig.database.send(message(epoch, &format!("t{at}"), at))).await.unwrap()
}

/// P-APP-01, path Message.create and the answer: the chat row points at the new message at the revision of the receipt, the answer swaps the temporary id for the server id.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_last_message() {
    let (rig, rows) = rig_with_chat().await;
    let hold = rig.world.hold_next("SendMessage");

    let receipt = send(&rig, 100).await;
    hold.entered().await;

    settled(&rig, "the chat shows the new message", || chat_cell(&rig, &rows, "lastMessageAt") == Some(int(100))).await;
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    assert_eq!(chat_cell(&rig, &rows, "lastMessageId"), Some(text(&temporary)));
    let first = rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == "c1" && cell(&rig, "Chat", row, "lastMessageAt") == int(100))).cloned().unwrap();
    assert!(first.revision >= receipt.revision, "the chat row changed before the receipt: {}", first.revision);
    hold.release();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    settled(&rig, "the chat points at the message of the server", || chat_cell(&rig, &rows, "lastMessageId") == Some(text("m1"))).await;
    assert_eq!(chat_cell(&rig, &rows, "lastMessageAt"), Some(int(100)));
    rig.database.close_now();
}
