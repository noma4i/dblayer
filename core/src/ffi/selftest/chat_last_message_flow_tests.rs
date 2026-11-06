use std::sync::Arc;

use serde_json::json;

use super::bridge_tests::message;
use super::rig_tests::{bound, Rig, World};
use super::flow_fixtures::{add_chat, cell, chat_row, int, land_event, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

fn chat_cell(rig: &Rig, rows: &Rows, field: &str) -> Option<Cell> {
    let rows = rows.0.lock().unwrap();
    rows.iter().find(|row| row.id == "c1").map(|row| cell(rig, "Chat", row, field))
}

async fn rig_with_chat() -> (Rig, Arc<Rows>) {
    let rig = Rig::signed_as(World::new("server", "A", true), false, add_chat).await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    yield_until("the chat list shows c1", || chat_cell(&rig, &rows, "name").is_some()).await;
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
    hold.entered_for("the send").await;

    yield_until("the chat shows the new message", || chat_cell(&rig, &rows, "lastMessageAt") == Some(int(100))).await;
    let temporary = receipt.optimistic_value.clone().unwrap().id;
    assert_eq!(chat_cell(&rig, &rows, "lastMessageId"), Some(text(&temporary)));
    assert_eq!(chat_cell(&rig, &rows, "lastActivityAt"), Some(int(100)));
    let first = rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == "c1" && cell(&rig, "Chat", row, "lastMessageAt") == int(100))).cloned().unwrap();
    assert_eq!(first.revision, receipt.revision, "the chat row did not change at the revision of the receipt");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the chat points at the message of the server", || chat_cell(&rig, &rows, "lastMessageId") == Some(text("m1"))).await;
    assert_eq!(chat_cell(&rig, &rows, "lastMessageAt"), Some(int(100)));
    rig.database.close_now();
}

/// P-APP-01, guard `newer:`: a message event older than the last one leaves the chat row alone in every delivery.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_last_message_ignores_an_older_message_event() {
    let (rig, rows) = rig_with_chat().await;
    let receipt = send(&rig, 100).await;
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the chat points at m1", || chat_cell(&rig, &rows, "lastMessageId") == Some(text("m1"))).await;
    let mark = rows.1.lock().unwrap().len();

    land_event(&rig, "Message", json!({"id":"m7","chatId":"c1","text":"late","at":60}));
    rig.settle().await;

    assert_eq!(chat_cell(&rig, &rows, "lastMessageId"), Some(text("m1")));
    let rolled_back = rows.1.lock().unwrap().iter().skip(mark).any(|seen| seen.rows.iter().any(|row| row.id == "c1" && (cell(&rig, "Chat", row, "lastMessageAt") != int(100) || cell(&rig, "Chat", row, "lastMessageId") != text("m1"))));
    assert!(!rolled_back, "an older message moved the pointer of the chat");
    rig.database.close_now();
}

/// P-APP-01, path event: a newer message that arrives as an event moves the chat row.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_last_message_follows_a_message_event() {
    let (rig, rows) = rig_with_chat().await;

    land_event(&rig, "Message", json!({"id":"m9","chatId":"c1","text":"event","at":200}));

    yield_until("the chat shows the event message", || chat_cell(&rig, &rows, "lastMessageId") == Some(text("m9"))).await;
    assert_eq!(chat_cell(&rig, &rows, "lastMessageAt"), Some(int(200)));
    assert_eq!(chat_cell(&rig, &rows, "lastActivityAt"), Some(int(200)));
    rig.database.close_now();
}
