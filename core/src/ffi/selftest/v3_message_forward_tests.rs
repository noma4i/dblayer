use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig, World};
use super::v3_fixtures::{add_chat, cell, chat_row, land_event, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{WireOperationOutcome, WireOperationReceipt};
use crate::test_wait::yield_until;

const TARGET_THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c2"}}"#;
const CHATS: &str = r#"{"model":"Chat","relation":"all","params":{}}"#;

async fn rig_with_source() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, add_chat).await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    for chat in ["c1", "c2"] { land_event(&rig, "Chat", chat_row(chat, 0)); }
    rig.world.seed_authored_message("A", "m1", "hello", 5, "u7");
    land_event(&rig, "Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 5, "authorId": "u7"}));
    rig
}

async fn watch(rig: &Rig, query: &str, shown: usize) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    yield_until("the list shows its rows", || rows.len() == shown).await;
    rows
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

fn field(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, model, row, name))
}

async fn forward(rig: &Rig) -> WireOperationReceipt {
    let epoch = rig.database.session_epoch();
    bound(rig.database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c2"}).to_string())).await.unwrap()
}

fn forwarded_by_me(rig: &Rig, rows: &Rows, id: &str) -> bool {
    field(rig, "Message", rows, id, "authorId") == Some(text("user"))
        && field(rig, "Message", rows, id, "isForwarded") == Some(Cell::Bool { value: true })
        && field(rig, "Message", rows, id, "forwardedFromUserId") == Some(text("u7"))
}

/// P-APP-16: the copy of a forward stands in the target thread at the revision of the receipt with the current user as author and the author of the source as origin, the target chat points at it, the answer replaces it without a second bubble, a refusal removes it and the pointer.
#[tokio::test(flavor = "multi_thread")]
async fn v3_message_forward_shows_the_copy_at_once_and_the_answer_replaces_it() {
    let rig = rig_with_source().await;
    let thread = watch(&rig, TARGET_THREAD, 0).await;
    let chats = watch(&rig, CHATS, 2).await;

    let refused = rig.world.hold_then("MessageForward", Fault::Refuse { code: "FORBIDDEN", confirmed: true });
    let receipt = forward(&rig).await;
    refused.entered_for("the refusal").await;
    yield_until("the copy stands in the thread", || thread.len() == 1).await;
    let copy = ids(&thread).remove(0);
    assert!(forwarded_by_me(&rig, &thread, &copy), "the copy lacks the author, the flag or the origin");
    assert_eq!(field(&rig, "Chat", &chats, "c2", "lastMessageId"), Some(text(&copy)), "the target chat does not point at the copy");
    assert_eq!(thread.1.lock().unwrap().iter().find(|seen| !seen.rows.is_empty()).map(|seen| seen.revision), Some(receipt.revision), "the copy came at another revision than the receipt");
    refused.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    yield_until("the refusal removes the copy and the pointer", || thread.len() == 0 && field(&rig, "Chat", &chats, "c2", "lastMessageId") == Some(Cell::Null)).await;

    let hold = rig.world.hold_next("MessageForward");
    let receipt = forward(&rig).await;
    hold.entered_for("the answer").await;
    yield_until("the copy stands again", || thread.len() == 1).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the answer replaced the copy", || ids(&thread) == ["forwarded-1"]).await;
    assert!(thread.1.lock().unwrap().iter().all(|seen| seen.size <= 1), "the thread showed the copy and the answer together");
    assert!(forwarded_by_me(&rig, &thread, "forwarded-1"));
    assert_eq!(field(&rig, "Chat", &chats, "c2", "lastMessageId"), Some(text("forwarded-1")));

    land_event(&rig, "Message", json!({"id": "f2", "chatId": "c2", "text": "again", "at": 9, "authorId": "user", "isForwarded": true, "forwardedFromUserId": "u7"}));
    yield_until("the event bubble stands in the thread", || ids(&thread).len() == 2 && forwarded_by_me(&rig, &thread, "f2")).await;
    rig.database.close_now();
}
