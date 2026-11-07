use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig};
use super::flow_fixtures::{add_chat, add_profile, add_search, add_wallet, cell, chat_row, int, land_event, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

async fn watch(rig: &Rig, query: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rows
}

fn field(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, model, row, name))
}

fn first_seen_at(rig: &Rig, model: &str, rows: &Rows, id: &str, name: &str, value: Cell) -> i64 {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == id && cell(rig, model, row, name) == value)).map(|seen| seen.revision).unwrap_or(-1)
}

/// P-APP-25: a gift lowers the balance at the revision of the receipt, and its answer adds the transaction to the history and the gift message to the thread and the chat row.
#[tokio::test(flavor = "multi_thread")]
async fn flow_wallet_gift() {
    let rig = Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_profile(models); add_wallet(models); }).await;
    rig.world.set_profile(json!({"balance": 100}));
    rig.login_ios("A").await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    let profile = watch(&rig, r#"{"model":"User","relation":"profile","params":{}}"#).await;
    let history = watch(&rig, r#"{"model":"WalletTransaction","relation":"history","params":{}}"#).await;
    let thread = watch(&rig, r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#).await;
    let chats = watch(&rig, r#"{"model":"Chat","relation":"all","params":{}}"#).await;
    yield_until("the balance shows 100", || field(&rig, "User", &profile, "user", "balance") == Some(int(100))).await;
    let epoch = rig.database.session_epoch();
    let answer = json!({"data":{"sendGift":{"user":{"__typename":"User","id":"user","balance":90},"transaction":{"__typename":"WalletTransaction","id":"t1","amount":-10},"message":{"__typename":"Message","id":"g1","chatId":"c1","text":"gift","at":7}}}});
    let hold = rig.world.hold_then("SendGift", Fault::Answer(answer));

    let receipt = bound(rig.database.act(epoch, "User".into(), "user".into(), "send_gift".into(), json!({"balance": 90}).to_string(), json!({"chatId": "c1", "amount": 10}).to_string())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("the balance shows 90", || field(&rig, "User", &profile, "user", "balance") == Some(int(90))).await;
    assert!(first_seen_at(&rig, "User", &profile, "user", "balance", int(90)) >= receipt.revision);
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the history, the thread and the chat row follow the answer", || history.len() == 1 && thread.len() == 1 && field(&rig, "Chat", &chats, "c1", "lastMessageId") == Some(text("g1"))).await;
    assert_eq!(field(&rig, "User", &profile, "user", "balance"), Some(int(90)));
    rig.database.close_now();
}

/// P-APP-27: the row of a chat in the search is the row of the inbox: a local change and an event reach both lists at one revision.
#[tokio::test(flavor = "multi_thread")]
async fn flow_search_follows_rows() {
    let rig = Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_search(models); }).await;
    rig.login_ios("A").await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    rig.world.answer_with("ChatSearch", json!({"data":{"chatSearch":[{"__typename":"Chat","id":"c1","name":"c1","pinned":false}]}}));
    let epoch = rig.database.session_epoch();
    let params = json!({"query": "c"});
    let inbox = watch(&rig, r#"{"model":"Chat","relation":"all","params":{}}"#).await;
    let found = watch(&rig, &json!({"model":"Chat","relation":"search","params":params}).to_string()).await;
    bound(rig.database.refresh(epoch, "Chat".into(), Some("search".into()), params.to_string())).await.unwrap();
    yield_until("the search finds c1", || found.len() == 1 && inbox.len() == 1).await;

    land_event(&rig, "Chat", json!({"id": "c1", "name": "renamed", "lastMessageAt": 0, "lastActivityAt": 0, "unreadCount": 0, "pinned": true, "muted": false}));
    yield_until("both lists show the event", || [&inbox, &found].iter().all(|rows| field(&rig, "Chat", rows, "c1", "name") == Some(text("renamed")))).await;
    let revisions: Vec<_> = [&inbox, &found].iter().map(|rows| first_seen_at(&rig, "Chat", rows, "c1", "name", text("renamed"))).collect();
    assert_eq!(revisions[0], revisions[1], "the lists changed at different revisions");

    let hold = rig.world.hold_next("ChatPin");
    let receipt = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "pin".into(), json!({"pinned": false}).to_string(), json!({"pinned": false}).to_string())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("both lists show the local change", || [&inbox, &found].iter().all(|rows| field(&rig, "Chat", rows, "c1", "pinned") == Some(Cell::Bool { value: false }))).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    rig.database.close_now();
}
