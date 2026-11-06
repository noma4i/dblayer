use std::sync::Arc;

use serde_json::{json, Value};

use super::bridge_tests::message;
use super::rig_tests::{bound, Fault, Rig, World};
use super::v3_fixtures::{add_chat, cell, chat_row, int, land_event};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{WireOperationOutcome, WireOperationReceipt};
use crate::test_wait::yield_until;

const ALL: &str = r#"{"model":"Chat","relation":"all","params":{}}"#;
const BY_ACTIVITY: &str = r#"{"model":"Chat","relation":"all","params":{},"order":[{"field":"lastActivityAt","direction":"desc"}]}"#;

async fn rig_with_chats(chats: &[Value]) -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, add_chat).await;
    for chat in chats { land_event(&rig, "Chat", chat.clone()); }
    rig
}

async fn watch(rig: &Rig, query: &str, shown: usize) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    yield_until("the list shows its rows", || rows.len() == shown).await;
    rows
}

fn field(rig: &Rig, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, "Chat", row, name))
}

fn order(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

fn first_seen_at(rig: &Rig, rows: &Rows, id: &str, name: &str, value: Cell) -> i64 {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == id && cell(rig, "Chat", row, name) == value)).map(|seen| seen.revision).unwrap_or(-1)
}

async fn act(rig: &Rig, action: &str, patch: Value, args: Value) -> WireOperationReceipt {
    let epoch = rig.database.session_epoch();
    bound(rig.database.act(epoch, "Chat".into(), "c1".into(), action.into(), patch.to_string(), args.to_string())).await.unwrap()
}

/// P-APP-06: pin and mute change the row, the pinned list and the muted flag at the revision of the receipt, the answer keeps them, an event of the server changes them back, a refusal rolls them back.
#[tokio::test(flavor = "multi_thread")]
async fn v3_chat_pin_mute_changes_every_place_and_a_refusal_rolls_all_back() {
    for (action, name) in [("pin", "pinned"), ("mute", "muted")] {
        let query = format!(r#"{{"model":"Chat","relation":"all","params":{{}},"where":{{"type":"equal","field":"{name}","value":true}}}}"#);
        let rig = rig_with_chats(&[chat_row("c1", 10)]).await;
        let all = watch(&rig, ALL, 1).await;
        let marked = watch(&rig, &query, 0).await;
        let on = Cell::Bool { value: true };

        let hold = rig.world.hold_next(if action == "pin" { "ChatPin" } else { "ChatMute" });
        let receipt = act(&rig, action, json!({ name: true }), json!({ name: true })).await;
        hold.entered_for("the answer").await;
        yield_until("the marked list shows the chat", || marked.len() == 1).await;
        assert_eq!(field(&rig, &all, "c1", name), Some(on.clone()), "{action}: the row of the list");
        assert_eq!(first_seen_at(&rig, &marked, "c1", name, on.clone()), receipt.revision, "{action}: the marked list changed at another revision than the receipt");
        hold.release();
        assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }), "{action}");
        rig.settle().await;
        assert_eq!((marked.len(), field(&rig, &all, "c1", name)), (1, Some(on.clone())), "{action}: the answer kept the flag");

        land_event(&rig, "Chat", json!({"id": "c1", name: false}));
        yield_until("the event clears the flag", || marked.len() == 0).await;
        assert_eq!(field(&rig, &all, "c1", name), Some(Cell::Bool { value: false }), "{action}: the event");

        let refused = rig.world.hold_then(if action == "pin" { "ChatPin" } else { "ChatMute" }, Fault::Business);
        let receipt = act(&rig, action, json!({ name: true }), json!({ name: true })).await;
        refused.entered_for("the refusal").await;
        yield_until("the refused change shows first", || marked.len() == 1).await;
        refused.release();
        assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }), "{action}");
        yield_until("the refusal rolls the flag back everywhere", || marked.len() == 0 && field(&rig, &all, "c1", name) == Some(Cell::Bool { value: false })).await;
        rig.database.close_now();
    }
}

/// P-APP-04: mark_read zeroes the badge of the row at the revision of the receipt, the answer keeps it, a counters event of the server sets a new value.
#[tokio::test(flavor = "multi_thread")]
async fn v3_chat_unread_counters_zero_the_badge_at_once_and_follow_events() {
    let mut chat = chat_row("c1", 10);
    chat["unreadCount"] = json!(5);
    let rig = rig_with_chats(&[chat]).await;
    let all = watch(&rig, ALL, 1).await;
    assert_eq!(field(&rig, &all, "c1", "unreadCount"), Some(int(5)));

    let hold = rig.world.hold_next("ChatMarkRead");
    let receipt = act(&rig, "mark_read", json!({"unreadCount": 0}), json!({})).await;
    hold.entered_for("the answer").await;
    yield_until("the badge is zero", || field(&rig, &all, "c1", "unreadCount") == Some(int(0))).await;
    assert_eq!(first_seen_at(&rig, &all, "c1", "unreadCount", int(0)), receipt.revision, "the badge changed at another revision than the receipt");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    rig.settle().await;
    assert_eq!(field(&rig, &all, "c1", "unreadCount"), Some(int(0)));

    land_event(&rig, "Chat", json!({"id": "c1", "unreadCount": 3}));
    yield_until("the counters event sets the badge", || field(&rig, &all, "c1", "unreadCount") == Some(int(3))).await;
    rig.database.close_now();
}

/// P-APP-02: a new message lifts its chat to the head of the list at the revision of the receipt, an answer keeps the order, a message event of another chat lifts that chat.
#[tokio::test(flavor = "multi_thread")]
async fn v3_chat_list_order_lifts_the_chat_of_a_new_message_without_a_reload() {
    let rig = rig_with_chats(&[chat_row("c1", 10), chat_row("c2", 20)]).await;
    let rows = watch(&rig, BY_ACTIVITY, 2).await;
    assert_eq!(order(&rows), ["c2", "c1"]);
    let hold = rig.world.hold_next("SendMessage");
    let epoch = rig.database.session_epoch();

    let receipt = bound(rig.database.send(message(epoch, "up", 30))).await.unwrap();
    hold.entered_for("the send").await;
    yield_until("c1 leads the list", || order(&rows) == ["c1", "c2"]).await;
    let lifted = rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == "c1") && seen.revision >= receipt.revision).map(|seen| seen.revision);
    assert_eq!(lifted, Some(receipt.revision), "the list changed at another revision than the receipt");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    rig.settle().await;
    assert_eq!(order(&rows), ["c1", "c2"]);

    land_event(&rig, "Message", json!({"id": "m9", "chatId": "c2", "text": "event", "at": 40}));
    yield_until("c2 leads the list again", || order(&rows) == ["c2", "c1"]).await;
    rig.database.close_now();
}
