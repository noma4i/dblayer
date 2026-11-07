use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig};
use super::rig_held::Held;
use super::flow_fixtures::{one_revision, add_chat, add_profile, cell, int, land_event, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

async fn rig() -> Rig {
    Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_profile(models); }).await
}

fn field(rig: &Rig, rows: &Rows, id: &str, name: &str) -> Option<Cell> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).map(|row| cell(rig, "User", row, name))
}

fn first_seen_at(rig: &Rig, rows: &Rows, id: &str, name: &str, value: Cell) -> i64 {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == id && cell(rig, "User", row, name) == value)).map(|seen| seen.revision).unwrap_or(-1)
}

/// P-APP-21: a new name of the account shows in the profile, the entity and the user list at the revision of the receipt, the answer and its refetch keep it, an event changes it, a refusal brings the old name back.
#[tokio::test(flavor = "multi_thread")]
async fn flow_current_user_profile() {
    let rig = rig().await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    held.lists_show("A").await;
    let epoch = rig.database.session_epoch();
    let places = [&held.single, &held.entity, &held.rows];

    let refused = rig.world.hold_then("ProfileUpdate", Fault::Business);
    let receipt = bound(rig.database.act(epoch, "User".into(), "user".into(), "update".into(), json!({"name": "Draft"}).to_string(), json!({"name": "Draft"}).to_string())).await.unwrap();
    refused.entered_for("the refusal").await;
    yield_until("every place shows the draft", || places.iter().all(|rows| field(&rig, rows, "user", "name") == Some(text("Draft")))).await;
    refused.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    yield_until("the refusal brings the old name back everywhere", || places.iter().all(|rows| field(&rig, rows, "user", "name") == Some(text("A")))).await;

    let hold = rig.world.hold_next("ProfileUpdate");
    let receipt = bound(rig.database.act(epoch, "User".into(), "user".into(), "update".into(), json!({"name": "New"}).to_string(), json!({"name": "New"}).to_string())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("every place shows the new name", || places.iter().all(|rows| field(&rig, rows, "user", "name") == Some(text("New")))).await;
    let revisions: Vec<_> = places.iter().map(|rows| first_seen_at(&rig, rows, "user", "name", text("New"))).collect();
    assert!(revisions.iter().all(|revision| *revision >= receipt.revision), "a place showed the name before the receipt or never {revisions:?}, receipt {}", receipt.revision);
    one_revision(&places.map(|rows| &**rows)).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the refetch of the profile keeps the name", || rig.world.count("MyProfile") >= 2).await;
    rig.settle().await;
    assert!(places.iter().all(|rows| field(&rig, rows, "user", "name") == Some(text("New"))));

    land_event(&rig, "User", json!({"id": "user", "name": "Event"}));
    yield_until("an event changes the name everywhere", || places.iter().all(|rows| field(&rig, rows, "user", "name") == Some(text("Event")))).await;
    rig.database.close_now();
}

/// P-APP-19 and P-APP-05: accepting a request shows the user as connected at the revision of the receipt, the answer lands the chat as connected and the refetch of the profile lowers the counters, and a counters event sets them again.
#[tokio::test(flavor = "multi_thread")]
async fn flow_connection_accept() {
    let rig = rig().await;
    rig.world.set_profile(json!({"secondaryChatsCount": 3, "unreadSecondaryChatsCount": 2}));
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    land_event(&rig, "User", json!({"id": "u5", "name": "Eve", "connectionStatus": "PENDING"}));
    land_event(&rig, "Chat", json!({"id": "c1", "name": "Eve", "connectionStatus": "PENDING"}));
    let epoch = rig.database.session_epoch();
    let counters = |rows: &Rows| (field(&rig, rows, "user", "secondaryChatsCount"), field(&rig, rows, "user", "unreadSecondaryChatsCount"));
    yield_until("the counters show", || counters(&held.single) == (Some(int(3)), Some(int(2)))).await;
    let people = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"User"}"#, people.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let chats = Arc::new(Rows::default());
    let chat_stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, chats.clone());
    bound(chat_stream.wait_for_initial_load()).await.unwrap();
    yield_until("the list and the chat show", || field(&rig, &people, "u5", "connectionStatus") == Some(text("PENDING")) && chats.len() == 1).await;
    let life = rig.database.life_handle();
    assert!(bound(tokio::task::spawn_blocking(move || life.barrier(std::time::Duration::from_secs(5)))).await.unwrap(), "the loads of the windows ended before the act");
    let hold = rig.world.hold_next("AnswerConnection");

    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "answer_connection".into(), json!({"connectionStatus": "CONNECTED"}).to_string(), json!({"chatId": "c1"}).to_string())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("the user shows as connected", || field(&rig, &people, "u5", "connectionStatus") == Some(text("CONNECTED"))).await;
    assert!(first_seen_at(&rig, &people, "u5", "connectionStatus", text("CONNECTED")) >= receipt.revision, "the user showed the status before the receipt or never");
    rig.world.set_profile(json!({"secondaryChatsCount": 2, "unreadSecondaryChatsCount": 1}));
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the chat and the counters follow the answer", || {
        chats.0.lock().unwrap().iter().any(|row| row.id == "c1" && cell(&rig, "Chat", row, "connectionStatus") == text("CONNECTED"))
            && [&held.single, &held.entity, &held.rows].iter().all(|rows| counters(rows) == (Some(int(2)), Some(int(1))))
    }).await;
    assert_eq!(field(&rig, &people, "u5", "connectionStatus"), Some(text("CONNECTED")));

    land_event(&rig, "User", json!({"id": "user", "secondaryChatsCount": 0, "unreadSecondaryChatsCount": 0}));
    yield_until("a counters event sets every place", || [&held.single, &held.entity, &held.rows].iter().all(|rows| counters(rows) == (Some(int(0)), Some(int(0))))).await;
    rig.database.close_now();
}

/// P-APP-20: a block changes the blocked list only after the answer, an unblock takes the user out after its answer, a refused block changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn flow_user_block() {
    let rig = rig().await;
    rig.login_ios("A").await;
    land_event(&rig, "User", json!({"id": "u5", "name": "Eve"}));
    let epoch = rig.database.session_epoch();
    let blocked = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"User","relation":"blocked","params":{}}"#, blocked.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    assert_eq!(blocked.len(), 0);

    let refused = rig.world.hold_then("BlockUser", Fault::Business);
    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "block".into(), "{}".into(), "{}".into())).await.unwrap();
    refused.entered_for("the refusal").await;
    assert_eq!(blocked.len(), 0, "the blocked list changed before the answer");
    refused.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
    rig.settle().await;
    assert_eq!(blocked.len(), 0, "a refused block changed the list");

    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "block".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the blocked list shows the user", || blocked.len() == 1).await;

    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "unblock".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the blocked list is empty again", || blocked.len() == 0).await;
    rig.database.close_now();
}

/// P-APP-05: the three counters of the requests show one value in the banner, the tabs and the feed after an answer with a refetch of the profile and after a counters event.
#[tokio::test(flavor = "multi_thread")]
async fn flow_pending_counters() {
    let rig = rig().await;
    rig.world.set_profile(json!({"secondaryChatsCount": 4, "premiumSecondaryChatsCount": 1, "unreadSecondaryChatsCount": 3}));
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows("A").await;
    land_event(&rig, "User", json!({"id": "u5", "name": "Eve", "connectionStatus": "PENDING"}));
    land_event(&rig, "Chat", json!({"id": "c1", "name": "Eve", "connectionStatus": "PENDING"}));
    let epoch = rig.database.session_epoch();
    let counters = |rows: &Rows| ["secondaryChatsCount", "premiumSecondaryChatsCount", "unreadSecondaryChatsCount"].map(|name| field(&rig, rows, "user", name));
    let places = [&held.single, &held.entity, &held.rows];
    let all = |values: [i64; 3]| places.iter().all(|rows| counters(rows) == values.map(|value| Some(int(value))));
    yield_until("every place shows the profile counters", || all([4, 1, 3])).await;

    rig.world.set_profile(json!({"secondaryChatsCount": 3, "premiumSecondaryChatsCount": 1, "unreadSecondaryChatsCount": 2}));
    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "answer_connection".into(), json!({"connectionStatus": "CONNECTED"}).to_string(), json!({"chatId": "c1"}).to_string())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the refetch of the profile sets every place to one value", || all([3, 1, 2])).await;

    land_event(&rig, "User", json!({"id": "user", "secondaryChatsCount": 0, "premiumSecondaryChatsCount": 0, "unreadSecondaryChatsCount": 0}));
    yield_until("a counters event sets every place to one value", || all([0, 0, 0])).await;
    rig.database.close_now();
}
