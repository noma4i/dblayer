use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Rig};
use super::flow_fixtures::{one_revision, add_chat, add_lists, add_profile, cell, land_event, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

const PRIMARY: &str = r#"{"model":"Chat","relation":"list","params":{"list":"PRIMARY"}}"#;
const SECONDARY: &str = r#"{"model":"Chat","relation":"list","params":{"list":"SECONDARY"}}"#;

async fn rig() -> Rig {
    let rig = Rig::encrypted_edited(false, false, false, |models| { add_chat(models); add_profile(models); add_lists(models); }).await;
    rig.world.answer_with("ChatList", json!({"data":{"chats":[]}}));
    rig.login_ios("A").await;
    rig
}

async fn watch(rig: &Rig, query: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rows
}

fn names(rig: &Rig, rows: &Rows) -> Vec<(String, Cell)> { rows.0.lock().unwrap().iter().map(|row| (row.id.clone(), cell(rig, "Chat", row, "name"))).collect() }

/// P-APP-03: a chat that is not in the cache appears in its list by an event with the chat and by the answer of a request that lands it, without a reread of the list.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_appears_from_event() {
    let rig = rig().await;
    let (primary, secondary) = (watch(&rig, PRIMARY).await, watch(&rig, SECONDARY).await);
    let epoch = rig.database.session_epoch();
    let reads = rig.world.count("ChatList");

    land_event(&rig, "Chat", json!({"id": "c1", "name": "event", "lists": ["PRIMARY"]}));
    yield_until("the event chat stands in PRIMARY", || primary.len() == 1 && secondary.emptied()).await;

    rig.world.answer_with("AnswerConnection", json!({"data":{"answerConnection":{"user":{"__typename":"User","id":"u5","connectionStatus":"PENDING"},"chat":{"__typename":"Chat","id":"c2","connectionStatus":"PENDING","name":"answer","lists":["SECONDARY"]}}}}));
    land_event(&rig, "User", json!({"id": "u5", "name": "Eve", "connectionStatus": "PENDING"}));
    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "answer_connection".into(), "{}".into(), json!({"chatId": "c2"}).to_string())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the chat of the answer stands in SECONDARY", || secondary.len() == 1).await;

    land_event(&rig, "Chat", json!({"id": "c2", "name": "answer", "lists": ["PRIMARY"]}));
    yield_until("the accepted chat moves to PRIMARY", || primary.len() == 2 && secondary.emptied()).await;
    assert_eq!(rig.world.count("ChatList"), reads, "the app reread a list");
    rig.database.close_now();
}

/// P-APP-09: the forward picker reads the PRIMARY list: it holds the same chats with the same data as the inbox, also after a rename.
#[tokio::test(flavor = "multi_thread")]
async fn flow_forward_picker() {
    let rig = rig().await;
    let (inbox, picker) = (watch(&rig, PRIMARY).await, watch(&rig, PRIMARY).await);
    land_event(&rig, "Chat", json!({"id": "c1", "name": "one", "lists": ["PRIMARY"]}));
    land_event(&rig, "Chat", json!({"id": "c2", "name": "two", "lists": ["PRIMARY"]}));
    yield_until("both lists hold two chats", || inbox.len() == 2 && picker.len() == 2).await;
    land_event(&rig, "Chat", json!({"id": "c2", "name": "renamed", "lists": ["PRIMARY"]}));
    yield_until("both lists show the rename", || [&inbox, &picker].iter().all(|rows| names(&rig, rows).contains(&("c2".to_string(), text("renamed"))))).await;
    assert_eq!(names(&rig, &inbox), names(&rig, &picker));
    rig.database.close_now();
}

/// P-APP-08: a new name of a group shows in the list and in the entity of the chat at the revision of the receipt, the answer keeps it, and an event changes it.
#[tokio::test(flavor = "multi_thread")]
async fn flow_group_name_logo() {
    let rig = rig().await;
    let list = watch(&rig, PRIMARY).await;
    let entity = Arc::new(Rows::default());
    drop(rig.database.watch(crate::ffi::ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: crate::ffi::ReadFetch::Never }, entity.clone()));
    land_event(&rig, "Chat", json!({"id": "c1", "name": "old", "lists": ["PRIMARY"]}));
    yield_until("both places show the old name", || list.len() == 1 && entity.len() == 1).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("ChatUpdate");

    let receipt = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "update".into(), json!({"name": "new"}).to_string(), json!({"name": "new"}).to_string())).await.unwrap();
    hold.entered_for("the answer").await;
    yield_until("both places show the new name", || [&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("new"))])).await;
    let revisions: Vec<_> = [&list, &entity].iter().map(|rows| rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| cell(&rig, "Chat", row, "name") == text("new"))).map(|seen| seen.revision)).collect();
    assert!(revisions.iter().all(|revision| revision.is_some_and(|revision| revision >= receipt.revision)), "a place showed the name before the receipt or never {revisions:?}");
    one_revision(&[&list, &entity]).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    rig.settle().await;
    assert!([&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("new"))]));

    land_event(&rig, "Chat", json!({"id": "c1", "name": "event", "lists": ["PRIMARY"]}));
    yield_until("an event renames both places", || [&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("event"))])).await;
    rig.database.close_now();
}
