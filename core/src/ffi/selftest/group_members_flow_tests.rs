use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::{group, seed_members};
use super::rig_tests::{bound, Fault, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::WireRow;
use crate::test_wait::settled;

fn has(rows: &Rows, id: &str) -> bool { rows.0.lock().unwrap().iter().any(|row| row.id == id) }

async fn chat_with_members() -> (Rig, WireRow, Arc<Rows>, serde_json::Value) {
    let world = World::new("server", "A", true);
    let rig = Rig::plain(world.clone(), false).await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let chat = bound(rig.database.confirmed(receipt)).await.unwrap().value.expect("the chat was not confirmed");
    seed_members(&rig, &chat.id).await;
    let params = json!({"groupId": chat.id, "order": "JOINED"});
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(&json!({"model":"Member","relation":"group_members","params":params}).to_string(), rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the member list shows the chat members", || has(&rows, "u1") && has(&rows, "u3")).await;
    (rig, chat, rows, params)
}

/// P-APP-26: the member list of a chat shows the added user at the revision of the receipt, before the server answers, and keeps the user after the answer.
#[tokio::test(flavor = "multi_thread")]
async fn flow_group_members() {
    let (rig, chat, rows, _) = chat_with_members().await;
    let epoch = rig.database.session_epoch();
    rig.world.set_members("JOINED", &["u1", "u2", "u3", "u4"]);
    let hold = rig.world.hold_next("AddMember");

    let receipt = bound(rig.database.act(epoch, "Group".into(), chat.id.clone(), "add_member".into(), "{}".into(), json!({"userIds":["u4"]}).to_string())).await.unwrap();
    hold.entered().await;

    settled(&rig, "the member is in the list before the answer", || has(&rows, "u4")).await;
    let first = rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.id == "u4")).cloned().unwrap();
    assert!(first.revision >= receipt.revision, "the member appeared before the receipt: {}", first.revision);
    hold.release();
    assert!(bound(rig.database.confirmed(receipt)).await.is_ok());
    rig.settle().await;
    assert!(has(&rows, "u4"), "the member left the list after the answer");
    let gone = rows.1.lock().unwrap().iter().skip_while(|seen| !seen.rows.iter().any(|row| row.id == "u4")).any(|seen| seen.removed.iter().any(|id| id == "u4"));
    assert!(!gone, "the member flickered out of the list after it appeared");
    rig.database.close_now();
}

/// P-APP-26: a refusal of the server removes the added user from the list.
#[tokio::test(flavor = "multi_thread")]
async fn flow_group_members_refused_add_removes_the_user_from_the_list() {
    let (rig, chat, rows, _) = chat_with_members().await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_then("AddMember", Fault::Business);

    let receipt = bound(rig.database.act(epoch, "Group".into(), chat.id.clone(), "add_member".into(), "{}".into(), json!({"userIds":["u4"]}).to_string())).await.unwrap();
    hold.entered().await;
    settled(&rig, "the member is in the list before the answer", || has(&rows, "u4")).await;
    hold.release();

    assert!(matches!(bound(rig.database.confirmed(receipt)).await, Err(crate::ffi::DbError::Rejected { kept: false, .. })));
    settled(&rig, "the refused member left the list", || !has(&rows, "u4")).await;
    assert!(has(&rows, "u1") && has(&rows, "u3"));
    rig.database.close_now();
}
