//! message.dbl:284 `optimistic reactions: { by: :userId }` against outbox.md P-OUT-23 (out_optimistic_keyed_element) on the schema of the app.
use serde_json::{json, Value};

use super::stand::{message, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::{ReadRequest, ReadStream, ReadValue, RowEdit};

fn seeded() -> Value {
    let mut row = message("m1", "c1", "hi");
    row["reactions"] = json!([{"id": "r2", "body": "x", "user": {"id": "u2"}}]);
    json!({"area": "MESSAGE", "kind": "UPDATED", "entityId": "m1", "message": row})
}

fn reactions(stand: &Stand, row: &WireRow) -> Vec<(String, String)> {
    let names: Vec<String> = stand.database.db.spec("Message").unwrap().field("reactions").unwrap().fields.iter().map(|member| member.name.clone()).collect();
    let at = |name: &str| names.iter().position(|member| member == name).unwrap();
    let Cell::Seq { items } = stand.cell("Message", row, "reactions") else { panic!("reactions is no list") };
    let text = |cell: &Cell| match cell { Cell::Text { value } => value.clone(), other => panic!("{other:?}") };
    items.iter().map(|item| { let Cell::Seq { items } = item else { panic!("{item:?}") }; (text(&items[at("userId")]), text(&items[at("body")])) }).collect()
}

async fn shown_at(stream: &ReadStream, shown: &mut Vec<WireRow>, revision: i64) {
    loop {
        let value = bound(stream.pull_next()).await.expect("the stream is open");
        let at = match value {
            ReadValue::Snapshot { revision, rows, .. } => { *shown = rows; revision }
            ReadValue::Delta { revision, edits, .. } => { RowEdit::replay(shown, &edits); revision }
            _ => continue,
        };
        if at >= revision { return; }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_reaction_keyed_by_user() {
    let stand = Stand::open().await;
    let (_events, token) = stand.events("Message").await;
    stand.push(&token, seeded());
    stand.settle().await;
    let epoch = stand.database.session_epoch();
    let stream = stand.database.subscribe_values(epoch, ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: crate::ffi::ReadFetch::Never });
    let mut shown = Vec::new();
    stand.hold(&["MessageReact"]);
    let patch = json!({"reactions": {"$keyed": {"by": "userId", "element": {"userId": {"$arg": "userId"}, "body": {"$arg": "reactionBody"}}}}}).to_string();
    let mut seen = Vec::new();
    for (user, body) in [("u1", "a"), ("u1", "b"), ("u3", "c")] {
        let args = json!({"userId": user, "reactionBody": body}).to_string();
        let receipt = bound(stand.database.act(epoch, "Message".into(), "m1".into(), "react".into(), patch.clone(), args)).await.unwrap();
        shown_at(&stream, &mut shown, receipt.revision).await;
        seen.push(reactions(&stand, &shown[0]));
    }
    let pair = |user: &str, body: &str| (user.to_string(), body.to_string());
    assert_eq!(seen, vec![
        vec![pair("u2", "x"), pair("u1", "a")],
        vec![pair("u2", "x"), pair("u1", "b")],
        vec![pair("u2", "x"), pair("u1", "b"), pair("u3", "c")],
    ]);
}
