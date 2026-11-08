use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Rig, World};
use super::flow_fixtures::{one_revision, add_chat, add_typing, land_event, land_owned};
use crate::ffi::tape::Rows;
use crate::test_wait::settled;

const HEADER: &str = r#"{"model":"ChatAction","where":{"type":"equal","field":"chatId","value":"c1"}}"#;
const ROW: &str = r#"{"model":"ChatAction"}"#;

async fn watch(rig: &Rig, query: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(query, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    rows
}

/// P-APP-07: a typing row from a local write or an event of the server shows in the header and in the chat row at one revision, and every path ends it: a destroy, an event of the server and the expiry after 8 seconds.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_typing() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| { add_chat(models); add_typing(models); }).await;
    let (header, row) = (watch(&rig, HEADER).await, watch(&rig, ROW).await);
    let places = [&header, &row];
    let epoch = rig.database.session_epoch();

    let receipt = bound(rig.database.create(epoch, "ChatAction".into(), json!({"id": "c1:u1", "chatId": "c1", "userId": "u1", "kind": "typing"}).to_string(), "{}".into())).await.unwrap();
    settled(&rig, "both places show the typing user", || places.iter().all(|rows| rows.len() == 1)).await;
    let revisions: Vec<_> = places.iter().map(|rows| rows.1.lock().unwrap().iter().find(|seen| !seen.rows.is_empty()).map(|seen| seen.revision)).collect();
    assert!(revisions.iter().all(|revision| revision.is_some_and(|revision| revision >= receipt.revision)), "a place showed the row before the receipt or never {revisions:?}");
    one_revision(&rig, &[&header, &row]).await;

    bound(rig.database.destroy(epoch, "ChatAction".into(), "c1:u1".into())).await.unwrap();
    settled(&rig, "a destroy empties both places", || places.iter().all(|rows| rows.emptied())).await;

    land_owned(&rig, "ChatAction", json!({"id": "c1:u2", "chatId": "c1", "userId": "u2", "kind": "recording"}));
    settled(&rig, "an event shows the user in both places", || places.iter().all(|rows| rows.len() == 1)).await;
    land_owned(&rig, "ChatAction", json!({"id": "c1:u3", "chatId": "c1", "userId": "u3", "kind": "typing"}));
    settled(&rig, "a second user shows", || places.iter().all(|rows| rows.len() == 2)).await;
    land_event(&rig, "Message", json!({"id": "m9", "chatId": "c1", "text": "hello", "at": 5, "authorId": "u2"}));
    settled(&rig, "the message of u2 ends only the typing of u2", || places.iter().all(|rows| rows.len() == 1 && rows.0.lock().unwrap()[0].id == "c1:u3")).await;
    let sent = bound(rig.database.send(crate::ffi::CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId": "c1", "text": "mine", "at": 6, "authorId": "u3"}).to_string(), args_json: "{}".into(), upload: None })).await.unwrap();
    assert!(sent.optimistic_value.is_some());
    settled(&rig, "a local message of u3 ends the typing of u3", || places.iter().all(|rows| rows.emptied())).await;
    land_owned(&rig, "ChatAction", json!({"id": "c1:u2", "chatId": "c1", "userId": "u2", "kind": "recording"}));
    settled(&rig, "the user types again", || places.iter().all(|rows| rows.len() == 1)).await;
    rig.clock.advance(8_000);
    settled(&rig, "the expiry empties both places", || places.iter().all(|rows| rows.emptied())).await;
    rig.database.close_now();
}
