use std::sync::Arc;

use serde_json::json;

use super::rig_tests::{bound, Rig, World};
use super::v3_fixtures::{add_chat, add_typing, land_event};
use crate::ffi::tape::Rows;
use crate::test_wait::yield_until;

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
async fn v3_chat_typing_shows_in_both_places_and_ends_by_every_path() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| { add_chat(models); add_typing(models); }).await;
    let (header, row) = (watch(&rig, HEADER).await, watch(&rig, ROW).await);
    let places = [&header, &row];
    let epoch = rig.database.session_epoch();

    let receipt = bound(rig.database.create(epoch, "ChatAction".into(), json!({"id": "c1:u1", "chatId": "c1", "userId": "u1", "kind": "typing"}).to_string(), "{}".into())).await.unwrap();
    yield_until("both places show the typing user", || places.iter().all(|rows| rows.len() == 1)).await;
    let revisions: Vec<_> = places.iter().map(|rows| rows.1.lock().unwrap().iter().find(|seen| !seen.rows.is_empty()).map(|seen| seen.revision)).collect();
    assert!(revisions.iter().all(|revision| *revision == revisions[0] && revision.is_some_and(|revision| revision >= receipt.revision)), "the places showed the row at different revisions {revisions:?}");

    bound(rig.database.destroy(epoch, "ChatAction".into(), "c1:u1".into())).await.unwrap();
    yield_until("a destroy empties both places", || places.iter().all(|rows| rows.len() == 0)).await;

    land_event(&rig, "ChatAction", json!({"id": "c1:u2", "chatId": "c1", "userId": "u2", "kind": "recording"}));
    yield_until("an event shows the user in both places", || places.iter().all(|rows| rows.len() == 1)).await;
    rig.clock.advance(9_000);
    yield_until("the expiry empties both places", || places.iter().all(|rows| rows.len() == 0)).await;
    rig.database.close_now();
}
