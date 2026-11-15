use std::sync::Arc;

use serde_json::json;

use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use super::rig_held::Held;
use super::flow_fixtures::{add_chat, chat_row};
use crate::ffi::tape::Rows;
use crate::ffi::EndCause;
use crate::test_wait::settled;

fn first_named(rows: &Rows, name: &str) -> Option<i64> {
    rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| crate::ffi::tests::lifecycle_tests::named(row, name))).map(|seen| seen.revision)
}

/// P-APP-18: one row of the user gives one name in every place that shows it, whether it comes with an answer of the server or with an event.
#[tokio::test(flavor = "multi_thread")]
async fn flow_user_everywhere() {
    let rig = Rig::encrypted_edited(false, false, false, events(|_| {})).await;
    rig.login_ios("A").await;
    let held = Held::open(&rig.database);
    held.shows(&rig, "A").await;
    held.lists_show(&rig, "A").await;
    let epoch = rig.database.session_epoch();

    rig.world.set_account("A2");
    rig.clock.advance(1_000);
    bound(rig.database.refresh(epoch, "User".into(), Some("all".into()), "{}".into())).await.unwrap();
    held.shows(&rig, "A2").await;
    held.lists_show(&rig, "A2").await;
    settled(&rig, "all four places show the answer", || [&held.single, &held.entity, &held.rows, &held.window_rows].iter().all(|rows| first_named(rows, "A2").is_some())).await;

    rig.land("User", json!({"id": "user", "name": "A3"})).await;
    held.shows(&rig, "A3").await;
    held.lists_show(&rig, "A3").await;
    settled(&rig, "all four places show the event", || [&held.single, &held.entity, &held.rows, &held.window_rows].iter().all(|rows| first_named(rows, "A3").is_some())).await;
    rig.database.close_now();
}

/// P-APP-28: after the exit every stream of the old account ends, and the next account sees no chat, no message and no user of the old one.
#[tokio::test(flavor = "multi_thread")]
async fn flow_account_switch_clean() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "secret", "at": 3})).await;
    let chats = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, chats.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the chat list shows c1", || chats.len() == 1).await;
    let thread = Arc::new(Rows::default());
    let thread_stream = rig.database.watch_query(r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#, thread.clone());
    bound(thread_stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the thread shows m1", || thread.len() == 1).await;

    bound(rig.database.session_call("logout".into(), "{}".into())).await.unwrap();
    rig.database.session_reset().await.unwrap();
    settled(&rig, "both streams of the old account ended", || chats.ended() == vec![EndCause::SessionEnded] && thread.ended() == vec![EndCause::SessionEnded]).await;

    bound(rig.database.session_call("login_link".into(), json!({"account": "B"}).to_string())).await.unwrap();
    let fresh = Arc::new(Rows::default());
    let fresh_stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, fresh.clone());
    bound(fresh_stream.wait_for_initial_load()).await.unwrap();
    let again = Arc::new(Rows::default());
    let again_stream = rig.database.watch_query(r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#, again.clone());
    bound(again_stream.wait_for_initial_load()).await.unwrap();
    rig.settle().await;
    assert_eq!((fresh.len(), again.len()), (0, 0), "the new account sees rows of the old one");
    rig.database.close_now();
}
