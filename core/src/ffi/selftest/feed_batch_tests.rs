use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_lists, chat_row};
use super::rig_models::add_reactions;
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::ReadValue;

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

/// `Message` listens to a second root of the same subscription: 1 frame carries 2 events of 1 model, so they share a batch.
fn models(models: &mut Value) {
    events(|models| { add_chat(models); add_lists(models); add_reactions(models); })(models);
    let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
    let mut second = message["listens"].as_array().unwrap().last().unwrap().clone();
    second["root"] = json!("rigEvents.MessageSecond");
    message["listens"].as_array_mut().unwrap().push(second);
}

fn message(id: &str) -> Value { json!({"id": id, "chatId": "c1", "text": id, "at": 1}) }

fn chat(id: &str) -> Value {
    let mut row = chat_row(id, 0);
    row["lists"] = json!(["SECONDARY"]);
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_one_bad_event_keeps_the_rest() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, models).await;
    rig.land("Chat", chat("c1")).await;
    let epoch = rig.database.session_epoch();
    let chats = || bound(rig.database.load(epoch, "Chat".into(), "list".into(), json!({"list": "SECONDARY"}).to_string()));
    let thread = || bound(rig.database.load(epoch, "Message".into(), "thread".into(), json!({"chatId": "c1"}).to_string()));
    chats().await.unwrap();
    thread().await.unwrap();
    let rows = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, rows.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let (chat_asked, thread_asked) = (rig.world.count("ChatList"), rig.world.count("Thread"));
    let bad = json!({"id": "m2", "chatId": "c1", "text": "m2", "at": 1, "reactions": [{"body": 5, "userId": "u1"}]});
    let seen = rig.land_frame(json!({"Message": message("m1"), "MessageSecond": bad}), &["Message", "Message"]).await;
    assert!(seen.iter().any(|value| matches!(value, Some(ReadValue::Lost { first, last }) if first == last)), "the bad event is Lost at its own position: {seen:?}");
    assert!(seen.iter().any(|value| matches!(value, Some(ReadValue::Event { .. }))), "the good event of the batch reaches Events: {seen:?}");
    assert_eq!(rows.1.lock().unwrap().last().map(|seen| seen.rows.len()), Some(1), "the row of the good event is visible, the bad row is not");
    chats().await.unwrap();
    thread().await.unwrap();
    assert_eq!(rig.world.count("ChatList"), chat_asked, "the window of the other model asks nothing");
    assert_eq!(rig.world.count("Thread"), thread_asked + 1, "the window of the bad event's model rereads once");
    rig.database.close_now();
}
