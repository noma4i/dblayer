use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, add_lists, chat_row};
use super::rig_models::add_reactions;
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::ReadValue;

const DEBOUNCE: u64 = 50;
const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

fn debounced(models: &mut Value) {
    events(|models| { add_chat(models); add_lists(models); add_reactions(models); })(models);
    for model in models.as_array_mut().unwrap() {
        for listen in model["listens"].as_array_mut().unwrap() { listen["debounce"] = json!("50ms"); }
    }
}

fn message(id: &str) -> Value { json!({"id": id, "chatId": "c1", "text": id, "at": 1}) }

#[tokio::test(flavor = "multi_thread")]
async fn feed_one_bad_event_keeps_the_rest() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, debounced).await;
    rig.land_together(&[("Chat", chat_row("c1", 0))], DEBOUNCE).await;
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
    let seen = rig.land_together(&[("Message", message("m1")), ("Message", bad), ("Chat", chat_row("c2", 0))], DEBOUNCE).await;
    assert!(matches!(seen[0], Some(ReadValue::Event { .. })), "the first event reaches Events: {:?}", seen[0]);
    assert!(matches!(seen[1], Some(ReadValue::Lost { first, last }) if first == last), "the bad event is Lost at its own position: {:?}", seen[1]);
    assert!(matches!(seen[2], Some(ReadValue::Event { .. })), "the third event reaches Events: {:?}", seen[2]);
    assert_eq!(rows.1.lock().unwrap().last().map(|seen| seen.rows.len()), Some(1), "the row of the first event is visible, the bad row is not");
    chats().await.unwrap();
    thread().await.unwrap();
    assert_eq!(rig.world.count("ChatList"), chat_asked, "the window of the other model asks nothing");
    assert_eq!(rig.world.count("Thread"), thread_asked + 1, "the window of the bad event's model rereads once");
    rig.database.close_now();
}
