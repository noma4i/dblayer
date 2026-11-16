use std::sync::Arc;

use serde_json::{json, Value};

use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig, World};
use super::rig_models::add_reactions;
use super::flow_fixtures::{one_revision, add_chat, add_reactor_user, cell, chat_row, texts};
use crate::ffi::tape::Rows;
use crate::ffi::{ReadFetch, ReadRequest};
use crate::test_wait::settled;

const THREAD: &str = r#"{"model":"Message","where":{"type":"equal","field":"chatId","value":"c1"}}"#;

fn reactions(rig: &Rig, rows: &Rows) -> Vec<String> {
    rows.0.lock().unwrap().iter().find(|row| row.id == "m1").map(|row| texts(&cell(rig, "Message", row, "reactions"))).unwrap_or_default()
}

fn has(rig: &Rig, rows: &Rows, words: &[&str]) -> bool {
    let shown = reactions(rig, rows);
    words.iter().all(|word| shown.iter().any(|text| text == word))
}

fn answer(reactions: Value) -> Fault { Fault::Answer(json!({"data":{"messageReact":{"__typename":"Message","id":"m1","chatId":"c1","text":"hello","at":1,"reactions":reactions}}})) }

/// P-APP-10: a reaction shows in the thread and in the entity of the message at the revision of the receipt, the answer and an event keep it.
#[tokio::test(flavor = "multi_thread")]
async fn flow_message_reactions() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| { add_chat(models); add_reactions(models); add_reactor_user(models); })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    for (id, name) in [("u1", "Ann"), ("u2", "Bob")] { rig.land("User", json!({"id": id, "name": name})).await; }
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 1, "reactions": [{"body": "B", "userId": "u2", "user": {"name": "Bob"}}]})).await;
    let thread = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, thread.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let entity = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: ReadFetch::Never }, entity.clone()));
    settled(&rig, "both places show the first reaction", || has(&rig, &thread, &["B", "Bob"]) && has(&rig, &entity, &["B", "Bob"])).await;
    let epoch = rig.database.session_epoch();
    let patch = json!({"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"body"},"userId":{"$arg":"userId"}}}}}).to_string();
    let hold = rig.world.hold_then("MessageReact", answer(json!([{"body":"B","userId":"u2"},{"body":"A","userId":"u1"}])));

    let receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "react".into(), patch, json!({"body":"A","userId":"u1"}).to_string())).await.unwrap();
    hold.entered().await;
    settled(&rig, "both places show the new reaction with the name of the reactor", || has(&rig, &thread, &["A", "Ann"]) && has(&rig, &entity, &["A", "Ann"])).await;
    for rows in [&thread, &entity] {
        let first = rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| texts(&cell(&rig, "Message", row, "reactions")).contains(&"A".to_string()))).map(|seen| seen.revision);
        assert!(first.is_some_and(|revision| revision >= receipt.revision), "a place showed the reaction before the receipt or never: {first:?}");
    }
    one_revision(&rig, &[&thread, &entity]).await;
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    rig.settle().await;
    assert!(has(&rig, &thread, &["A", "B"]) && has(&rig, &entity, &["A", "B"]));

    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 1, "reactions": [{"body": "B", "userId": "u2", "user": {"name": "Bob"}}, {"body": "A", "userId": "u1", "user": {"name": "Ann"}}, {"body": "C", "userId": "u3", "user": {"name": "Cy"}}]})).await;
    settled(&rig, "the event reaction shows in both places", || has(&rig, &thread, &["C"]) && has(&rig, &entity, &["C"])).await;
    rig.database.close_now();
}

/// P-DLV-19: the name of the reactor is read from the row of the user, a change of that row reaches the thread and the entity.
#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_nested_ref_follows_row() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| { add_chat(models); add_reactions(models); add_reactor_user(models); })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    rig.land("User", json!({"id": "u2", "name": "Bob"})).await;
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 1, "reactions": [{"body": "B", "userId": "u2", "user": {"name": "Old"}}]})).await;
    let thread = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, thread.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    let entity = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: ReadFetch::Never }, entity.clone()));
    settled(&rig, "both places read the name from the row of the user", || has(&rig, &thread, &["Bob"]) && has(&rig, &entity, &["Bob"])).await;
    assert!(!has(&rig, &thread, &["Old"]), "the record written with the message is not what the read gives");
    rig.land("User", json!({"id": "u2", "name": "Robert"})).await;
    settled(&rig, "a commit of the user row changes every stream that references it", || has(&rig, &thread, &["Robert"]) && has(&rig, &entity, &["Robert"])).await;
    rig.land("User", json!({"id": "u2", "name": "Rob"})).await;
    rig.land_removed("User", "u2").await;
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 1, "reactions": [{"body": "B", "userId": "u2", "user": {"name": "Kept"}}]})).await;
    settled(&rig, "without the row of the user the nested value is as written", || has(&rig, &thread, &["Kept"]) && has(&rig, &entity, &["Kept"])).await;
    rig.database.close_now();
}

/// D-485 M81: an optional parameter without a value in the call leaves the field of the row as it is before the answer: the keyed reaction of the user stays, it is not removed.
#[tokio::test(flavor = "multi_thread")]
async fn an_unset_optional_parameter_leaves_the_reaction_of_the_row() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| { add_chat(models); add_reactions(models); add_reactor_user(models); })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    rig.land("User", json!({"id": "u1", "name": "Ann"})).await;
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hello", "at": 1, "reactions": [{"body": "A", "userId": "u1", "user": {"name": "Ann"}}]})).await;
    let entity = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: "Message".into(), id: "m1".into(), fetch: ReadFetch::Never }, entity.clone()));
    settled(&rig, "the entity shows the reaction", || has(&rig, &entity, &["A", "Ann"])).await;
    let epoch = rig.database.session_epoch();
    let patch = json!({"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"body"},"userId":{"$arg":"userId"}}}}}).to_string();
    let hold = rig.world.hold_then("MessageReact", answer(json!([{"body":"A","userId":"u1"}])));
    let receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "react".into(), patch, json!({"userId":"u1"}).to_string())).await.unwrap();
    hold.entered().await;
    settled(&rig, "the entity reached the revision of the receipt", || entity.1.lock().unwrap().iter().any(|seen| seen.revision >= receipt.revision)).await;
    assert!(has(&rig, &entity, &["A", "Ann"]), "the reaction stays while the answer is out: {:?}", reactions(&rig, &entity));
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    rig.database.close_now();
}
