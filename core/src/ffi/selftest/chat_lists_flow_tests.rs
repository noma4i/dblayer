use std::sync::Arc;

use serde_json::json;

use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig};
use super::flow_fixtures::{one_revision, add_chat, add_lists, add_profile, cell, text};
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::test_wait::settled;

const PRIMARY: &str = r#"{"model":"Chat","relation":"list","params":{"list":"PRIMARY"}}"#;
const SECONDARY: &str = r#"{"model":"Chat","relation":"list","params":{"list":"SECONDARY"}}"#;

async fn rig() -> Rig {
    let rig = Rig::encrypted_edited(false, false, false, events(|models| { add_chat(models); add_profile(models); add_lists(models); })).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn connection_removal_hides_chat_before_the_reply_and_defers_its_cascade() {
    for (decision, refused, deleted_by_feed) in [("REMOVE", true, false), ("REMOVE", false, false), ("REMOVE", true, true), ("ACCEPT", false, false)] {
        let rig = Rig::encrypted_edited(false, false, false, events(|models| {
            add_chat(models);
            add_profile(models);
            add_lists(models);
            let models = models.as_array_mut().unwrap();
            let user = models.iter_mut().find(|model| model["key"] == "User").unwrap();
            user["fields"].as_array_mut().unwrap().push(serde_json::to_value(crate::schema::FieldSpec::scalar("connectionChatId", crate::value::ScalarKind::Id).optional()).unwrap());
            let action = user["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "answer_connection").unwrap();
            action["args"]["decision"] = json!("string");
            action["removes_row"] = json!({"model":"Chat","id_field":"connectionChatId","parameter":"decision","equals":"REMOVE"});
            let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
            let mut messages = crate::schema::AssociationSpec::new("messages", crate::schema::AssociationKind::HasMany, "Message", "chatId");
            messages.dependent = Some(crate::schema::Dependent::Destroy);
            chat["associations"].as_array_mut().unwrap().push(serde_json::to_value(messages).unwrap());
        })).await;
        rig.world.answer_with("ChatList", json!({"data":{"chats":[]}}));
        rig.login_ios("A").await;
        let list = watch(&rig, PRIMARY).await;
        let entity = Arc::new(Rows::default());
        let _stream = rig.database.watch(crate::ffi::ReadRequest::Entity { model: "Chat".into(), id: "c1".into(), fetch: crate::ffi::ReadFetch::Never }, entity.clone());
        rig.land("User", json!({"id":"u5","name":"Eve","connectionStatus":"CONNECTED","connectionChatId":"c1"})).await;
        rig.land("Chat", json!({"id":"c1","name":"chat","lists":["PRIMARY"]})).await;
        rig.land("Message", json!({"id":"m1","chatId":"c1","text":"history","at":1})).await;
        settled(&rig, "list and entity show the chat", || list.len() == 1 && entity.len() == 1).await;
        let answer = if refused { Fault::Business } else { Fault::Answer(json!({"data":{"answerConnection":{"user":{"__typename":"User","id":"u5","connectionStatus":"NONE"},"chat":null}}})) };
        let hold = rig.world.hold_then("AnswerConnection", answer);
        let epoch = rig.database.session_epoch();
        let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "answer_connection".into(), json!({"connectionStatus":"NONE"}).to_string(), json!({"chatId":"c1","decision":decision}).to_string())).await.unwrap();
        hold.entered().await;
        let removing = decision == "REMOVE";
        settled(&rig, "only REMOVE hides the chat before the server reply", || list.emptied() == removing && entity.emptied() == removing).await;
        let history = || rig.database.peek_row_now(epoch, "Message".into(), "m1".into()).unwrap().is_some();
        assert!(history(), "the pending removal must preserve message history");
        if deleted_by_feed {
            rig.land_unsettled("Chat", json!({"id":"c1","deleted":true})).await;
            assert!(!history(), "the server event deletes the history while the answer waits");
        }
        hold.release();
        assert_eq!(bound(rig.database.confirmed(receipt, false)).await.is_err(), refused);
        let deleted = deleted_by_feed || (removing && !refused);
        settled(&rig, "the outcome preserves the newest chat and history state", || list.emptied() == deleted && entity.emptied() == deleted && history() != deleted).await;
        assert_eq!(rig.world.log().iter().filter(|request| request.operation == "AnswerConnection").count(), 1);
        rig.database.close_now();
    }
}

/// P-APP-03: a chat that is not in the cache appears in its list by an event with the chat and by the answer of a request that lands it, without a reread of the list.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_appears_from_event() {
    let rig = rig().await;
    let (primary, secondary) = (watch(&rig, PRIMARY).await, watch(&rig, SECONDARY).await);
    let epoch = rig.database.session_epoch();
    let reads = rig.world.count("ChatList");

    rig.land("Chat", json!({"id": "c1", "name": "event", "lists": ["PRIMARY"]})).await;
    settled(&rig, "the event chat stands in PRIMARY", || primary.len() == 1 && secondary.emptied()).await;

    rig.world.answer_with("AnswerConnection", json!({"data":{"answerConnection":{"user":{"__typename":"User","id":"u5","connectionStatus":"PENDING"},"chat":{"__typename":"Chat","id":"c2","connectionStatus":"PENDING","name":"answer","lists":["SECONDARY"]}}}}));
    rig.land("User", json!({"id": "u5", "name": "Eve", "connectionStatus": "PENDING"})).await;
    let receipt = bound(rig.database.act(epoch, "User".into(), "u5".into(), "answer_connection".into(), "{}".into(), json!({"chatId": "c2"}).to_string())).await.unwrap();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    settled(&rig, "the chat of the answer stands in SECONDARY", || secondary.len() == 1).await;

    rig.land("Chat", json!({"id": "c2", "name": "answer", "lists": ["PRIMARY"]})).await;
    settled(&rig, "the accepted chat moves to PRIMARY", || primary.len() == 2 && secondary.emptied()).await;
    assert_eq!(rig.world.count("ChatList"), reads, "the app reread a list");
    rig.database.close_now();
}

/// P-APP-09: the forward picker reads the PRIMARY list: it holds the same chats with the same data as the inbox, also after a rename.
#[tokio::test(flavor = "multi_thread")]
async fn flow_forward_picker() {
    let rig = rig().await;
    let (inbox, picker) = (watch(&rig, PRIMARY).await, watch(&rig, PRIMARY).await);
    rig.land("Chat", json!({"id": "c1", "name": "one", "lists": ["PRIMARY"]})).await;
    rig.land("Chat", json!({"id": "c2", "name": "two", "lists": ["PRIMARY"]})).await;
    settled(&rig, "both lists hold two chats", || inbox.len() == 2 && picker.len() == 2).await;
    rig.land("Chat", json!({"id": "c2", "name": "renamed", "lists": ["PRIMARY"]})).await;
    settled(&rig, "both lists show the rename", || [&inbox, &picker].iter().all(|rows| names(&rig, rows).contains(&("c2".to_string(), text("renamed"))))).await;
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
    rig.land("Chat", json!({"id": "c1", "name": "old", "lists": ["PRIMARY"]})).await;
    settled(&rig, "both places show the old name", || list.len() == 1 && entity.len() == 1).await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("ChatUpdate");

    let receipt = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "update".into(), json!({"name": "new"}).to_string(), json!({"name": "new"}).to_string())).await.unwrap();
    hold.entered().await;
    settled(&rig, "both places show the new name", || [&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("new"))])).await;
    let revisions: Vec<_> = [&list, &entity].iter().map(|rows| rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| cell(&rig, "Chat", row, "name") == text("new"))).map(|seen| seen.revision)).collect();
    assert!(revisions.iter().all(|revision| revision.is_some_and(|revision| revision >= receipt.revision)), "a place showed the name before the receipt or never {revisions:?}");
    one_revision(&rig, &[&list, &entity]).await;
    hold.release();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    rig.settle().await;
    assert!([&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("new"))]));

    rig.land("Chat", json!({"id": "c1", "name": "event", "lists": ["PRIMARY"]})).await;
    settled(&rig, "an event renames both places", || [&list, &entity].iter().all(|rows| names(&rig, rows) == [("c1".to_string(), text("event"))])).await;
    rig.database.close_now();
}
