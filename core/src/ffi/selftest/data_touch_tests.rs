use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, chat_row};
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::{ReadFetch, ReadRequest};
use crate::schema::{AssociationKind, AssociationSpec, FieldSpec};
use crate::test_wait::settled;
use crate::value::ScalarKind;

const THREAD: &str = r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#;

/// Message -> Chat -> Group, `touch: true` on both links; the chat keeps no derived fields of its last message, so a value of the chat stream can only come from a touch.
fn touching(models: &mut Value) {
    add_chat(models);
    let models = models.as_array_mut().unwrap();
    let message = models.iter_mut().find(|model| model["key"] == "Message").unwrap();
    message["associations"].as_array_mut().unwrap().retain(|association| association["name"] != "chat");
    let mut chat_link = AssociationSpec::new("chat", AssociationKind::BelongsTo, "Chat", "chatId");
    chat_link.touches_parent = true;
    message["associations"].as_array_mut().unwrap().push(serde_json::to_value(chat_link).unwrap());
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(FieldSpec::scalar("groupId", ScalarKind::Id).nullable().optional().indexed()).unwrap());
    let mut group_link = AssociationSpec::new("group", AssociationKind::BelongsTo, "Group", "groupId");
    group_link.touches_parent = true;
    chat["associations"].as_array_mut().unwrap().push(serde_json::to_value(group_link).unwrap());
}

fn entity(rig: &Rig, model: &str, id: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: model.into(), id: id.into(), fetch: ReadFetch::Never }, rows.clone()));
    rows
}

fn values(rows: &Rows) -> usize { rows.1.lock().unwrap().len() }

struct Chain {
    rig: Rig,
    chat: Arc<Rows>,
    group: Arc<Rows>,
}

/// The chain c1 -> g1 is resident and watched, `with_message` also holds the message m1 of the chat.
async fn chain(with_message: bool) -> Chain {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(touching)).await;
    rig.land("Group", json!({"id": "g1", "name": "group", "left": false, "membership": null})).await;
    let mut chat = chat_row("c1", 0);
    chat["groupId"] = json!("g1");
    rig.land("Chat", chat).await;
    if with_message { rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "first", "at": 1})).await; }
    let (chat, group) = (entity(&rig, "Chat", "c1"), entity(&rig, "Group", "g1"));
    settled(&rig, "both parents show their row", || chat.len() == 1 && group.len() == 1).await;
    rig.settle().await;
    Chain { rig, chat, group }
}

#[derive(Clone, Copy, Debug)]
enum Path { Create, EventChange, EventSameValues, EventDelete, OperationValue, Page }

/// P-DATA-115: the parent gets 1 new value per batch of a child change, whatever the path of the change; a write of equal child values gives it none.
#[tokio::test(flavor = "multi_thread")]
async fn data_touch_once_any_path() {
    // path, whether the message m1 exists first, new values of the chat stream (a create is 2 batches: the optimistic row and the answer)
    let cases = [
        (Path::Create, false, 2),
        (Path::EventChange, true, 1),
        (Path::EventSameValues, true, 0),
        (Path::EventDelete, true, 1),
        (Path::OperationValue, true, 2),
        (Path::Page, false, 1),
    ];
    for (path, existing, expected) in cases {
        let Chain { rig, chat, .. } = chain(existing).await;
        let epoch = rig.database.session_epoch();
        let before = values(&chat);
        match path {
            Path::Create => {
                let hold = rig.world.hold_next("SendMessage");
                bound(rig.database.create(epoch, "Message".into(), json!({"chatId": "c1", "text": "new", "at": 5}).to_string(), "{}".into())).await.unwrap();
                hold.entered().await;
                settled(&rig, "the optimistic create reached the chat", || values(&chat) > before).await;
                assert_eq!(values(&chat), before + 1, "{path:?}: the optimistic create is 1 batch");
                hold.release();
            }
            Path::EventChange => rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "changed", "at": 1})).await,
            Path::EventSameValues => rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "first", "at": 1})).await,
            Path::EventDelete => rig.land_removed("Message", "m1").await,
            Path::OperationValue => {
                let refused = rig.world.hold_then("MessageEdit", Fault::Business);
                let receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "update".into(), json!({"text": "edited"}).to_string(), json!({"text": "edited"}).to_string())).await.unwrap();
                refused.entered().await;
                settled(&rig, "the operation value reached the chat", || values(&chat) > before).await;
                assert_eq!(values(&chat), before + 1, "{path:?}: the value of an operation is 1 batch");
                refused.release();
                assert!(matches!(bound(rig.database.confirmed(receipt, false)).await, Err(crate::ffi::DbError::Rejected { .. })));
            }
            Path::Page => {
                for (id, at) in [("p1", 1), ("p2", 2), ("p3", 3)] { rig.world.seed_message("A", id, id, at); }
                let list = Arc::new(Rows::default());
                let stream = rig.database.watch_query(THREAD, list.clone());
                bound(stream.wait_for_initial_load()).await.unwrap();
                settled(&rig, "the page shows its 3 children", || list.len() == 3).await;
            }
        }
        rig.settle().await;
        assert_eq!(values(&chat) - before, expected, "{path:?}: new values of the parent stream");
        rig.database.close_now();
    }
}

/// P-DATA-115: a touch goes up the chain, the chat and then the group each get 1 new value per batch, also for a page of several children, and a write of equal values gives none.
#[tokio::test(flavor = "multi_thread")]
async fn data_touch_chain_reaches_every_ancestor_once() {
    let Chain { rig, chat, group } = chain(true).await;
    let seen = || (values(&chat), values(&group));

    let before = seen();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "changed", "at": 1})).await;
    assert_eq!(seen(), (before.0 + 1, before.1 + 1), "an event of the child reaches the chat and the group once");

    let before = seen();
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "changed", "at": 1})).await;
    assert_eq!(seen(), before, "equal values touch no ancestor");

    for (id, at) in [("p1", 2), ("p2", 3), ("p3", 4)] { rig.world.seed_message("A", id, id, at); }
    let before = seen();
    let list = Arc::new(Rows::default());
    let stream = rig.database.watch_query(THREAD, list.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    settled(&rig, "the page shows its children", || list.len() == 3).await;
    rig.settle().await;
    assert_eq!(seen(), (before.0 + 1, before.1 + 1), "a page of 3 new children reaches the chat and the group once");
    rig.database.close_now();
}
