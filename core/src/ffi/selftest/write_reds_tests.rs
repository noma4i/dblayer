use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Fault, Rig, World};

const PARENT_AND_CHILD: &str = r#"[{"name":"CreateContainer","document":"mutation CreateContainer($title: String!) { createContainer(title: $title) { id title } }"},{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"}]"#;

fn strings(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::String(text) => found.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, found)),
        Value::Object(members) => members.values().for_each(|member| strings(member, found)),
        _ => {}
    }
}

/// Every string of the variables of every request the server got for `operation`.
fn sent_strings(rig: &Rig, operation: &str) -> Vec<Vec<String>> {
    rig.world.log().iter().filter(|entry| entry.operation == operation).map(|entry| {
        let body: Value = serde_json::from_str(&entry.body).unwrap();
        let mut found = Vec::new();
        strings(&body["variables"], &mut found);
        found
    }).collect()
}

async fn parent_child_rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(PARENT_AND_CHILD.into()).await.unwrap();
    rig.world.answer_with("CreateContainer", json!({"data":{"createContainer":{"__typename":"Container","id":"k1","title":"box"}}}));
    rig.world.answer_with("CreateRecord", json!({"data":{"createRecord":{"__typename":"Record","id":"r1","containerId":"k1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[]}}}));
    rig
}

/// I4: no request carries a temporary id. A child created on a parent that is still unanswered goes out with the real id of the parent.
#[tokio::test(flavor = "multi_thread")]
async fn write_child_create_sends_the_real_parent_id() {
    let rig = parent_child_rig().await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("CreateContainer");
    let parent = bound(rig.database.create(epoch, "Container".into(), json!({"title":"box"}).to_string(), "{}".into())).await.unwrap();
    let temp = parent.optimistic_value.clone().unwrap().id;
    held.entered().await;
    let child = bound(rig.database.create(epoch, "Record".into(), json!({"containerId": temp, "authorId":"u1", "text":"hello"}).to_string(), "{}".into())).await.unwrap();
    held.release();
    assert!(bound(rig.database.confirmed(parent, false)).await.is_ok());
    let _ = bound(rig.database.confirmed(child, false)).await;
    let sent = sent_strings(&rig, "CreateRecord");
    assert!(!sent.is_empty(), "the child request never reached the server");
    for variables in &sent {
        assert!(variables.iter().any(|value| value == "k1"), "the child request does not carry the real parent id k1: {variables:?}");
        assert!(!variables.contains(&temp), "the child request carries the temporary id {temp}: {variables:?}");
    }
    rig.database.close_now();
}

async fn message_rig() -> Rig {
    Rig::signed_as(World::new("server", "A", true), false, super::rig_events::events(|_| {})).await
}

/// I4 and D-288: operations queued on a temp row end when the create of that row is refused. They do not go to the server with the temp id.
#[tokio::test(flavor = "multi_thread")]
async fn write_ops_on_a_temp_row_end_when_its_create_is_refused() {
    let rig = message_rig().await;
    let epoch = rig.database.session_epoch();
    let refused = rig.world.hold_then("SendMessage", Fault::Business);
    let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
    let temp = create.optimistic_value.clone().unwrap().id;
    refused.entered().await;
    let edit = bound(rig.database.act(epoch, "Message".into(), temp.clone(), "edit".into(), json!({"text":"edited"}).to_string(), "{}".into())).await.unwrap();
    refused.release();
    let created = bound(rig.database.confirmed(create, false)).await;
    assert!(created.is_err(), "the refused create did not end in an error: {created:?}");
    let ended = bound(rig.database.confirmed(edit, false)).await;
    println!("confirmed(edit) = {ended:?}");
    assert!(ended.is_err(), "the operation on the refused temp row did not end in an error: {ended:?}");
    let sent = sent_strings(&rig, "MessageEdit");
    assert!(sent.is_empty(), "the server got an edit of the refused temp row {temp}: {sent:?}");
    rig.database.close_now();
}
