use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Fault, Rig, World};
use super::upload_stand::{plain, request, watched, Stand, CREATE_MOMENT};
use crate::ffi::{DbError, WireConfirmed};
use crate::test_wait::settled;

const PARENT_AND_CHILD: &str = r#"[{"name":"CreateContainer","document":"mutation CreateContainer($title: String!) { createContainer(title: $title) { id title } }"},{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt reactions { body userId } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($media: JSON, $text: String) { createMoment(media: $media, text: $text) { id uuid likedByMe media createdAt } }"}]"#;

const MOMENT_ANSWER: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

const LIKE_MOMENT: (&str, &str) = ("LikeMoment", "mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id uuid likedByMe media createdAt } }");

fn strings(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::String(text) => found.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, found)),
        Value::Object(members) => members.values().for_each(|member| strings(member, found)),
        _ => {}
    }
}

fn strings_of(variables: &Value) -> Vec<String> {
    let mut found = Vec::new();
    strings(variables, &mut found);
    found
}

fn sent_strings(rig: &Rig, operation: &str) -> Vec<Vec<String>> {
    rig.world.log().iter().filter(|entry| entry.operation == operation).map(|entry| strings_of(&serde_json::from_str::<Value>(&entry.body).unwrap()["variables"])).collect()
}

async fn parent_child_rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(PARENT_AND_CHILD.into()).await.unwrap();
    rig.world.answer_with("CreateContainer", json!({"data":{"createContainer":{"__typename":"Container","id":"k1","title":"box"}}}));
    rig.world.answer_with("CreateRecord", json!({"data":{"createRecord":{"__typename":"Record","id":"r1","containerId":"k1","authorId":"u1","text":"hello","createdAt":"2026-10-08T00:00:00.000Z","reactions":[]}}}));
    rig.world.answer_with("CreateMoment", serde_json::from_str(MOMENT_ANSWER).unwrap());
    rig
}

async fn message_rig() -> Rig {
    Rig::signed_as(World::new("server", "A", true), false, super::rig_events::events(|_| {})).await
}

fn assert_real_parent(sent: &[Vec<String>], temp: &str, variant: &str) {
    assert!(!sent.is_empty(), "{variant}: the child request never reached the server");
    for variables in sent {
        assert!(variables.iter().any(|value| value == "k1"), "{variant}: the child request does not carry the real parent id k1: {variables:?}");
        assert!(!variables.iter().any(|value| value == temp), "{variant}: the child request carries the temporary id {temp}: {variables:?}");
    }
}

fn assert_refused(ended: &Result<WireConfirmed, DbError>, variant: &str) {
    assert!(matches!(ended, Err(DbError::Rejected { kept: false, .. })), "{variant}: the dependent operation did not end as Rejected{{kept: false}}: {ended:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_child_sends_the_real_parent_id() {
    let cases: [(&str, &str, &str, fn(&str) -> Value); 2] = [
        ("model field", "Record", "CreateRecord", |temp: &str| json!({"containerId": temp, "authorId":"u1", "text":"hello"})),
        ("nested object and array", "Moment", "CreateMoment", |temp: &str| json!({"text":"m", "media": {"ref": temp, "list": [temp, {"deep": temp}]}})),
    ];
    for (variant, model, operation, values) in cases {
        let rig = parent_child_rig().await;
        let epoch = rig.database.session_epoch();
        let held = rig.world.hold_next("CreateContainer");
        let parent = bound(rig.database.create(epoch, "Container".into(), json!({"title":"box"}).to_string(), "{}".into())).await.unwrap();
        let temp = parent.optimistic_value.clone().unwrap().id;
        held.entered().await;
        let child = bound(rig.database.create(epoch, model.into(), values(&temp).to_string(), "{}".into())).await.unwrap();
        held.release();
        assert!(bound(rig.database.confirmed(parent, false)).await.is_ok(), "{variant}: the parent was not confirmed");
        let confirmed = bound(rig.database.confirmed(child, false)).await;
        assert!(confirmed.is_ok(), "{variant}: the child was not confirmed: {confirmed:?}");
        assert_real_parent(&sent_strings(&rig, operation), &temp, variant);
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn write_restored_child_sends_the_real_parent_id() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let operations = [CREATE_MOMENT, LIKE_MOMENT];
    let database = stand.open(Some(&path), &operations).await;
    let held = stand.script.hold("CreateMoment", MOMENT_ANSWER);
    let other = stand.script.hold("CreateMoment", &MOMENT_ANSWER.replace("m77", "m78"));
    let epoch = database.session_epoch();
    let parent = bound(database.send(plain("Moment", r#"{"text":"p"}"#, epoch))).await.unwrap();
    let temp = parent.optimistic_value.clone().unwrap().id;
    held.entered().await;
    bound(database.send(plain("Moment", r#"{"text":"other"}"#, epoch))).await.unwrap();
    bound(database.act(epoch, "Moment".into(), temp.clone(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    held.release();
    assert!(bound(database.confirmed(parent, false)).await.is_ok(), "the parent was not confirmed");
    other.entered().await;
    assert_eq!(stand.script.count("LikeMoment"), 0, "the child went out while the queue slot was taken");
    database.close_now();
    stand.script.data("CreateMoment", &MOMENT_ANSWER.replace("m77", "m78"));
    stand.script.data("LikeMoment", r#"{"data":{"likeMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#);
    let reopened = stand.open(Some(&path), &operations).await;
    settled(&reopened, "the restored child is sent", || stand.script.count("LikeMoment") == 1).await;
    let sent: Vec<Vec<String>> = stand.script.variables("LikeMoment").iter().map(strings_of).collect();
    for variables in &sent {
        assert!(variables.iter().any(|value| value == "m77"), "the restored child does not carry the real id m77: {variables:?}");
        assert!(!variables.iter().any(|value| *value == temp), "the restored child carries the temporary id {temp}: {variables:?}");
    }
    reopened.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn write_dependents_of_an_ended_create_end_refused() {
    for variant in ["act on a refused create", "child of a refused create", "act on a discarded create"] {
        let (ended, sent, gone) = match variant {
            "act on a refused create" => {
                let rig = message_rig().await;
                let epoch = rig.database.session_epoch();
                let refused = rig.world.hold_then("SendMessage", Fault::Business);
                let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
                let temp = create.optimistic_value.clone().unwrap().id;
                let rows = watched(&rig.database, "Message", &temp);
                refused.entered().await;
                let edit = bound(rig.database.act(epoch, "Message".into(), temp, "edit".into(), json!({"text":"edited"}).to_string(), "{}".into())).await.unwrap();
                refused.release();
                let ended = bound(rig.database.confirmed(edit, false)).await;
                settled(&rig.database, "the refused row is gone", || rows.emptied()).await;
                let outcome = (ended, sent_strings(&rig, "MessageEdit").len(), rows.emptied());
                rig.database.close_now();
                outcome
            }
            "child of a refused create" => {
                let rig = parent_child_rig().await;
                let epoch = rig.database.session_epoch();
                let refused = rig.world.hold_then("CreateContainer", Fault::Business);
                let parent = bound(rig.database.create(epoch, "Container".into(), json!({"title":"box"}).to_string(), "{}".into())).await.unwrap();
                let temp = parent.optimistic_value.clone().unwrap().id;
                refused.entered().await;
                let child = bound(rig.database.create(epoch, "Record".into(), json!({"containerId": temp, "authorId":"u1", "text":"hello"}).to_string(), "{}".into())).await.unwrap();
                let rows = watched(&rig.database, "Record", &child.optimistic_value.clone().unwrap().id);
                refused.release();
                let ended = bound(rig.database.confirmed(child, false)).await;
                settled(&rig.database, "the row of the child is gone", || rows.emptied()).await;
                let outcome = (ended, sent_strings(&rig, "CreateRecord").len(), rows.emptied());
                rig.database.close_now();
                outcome
            }
            _ => {
                let rig = message_rig().await;
                let epoch = rig.database.session_epoch();
                rig.database.set_online(false).unwrap();
                let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
                let temp = create.optimistic_value.clone().unwrap().id;
                let rows = watched(&rig.database, "Message", &temp);
                let edit = bound(rig.database.act(epoch, "Message".into(), temp.clone(), "edit".into(), json!({"text":"edited"}).to_string(), "{}".into())).await.unwrap();
                bound(rig.database.discard(epoch, "Message".into(), temp, None)).await.unwrap();
                rig.database.set_online(true).unwrap();
                let ended = bound(rig.database.confirmed(edit, false)).await;
                settled(&rig.database, "the discarded row is gone", || rows.emptied()).await;
                let outcome = (ended, sent_strings(&rig, "MessageEdit").len(), rows.emptied());
                rig.database.close_now();
                outcome
            }
        };
        assert_refused(&ended, variant);
        assert_eq!(sent, 0, "{variant}: the dependent operation went to the server");
        assert!(gone, "{variant}: the row of the ended create is still visible");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn write_upload_on_a_refused_temp_row_ends_refused() {
    let rig = message_rig().await;
    rig.world.answer_with("SendMessageFile", json!({"data":{"sendMessageFile":{"__typename":"Message","id":"m9","chatId":"c1","text":"hi","at":1}}}));
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.bin");
    std::fs::write(&source, b"clip").unwrap();
    let epoch = rig.database.session_epoch();
    let refused = rig.world.hold_then("SendMessage", Fault::Business);
    let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
    let temp = create.optimistic_value.clone().unwrap().id;
    refused.entered().await;
    let upload = bound(rig.database.upload(epoch, "Message".into(), temp, "upload".into(), request(&source))).await.unwrap();
    refused.release();
    let ended = bound(rig.database.confirmed(upload, false)).await;
    assert_refused(&ended, "upload on a refused create");
    let final_mutations = rig.world.log().iter().filter(|entry| entry.operation == "SendMessageFile").count();
    assert_eq!(final_mutations, 0, "the final mutation of the upload went to the server");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn write_calls_refuse_a_temp_id() {
    let rig = message_rig().await;
    let epoch = rig.database.session_epoch();
    let refused = rig.world.hold_then("SendMessage", Fault::Business);
    let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
    let temp = create.optimistic_value.clone().unwrap().id;
    refused.entered().await;
    let (run_db, session_db, run_temp, session_temp) = (rig.database.clone(), rig.database.clone(), temp.clone(), temp.clone());
    let run = rig.spawn(async move { run_db.run(epoch, "UnregisterDevice".into(), json!({"id": run_temp}).to_string()).await });
    let session = rig.spawn(async move { session_db.session_call("otp".into(), json!({"x": session_temp}).to_string()).await });
    refused.release();
    let run = bound(run).await.unwrap();
    assert!(matches!(run, Err(DbError::Invalid { .. })), "the call with a temporary id was not refused as Invalid: {run:?}");
    let session = bound(session).await.unwrap();
    assert!(matches!(session, Err(DbError::Invalid { .. })), "the session call with a temporary id was not refused as Invalid: {session:?}");
    for operation in ["UnregisterDevice", "otp"] {
        assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == operation).count(), 0, "{operation} went to the server with a temporary id");
    }
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn write_declared_id_variable_never_carries_the_temp_id() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        wrapper_models(models);
        let create = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Container").unwrap()["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "create").unwrap();
        create["variables"] = json!({"title": "title", "containerId": "id"});
    }).await;
    rig.database.register_operations(PARENT_AND_CHILD.into()).await.unwrap();
    rig.world.answer_with("CreateContainer", json!({"data":{"createContainer":{"__typename":"Container","id":"k1","title":"box"}}}));
    let epoch = rig.database.session_epoch();
    let parent = bound(rig.database.create(epoch, "Container".into(), json!({"title":"box"}).to_string(), "{}".into())).await.unwrap();
    let temp = parent.optimistic_value.clone().unwrap().id;
    assert!(bound(rig.database.confirmed(parent, false)).await.is_ok(), "the create was not confirmed");
    let sent = sent_strings(&rig, "CreateContainer");
    assert!(!sent.is_empty(), "the create never reached the server");
    for variables in &sent {
        assert!(!variables.iter().any(|value| *value == temp), "the create carries its own temporary id {temp}: {variables:?}");
    }
    rig.database.close_now();
}
