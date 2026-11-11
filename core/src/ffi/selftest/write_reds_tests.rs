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

fn real_parent_failures(sent: &[Vec<String>], temp: &str, variant: &str) -> Vec<String> {
    if sent.is_empty() {
        return vec![format!("{variant}: the child request never reached the server")];
    }
    let mut failed = Vec::new();
    for variables in sent {
        if !variables.iter().any(|value| value == "k1") {
            failed.push(format!("{variant}: the child request does not carry the real parent id k1: {variables:?}"));
        }
        if variables.iter().any(|value| value == temp) {
            failed.push(format!("{variant}: the child request carries the temporary id {temp}: {variables:?}"));
        }
    }
    failed
}

fn refusal_failure(ended: &Result<WireConfirmed, DbError>, variant: &str) -> Option<String> {
    (!matches!(ended, Err(DbError::Rejected { kept: false, .. }))).then(|| format!("{variant}: the dependent operation did not end as Rejected{{kept: false}}: {ended:?}"))
}

fn assert_none_red(failed: &[String]) {
    assert!(failed.is_empty(), "{} red variants:\n{}", failed.len(), failed.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn write_child_sends_the_real_parent_id() {
    let cases: [(&str, &str, &str, fn(&str) -> Value); 2] = [
        ("model field", "Record", "CreateRecord", |temp: &str| json!({"containerId": temp, "authorId":"u1", "text":"hello"})),
        ("nested object and array", "Moment", "CreateMoment", |temp: &str| json!({"text":"m", "media": {"ref": temp, "list": [temp, {"deep": temp}]}})),
    ];
    let mut failed = Vec::new();
    for (variant, model, operation, values) in cases {
        let rig = parent_child_rig().await;
        let epoch = rig.database.session_epoch();
        let held = rig.world.hold_next("CreateContainer");
        let parent = bound(rig.database.create(epoch, "Container".into(), json!({"title":"box"}).to_string(), "{}".into())).await.unwrap();
        let temp = parent.optimistic_value.clone().unwrap().id;
        held.entered().await;
        let child = bound(rig.database.create(epoch, model.into(), values(&temp).to_string(), "{}".into())).await.unwrap();
        held.release();
        if let Err(error) = bound(rig.database.confirmed(parent, false)).await {
            failed.push(format!("{variant}: the parent was not confirmed: {error:?}"));
        }
        if let Err(error) = bound(rig.database.confirmed(child, false)).await {
            failed.push(format!("{variant}: the child was not confirmed: {error:?}"));
        }
        failed.extend(real_parent_failures(&sent_strings(&rig, operation), &temp, variant));
        rig.database.close_now();
    }
    assert_none_red(&failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_restored_child_sends_the_real_parent_id() {
    let mut failed = Vec::new();
    for variant in ["act on the temp row", "create with the temp id in a foreign field"] {
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
        let like = variant == "act on the temp row";
        if like {
            bound(database.act(epoch, "Moment".into(), temp.clone(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
        } else {
            let values = json!({"text":"child","media":{"ref":temp,"list":[temp,{"deep":temp}]}}).to_string();
            bound(database.send(plain("Moment", &values, epoch))).await.unwrap();
        }
        held.release();
        if let Err(error) = bound(database.confirmed(parent, false)).await {
            failed.push(format!("{variant}: the parent was not confirmed: {error:?}"));
        }
        other.entered().await;
        let sent_of = |stand: &Stand| -> Vec<Vec<String>> {
            let variables = if like { stand.script.variables("LikeMoment") } else { stand.script.variables("CreateMoment").into_iter().filter(|variables| variables["text"] == "child").collect() };
            variables.iter().map(strings_of).collect()
        };
        if !sent_of(&stand).is_empty() {
            failed.push(format!("{variant}: the child went out while the queue slot was taken"));
        }
        database.close_now();
        stand.script.data("CreateMoment", &MOMENT_ANSWER.replace("m77", "m78"));
        if like {
            stand.script.data("LikeMoment", r#"{"data":{"likeMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#);
        } else {
            stand.script.data("CreateMoment", &MOMENT_ANSWER.replace("m77", "m79"));
        }
        let reopened = stand.open(Some(&path), &operations).await;
        settled(&reopened, "the restored child is sent", || sent_of(&stand).len() == 1).await;
        for variables in sent_of(&stand) {
            if !variables.iter().any(|value| value == "m77") {
                failed.push(format!("{variant}: the restored child does not carry the real id m77: {variables:?}"));
            }
            if variables.iter().any(|value| *value == temp) {
                failed.push(format!("{variant}: the restored child carries the temporary id {temp}: {variables:?}"));
            }
        }
        reopened.close_now();
    }
    assert_none_red(&failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_dependents_of_an_ended_create_end_refused() {
    let mut failed = Vec::new();
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
                rig.settle().await;
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
                rig.settle().await;
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
                rig.settle().await;
                let outcome = (ended, sent_strings(&rig, "MessageEdit").len(), rows.emptied());
                rig.database.close_now();
                outcome
            }
        };
        failed.extend(refusal_failure(&ended, variant));
        if sent != 0 {
            failed.push(format!("{variant}: the dependent operation went to the server {sent} times"));
        }
        if !gone {
            failed.push(format!("{variant}: the row of the ended create is still visible"));
        }
    }
    assert_none_red(&failed);
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
    let rows = watched(&rig.database, "Message", &temp);
    refused.entered().await;
    let upload = bound(rig.database.upload(epoch, "Message".into(), temp, "upload".into(), request(&source))).await.unwrap();
    refused.release();
    let ended = bound(rig.database.confirmed(upload, false)).await;
    rig.settle().await;
    let mut failed = Vec::new();
    failed.extend(refusal_failure(&ended, "upload on a refused create"));
    if !rows.emptied() {
        failed.push("upload on a refused create: the row with the temporary id is still visible".into());
    }
    let final_mutations = rig.world.log().iter().filter(|entry| entry.operation == "SendMessageFile").count();
    if final_mutations != 0 {
        failed.push(format!("the final mutation of the upload went to the server {final_mutations} times"));
    }
    rig.database.close_now();
    assert_none_red(&failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_calls_refuse_a_temp_id() {
    let rig = message_rig().await;
    let epoch = rig.database.session_epoch();
    let refused = rig.world.hold_then("SendMessage", Fault::Business);
    let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"hi","at":1}).to_string(), "{}".into())).await.unwrap();
    let temp = create.optimistic_value.clone().unwrap().id;
    refused.entered().await;
    let (mutation_db, query_db, session_db) = (rig.database.clone(), rig.database.clone(), rig.database.clone());
    let (mutation_temp, query_temp, session_temp) = (temp.clone(), temp.clone(), temp.clone());
    let mutation = rig.spawn(async move { mutation_db.run(epoch, "UnregisterDevice".into(), json!({"id": mutation_temp}).to_string()).await });
    let query = rig.spawn(async move { query_db.run(epoch, "UserNode".into(), json!({"id": query_temp}).to_string()).await });
    let session = rig.spawn(async move { session_db.session_call("otp".into(), json!({"x": session_temp}).to_string()).await });
    refused.release();
    let mut failed = Vec::new();
    for (variant, operation, ended) in [("run mutation", "UnregisterDevice", bound(mutation).await.unwrap()), ("run query", "UserNode", bound(query).await.unwrap()), ("session call", "otp", bound(session).await.unwrap())] {
        if !matches!(ended, Err(DbError::Invalid { .. })) {
            failed.push(format!("{variant}: the call with a temporary id was not refused as Invalid: {ended:?}"));
        }
        let sent = rig.world.log().iter().filter(|entry| entry.operation == operation).count();
        if sent != 0 {
            failed.push(format!("{variant}: {operation} went to the server {sent} times with a temporary id"));
        }
    }
    rig.database.close_now();
    assert_none_red(&failed);
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
    let failed: Vec<String> = sent.iter().filter(|variables| variables.iter().any(|value| *value == temp)).map(|variables| format!("the create carries its own temporary id {temp}: {variables:?}")).collect();
    rig.database.close_now();
    assert_none_red(&failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_sends_a_value_that_only_starts_with_the_temp_prefix() {
    let mut failed = Vec::new();
    for value in ["temp-hello", "temp-"] {
        let rig = message_rig().await;
        rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "old", "at": 1})).await;
        rig.world.answer_with("Thread", json!({"data":{"thread":[]}}));
        let epoch = rig.database.session_epoch();
        let create = bound(rig.database.create(epoch, "Message".into(), json!({"chatId":"c1","text":value,"at":2}).to_string(), "{}".into())).await.unwrap();
        let created = bound(rig.database.confirmed(create, false)).await;
        let edit = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "edit".into(), json!({"text": value}).to_string(), "{}".into())).await.unwrap();
        let edited = bound(rig.database.confirmed(edit, false)).await;
        let run = bound(rig.database.run(epoch, "UnregisterDevice".into(), json!({"id": value}).to_string())).await;
        let session = bound(rig.database.session_call("otp".into(), json!({"x": value}).to_string())).await;
        let window = bound(rig.database.load(epoch, "Message".into(), "thread".into(), json!({"chatId":"c1","note":value}).to_string())).await;
        let paths = [
            ("create", "SendMessage", created.map(|_| ())),
            ("update", "MessageEdit", edited.map(|_| ())),
            ("run", "UnregisterDevice", run.map(|_| ())),
            ("session call", "otp", session.map(|_| ())),
            ("window read", "Thread", window.map(|_| ())),
        ];
        for (path, operation, ended) in paths {
            if matches!(ended, Err(DbError::Invalid { .. })) {
                failed.push(format!("{path} {value}: refused as Invalid: {ended:?}"));
            }
            let sent = rig.world.log().iter().filter(|entry| entry.operation == operation).count();
            if sent != 1 {
                failed.push(format!("{path} {value}: {operation} went to the server {sent} times, expected 1"));
            }
        }
        for (path, operation) in [("create", "SendMessage"), ("update", "MessageEdit"), ("run", "UnregisterDevice"), ("session call", "otp")] {
            let carried = rig.world.log().iter().filter(|entry| entry.operation == operation).any(|entry| entry.body.contains(&format!("\"{value}\"")));
            if !carried {
                failed.push(format!("{path} {value}: the request does not carry the value as it is"));
            }
        }
        rig.database.close_now();
    }
    assert_none_red(&failed);
}
