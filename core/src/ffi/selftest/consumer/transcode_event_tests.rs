use serde_json::{json, Value};
use super::stand::{message, moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::Rows;
use crate::ffi::wire::Cell;
use crate::ffi::{ReadFetch, ReadValue};
use crate::test_wait::settled;

fn media_row(mut row: Value, status: &str, progress: i64) -> Value {
    row["media"] = json!({"transcodeStatus": status, "transcodeProgress": progress});
    row
}

fn media_status(stand: &Stand, rows: &Rows, model: &str, id: &str) -> Option<String> {
    let rows = rows.0.lock().unwrap();
    let row = rows.iter().find(|row| row.id == id)?;
    let spec = stand.database.db.spec(model).unwrap();
    let members = &spec.field("media").unwrap().fields;
    let index = members.iter().position(|member| member.name == "transcodeStatus").unwrap();
    let Cell::Seq { items } = stand.cell(model, row, "media") else { return None };
    match &items[index] {
        Cell::Text { value } => Some(value.clone()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_message_transcode_event_updates_reader() {
    let stand = Stand::open().await;
    let (events, token) = stand.events("Message").await;
    let rows = stand.entity("Message", "m1", ReadFetch::Never);
    let asked = stand.requests("MessageNode");
    let before = media_row(message("m1", "c1", "clip"), "PROGRESSING", 40);
    stand.push(&token, json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":"m1", "parentId":"c1", "message":before}));
    settled(&stand, "message begins transcoding", || media_status(&stand, &rows, "Message", "m1").as_deref() == Some("PROGRESSING")).await;
    let after = media_row(message("m1", "c1", "clip"), "COMPLETED", 100);
    stand.push(&token, json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":"m1", "parentId":"c1", "message":after}));
    settled(&stand, "message is transcoded", || media_status(&stand, &rows, "Message", "m1").as_deref() == Some("COMPLETED")).await;
    assert_eq!(stand.requests("MessageNode"), asked, "payload landing needs no node query");
    drop(events);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_transcode_event_rereads_node() {
    let stand = Stand::open().await;
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "PROGRESSING", 40)}));
    let (events, token) = stand.events("Moment").await;
    let rows = stand.entity("Moment", "m1", ReadFetch::Stale);
    settled(&stand, "moment begins transcoding", || media_status(&stand, &rows, "Moment", "m1").as_deref() == Some("PROGRESSING")).await;
    let asked = stand.requests("MomentNode");
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "COMPLETED", 100)}));
    stand.push(&token, json!({"area":"MOMENT", "kind":"UPDATED", "entityId":"m1"}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { .. })));
    settled(&stand, "moment is transcoded", || media_status(&stand, &rows, "Moment", "m1").as_deref() == Some("COMPLETED")).await;
    assert_eq!(stand.requests("MomentNode"), asked + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_temp_entity_swap() {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

    let stand = Stand::open().await;
    let database = &stand.database;
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("moment.jpg");
    std::fs::write(&path, b"image").unwrap();
    let command = CommandRequest {
        epoch,
        model: "Moment".into(),
        values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
        args_json: "{}".into(),
        upload: Some(CommandUpload {
            action: "create_file".into(),
            request: WireUploadRequest {
                source: WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None },
                variables_json: "{}".into(),
            },
        }),
    };
    let receipt = bound(database.send(command)).await.unwrap();
    let temporary = receipt.optimistic_value.as_ref().expect("optimistic Moment").id.clone();
    assert!(temporary.starts_with("temp-"));
    let rows = stand.entity("Moment", &temporary, ReadFetch::Stale);
    settled(&stand, "temporary Moment is visible", || rows.0.lock().unwrap().iter().any(|row| row.id == temporary)).await;
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
    stand.script_put(200, Some("etag"));
    stand.answer("MomentCreateFile", json!({"momentCreate":{"moment":media_row(moment("m1", 0), "PROGRESSING", 40)}}));
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "PROGRESSING", 40)}));
    database.set_online(true).unwrap();
    settled(&stand, "same reader sees the server Moment", || rows.0.lock().unwrap().iter().any(|row| row.id == "m1")).await;
    let (_events, token) = stand.events("Moment").await;
    let before = stand.requests_with("MomentNode", "momentId", "m1");
    stand.answer("MomentNode", json!({"node":media_row(moment("m1", 0), "COMPLETED", 100)}));
    stand.push(&token, json!({"area":"MOMENT", "kind":"UPDATED", "entityId":"m1"}));
    settled(&stand, "same reader sees completed", || media_status(&stand, &rows, "Moment", "m1").as_deref() == Some("COMPLETED")).await;
    assert_eq!(stand.requests_with("MomentNode", "momentId", "m1"), before + 1);
    assert!(stand.asked().iter().filter(|(name, _)| name == "MomentNode").all(|(_, variables)| !variables["momentId"].as_str().is_some_and(|id| id.starts_with("temp-"))));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_moment_create_then_stale_entity_reads_node() {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

    let stand = Stand::open().await;
    let database = &stand.database;
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("moment.jpg");
    std::fs::write(&path, b"image").unwrap();
    let command = CommandRequest {
        epoch,
        model: "Moment".into(),
        values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
        args_json: "{}".into(),
        upload: Some(CommandUpload {
            action: "create_file".into(),
            request: WireUploadRequest {
                source: WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None },
                variables_json: "{}".into(),
            },
        }),
    };
    let receipt = bound(database.send(command)).await.unwrap();
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
    stand.script_put(200, Some("etag"));
    stand.answer("MomentCreateFile", json!({"momentCreate":{"moment":media_row(moment("77", 0), "PROGRESSING", 40)}}));
    stand.answer("MomentNode", json!({"node":media_row(moment("77", 0), "PROGRESSING", 40)}));
    database.set_online(true).unwrap();
    bound(database.confirmed(receipt, false)).await.unwrap();
    assert_eq!(stand.requests_with("MomentNode", "momentId", "77"), 0);
    let rows = stand.entity("Moment", "77", ReadFetch::Stale);
    settled(&stand, "the stale entity reads its node", || stand.requests_with("MomentNode", "momentId", "77") == 1).await;
    assert!(rows.0.lock().unwrap().iter().any(|row| row.id == "77"));
    assert_eq!(stand.requests_with("MomentNode", "momentId", "77"), 1);
    database.close_now();
}
