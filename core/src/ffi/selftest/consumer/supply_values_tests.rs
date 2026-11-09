use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest, WireUploadSource};

const BLOB: &str = r#"{"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}"#;

fn values_of(path: &str) -> String {
    json!({ "localPreviewUrl": format!("file://{path}"), "media": { "kind": "VIDEO", "fileUrl": format!("file://{path}"), "width": 640, "height": 480 } }).to_string()
}

fn row_of(stand: &Stand, id: &str) -> Value {
    stand.database.db.read(&["Message"], |reader| reader.point("Message", id)).unwrap().map(Value::Object).unwrap()
}

/// The supplied values are in the row when `supply_source` answers and when the blob transfer asks the server: the answer belongs to `PersistingCopy x Committed`, before any request.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_supplied_values_reach_the_row_before_the_blob_transfer() {
    for mode in ["single", "batch"] {
        let stand = Stand::open().await;
        let database = &stand.database;
        let epoch = database.session_epoch();
        stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
        stand.hold(&["CreateBlob"]);
        let id = if mode == "single" {
            let request = WireUploadRequest { source: crate::ffi::upload_wire::WireSource::Pending, variables_json: r#"{"chatId":"c1","kind":"VIDEO"}"#.into() };
            let command = CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","kind":"VIDEO","clientKey":"k1","media":{"kind":"VIDEO"}}).to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "send_file".into(), request }) };
            bound(database.send(command)).await.unwrap().optimistic_value.expect("the optimistic row").id
        } else {
            let items = (0..2).map(|index| WireUploadItem { values_json: json!({"chatId":"c1","kind":"VIDEO","clientKey":format!("k{index}"),"mediaGroup":{"id":"album","position":index,"size":2},"media":{"kind":"VIDEO"}}).to_string(), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
            bound(database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap().items[0].optimistic_value.id.clone()
        };
        let action = if mode == "single" { "send_file" } else { "send_files" };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("clip.mp4");
        std::fs::write(&path, b"clip").unwrap();
        let path = path.to_str().unwrap().to_string();
        let source = WireUploadSource { path: path.clone(), name: Some("clip.mp4".into()), mime: Some("video/mp4".into()), values_json: values_of(&path), variables_json: "{}".into() };

        let supplied = bound(database.supply_source(epoch, "Message".into(), id.clone(), action.into(), source)).await;

        assert!(supplied.is_ok(), "{mode}: supply_source answered while the blob request was held: {supplied:?}");
        let row = row_of(&stand, &id);
        assert_eq!(row["localPreviewUrl"], json!(format!("file://{path}")), "{mode}: the local value is in the row at the answer: {row}");
        assert_eq!(row["media"]["fileUrl"], json!(format!("file://{path}")), "{mode}: the media values are in the row at the answer");
        stand.release(&["CreateBlob"]);
        database.close_now();
    }
}
