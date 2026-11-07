use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, cell, chat_row, land_event, land_owned, one_revision};
use crate::ffi::wire::Cell;
use super::rig_tests::{bound, Fault, Rig, World};
use super::upload_fixtures::add_uploads;
use crate::ffi::tape::Rows;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::{ReadFetch, ReadRequest, WireOperationOutcome};
use crate::test_wait::yield_until;
use crate::owners::write::upload::UploadFile;

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        add_chat(models);
        add_uploads(models);
    }).await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    rig
}

fn source(name: &str, bytes: &[u8]) -> (tempfile::TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join(name);
    std::fs::write(&path, bytes).unwrap();
    let path = path.to_str().unwrap().to_string();
    (directory, path)
}

fn request(path: &str, variables: Value) -> WireUploadRequest {
    let mut file = UploadFile::new(path);
    file.variables = variables.as_object().unwrap().clone();
    WireUploadRequest::of(&file)
}

fn entity(rig: &Rig, model: &str, id: &str) -> Arc<Rows> {
    let rows = Arc::new(Rows::default());
    drop(rig.database.watch(ReadRequest::Entity { model: model.into(), id: id.into(), fetch: ReadFetch::Never }, rows.clone()));
    rows
}

/// The status of `uploads[action]` of a row the places show.
fn status(rows: &Rows, id: &str, action: &str) -> Option<WireUploadStatus> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).and_then(|row| row.uploads.iter().find(|entry| entry.action == action).map(|entry| entry.upload.status))
}

/// The file URL of the copy of the library `uploads[action]` of a row shows.
fn local_url(rows: &Rows, id: &str, action: &str) -> Option<String> {
    rows.0.lock().unwrap().iter().find(|row| row.id == id).and_then(|row| row.uploads.iter().find(|entry| entry.action == action).and_then(|entry| entry.upload.local_url.clone()))
}

fn puts(rig: &Rig) -> usize { rig.world.log().iter().filter(|entry| entry.operation == "PUT").count() }

/// P-APP-08: the photo of a group goes through the steps of the project, the chat shows `uploads["update_photo"]` from the commit that queued it until the commit that ends it, and the logo of the answer after it.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_photo_upload() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    yield_until("the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered_for("the file in the network").await;
    yield_until("the chat shows the queued upload while the file is in the network", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Queued)).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the logo of the answer stands and the upload is gone", || cell(&rig, "Chat", &chat.0.lock().unwrap()[0], "logoUrl") == super::flow_fixtures::text("https://cdn/logo-sig1") && status(&chat, "c1", "update_photo").is_none()).await;
    assert_eq!((rig.world.count("CreateBlob"), puts(&rig), rig.world.count("ChatUpdatePhoto")), (1, 1, 1), "one blob, one PUT, one mutation");
    rig.database.close_now();
}

/// P-XFER-23: the row shows the file URL of the copy from the end of the copy until the transfer is removed.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_local_url() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    yield_until("the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered_for("the file in the network").await;
    yield_until("the copy has a URL", || local_url(&chat, "c1", "update_photo").is_some()).await;
    let url = local_url(&chat, "c1", "update_photo").unwrap();
    assert!(url.starts_with("file://") && std::path::Path::new(&url["file://".len()..]).exists(), "{url}");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the URL goes with the transfer", || local_url(&chat, "c1", "update_photo").is_none() && status(&chat, "c1", "update_photo").is_none()).await;
    rig.database.close_now();
}

/// P-APP-14, P-APP-08: a refused file fails its upload on the row with the error, `retry` sends it again, a second refusal is removed by `discard`.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_photo_retry_and_discard() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    yield_until("the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();

    rig.world.fail_next("PUT", Fault::Http(400));
    bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    yield_until("the upload failed on the row", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;

    let again = bound(rig.database.retry(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()), None)).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(again)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the retry delivered the logo", || cell(&rig, "Chat", &chat.0.lock().unwrap()[0], "logoUrl") != Cell::Null && status(&chat, "c1", "update_photo").is_none()).await;

    let (_other_directory, other) = source("logo2.jpg", b"another jpeg");
    rig.world.fail_next("PUT", Fault::Http(400));
    bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&other, json!({"id": "c1"})))).await.unwrap();
    yield_until("the second upload failed", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
    bound(rig.database.discard(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()))).await.unwrap();
    yield_until("the discard removed the upload", || status(&chat, "c1", "update_photo").is_none()).await;
    assert_eq!(rig.world.count("ChatUpdatePhoto"), 1, "only the retry reached the server");
    rig.database.close_now();
}

/// P-XFER-29 xfer_prop_upload_state_reaches_row_streams: the commit of the `sync_upload` row of X gives every stream of X the new `uploads[action]` at one revision.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_upload_state_reaches_row_streams() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    let list = Arc::new(Rows::default());
    let stream = rig.database.watch_query(r#"{"model":"Chat","relation":"all","params":{}}"#, list.clone());
    bound(stream.wait_for_initial_load()).await.unwrap();
    yield_until("both places show the chat", || chat.len() == 1 && list.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered_for("the file in the network").await;
    yield_until("both places show the queued upload", || [&chat, &list].iter().all(|rows| status(rows, "c1", "update_photo") == Some(WireUploadStatus::Queued))).await;
    let first = |rows: &Rows| rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.uploads.iter().any(|entry| entry.action == "update_photo"))).map(|seen| seen.revision);
    assert!(first(&chat).is_some() && first(&list).is_some(), "a place never showed the upload {:?} {:?}", first(&chat), first(&list));
    one_revision(&[&chat, &list]).await;
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("both places end the upload", || [&chat, &list].iter().all(|rows| status(rows, "c1", "update_photo").is_none())).await;
    rig.database.close_now();
}

async fn status_until(stream: &crate::ffi::ReadStream, what: &str, wanted: impl Fn(&crate::ffi::ReadValue) -> bool) -> crate::ffi::ReadValue {
    loop {
        let value = crate::test_wait::within(stream.pull()).await.unwrap_or_else(|| panic!("the stream ended before {what}"));
        if wanted(&value) { return value; }
    }
}

fn blob_stream(rig: &Rig, action: &str) -> Arc<crate::ffi::ReadStream> {
    rig.database.subscribe_values(rig.database.session_epoch(), ReadRequest::Blob { model: "Chat".into(), id: "c1".into(), action: action.into() })
}

/// P-XFER-23 through the stream `Blob`: the status carries the file URL of the copy from the end of the copy until the transfer is removed.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_status_local_url() {
    use crate::ffi::ReadValue;
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    yield_until("the chat shows", || chat.len() == 1).await;
    let stream = blob_stream(&rig, "update_photo");
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered_for("the file in the network").await;
    let ReadValue::Status { local_url: Some(url), .. } = status_until(&stream, "the copy has a URL", |value| matches!(value, ReadValue::Status { local_url: Some(_), .. })).await else { unreachable!() };
    assert!(url.starts_with("file://") && std::path::Path::new(&url["file://".len()..]).exists(), "{url}");
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    status_until(&stream, "the URL goes with the transfer", |value| matches!(value, ReadValue::Status { local_url: None, .. })).await;
    rig.database.close_now();
}

/// P-XFER-26 through the stream `Blob`: the status carries `processing` while the row of the server says the file is not ready, and drops it at the terminal value.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_status_processing() {
    use crate::ffi::{ReadValue, WireProcessing};
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        add_chat(models);
        add_uploads(models);
        let chat = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
        let fields = chat["fields"].as_array_mut().unwrap();
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoState", crate::value::ScalarKind::Str).nullable().optional()).unwrap());
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoProgress", crate::value::ScalarKind::Num).nullable().optional()).unwrap());
        let action = chat["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "update_photo").unwrap();
        action["processing"] = json!({"field": "photoState", "done": ["ready"], "progress": "photoProgress"});
    }).await;
    land_event(&rig, "Chat", chat_row("c1", 0));
    let stream = blob_stream(&rig, "update_photo");
    let mut working = chat_row("c1", 0);
    working["photoState"] = json!("working");
    working["photoProgress"] = json!(0.5);
    land_owned(&rig, "Chat", working);
    let shown = status_until(&stream, "the processing shows", |value| matches!(value, ReadValue::Status { processing: Some(_), .. })).await;
    assert!(matches!(shown, ReadValue::Status { processing: Some(WireProcessing { progress: Some(progress) }), .. } if (progress - 0.5).abs() < f64::EPSILON));
    let mut ready = chat_row("c1", 0);
    ready["photoState"] = json!("ready");
    land_owned(&rig, "Chat", ready);
    status_until(&stream, "the terminal value drops the processing", |value| matches!(value, ReadValue::Status { processing: None, .. })).await;
    rig.database.close_now();
}

/// A commit of another row wakes no `Blob` stream and reads no row: the commit of its own row wakes it once.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_status_quiet_on_other_rows() {
    let rig = rig().await;
    let _stream = blob_stream(&rig, "update_photo");
    yield_until("the stream read its row at the open", || rig.database.shows.row_reads() == 1).await;
    land_owned(&rig, "Chat", chat_row("c2", 0));
    land_owned(&rig, "Chat", chat_row("c1", 1));
    yield_until("the commit of its own row woke the stream", || rig.database.shows.row_reads() >= 2).await;
    assert_eq!(rig.database.shows.row_reads(), 2, "the commit of the other row read a row");
    rig.database.close_now();
}
