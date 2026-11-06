use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, cell, chat_row, land_event};
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

    rig.world.fail_next("PUT", Fault::Http(403));
    bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    yield_until("the upload failed on the row", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;

    let again = bound(rig.database.retry(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()), None)).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(again)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("the retry delivered the logo", || cell(&rig, "Chat", &chat.0.lock().unwrap()[0], "logoUrl") != Cell::Null && status(&chat, "c1", "update_photo").is_none()).await;

    let (_other_directory, other) = source("logo2.jpg", b"another jpeg");
    rig.world.fail_next("PUT", Fault::Http(403));
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
    assert!(first(&chat).is_some() && first(&chat) == first(&list), "the places showed the upload at different revisions {:?} {:?}", first(&chat), first(&list));
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    yield_until("both places end the upload", || [&chat, &list].iter().all(|rows| status(rows, "c1", "update_photo").is_none())).await;
    rig.database.close_now();
}
