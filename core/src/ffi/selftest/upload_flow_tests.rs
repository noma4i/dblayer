use std::sync::Arc;

use serde_json::{json, Value};

use super::flow_fixtures::{add_chat, cell, chat_row, one_revision};
use crate::ffi::wire::Cell;
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig, World};
use super::upload_fixtures::add_uploads;
use crate::ffi::tape::Rows;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::{ReadFetch, ReadRequest};
use crate::test_wait::settled;
use crate::owners::write::upload::UploadFile;

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
        add_chat(models);
        add_uploads(models);
    })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn confirmed_after_discard_same_as_explicit_discard() {
    let mut observed = Vec::new();
    for automatic in [false, true] {
        let rig = rig().await;
        let chat = entity(&rig, "Chat", "c1");
        settled(&rig, "the chat is visible", || chat.len() == 1).await;
        let (_directory, path) = source("logo.jpg", b"jpeg");
        let epoch = rig.database.session_epoch();
        rig.world.fail_next("PUT", Fault::Http(400));
        let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
        settled(&rig, "the upload failed", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
        let url = local_url(&chat, "c1", "update_photo").unwrap();
        let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
        assert!(copy.exists());
        if automatic {
            assert!(matches!(bound(rig.database.confirmed(receipt.clone(), true)).await, Err(crate::ffi::DbError::Rejected { kept: false, .. })));
        } else {
            bound(rig.database.discard(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()))).await.unwrap();
        }
        settled(&rig, "the discarded upload is gone", || status(&chat, "c1", "update_photo").is_none()).await;
        let repeated = format!("{:?}", bound(rig.database.confirmed(receipt, false)).await);
        let queue = crate::ffi::tests::queue_view_tests::queued(&rig.database).len();
        let visible = status(&chat, "c1", "update_photo");
        observed.push((queue, copy.exists(), visible, repeated));
        bound(rig.database.close_database()).await.unwrap();
    }
    assert_eq!(observed[0], observed[1], "the two discard paths leave the same public state");
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_put_response_limit() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat is visible", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let limit = crate::transport::MAX_PUT_RESPONSE_BYTES;
    rig.world.fail_next("PUT", Fault::ResponseTooLarge(limit));
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    let result = bound(rig.database.confirmed(receipt, false)).await;
    assert!(matches!(result, Err(crate::ffi::DbError::Rejected { failure, kept: true }) if failure.kind == "response_too_large" && failure.response_limit == Some(limit)), "the upload keeps the typed response limit");
    settled(&rig, "the upload failed", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
    assert_eq!(puts(&rig), 1);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_confirmed_discard_exact_op() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat is visible", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let other = bound(rig.database.act(epoch, "Chat".into(), "c1".into(), "pin".into(), "{}".into(), json!({"pinned": true}).to_string())).await.unwrap();
    let upload = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    let held = rig.world.hold_next("ChatPin");
    rig.world.fail_next("PUT", Fault::Http(400));
    rig.database.set_online(true).unwrap();
    held.entered().await;
    settled(&rig, "the upload fails while the other operation is held", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
    let copy = local_url(&chat, "c1", "update_photo").unwrap();
    let before = bound(rig.database.read_row(epoch, "Chat".into(), "c1".into())).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.confirmed(upload, true)).await, Err(crate::ffi::DbError::Rejected { kept: false, .. })));
    let after = bound(rig.database.read_row(epoch, "Chat".into(), "c1".into())).await.unwrap().unwrap();
    assert_eq!(after.cells, before.cells);
    assert!(!std::path::Path::new(copy.strip_prefix("file://").unwrap()).exists());
    assert!(crate::ffi::tests::queue_view_tests::queued(&rig.database).iter().any(|entry| Some(entry.id.0) == other.operation));
    held.release();
    assert!(bound(rig.database.confirmed(other, false)).await.is_ok());
    assert_eq!(rig.world.count("ChatPin"), 1);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_confirmed_parallel_discard() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat is visible", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_then("PUT", Fault::Http(400));
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    let operation = receipt.operation.unwrap();
    held.entered().await;
    let before = rig.database.life.waiting_calls();
    let first = { let db = rig.database.clone(); let receipt = receipt.clone(); rig.spawn(async move { db.confirmed(receipt, true).await }) };
    let second = { let db = rig.database.clone(); rig.spawn(async move { db.confirmed(receipt, false).await }) };
    settled(&rig, "both receipts wait for failure", || rig.database.life.waiting_calls() >= before + 2).await;
    let shared = rig.database.db.clone();
    let entered = Arc::new(crate::test_wait::Flag::default());
    let release = Arc::new(crate::test_wait::Flag::default());
    let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (probe, done, count) = (entered.clone(), release.clone(), commits.clone());
    *shared.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if !result.queue_operations.get(&operation).is_some_and(Option::is_none) { return; }
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        probe.set();
        done.wait_blocking("release the rollback announcement");
    }));
    held.release();
    entered.wait("the rollback commits").await;
    assert!(!first.is_finished() && !second.is_finished());
    release.set();
    let first = bound(first).await.unwrap();
    let second = bound(second).await.unwrap();
    match (first, second) {
        (Err(crate::ffi::DbError::Rejected { failure: a, kept: false }), Err(crate::ffi::DbError::Rejected { failure: b, kept: false })) => {
            assert_eq!((a.kind, a.message), (b.kind, b.message));
        }
        pair => panic!("both waiters must receive the discarded failure: {pair:?}"),
    }
    settled(&rig, "the discarded upload is gone", || status(&chat, "c1", "update_photo").is_none()).await;
    assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 1);
    *shared.after_commit.lock().unwrap() = None;
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_confirmed_old_failed_cycle_does_not_discard_retry() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat is visible", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("PUT", Fault::Http(400));
    let first = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    settled(&rig, "the first cycle failed", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
    let copy = local_url(&chat, "c1", "update_photo").unwrap();
    let held = rig.world.hold_next("PUT");
    let retry = bound(rig.database.retry(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()), None)).await.unwrap();
    held.entered().await;
    assert!(matches!(bound(rig.database.confirmed(first, true)).await, Err(crate::ffi::DbError::Rejected { kept: true, .. })));
    assert!(std::path::Path::new(copy.strip_prefix("file://").unwrap()).exists());
    assert!(local_url(&chat, "c1", "update_photo").is_some());
    held.release();
    assert!(bound(rig.database.confirmed(retry, false)).await.is_ok());
    rig.database.close_now();
}

/// P-APP-08: the photo of a group goes through the steps of the project, the chat shows `uploads["update_photo"]` from the commit that queued it until the commit that ends it, and the logo of the answer after it.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_photo_upload() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered().await;
    settled(&rig, "the chat shows the upload while the file is in the network", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Uploading)).await;
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    settled(&rig, "the logo of the answer stands and the upload is gone", || cell(&rig, "Chat", &chat.0.lock().unwrap()[0], "logoUrl") == super::flow_fixtures::text("https://cdn/logo-sig1") && status(&chat, "c1", "update_photo").is_none()).await;
    assert_eq!((rig.world.count("CreateBlob"), puts(&rig), rig.world.count("ChatUpdatePhoto")), (1, 1, 1), "one blob, one PUT, one mutation");
    rig.database.close_now();
}

/// P-XFER-23: the row shows the file URL of the copy from the end of the copy until the transfer is removed.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_local_url() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered().await;
    settled(&rig, "the copy has a URL", || local_url(&chat, "c1", "update_photo").is_some()).await;
    let url = local_url(&chat, "c1", "update_photo").unwrap();
    assert!(url.starts_with("file://") && std::path::Path::new(&url["file://".len()..]).exists(), "{url}");
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    settled(&rig, "the URL goes with the transfer", || local_url(&chat, "c1", "update_photo").is_none() && status(&chat, "c1", "update_photo").is_none()).await;
    rig.database.close_now();
}

/// P-APP-14, P-APP-08: a refused file fails its upload on the row with the error, `retry` sends it again, a second refusal is removed by `discard`.
#[tokio::test(flavor = "multi_thread")]
async fn flow_chat_photo_retry_and_discard() {
    let rig = rig().await;
    let chat = entity(&rig, "Chat", "c1");
    settled(&rig, "the chat shows", || chat.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();

    rig.world.fail_next("PUT", Fault::Http(400));
    bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    settled(&rig, "the upload failed on the row", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;

    let again = bound(rig.database.retry(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()), None)).await.unwrap();
    bound(rig.database.confirmed(again, false)).await.unwrap();
    settled(&rig, "the retry delivered the logo", || cell(&rig, "Chat", &chat.0.lock().unwrap()[0], "logoUrl") != Cell::Null && status(&chat, "c1", "update_photo").is_none()).await;

    let (_other_directory, other) = source("logo2.jpg", b"another jpeg");
    rig.world.fail_next("PUT", Fault::Http(400));
    bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&other, json!({"id": "c1"})))).await.unwrap();
    settled(&rig, "the second upload failed", || status(&chat, "c1", "update_photo") == Some(WireUploadStatus::Failed)).await;
    bound(rig.database.discard(epoch, "Chat".into(), "c1".into(), Some("update_photo".into()))).await.unwrap();
    settled(&rig, "the discard removed the upload", || status(&chat, "c1", "update_photo").is_none()).await;
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
    settled(&rig, "both places show the chat", || chat.len() == 1 && list.len() == 1).await;
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");

    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered().await;
    settled(&rig, "both places show the uploading upload", || [&chat, &list].iter().all(|rows| status(rows, "c1", "update_photo") == Some(WireUploadStatus::Uploading))).await;
    let first = |rows: &Rows| rows.1.lock().unwrap().iter().find(|seen| seen.rows.iter().any(|row| row.uploads.iter().any(|entry| entry.action == "update_photo"))).map(|seen| seen.revision);
    assert!(first(&chat).is_some() && first(&list).is_some(), "a place never showed the upload {:?} {:?}", first(&chat), first(&list));
    one_revision(&rig, &[&chat, &list]).await;
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    settled(&rig, "both places end the upload", || [&chat, &list].iter().all(|rows| status(rows, "c1", "update_photo").is_none())).await;
    rig.database.close_now();
}

async fn status_until(stream: &crate::ffi::ReadStream, what: &str, wanted: impl Fn(&crate::ffi::ReadValue) -> bool) -> crate::ffi::ReadValue {
    loop {
        let value = crate::test_wait::within(stream.pull_next()).await.unwrap_or_else(|| panic!("the stream ended before {what}"));
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
    settled(&rig, "the chat shows", || chat.len() == 1).await;
    let stream = blob_stream(&rig, "update_photo");
    let (_directory, path) = source("logo.jpg", b"jpeg");
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_next("PUT");
    let receipt = bound(rig.database.upload(epoch, "Chat".into(), "c1".into(), "update_photo".into(), request(&path, json!({"id": "c1"})))).await.unwrap();
    hold.entered().await;
    let ReadValue::Status { local_url: Some(url), .. } = status_until(&stream, "the copy has a URL", |value| matches!(value, ReadValue::Status { local_url: Some(_), .. })).await else { unreachable!() };
    assert!(url.starts_with("file://") && std::path::Path::new(&url["file://".len()..]).exists(), "{url}");
    hold.release();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    status_until(&stream, "the URL goes with the transfer", |value| matches!(value, ReadValue::Status { local_url: None, .. })).await;
    rig.database.close_now();
}

/// P-XFER-26 through the stream `Blob`: the status carries `processing` while the row of the server says the file is not ready, and drops it at the terminal value.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_status_processing() {
    use crate::ffi::{ReadValue, WireProcessing};
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
        add_chat(models);
        add_uploads(models);
        let chat = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
        let fields = chat["fields"].as_array_mut().unwrap();
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoState", crate::value::ScalarKind::Str).nullable().optional()).unwrap());
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoProgress", crate::value::ScalarKind::Num).nullable().optional()).unwrap());
        let action = chat["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "update_photo").unwrap();
        action["processing"] = json!({"field": "photoState", "done": ["ready"], "progress": "photoProgress"});
    })).await;
    rig.land("Chat", chat_row("c1", 0)).await;
    let stream = blob_stream(&rig, "update_photo");
    let mut working = chat_row("c1", 0);
    working["photoState"] = json!("working");
    working["photoProgress"] = json!(0.5);
    rig.land("Chat", working).await;
    let shown = status_until(&stream, "the processing shows", |value| matches!(value, ReadValue::Status { processing: Some(_), .. })).await;
    assert!(matches!(shown, ReadValue::Status { processing: Some(WireProcessing { progress: Some(progress) }), .. } if (progress - 0.5).abs() < f64::EPSILON));
    let mut ready = chat_row("c1", 0);
    ready["photoState"] = json!("ready");
    rig.land("Chat", ready).await;
    status_until(&stream, "the terminal value drops the processing", |value| matches!(value, ReadValue::Status { processing: None, .. })).await;
    rig.database.close_now();
}


/// xfer_rowless_processing: for a key whose row does not exist the status carries no `processing`; when a row of the server lands and says the file is not ready, the same stream shows its processing, and the terminal value drops it.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_rowless_processing() {
    use crate::ffi::{ReadValue, WireProcessing};
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|models| {
        add_chat(models);
        add_uploads(models);
        let chat = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
        let fields = chat["fields"].as_array_mut().unwrap();
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoState", crate::value::ScalarKind::Str).nullable().optional()).unwrap());
        fields.push(serde_json::to_value(crate::schema::FieldSpec::scalar("photoProgress", crate::value::ScalarKind::Num).nullable().optional()).unwrap());
        let action = chat["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "update_photo").unwrap();
        action["processing"] = json!({"field": "photoState", "done": ["ready"], "progress": "photoProgress"});
    })).await;
    let stream = blob_stream(&rig, "update_photo");
    let first = status_until(&stream, "the status of a missing row opens", |value| matches!(value, ReadValue::Status { .. })).await;
    assert!(matches!(first, ReadValue::Status { processing: None, .. }), "no processing before a row exists: {first:?}");
    let mut working = chat_row("c1", 0);
    working["photoState"] = json!("working");
    working["photoProgress"] = json!(0.5);
    rig.land("Chat", working).await;
    let shown = status_until(&stream, "the processing of the landed row shows", |value| matches!(value, ReadValue::Status { processing: Some(_), .. })).await;
    assert!(matches!(shown, ReadValue::Status { processing: Some(WireProcessing { progress: Some(progress) }), .. } if (progress - 0.5).abs() < f64::EPSILON), "{shown:?}");
    let mut ready = chat_row("c1", 0);
    ready["photoState"] = json!("ready");
    rig.land("Chat", ready).await;
    status_until(&stream, "the terminal value drops the processing", |value| matches!(value, ReadValue::Status { processing: None, .. })).await;
    rig.database.close_now();
}
