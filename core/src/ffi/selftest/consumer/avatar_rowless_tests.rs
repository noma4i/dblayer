//! D-287 on the schemas of the app: the avatar of the registration step uploads to a CurrentUser row that does not exist yet, in the order of calls of the app through the public `Database`; only the transport is replaced.
use std::sync::Arc;

use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{DbError, OperationInfo, ReadFetch, ReadRequest, ReadValue};
use crate::owners::write::upload::UploadFile;

const BLOB: &str = r#"{"directUploadCreate":{"directUpload":{"url":"https://storage/avatar","headers":[],"signedId":"signed-avatar","blobId":"b1"}}}"#;

fn viewer() -> Value {
    json!({"user": {"id": "u1", "fullName": "Ann", "username": "ann", "online": true, "shareUrl": "s", "createdAt": "2026-11-08T00:00:00Z", "avatarUrl": "a"}, "profile": {"updatedAt": "2026-11-08T00:00:00Z", "registrationCompleted": false, "kind": "REGULAR", "hasCompass": false, "hasMoments": false, "hasPhoto": true, "hasGoodPhoto": true, "hasInitialMoment": true, "stickyLocation": false}, "settings": {"notifyMessages": true}, "counters": {"primaryChats": 0}})
}

fn good() -> Value {
    json!({"userPhotoUpdate": {"moment": {"id": "m1"}, "userPhoto": {"reason": null, "status": "GOOD", "media": {"thumbUrl": "https://t/1"}}, "viewer": viewer()}})
}

fn photo(directory: &tempfile::TempDir) -> WireUploadRequest {
    let path = directory.path().join("avatar.jpg");
    std::fs::write(&path, b"jpeg-bytes").unwrap();
    let mut file = UploadFile::new(path.to_str().unwrap());
    file.variables = json!({"createInitialMoment": true}).as_object().unwrap().clone();
    WireUploadRequest::of(&file)
}

async fn operations(stand: &Stand) -> Vec<OperationInfo> {
    let stream = stand.database.subscribe_values(stand.database.session_epoch(), ReadRequest::Operations);
    match bound(stream.pull_next()).await {
        Some(ReadValue::Operations { items, .. }) => items,
        other => panic!("the operations stream opened with {other:?}"),
    }
}

/// transfer.md xfer_accept_rowless, xfer_rowless_confirmed: CreateBlob, PUT and UserPhotoUpdate run in this order with no CurrentUser row, the receipt shows no optimistic row, and the final answer lands the row and carries the typed answer.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_rowless_upload() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", good());
    stand.script_put(200, Some("etag"));
    stand.hold(&["CreateBlob", "UserPhotoUpdate"]);
    let directory = tempfile::tempdir().unwrap();
    let rows = stand.entity("CurrentUser", "u1", ReadFetch::Never);
    let (me, landed) = (crate::identity::RowId::new("me").unwrap(), crate::identity::RowId::new("u1").unwrap());
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_none() && stand.database.db.find("CurrentUser", &me).unwrap().is_none(), "no CurrentUser row before the upload");
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none(), "an upload without a row shows no optimistic row");
    arrived(&stand, "CreateBlob").await;
    assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), (1, 0, 0), "CreateBlob is the first step: no PUT and no final mutation yet");
    stand.release(&["CreateBlob"]);
    arrived(&stand, "UserPhotoUpdate").await;
    assert_eq!((stand.puts(), stand.requests("UserPhotoUpdate")), (1, 1), "the PUT of the file is done before the final mutation is received");
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_none(), "no row before the answer of the final mutation");
    stand.release(&["UserPhotoUpdate"]);
    let confirmed = bound(stand.database.confirmed(receipt)).await.unwrap();
    let answer: Value = serde_json::from_str(&confirmed.answer_json.expect("the typed answer")).unwrap();
    assert_eq!((answer["moment"]["id"].as_str(), answer["userPhoto"]["status"].as_str(), answer["userPhoto"]["media"]["thumbUrl"].as_str()), (Some("m1"), Some("GOOD"), Some("https://t/1")), "the answer of the final mutation reaches the app by name");
    assert!(matches!(confirmed.value, Some(ref row) if row.id == "u1"), "the answer landed the row: {:?}", confirmed.value);
    stand.settle().await;
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_some(), "the row of the answer is in the base");
    assert_eq!(stand.text_of(&rows, "CurrentUser", "u1", "fullName").as_deref(), Some("Ann"), "the row of the answer is shown");
    let sent = stand.asked().into_iter().find(|(operation, _)| operation == "UserPhotoUpdate").unwrap().1;
    assert_eq!((sent["input"]["signedId"].as_str(), sent["input"]["createInitialMoment"].as_bool()), (Some("signed-avatar"), Some(true)), "the signed id of the file goes into the input: {sent}");
    stand.database.close_now();
}

/// transfer.md xfer_rowless_confirmed (refusal): a permanent refusal of the photo check reaches `confirmed` with its extensions and leaves no row behind.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_rowless_rejected() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN", "reasonCode": "no_face_detected"}}], "data": null}));
    stand.script_put(200, Some("etag"));
    let directory = tempfile::tempdir().unwrap();
    let rows = stand.entity("CurrentUser", "u1", ReadFetch::Never);
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    let outcome = bound(stand.database.confirmed(receipt)).await;
    let Err(DbError::Rejected { failure, kept }) = outcome else { panic!("the refusal reaches the caller: {outcome:?}") };
    assert_eq!((failure.message.as_str(), failure.kind.as_str(), kept), ("no face", "rejected", true), "an upload keeps its failed operation, like every upload: {failure:?}");
    let extensions = failure.extensions.expect("the extensions of the refusal");
    assert_eq!(extensions.code.as_deref(), Some("FORBIDDEN"));
    assert_eq!(extensions.fields.get("reasonCode").map(String::as_str), Some("\"no_face_detected\""), "{extensions:?}");
    let items = operations(&stand).await;
    assert_eq!(items.len(), 1, "the refused upload remains queued");
    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert!(copy.exists(), "the refused upload retains its copy");
    bound(stand.database.discard(epoch, "CurrentUser".into(), "me".into(), Some("update_photo".into()))).await.unwrap();
    assert!(operations(&stand).await.is_empty(), "discard removes the queued upload");
    assert!(!copy.exists(), "discard removes its copy");
    stand.settle().await;
    assert!(rows.0.lock().unwrap().is_empty(), "a refused upload leaves no row");
    assert_eq!(stand.requests("UserPhotoUpdate"), 1, "a permanent refusal is not repeated");
    stand.database.close_now();
}

/// The server has received the first request of `operation`.
async fn arrived(stand: &Stand, operation: &str) {
    while stand.requests(operation) == 0 { stand.received(stand.asked().len() + 1).await; }
}

async fn operations_until(stream: &crate::ffi::ReadStream, what: &str, found: impl Fn(&[OperationInfo]) -> bool) -> (i64, Vec<OperationInfo>) {
    loop {
        match bound(stream.pull_next()).await {
            Some(ReadValue::Operations { revision, items }) if found(&items) => return (revision, items),
            Some(_) => {}
            None => panic!("the operations stream ended before {what}"),
        }
    }
}

/// transfer.md xfer_rowless_status_and_discover: the Operations view opened before the acceptance shows the transfer with no row and no receipt, then its start at a newer revision, once and with the value of `uploads[action]`; after a restart it shows the restored transfer with the same content.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_rowless_status_and_discover() {
    use crate::ffi::WireUploadStatus;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let stream = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    assert!(matches!(bound(stream.pull_next()).await, Some(ReadValue::Operations { ref items, .. }) if items.is_empty()), "nothing is queued before the acceptance");
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.hold_put();
    stand.script_put(200, Some("etag"));
    let _receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    let (shown_at, items) = operations_until(&stream, "the transfer shows", |items| items.len() == 1).await;
    assert_eq!((items[0].model.as_str(), items[0].row_id.as_str(), items[0].action.as_str()), ("CurrentUser", "me", "update_photo"), "the key of the transfer");
    assert!(items[0].upload.is_some(), "the value of the transfer is the one of uploads[action]: {items:?}");
    stand.put_started().await;
    let (moved_at, items) = operations_until(&stream, "the upload starts", |items| items.iter().any(|item| item.upload.as_ref().is_some_and(|upload| upload.status == WireUploadStatus::Uploading))).await;
    let upload = items[0].upload.clone().unwrap();
    assert_eq!((items.len(), upload.status, upload.total_bytes, upload.attempts), (1, WireUploadStatus::Uploading, 10, 1), "one entry with the status, the size and the attempt of the transfer: {items:?}");
    assert_eq!((items[0].status, upload.local_url.is_some()), (crate::ffi::OperationQueueStatus::Sending, true), "the copy of the file is shown: {items:?}");
    assert!(moved_at > shown_at, "the start of the upload comes at a newer revision: {shown_at} then {moved_at}");
    bound(stand.database.close_database()).await.unwrap();
    let reopened = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await;
    reopened.hold_put();
    reopened.script_put(200, Some("etag"));
    reopened.answer("UserPhotoUpdate", good());
    reopened.sign_in().await;
    let items = operations(&reopened).await;
    assert_eq!(items.len(), 1, "the transfer is found once after the restart, with no row and no receipt: {items:?}");
    let restored = items[0].upload.clone().expect("the value of the restored transfer");
    assert_eq!((items[0].model.as_str(), items[0].row_id.as_str(), items[0].action.as_str(), restored.total_bytes), ("CurrentUser", "me", "update_photo", 10), "{items:?}");
    reopened.put_started().await;
    reopened.release_put();
    arrived(&reopened, "UserPhotoUpdate").await;
    reopened.settle().await;
    assert_eq!(reopened.requests("UserPhotoUpdate"), 1, "the restored upload completes its mutation");
    assert!(operations(&reopened).await.is_empty(), "the completed upload leaves the queue");
    reopened.database.close_now();
}
