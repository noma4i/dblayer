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

/// transfer.md xfer_accept_rowless, xfer_rowless_confirmed: CreateBlob, PUT and UserPhotoUpdate run in the order of the app with no CurrentUser row, the receipt shows no optimistic row, and the final answer lands the row and carries the typed answer.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_rowless_upload() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", good());
    stand.script_put(200, Some("etag"));
    let directory = tempfile::tempdir().unwrap();
    let rows = stand.entity("CurrentUser", "u1", ReadFetch::Never);
    assert!(rows.0.lock().unwrap().is_empty(), "no CurrentUser row before the upload");
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none(), "an upload without a row shows no optimistic row");
    let confirmed = bound(stand.database.confirmed(receipt)).await.unwrap();
    let answer: Value = serde_json::from_str(&confirmed.answer_json.expect("the typed answer")).unwrap();
    assert_eq!((answer["moment"]["id"].as_str(), answer["userPhoto"]["status"].as_str(), answer["userPhoto"]["media"]["thumbUrl"].as_str()), (Some("m1"), Some("GOOD"), Some("https://t/1")), "the answer of the final mutation reaches the app by name");
    assert!(matches!(confirmed.value, Some(ref row) if row.id == "u1"), "the answer landed the row: {:?}", confirmed.value);
    stand.settle().await;
    assert_eq!(stand.text_of(&rows, "CurrentUser", "u1", "fullName").as_deref(), Some("Ann"), "the row of the answer is shown");
    let order: Vec<String> = stand.asked().into_iter().map(|(operation, _)| operation).filter(|operation| operation == "CreateBlob" || operation == "UserPhotoUpdate").collect();
    assert_eq!((order, stand.puts()), (vec!["CreateBlob".to_string(), "UserPhotoUpdate".to_string()], 1), "the order of the steps and one PUT");
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
    assert_eq!((failure.message.as_str(), failure.kind.as_str(), kept), ("no face", "rejected", false), "{failure:?}");
    let extensions = failure.extensions.expect("the extensions of the refusal");
    assert_eq!(serde_json::to_value(&extensions).unwrap()["reasonCode"], "no_face_detected");
    stand.settle().await;
    assert!(rows.0.lock().unwrap().is_empty(), "a refused upload leaves no row");
    assert_eq!(stand.requests("UserPhotoUpdate"), 1, "a permanent refusal is not repeated");
    stand.database.close_now();
}

/// transfer.md xfer_rowless_status_and_discover: the Operations view shows the active upload with no row and no Outbox intent, once, with its status; after a restart it shows the restored transfer again.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_rowless_status_and_discover() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.hold_put();
    stand.script_put(200, Some("etag"));
    let _receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    stand.put_started().await;
    let items = operations(&stand).await;
    assert_eq!(items.len(), 1, "the active upload shows once: {items:?}");
    assert_eq!((items[0].model.as_str(), items[0].row_id.as_str(), items[0].action.as_str()), ("CurrentUser", "me", "update_photo"), "the key of the transfer");
    assert!(items[0].upload.is_some(), "the value of the transfer is the one of uploads[action]: {items:?}");
    bound(stand.database.close_database()).await.unwrap();
    let reopened = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await;
    reopened.hold_put();
    reopened.script_put(200, Some("etag"));
    reopened.sign_in().await;
    let items = operations(&reopened).await;
    assert_eq!(items.len(), 1, "the transfer is found after the restart with no row and no receipt: {items:?}");
    assert_eq!((items[0].model.as_str(), items[0].action.as_str()), ("CurrentUser", "update_photo"));
    reopened.database.close_now();
}
