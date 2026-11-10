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
    document_answer()
}

fn document_answer() -> Value {
    json!({"userPhotoUpdate": {"moment": {"id": "m1"}, "userPhoto": {"id":"p1","kind":"MAIN_PICTURE","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","reason":null,"status":"GOOD","media":{"thumbUrl":"https://t/1"}}, "viewer": {"user": {"id":"u1","fullName":"Ann","username":"ann","online":true,"shareUrl":"s","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","avatarUrl":"a","fullAvatarUrl":null,"age":null,"gender":null,"description":null,"story":null,"lastSeenAt":null,"distance":null,"relationship":null}}}})
}

async fn seed_moment(stand: &Stand) {
    stand.answer("MomentNode", json!({"node": super::stand::moment("m1", 0)}));
    bound(stand.database.node(stand.database.session_epoch(), "Moment".into(), "node".into(), "m1".into(), true)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_confirmed_after_reopen_waits_for_new_upload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    first.answer("UserPhotoUpdate", good());
    first.script_put(200, Some("etag"));
    let _receipt = bound(first.database.upload(first.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    arrived(&first, "UserPhotoUpdate").await;
    first.settle().await;
    assert!(operations(&first).await.is_empty());
    bound(first.database.close_database()).await.unwrap();

    let reopened = Stand::prepare_on(Some(path), credentials).await;
    reopened.sign_in().await;
    reopened.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    reopened.answer_next("UserPhotoUpdate", json!({"data":{"userPhotoUpdate":null},"errors":[{"message":"Please select a profile picture with a clearly visible face.","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
    reopened.hold(&["UserPhotoUpdate"]);
    reopened.script_put(200, Some("etag"));
    let receipt = bound(reopened.database.upload(reopened.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    arrived(&reopened, "UserPhotoUpdate").await;
    let before = reopened.database.life.waiting_calls();
    let db = reopened.database.clone();
    let waiting = tokio::spawn(crate::runtime::counted(Some(db.life.launch()), async move { db.confirmed(receipt, true).await }));
    crate::test_wait::settled(&reopened, "receipt waits for its own mutation", || waiting.is_finished() || reopened.database.life.waiting_calls() > before).await;
    if waiting.is_finished() { panic!("new receipt returned before its mutation: {:?}", bound(waiting).await.unwrap()); }
    reopened.release(&["UserPhotoUpdate"]);
    let result = bound(waiting).await.unwrap();
    assert!(matches!(result, Err(DbError::Rejected { kept: false, .. })), "new receipt: {result:?}");
    assert!(operations(&reopened).await.is_empty());
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_reopen_discards_orphan_and_sends_pending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    first.sign_in().await;
    first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    first.answer_next("UserPhotoUpdate", json!({"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
    first.script_put(200, Some("etag"));
    let epoch = first.database.session_epoch();
    let failed = bound(first.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(matches!(bound(first.database.confirmed(failed, false)).await, Err(DbError::Rejected { kept: true, .. })));
    first.hold(&["UserPhotoUpdate"]);
    first.script_put(200, Some("etag"));
    let _pending = bound(first.database.upload(epoch, "CurrentUser".into(), "other".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    loop {
        let received = first.asked().len();
        if first.requests("UserPhotoUpdate") >= 2 { break; }
        first.received(received + 1).await;
    }
    assert_eq!(first.requests("UserPhotoUpdate"), 2);
    let items = operations(&first).await;
    let url = items.iter().find(|item| item.row_id == "other").unwrap().upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert!(copy.exists());
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute("DELETE FROM sync_upload WHERE model = 'CurrentUser' AND row_id = 'singleton'", []).unwrap();
    assert_eq!(connection.execute("UPDATE sync_upload SET row_id = 'singleton' WHERE model = 'CurrentUser' AND row_id = 'other'", []).unwrap(), 1);
    connection.execute("UPDATE op_row SET row_id = 'singleton' WHERE row_id = 'other'", []).unwrap();
    connection.execute("UPDATE op SET temp_id = 'singleton' WHERE temp_id = 'other'", []).unwrap();
    assert_eq!(connection.execute("UPDATE op SET status = 'pending', attempts = 0 WHERE transfer_id = (SELECT transfer_id FROM sync_upload WHERE model = 'CurrentUser')", []).unwrap(), 1, "the pending mutation is saved in the image");
    let saved: (i64, i64) = connection.query_row("SELECT completed, op_id FROM sync_upload WHERE model = 'CurrentUser'", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(saved.0, 1, "the saved upload reached its mutation: {saved:?}");
    connection.execute("DELETE FROM encrypted_pending", []).unwrap();
    drop(connection);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials, None, false).await;
    reopened.answer("UserPhotoUpdate", good());
    reopened.database.set_online(true).unwrap();
    reopened.settle().await;
    let items = operations(&reopened).await;
    assert!(items.is_empty(), "the orphan is removed and the pending upload completes: {items:?}");
    assert!(!copy.exists(), "the completed upload copy is removed");
    assert_eq!(reopened.requests("UserPhotoUpdate"), 1);
    let connection = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(connection.query_row("SELECT kind FROM transfer_outcome WHERE transfer_id = 1 AND cycle_id = 1", [], |row| row.get::<_, String>(0)).unwrap(), "discarded");
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_forbidden_discard_and_next_upload() {
    let mut cases = Vec::new();
    for data_present in [false, true] {
        for wait_before_failure in [false, true] {
            for discard in [false, true] {
                cases.push(tokio::spawn(async move {
                    let stand = Stand::open().await;
                    let epoch = stand.database.session_epoch();
                    stand.answer("myProfile", json!({"viewer": viewer()}));
                    bound(stand.database.refresh(epoch, "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
                    assert!(bound(stand.database.read_row(epoch, "CurrentUser".into(), "u1".into())).await.unwrap().is_some());
                    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
                    stand.answer("UserPhotoUpdate", document_answer());
                    let mut refusal = json!({"errors":[{"message":"Please select a profile picture with a clearly visible face.","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]});
                    if data_present { refusal["data"] = json!({"userPhotoUpdate":null}); }
                    stand.answer_next("UserPhotoUpdate", refusal);
                    stand.hold(&["UserPhotoUpdate"]);
                    stand.script_put(200, Some("etag"));
                    let directory = tempfile::tempdir().unwrap();
                    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
                    arrived(&stand, "UserPhotoUpdate").await;
                    let items = operations(&stand).await;
                    assert_eq!(items.len(), 1);
                    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
                    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
                    assert!(copy.exists());
                    let before = stand.database.life.waiting_calls();
                    let waiting = if wait_before_failure {
                        let db = stand.database.clone();
                        let receipt = receipt.clone();
                        Some(tokio::spawn(crate::runtime::counted(Some(db.life.launch()), async move { db.confirmed(receipt, discard).await })))
                    } else { None };
                    if waiting.is_some() { crate::test_wait::settled(&stand, "confirmed waits for refusal", || stand.database.life.waiting_calls() > before).await; }
                    stand.release(&["UserPhotoUpdate"]);
                    if waiting.is_none() { stand.settle().await; }
                    let answer = if let Some(waiting) = waiting { bound(waiting).await.unwrap() } else { bound(stand.database.confirmed(receipt, discard)).await };
                    let Err(DbError::Rejected { failure, kept }) = answer else { panic!("unexpected outcome: {answer:?}") };
                    assert_eq!(failure.kind, "rejected");
                    assert_eq!(failure.message, "Please select a profile picture with a clearly visible face.");
                    let extensions = failure.extensions.unwrap();
                    assert_eq!(extensions.code.as_deref(), Some("FORBIDDEN"));
                    assert_eq!(extensions.fields.get("reasonCode").map(String::as_str), Some("\"no_face_detected\""));
                    assert_eq!(kept, !discard, "data={data_present}, waiting={wait_before_failure}");
                    assert_eq!(operations(&stand).await.len(), usize::from(!discard));
                    assert_eq!(copy.exists(), !discard);
                    if discard {
                        stand.script_put(200, Some("etag"));
                        let second = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
                        let outcome = bound(stand.database.confirmed(second, false)).await;
                        assert!(matches!(outcome, Ok(ref confirmed) if confirmed.value.as_ref().is_some_and(|row| row.id == "u1")), "second upload merges the existing row: {outcome:?}");
                        assert_eq!(stand.requests("UserPhotoUpdate"), 2);
                    } else {
                        let second = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await;
                        assert!(matches!(second, Err(DbError::InFlight { .. })), "transfer.md:133: {second:?}");
                        assert_eq!(stand.requests("UserPhotoUpdate"), 1);
                    }
                    stand.database.close_now();
                }));
            }
        }
    }
    for case in cases { bound(case).await.unwrap(); }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_document_response_lands_row() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    seed_moment(&stand).await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", document_answer());
    stand.script_put(200, Some("etag"));
    let directory = tempfile::tempdir().unwrap();
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    let answer = bound(stand.database.confirmed(receipt, false)).await.unwrap();
    assert!(answer.value.is_none(), "the incomplete target row is not created: {:?}", answer.value);
    assert!(bound(stand.database.read_row(epoch, "Moment".into(), "m1".into())).await.unwrap().is_some(), "the declared moment is retained");
    stand.database.close_now();
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
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    seed_moment(&stand).await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", good());
    stand.script_put(200, Some("etag"));
    stand.hold(&["CreateBlob", "UserPhotoUpdate"]);
    let (me, landed) = (crate::identity::RowId::new("me").unwrap(), crate::identity::RowId::new("u1").unwrap());
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_none() && stand.database.db.find("CurrentUser", &me).unwrap().is_none(), "no CurrentUser row before the upload");
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none(), "an upload without a row shows no optimistic row");
    let items = operations(&stand).await;
    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::Path::new(url.strip_prefix("file://").unwrap());
    assert_eq!(std::fs::read(copy).unwrap(), b"jpeg-bytes");
    arrived(&stand, "CreateBlob").await;
    assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), (1, 0, 0), "CreateBlob is the first step: no PUT and no final mutation yet");
    stand.release(&["CreateBlob"]);
    arrived(&stand, "UserPhotoUpdate").await;
    assert_eq!((stand.puts(), stand.requests("UserPhotoUpdate")), (1, 1), "the PUT of the file is done before the final mutation is received");
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_none(), "no row before the answer of the final mutation");
    stand.release(&["UserPhotoUpdate"]);
    let confirmed = bound(stand.database.confirmed(receipt, false)).await.unwrap();
    let answer: Value = serde_json::from_str(&confirmed.answer_json.expect("the typed answer")).unwrap();
    assert_eq!((answer["moment"]["id"].as_str(), answer["userPhoto"]["status"].as_str(), answer["userPhoto"]["media"]["thumbUrl"].as_str()), (Some("m1"), Some("GOOD"), Some("https://t/1")), "the answer of the final mutation reaches the app by name");
    assert!(confirmed.value.is_none(), "the incomplete target row is not created: {:?}", confirmed.value);
    stand.settle().await;
    assert!(stand.database.db.find("CurrentUser", &landed).unwrap().is_none(), "the incomplete target is absent");
    assert!(bound(stand.database.read_row(epoch, "Moment".into(), "m1".into())).await.unwrap().is_some(), "the declared moment is retained");
    assert!(operations(&stand).await.is_empty());
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
    let outcome = bound(stand.database.confirmed(receipt, false)).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_retry_after_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    seed_moment(&stand).await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN"}}], "data": null}));
    stand.answer("UserPhotoUpdate", good());
    stand.script_put(200, Some("etag"));
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    assert!(matches!(bound(stand.database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));
    let items = operations(&stand).await;
    assert_eq!(items.len(), 1);
    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert_eq!(std::fs::read(&copy).unwrap(), b"jpeg-bytes");
    assert!(bound(stand.database.read_row(epoch, "CurrentUser".into(), "me".into())).await.unwrap().is_none());
    let retried = bound(stand.database.retry(epoch, "CurrentUser".into(), "me".into(), Some("update_photo".into()), None)).await.unwrap();
    assert!(retried.optimistic_value.is_none());
    let confirmed = bound(stand.database.confirmed(retried, false)).await.unwrap();
    assert!(confirmed.value.is_none());
    assert!(bound(stand.database.read_row(epoch, "Moment".into(), "m1".into())).await.unwrap().is_some());
    assert_eq!(stand.requests("UserPhotoUpdate"), 2);
    assert!(operations(&stand).await.is_empty());
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
    seed_moment(&stand).await;
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
    let row = bound(reopened.database.read_row(reopened.database.session_epoch(), "Moment".into(), "m1".into())).await.unwrap();
    assert!(row.is_some(), "the declared moment is retained");
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn rowless_upload_session_reset_clears_pending_work() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.hold_put();
    stand.script_put(200, Some("etag"));
    let directory = tempfile::tempdir().unwrap();
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    stand.put_started().await;
    assert_eq!(operations(&stand).await.len(), 1);
    stand.settle().await;
    let requests_before_reset = stand.asked().len();
    bound(stand.database.session_reset()).await.unwrap();
    stand.release_put();
    stand.settle().await;
    assert_eq!(stand.asked().len(), requests_before_reset, "no request follows the end of the session");
    assert_eq!(stand.requests("UserPhotoUpdate"), 0, "the old upload cannot send its final mutation");
    stand.sign_in().await;
    assert!(operations(&stand).await.is_empty(), "the old upload is removed after reset");
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_end_during_upload() {
    for stage in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.sqlite");
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
        stand.sign_in().await;
        let epoch = stand.database.session_epoch();
        stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
        stand.answer("UserPhotoUpdate", good());
        stand.script_put(200, Some("etag"));
        match stage {
            0 => stand.database.set_online(false).unwrap(),
            1 => stand.hold(&["CreateBlob"]),
            2 => stand.hold_put(),
            3 => stand.hold(&["UserPhotoUpdate"]),
            _ => unreachable!(),
        }
        let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
        assert!(receipt.optimistic_value.is_none());
        let items = operations(&stand).await;
        assert_eq!(items.len(), 1, "stage {stage}");
        let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
        let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
        assert!(copy.exists(), "stage {stage}");
        match stage {
            0 => assert_eq!(stand.requests("CreateBlob"), 0),
            1 => arrived(&stand, "CreateBlob").await,
            2 => stand.put_started().await,
            3 => arrived(&stand, "UserPhotoUpdate").await,
            _ => unreachable!(),
        }
        let before = (stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate"));
        bound(stand.database.session_reset()).await.unwrap();
        stand.release(&["CreateBlob", "UserPhotoUpdate"]);
        stand.release_put();
        stand.settle().await;
        assert!(!copy.exists(), "stage {stage}: copy remains after reset");
        assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), before, "stage {stage}: request after reset");
        stand.database.set_online(true).unwrap();
        stand.sign_in().await;
        stand.settle().await;
        assert!(operations(&stand).await.is_empty(), "stage {stage}: queue survives reset");
        assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), before, "stage {stage}: request in next session");
        stand.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_cancel_removes_copy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    let items = operations(&stand).await;
    assert_eq!(items.len(), 1);
    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert_eq!(std::fs::read(&copy).unwrap(), b"jpeg-bytes");
    assert!(bound(stand.database.cancel_upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into())).await.unwrap());
    assert!(operations(&stand).await.is_empty());
    assert!(!copy.exists());
    assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), (0, 0, 0));
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_failed_expiry_removes_copy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN"}}], "data": null}));
    stand.script_put(200, Some("etag"));
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(matches!(bound(stand.database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));
    let items = operations(&stand).await;
    assert_eq!(items.len(), 1);
    let url = items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert!(copy.exists());
    stand.database.set_online(false).unwrap();
    stand.clock.advance(7 * 24 * 60 * 60 * 1000 + 1);
    stand.database.set_online(true).unwrap();
    stand.settle().await;
    assert!(operations(&stand).await.is_empty());
    assert!(!copy.exists());
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_upload_rejected_and_expired() {
    for discard in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("app.sqlite");
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
        stand.sign_in().await;
        let epoch = stand.database.session_epoch();
        stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
        stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN"}}], "data": null}));
        stand.script_put(200, Some("etag"));
        let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
        assert!(matches!(bound(stand.database.confirmed(receipt.clone(), false)).await, Err(DbError::Rejected { kept: true, .. })));
        let first = operations(&stand).await;
        assert_eq!(first.len(), 1);
        let copy = std::path::PathBuf::from(first[0].upload.as_ref().unwrap().local_url.as_ref().unwrap().strip_prefix("file://").unwrap());
        assert!(copy.exists());
        stand.database.set_online(false).unwrap();
        let pending = WireUploadRequest { source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() };
        let neighbour = bound(stand.database.upload(epoch, "CurrentUser".into(), "other".into(), "update_photo".into(), pending)).await.unwrap();
        assert!(neighbour.optimistic_value.is_none());
        assert_eq!(operations(&stand).await.len(), 2);
        if discard {
            assert!(matches!(bound(stand.database.confirmed(receipt, true)).await, Err(DbError::Rejected { kept: false, .. })));
        } else {
            stand.clock.advance(7 * 24 * 60 * 60 * 1000 + 1);
            stand.database.set_online(true).unwrap();
            stand.settle().await;
        }
        let remaining = operations(&stand).await;
        assert_eq!(remaining.len(), 1, "discard={discard}: {remaining:?}");
        assert_eq!((remaining[0].model.as_str(), remaining[0].row_id.as_str(), remaining[0].action.as_str()), ("CurrentUser", "other", "update_photo"));
        assert_eq!(remaining[0].upload.as_ref().unwrap().status, crate::ffi::wire::WireUploadStatus::Waiting);
        assert!(!copy.exists(), "discard={discard}: failed upload copy survives");
        assert_eq!(stand.requests("UserPhotoUpdate"), 1);
        stand.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_accept_order() {
    let stand = Stand::open().await;
    let directory = tempfile::tempdir().unwrap();
    let epoch = stand.database.session_epoch();
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    let refused = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), photo(&directory))).await;
    assert!(matches!(refused, Err(DbError::InFlight { .. })), "{refused:?}");
    let invalid = || WireUploadRequest::of(&UploadFile::new(""));
    let refused = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), invalid())).await;
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "{refused:?}");
    assert_eq!(operations(&stand).await.len(), 1);
    assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), (0, 0, 0));
    bound(stand.database.session_reset()).await.unwrap();
    let refused = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), invalid())).await;
    assert!(matches!(refused, Err(DbError::NoSession { .. })), "{refused:?}");
    stand.database.close_now();
    let refused = bound(stand.database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), invalid())).await;
    assert!(matches!(refused, Err(DbError::Closed { .. })), "{refused:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rowless_multipart_steps() {
    use std::io::{Read, Seek, SeekFrom, Write};
    use super::super::upload_stand::{request, Stand as UploadStand, CREATE_MOMENT};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = UploadStand::new();
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let source = directory.path().join("large.jpg");
    let part_size = crate::owners::write::upload::UploadConfig::default().part_size as usize;
    let mut file = std::fs::File::create(&source).unwrap();
    file.write_all(&vec![b'a'; part_size]).unwrap();
    file.write_all(&vec![b'b'; part_size]).unwrap();
    drop(file);
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"}]}}}"#);
    stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"}]}}}"#);
    stand.script.data("CompleteMultipart", r#"{"data":{"completeMultipart":{"signedId":"multipart-signed","blobId":"b"}}}"#);
    let final_mutation = stand.script.hold("CreateMoment", r#"{"data":{"createMoment":{"id":"42","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#);
    let first = stand.puts.hold_next();
    let epoch = database.session_epoch();
    let receipt = bound(database.upload(epoch, "Moment".into(), "42".into(), "upload_clip".into(), request(&source))).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    tokio::select! {
        () = first.entered() => {},
        outcome = bound(database.confirmed(receipt.clone(), false)) => panic!("multipart ended before the first part: {outcome:?}"),
    }
    assert_eq!(stand.puts.seen.lock().unwrap().len(), 1);
    first.release();
    final_mutation.entered().await;
    let parts = stand.puts.seen.lock().unwrap().clone();
    assert_eq!(parts.len(), 2);
    for (index, part) in parts.iter().enumerate() {
        assert_eq!((part.offset, part.length), ((index * part_size) as u64, part_size as u64));
        let mut copy = std::fs::File::open(&part.path).unwrap();
        copy.seek(SeekFrom::Start(part.offset)).unwrap();
        let mut byte = [0];
        copy.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], [b'a', b'b'][index]);
    }
    let presigns = stand.script.variables("PresignParts");
    assert_eq!(presigns.len(), 2);
    assert_eq!((presigns[0]["uploadId"].as_str(), &presigns[0]["partNumbers"]), (Some("upload"), &json!([1])));
    assert_eq!((presigns[1]["uploadId"].as_str(), &presigns[1]["partNumbers"]), (Some("upload"), &json!([2])));
    assert_eq!(stand.script.count("CompleteMultipart"), 1);
    let final_requests = stand.script.variables("CreateMoment");
    assert_eq!(final_requests.len(), 1);
    assert!(final_requests[0].to_string().contains("multipart-signed"));
    final_mutation.release();
    bound(database.confirmed(receipt, false)).await.unwrap();
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_mutation_reports_the_end_cause() {
    for (close, in_flight) in [(false, false), (false, true), (true, false), (true, true)] {
        let stand = Stand::open().await;
        let epoch = stand.database.session_epoch();
        stand.answer("UserPhotoUpdate", good());
        stand.hold(&["UserPhotoUpdate"]);
        let running = if in_flight {
            let database = stand.database.clone();
            let running = tokio::spawn(async move { database.call(epoch, "UserPhotoUpdate".into(), "{}".into()).await });
            arrived(&stand, "UserPhotoUpdate").await;
            Some(running)
        } else { None };
        if close { stand.database.close_now(); } else { bound(stand.database.session_reset()).await.unwrap(); }
        let answer = match running {
            Some(running) => { stand.release(&["UserPhotoUpdate"]); bound(running).await.unwrap() }
            None => bound(stand.database.call(epoch, "UserPhotoUpdate".into(), "{}".into())).await,
        };
        if close {
            assert!(matches!(answer, Err(DbError::Closed { .. })), "close={close}, in_flight={in_flight}: {answer:?}");
        } else {
            assert!(matches!(answer, Err(DbError::NoSession { .. })), "close={close}, in_flight={in_flight}: {answer:?}");
        }
        assert_eq!(stand.requests("UserPhotoUpdate"), usize::from(in_flight));
        stand.database.close_now();
    }
}

fn second_photo(directory: &tempfile::TempDir) -> WireUploadRequest {
    let path = directory.path().join("second.jpg");
    std::fs::write(&path, b"second-jpeg-bytes").unwrap();
    let mut file = UploadFile::new(path.to_str().unwrap());
    file.variables = json!({"createInitialMoment": true}).as_object().unwrap().clone();
    WireUploadRequest::of(&file)
}

fn photo_answer(name: &str) -> Value {
    let mut result = good();
    result["userPhotoUpdate"]["viewer"]["user"]["fullName"] = json!(name);
    result
}

async fn send_photo(stand: &Stand, request: WireUploadRequest) -> Result<crate::ffi::WireOperationReceipt, DbError> {
    bound(stand.database.upload(stand.database.session_epoch(), "CurrentUser".into(), "me".into(), "update_photo".into(), request)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_second_upload_after_confirmed() {
    let stand = Stand::open().await;
    let directory = tempfile::tempdir().unwrap();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer_next("UserPhotoUpdate", json!({"data": photo_answer("First")}));
    stand.answer("UserPhotoUpdate", photo_answer("Second"));
    stand.script_put(200, Some("first"));
    stand.script_put(200, Some("second"));
    let first = send_photo(&stand, photo(&directory)).await.unwrap();
    let first_result = bound(stand.database.confirmed(first, false)).await.unwrap();
    assert!(first_result.value.is_some());
    let second = send_photo(&stand, second_photo(&directory)).await.unwrap();
    let second_result = bound(stand.database.confirmed(second, false)).await.unwrap();
    assert!(second_result.value.is_some());
    assert_eq!((stand.requests("CreateBlob"), stand.puts(), stand.requests("UserPhotoUpdate")), (2, 2, 2));
    let row = bound(stand.database.read_row(stand.database.session_epoch(), "CurrentUser".into(), "u1".into())).await.unwrap().unwrap();
    assert!(row.uploads.iter().all(|upload| upload.action != "update_photo"), "confirmed upload must be removed: {:?}", row.uploads);
    assert!(operations(&stand).await.is_empty());
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_second_upload_while_put_held() {
    let stand = Stand::open().await;
    let directory = tempfile::tempdir().unwrap();
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", good());
    stand.script_put(200, Some("first"));
    stand.hold_put();
    let first = send_photo(&stand, photo(&directory)).await.unwrap();
    stand.put_started().await;
    let blocked = send_photo(&stand, second_photo(&directory)).await;
    assert!(matches!(blocked, Err(DbError::InFlight { .. })), "the active upload occupies the key: {blocked:?}");
    stand.release_put();
    assert!(bound(stand.database.confirmed(first, false)).await.is_ok());
    assert_eq!((stand.puts(), stand.requests("UserPhotoUpdate")), (1, 1));
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_second_upload_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    stand.sign_in().await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer("UserPhotoUpdate", photo_answer("First"));
    stand.script_put(200, Some("first"));
    let first = send_photo(&stand, photo(&directory)).await.unwrap();
    assert!(bound(stand.database.confirmed(first, false)).await.is_ok());
    bound(stand.database.close_database()).await.unwrap();
    let reopened = Stand::prepare_on(Some(path), credentials).await;
    reopened.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    reopened.answer("UserPhotoUpdate", photo_answer("Second"));
    reopened.script_put(200, Some("second"));
    let second = send_photo(&reopened, second_photo(&directory)).await.unwrap();
    assert!(bound(reopened.database.confirmed(second, false)).await.is_ok());
    assert_eq!((reopened.requests("CreateBlob"), reopened.puts(), reopened.requests("UserPhotoUpdate")), (1, 1, 1));
    assert!(operations(&reopened).await.is_empty());
    reopened.database.close_now();
}
