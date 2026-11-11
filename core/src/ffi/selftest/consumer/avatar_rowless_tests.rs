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
    json!({"user": {"id": "u1", "fullName": "Ann", "username": "ann", "online": true, "shareUrl": "s", "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z", "avatarUrl": "a", "fullAvatarUrl": null, "age": null, "gender": null, "description": null, "story": null, "lastSeenAt": null, "distance": null, "relationship": null}, "profile": {"updatedAt": "2026-11-08T00:00:00Z", "registrationCompleted": false, "kind": "REGULAR", "hasCompass": false, "hasMoments": false, "hasPhoto": true, "hasGoodPhoto": true, "hasInitialMoment": true, "stickyLocation": false}, "settings": {"notifyMessages": true}, "counters": {"primaryChats": 0}})
}

fn good() -> Value {
    document_answer()
}

fn document_answer() -> Value {
    json!({"userPhotoUpdate": {"moment": {"id": "m1"}, "userPhoto": {"id":"p1","kind":"MAIN_PICTURE","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","reason":null,"status":"GOOD","media":{"thumbUrl":"https://t/1"}}, "viewer": {"user": {"id":"u1","fullName":"Ann","username":"ann","online":true,"shareUrl":"s","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","avatarUrl":"a","fullAvatarUrl":null,"age":null,"gender":null,"description":null,"story":null,"lastSeenAt":null,"distance":null,"relationship":null}}}})
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_upload_attempt_survives_death_after_mutate() {
    use rusqlite::OptionalExtension;

    fn copy_files(from: &std::path::Path, to: &std::path::Path) {
        if !from.exists() { return; }
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() { copy_files(&entry.path(), &to.join(entry.file_name())); }
            else { std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap(); }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("attempt.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.script_put(200, Some("etag"));
    stand.hold(&["UserPhotoUpdate"]);
    stand.settle().await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(connection);
    let mark = stand.request_mark();
    let _receipt = bound(stand.database.upload(stand.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    stand.receive_after(mark, "UserPhotoUpdate", None, 1).await;
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    let mut checked = 0;
    for (index, end) in commits.into_iter().enumerate() {
        let image = directory.path().join(format!("image-{index}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(std::path::Path::new(&format!("{}.files", path.display())), std::path::Path::new(&format!("{}.files", image.display())));
        let connection = rusqlite::Connection::open(&image).unwrap();
        let attempts: Option<i64> = connection.query_row("SELECT attempts FROM op WHERE origin_kind = 'transfer'", [], |row| row.get(0)).optional().unwrap();
        let pending: i64 = connection.query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap();
        drop(connection);
        let Some(attempts) = attempts else { continue };
        assert_eq!(attempts, 1, "commit {index}: first mutation attempt");
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.hold(&["UserPhotoUpdate"]);
        let mark = reopened.request_mark();
        reopened.database.set_online(true).unwrap();
        reopened.receive_after(mark, "UserPhotoUpdate", None, 1).await;
        let after: i64 = rusqlite::Connection::open(&image).unwrap().query_row("SELECT attempts FROM op WHERE origin_kind = 'transfer'", [], |row| row.get(0)).unwrap();
        assert_eq!(after, attempts + i64::from(pending > 0), "commit {index}, pending={pending}, requests after the reopen {}", reopened.requests_after(mark, "UserPhotoUpdate", None));
        checked += 1;
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert!(checked > 0, "WAL must contain the final mutation intent");
    bound(stand.database.close_database()).await.unwrap();
}

async fn seed_moment(stand: &Stand) {
    stand.answer("MomentNode", json!({"node": super::stand::moment("m1", 0)}));
    bound(stand.database.node(stand.database.session_epoch(), "Moment".into(), "node".into(), "m1".into(), true)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_power_saving_reaches_uploads() {
    use crate::ffi::wire::WireUploadStatus;

    for in_flight in [false, true] {
        let stand = Stand::open().await;
        let directory = tempfile::tempdir().unwrap();
        let mut profile = viewer();
        profile["user"]["id"] = json!("singleton");
        stand.answer("myProfile", json!({"viewer": profile}));
        let epoch = stand.database.session_epoch();
        bound(stand.database.refresh(epoch, "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
        stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
        stand.answer("UserPhotoUpdate", photo_answer("Confirmed"));
        stand.script_put(200, Some("etag"));
        stand.script_put(200, Some("etag"));
        let sink = stand.database.online_sink().unwrap();
        if in_flight {
            stand.hold_put();
        } else {
            sink.set_power_saving(true);
            stand.settle().await;
            stand.hold(&["CreateBlob"]);
        }
        let receipt = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
        if in_flight {
            stand.put_started().await;
            assert!(!stand.put_cancelled(0));
            sink.set_power_saving(true);
        }
        stand.settle().await;
        let row = bound(stand.database.read_row(epoch, "CurrentUser".into(), "singleton".into())).await.unwrap().unwrap();
        let upload = &row.uploads.iter().find(|entry| entry.action == "update_photo").unwrap().upload;
        assert_eq!((upload.status, upload.wait_reason.as_deref()), (WireUploadStatus::Queued, None), "in_flight={in_flight}: {upload:?}");
        if in_flight {
            assert!(stand.put_cancelled(0), "the in-flight PUT must be cancelled");
            assert_eq!(stand.puts(), 1);
        } else {
            assert_eq!((stand.requests("CreateBlob"), stand.puts()), (0, 0), "power saving blocks the first upload step");
        }
        assert_eq!(stand.requests("UserPhotoUpdate"), 0);
        sink.set_power_saving(false);
        stand.release(&["CreateBlob"]);
        stand.release_put();
        assert!(bound(stand.database.confirmed(receipt, false)).await.is_ok());
        stand.settle().await;
        assert_eq!(stand.puts(), if in_flight { 2 } else { 1 });
        assert!(stand.requests("CreateBlob") >= 1);
        assert_eq!(stand.requests("UserPhotoUpdate"), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_replaces_failed_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("failed.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    first.answer_next("UserPhotoUpdate", json!({"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
    first.script_put(200, Some("etag"));
    let _old = bound(first.database.upload(first.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    arrived(&first, "UserPhotoUpdate").await;
    first.settle().await;
    assert_eq!(operations(&first).await.len(), 1);
    bound(first.database.close_database()).await.unwrap();
    drop(first);

    let reopened = Stand::prepare_on(Some(path), credentials).await;
    reopened.sign_in().await;
    reopened.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    reopened.answer("UserPhotoUpdate", good());
    reopened.script_put(200, Some("etag"));
    let new = bound(reopened.database.upload(reopened.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(bound(reopened.database.confirmed(new, false)).await.is_ok());
    assert!(operations(&reopened).await.is_empty());
    assert_eq!(reopened.requests("UserPhotoUpdate"), 1);
    reopened.database.close_now();
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
    let failed_items = operations(&first).await;
    let failed_url = failed_items[0].upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let failed_copy = std::path::PathBuf::from(failed_url.strip_prefix("file://").unwrap());
    assert!(failed_copy.exists());
    first.hold(&["UserPhotoUpdate"]);
    first.script_put(200, Some("etag"));
    let mark = first.request_mark();
    let _pending = bound(first.database.upload(epoch, "CurrentUser".into(), "other".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    first.receive_after(mark, "UserPhotoUpdate", None, 1).await;
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
    assert!(!failed_copy.exists(), "the orphaned upload copy is removed");
    assert!(!copy.exists(), "the completed upload copy is removed");
    assert_eq!(reopened.requests("UserPhotoUpdate"), 1);
    let connection = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(connection.query_row("SELECT kind FROM transfer_outcome WHERE transfer_id = 1 AND cycle_id = 1", [], |row| row.get::<_, String>(0)).unwrap(), "discarded");
    reopened.database.close_now();
}

async fn repair_after_orphan(refused: bool) {
    use crate::ffi::{OperationQueueStatus, WireUploadStatus};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("repair.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    first.sign_in().await;
    let epoch = first.database.session_epoch();
    let mut profile = viewer();
    profile["user"]["id"] = json!("singleton");
    first.answer("myProfile", json!({"viewer": profile}));
    bound(first.database.refresh(epoch, "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
    first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    first.answer_next("UserPhotoUpdate", json!({"errors":[{"message":"old refusal","extensions":{"code":"FORBIDDEN"}}]}));
    first.script_put(200, Some("etag"));
    let old = bound(first.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(matches!(bound(first.database.confirmed(old, false)).await, Err(DbError::Rejected { kept: true, .. })));
    first.hold(&["UserPhotoUpdate"]);
    first.script_put(200, Some("etag"));
    let mark = first.request_mark();
    bound(first.database.upload(epoch, "CurrentUser".into(), "other".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    first.receive_after(mark, "UserPhotoUpdate", None, 1).await;
    let items = operations(&first).await;
    let copy_url = items.iter().find(|item| item.row_id == "other").unwrap().upload.as_ref().unwrap().local_url.as_ref().unwrap();
    let copy = std::path::PathBuf::from(copy_url.strip_prefix("file://").unwrap());
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let connection = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(connection.execute("DELETE FROM sync_upload WHERE model = 'CurrentUser' AND row_id = 'singleton'", []).unwrap(), 1);
    assert_eq!(connection.execute("UPDATE sync_upload SET row_id = 'singleton' WHERE model = 'CurrentUser' AND row_id = 'other'", []).unwrap(), 1);
    connection.execute("UPDATE op_row SET row_id = 'singleton' WHERE row_id = 'other'", []).unwrap();
    connection.execute("UPDATE op SET temp_id = 'singleton' WHERE temp_id = 'other'", []).unwrap();
    assert_eq!(connection.execute("UPDATE op SET status = 'pending', attempts = 0 WHERE transfer_id = (SELECT transfer_id FROM sync_upload WHERE model = 'CurrentUser')", []).unwrap(), 1);
    connection.execute("DELETE FROM encrypted_pending", []).unwrap();
    drop(connection);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials, None, false).await;
    let mut confirmed = good();
    confirmed["userPhotoUpdate"]["viewer"]["user"]["id"] = json!("singleton");
    confirmed["userPhotoUpdate"]["viewer"]["user"]["fullName"] = json!("Updated profile");
    reopened.answer("UserPhotoUpdate", confirmed);
    reopened.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    if refused {
        reopened.answer_next("UserPhotoUpdate", json!({"data":{"userPhotoUpdate":null},"errors":[{"message":"Please select a profile picture with a clearly visible face.","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
    }
    reopened.database.set_online(true).unwrap();
    reopened.settle().await;
    assert_eq!(reopened.requests("UserPhotoUpdate"), 1, "requests: {:?}", reopened.asked());
    let items = operations(&reopened).await;
    let epoch = reopened.database.session_epoch();
    if refused {
        assert_eq!(items.len(), 1, "failed operation must remain: {items:?}, requests: {:?}", reopened.asked());
        assert_eq!(items[0].status, OperationQueueStatus::Failed, "queue: {items:?}");
        let failure = items[0].error.as_ref().expect("failed operation has no error");
        assert_eq!(failure.kind, "rejected");
        assert_eq!(failure.extensions.as_ref().and_then(|extensions| extensions.fields.get("reasonCode")).map(String::as_str), Some("\"no_face_detected\""));
        let row = bound(reopened.database.read_row(epoch, "CurrentUser".into(), "singleton".into())).await.unwrap().unwrap();
        let upload = &row.uploads.iter().find(|upload| upload.action == "update_photo").unwrap().upload;
        assert_eq!(upload.status, WireUploadStatus::Failed);
        assert_eq!(upload.error.as_ref(), Some(failure));
        let connection = rusqlite::Connection::open(&path).unwrap();
        let retained: i64 = connection.query_row("SELECT COUNT(*) FROM op WHERE status = 'failed' AND seq = (SELECT op_id FROM sync_upload WHERE model = 'CurrentUser' AND row_id = 'singleton' AND action = 'update_photo')", [], |row| row.get(0)).unwrap();
        assert_eq!(retained, 1, "the saved operation must remain failed: {items:?}, requests: {:?}", reopened.asked());
        drop(connection);
        bound(reopened.database.discard(epoch, "CurrentUser".into(), "singleton".into(), Some("update_photo".into()))).await.unwrap();
        assert!(operations(&reopened).await.is_empty());
        assert!(!copy.exists());
        reopened.script_put(200, Some("etag"));
        let next = bound(reopened.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
        assert!(bound(reopened.database.confirmed(next, false)).await.is_ok());
    } else {
        assert!(items.is_empty(), "confirmed operation remains: {items:?}");
        assert!(!copy.exists());
        let row = reopened.database.db.read(&["CurrentUser"], |reader| reader.point("CurrentUser", "singleton")).unwrap().unwrap();
        assert_eq!(row["fullName"], json!("Updated profile"), "the mutation answer must update the profile");
    }
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_repair_refused_preserves_failed_operation() { repair_after_orphan(true).await; }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_repair_confirmed_clears_operation() { repair_after_orphan(false).await; }

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
                        stand.script_put(200, Some("etag"));
                        let second = bound(stand.database.upload(epoch, "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
                        let outcome = bound(stand.database.confirmed(second, false)).await;
                        assert!(matches!(outcome, Ok(ref confirmed) if confirmed.value.as_ref().is_some_and(|row| row.id == "u1")), "replacement upload merges the existing row: {outcome:?}");
                        assert!(!copy.exists());
                        assert_eq!(stand.requests("UserPhotoUpdate"), 2);
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
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN", "reasonCode": "no_face_detected"}}], "data": {"userPhotoUpdate": null}}));
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
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    seed_moment(&stand).await;
    stand.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN", "reasonCode": "no_face_detected"}}], "data": {"userPhotoUpdate": null}}));
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
    assert_eq!(stand.requests("UserPhotoUpdate"), 1);
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await;
    stand.sign_in().await;
    stand.answer("UserPhotoUpdate", good());
    let epoch = stand.database.session_epoch();
    assert_eq!(operations(&stand).await.len(), 1, "the failed upload survives reopening");
    assert_eq!(std::fs::read(&copy).unwrap(), b"jpeg-bytes");
    let retried = bound(stand.database.retry(epoch, "CurrentUser".into(), "me".into(), Some("update_photo".into()), None)).await.unwrap();
    assert!(retried.optimistic_value.is_none());
    let confirmed = bound(stand.database.confirmed(retried, false)).await.unwrap();
    assert!(confirmed.value.is_none());
    assert!(bound(stand.database.read_row(epoch, "Moment".into(), "m1".into())).await.unwrap().is_some());
    assert_eq!(stand.requests("UserPhotoUpdate"), 1);
    assert!(operations(&stand).await.is_empty());
    stand.database.close_now();
}

/// The server has received the first request of `operation`.
async fn arrived(stand: &Stand, operation: &str) {
    stand.receive_after(0, operation, None, 1).await;
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
    stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN", "reasonCode": "no_face_detected"}}], "data": {"userPhotoUpdate": null}}));
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
        stand.answer_next("UserPhotoUpdate", json!({"errors": [{"message": "no face", "extensions": {"code": "FORBIDDEN", "reasonCode": "no_face_detected"}}], "data": {"userPhotoUpdate": null}}));
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
    use crate::ffi::upload_wire::WireSource;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), Arc::new(WatchedHeld::default())).await;
    stand.sign_in().await;
    let database = &stand.database;
    let source = directory.path().join("large.jpg");
    let part_size = crate::owners::write::upload::UploadConfig::default().part_size as usize;
    let mut file = std::fs::File::create(&source).unwrap();
    file.write_all(&vec![b'a'; part_size]).unwrap();
    file.write_all(&vec![b'b'; part_size]).unwrap();
    drop(file);
    stand.answer_multipart();
    for part in 1..=2 {
        stand.answer_next("MultipartUploadPresignParts", json!({"data":{"multipartUploadPresign":{"parts":[{"partNumber":part,"url":format!("https://storage/{part}")}]}}}));
        stand.script_put(200, Some("etag"));
    }
    stand.answer("MultipartUploadComplete", json!({"multipartUploadComplete":{"signedId":"multipart-signed","blobId":"b"}}));
    stand.answer("UserPhotoUpdate", good());
    stand.hold(&["UserPhotoUpdate"]);
    stand.hold_put();
    let epoch = database.session_epoch();
    let request = WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: json!({"createInitialMoment":true}).to_string() };
    let receipt = bound(database.upload(epoch, "CurrentUser".into(), "me".into(), "update_photo".into(), request)).await.unwrap();
    assert!(receipt.optimistic_value.is_none());
    tokio::select! {
        () = stand.put_started() => {},
        outcome = bound(database.confirmed(receipt.clone(), false)) => panic!("multipart ended before the first part: {outcome:?}"),
    }
    assert_eq!(stand.puts(), 1);
    stand.release_put();
    arrived(&stand, "UserPhotoUpdate").await;
    let parts = stand.put_files();
    assert_eq!(parts.len(), 2);
    for (index, part) in parts.iter().enumerate() {
        assert_eq!((part.offset, part.length), ((index * part_size) as u64, part_size as u64));
        let mut copy = std::fs::File::open(&part.path).unwrap();
        copy.seek(SeekFrom::Start(part.offset)).unwrap();
        let mut byte = [0];
        copy.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], [b'a', b'b'][index]);
    }
    let requests = stand.asked();
    let presigns: Vec<_> = requests.iter().filter(|(operation, _)| operation == "MultipartUploadPresignParts").map(|(_, variables)| variables).collect();
    assert_eq!(presigns.len(), 2);
    assert_eq!((presigns[0]["uploadId"].as_str(), &presigns[0]["partNumbers"]), (Some("upload-1"), &json!([1])));
    assert_eq!((presigns[1]["uploadId"].as_str(), &presigns[1]["partNumbers"]), (Some("upload-1"), &json!([2])));
    assert_eq!(stand.requests("MultipartUploadComplete"), 1);
    let final_requests: Vec<_> = requests.iter().filter(|(operation, _)| operation == "UserPhotoUpdate").map(|(_, variables)| variables).collect();
    assert_eq!(final_requests.len(), 1);
    assert!(final_requests[0].to_string().contains("multipart-signed"));
    stand.release(&["UserPhotoUpdate"]);
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
    stand.answer("myProfile", json!({"viewer": viewer()}));
    bound(stand.database.refresh(stand.database.session_epoch(), "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn consumer_avatar_failure_kind_survives_reopen() {
    for initially_offline in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("failure.sqlite").to_string_lossy().into_owned();
        let credentials = Arc::new(WatchedHeld::default());
        let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
        first.sign_in().await;
        let mut profile = viewer();
        profile["user"]["id"] = json!("singleton");
        first.answer("myProfile", json!({"viewer": profile}));
        bound(first.database.refresh(first.database.session_epoch(), "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
        if initially_offline { first.database.set_online(false).unwrap(); }
        first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
        first.answer_next("UserPhotoUpdate", json!({"data":{"userPhotoUpdate":null},"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
        first.script_put(200, Some("etag"));
        let receipt = bound(first.database.upload(first.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
        if initially_offline {
            bound(first.database.close_database()).await.unwrap();
            drop(first);
            let reopened = Stand::prepare_online(Some(path.clone()), credentials.clone(), None, false).await;
            reopened.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
            reopened.answer_next("UserPhotoUpdate", json!({"data":{"userPhotoUpdate":null},"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
            reopened.script_put(200, Some("etag"));
            reopened.database.set_online(true).unwrap();
            reopened.settle().await;
            check_avatar_failure_kind(&reopened, &path).await;
            bound(reopened.database.close_database()).await.unwrap();
        } else {
            assert!(matches!(bound(first.database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));
            check_avatar_failure_kind(&first, &path).await;
            bound(first.database.close_database()).await.unwrap();
        }
        let reopened = Stand::prepare_online(Some(path.clone()), credentials, None, false).await;
        reopened.settle().await;
        check_avatar_failure_kind(&reopened, &path).await;
        bound(reopened.database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_failed_upload_consistent_at_every_commit() {
    use crate::ffi::OperationQueueStatus;

    fn copy_files(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() { copy_files(&entry.path(), &target); }
            else { std::fs::copy(entry.path(), target).unwrap(); }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    first.sign_in().await;
    let mut profile = viewer();
    profile["user"]["id"] = json!("singleton");
    first.answer("myProfile", json!({"viewer": profile}));
    bound(first.database.refresh(first.database.session_epoch(), "CurrentUser".into(), Some("profile".into()), "{}".into())).await.unwrap();
    first.settle().await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0, "checkpoint: {checkpoint:?}");
    drop(connection);
    first.answer("CreateBlob", serde_json::from_str(BLOB).unwrap());
    first.answer_next("UserPhotoUpdate", json!({"data":{"userPhotoUpdate":null},"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]}));
    first.script_put(200, Some("etag"));
    let receipt = bound(first.database.upload(first.database.session_epoch(), "CurrentUser".into(), "singleton".into(), "update_photo".into(), photo(&directory))).await.unwrap();
    assert!(matches!(bound(first.database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));
    first.settle().await;
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(commits.len() >= 2 && (wal.len() - 32) / frame_size < 1000, "WAL must retain multiple commits");
    let files = format!("{}.files", path.display());
    let mut observations = Vec::new();
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("image-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let image_files = format!("{}.files", image.display());
        if std::path::Path::new(&files).exists() {
            copy_files(std::path::Path::new(&files), std::path::Path::new(&image_files));
        }
        let (raw, terminal_at) = {
            let connection = rusqlite::Connection::open_with_flags(&image, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            let mut rows = Vec::new();
            for (table, query) in [
                ("op", "SELECT printf('%d|%s|%s|%d|%s|%s', seq, status, origin_kind, cycle_id, COALESCE(error_kind, ''), COALESCE(error_message, '')) FROM op"),
                ("sync_upload", "SELECT printf('%d|%d|%s|%s|%d|%s|%s', transfer_id, COALESCE(cycle_id, 0), status, stage, COALESCE(completed, 0), COALESCE(error_kind, ''), COALESCE(error_message, '')) FROM sync_upload"),
                ("transfer_outcome", "SELECT printf('%d|%d|%s|%s|%d', transfer_id, cycle_id, kind, COALESCE(value, ''), terminal_at) FROM transfer_outcome"),
            ] {
                let mut statement = connection.prepare(query).unwrap();
                let values = statement.query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
                rows.push(format!("{table}={values:?}"));
            }
            let terminal_at = rusqlite::OptionalExtension::optional(connection.query_row("SELECT terminal_at FROM transfer_outcome WHERE transfer_id = 1 AND cycle_id = 1 AND kind = 'failed'", [], |row| row.get::<_, i64>(0))).unwrap();
            (rows.join(" "), terminal_at)
        };
        let reopened = Stand::prepare_at(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false, 2_000_000).await;
        reopened.settle().await;
        let items = operations(&reopened).await;
        let operation = items.iter().find(|item| item.action == "update_photo");
        let row = bound(reopened.database.read_row(reopened.database.session_epoch(), "CurrentUser".into(), "singleton".into())).await.unwrap().unwrap();
        let upload = row.uploads.iter().find(|entry| entry.action == "update_photo").map(|entry| &entry.upload);
        observations.push(format!("k={k}: before=[{raw}], operation={operation:?}, upload={upload:?}"));
        if let Some(operation) = operation.filter(|operation| operation.status == OperationQueueStatus::Failed) {
            assert!(operation.error.is_some() && upload.and_then(|upload| upload.error.as_ref()) == operation.error.as_ref(), "{}", observations.join("\n"));
            if let Some(terminal_at) = terminal_at {
                let connection = rusqlite::Connection::open(&image).unwrap();
                let failed_at: Option<i64> = connection.query_row("SELECT failed_at FROM sync_upload WHERE transfer_id = 1", [], |row| row.get(0)).unwrap();
                assert_eq!(failed_at, Some(terminal_at), "{}", observations.join("\n"));
            }
        }
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(first.database.close_database()).await.unwrap();
}

async fn check_avatar_failure_kind(stand: &Stand, path: &str) {
    let items = operations(stand).await;
    let operation = items.iter().find(|item| item.action == "update_photo").expect("the refused operation remains");
    let failure = operation.error.as_ref().expect("the operation has a failure");
    assert_eq!(failure.kind, "rejected");
    assert_eq!(failure.extensions.as_ref().and_then(|e| e.fields.get("reasonCode")).map(String::as_str), Some("\"no_face_detected\""));
    let row = bound(stand.database.read_row(stand.database.session_epoch(), "CurrentUser".into(), "singleton".into())).await.unwrap().unwrap();
    let upload = &row.uploads.iter().find(|entry| entry.action == "update_photo").unwrap().upload;
    assert_eq!(upload.error.as_ref(), Some(failure), "the row and operation expose the same failure");
    let connection = rusqlite::Connection::open(path).unwrap();
    let (operation_kind, upload_kind): (String, String) = connection.query_row("SELECT op.error_kind, sync_upload.error_kind FROM op JOIN sync_upload ON sync_upload.op_id = op.seq WHERE sync_upload.model = 'CurrentUser' AND sync_upload.action = 'update_photo'", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(upload_kind, operation_kind, "the durable transfer and operation kinds agree");
}
