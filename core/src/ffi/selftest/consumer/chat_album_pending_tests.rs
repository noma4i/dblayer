use serde_json::json;

use super::stand::{message, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadSource};
use crate::ffi::{DbError, ReadRequest, ReadValue};

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_album_pending_source_cancel_fail_retry_and_confirm() {
    let stand = Stand::open().await;
    stand.script_put(200, Some("etag"));
    stand.script_put(200, Some("etag"));
    let database = &stand.database;
    let epoch = database.session_epoch();
    let items = (0..3).map(|index| WireUploadItem {
        values_json: json!({"chatId":"c1", "kind":"TEXT", "clientKey":format!("key-{index}"), "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z", "mediaGroup":{"id":"album", "position":index, "size":3}}).to_string(),
        source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into(),
    }).collect();
    let query = crate::ffi::QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let stream = database.subscribe_values(epoch, ReadRequest::List { query });
    bound(stream.pull_next()).await.unwrap();
    stand.settle().await;
    let before = stand.asked().len();
    let receipt = bound(database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    let snapshot_stream = database.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#) });
    let snapshot = bound(snapshot_stream.pull_next()).await.unwrap();
    assert!(matches!(snapshot, ReadValue::Snapshot { ref rows, revision, .. } if rows.len() == 3 && revision == receipt.items[0].receipt.revision));
    let requests = stand.asked();
    assert!(requests[before..].is_empty(), "unexpected requests before source: {:?}", &requests[before..]);
    assert_eq!(stand.puts(), 0);
    let ids: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    bound(database.cancel_upload(epoch, "Message".into(), ids[1].clone(), "send_files".into())).await.unwrap();
    for (id, position) in [(&ids[0], 0), (&ids[2], 1)] {
        let row = database.db.read(&["Message"], |reader| reader.point("Message", id)).unwrap().unwrap();
        assert_eq!(row["mediaGroup"]["position"], json!(position));
        assert_eq!(row["mediaGroup"]["size"], json!(2));
    }
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
    let mut first = message("m0", "c1", "");
    first["clientKey"] = json!("key-0");
    let mut third = message("m2", "c1", "");
    third["clientKey"] = json!("key-2");
    stand.answer("MessageAlbumSend", json!({"messageAlbumSend":{"messages":[first, third]}}));
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first");
    let third_path = directory.path().join("third");
    std::fs::write(&first_path, b"first-image").unwrap();
    std::fs::write(&third_path, b"third-image").unwrap();
    let source = |path: &std::path::Path| WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: r#"{"media":{"width":100,"height":80}}"#.into(), variables_json: "{}".into() };
    let status = database.subscribe_values(epoch, ReadRequest::Blob { model: "Message".into(), id: ids[2].clone(), action: "send_files".into() });
    bound(status.pull_next()).await.unwrap();
    stand.hold_put();
    bound(database.supply_source(epoch, "Message".into(), ids[2].clone(), "send_files".into(), source(&third_path))).await.unwrap();
    stand.put_started().await;
    loop {
        if matches!(bound(status.pull_next()).await, Some(ReadValue::Status { state, progress, .. }) if state == "uploading" && progress <= 100) { break; }
    }
    stand.release_put();
    bound(database.fail_source(epoch, "Message".into(), ids[0].clone(), "send_files".into(), "preparation".into())).await.unwrap();
    assert!(matches!(bound(database.confirmed(receipt.items[0].receipt.clone(), false)).await, Err(DbError::Rejected { failure, kept: true }) if failure.kind == "source_failed"));
    let retry = bound(database.retry(epoch, "Message".into(), ids[0].clone(), Some("send_files".into()), Some(source(&first_path)))).await.unwrap();
    assert!(matches!(bound(database.confirmed(retry, false)).await.unwrap().value, Some(row) if row.id == "m0"));
    assert!(matches!(bound(database.confirmed(receipt.items[2].receipt.clone(), false)).await.unwrap().value, Some(row) if row.id == "m2"));
    assert_eq!(stand.requests("MessageAlbumSend"), 1);
    assert_eq!(stand.puts(), 2);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_chat_album_pending_source_restart_and_reset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = std::sync::Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let items = (0..2).map(|index| WireUploadItem {
        values_json: json!({"chatId":"c1", "kind":"TEXT", "clientKey":format!("key-{index}"), "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
        source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into(),
    }).collect();
    let receipt = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    let ids: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);
    let reopened = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await;
    reopened.settle().await;
    let epoch = reopened.database.session_epoch();
    for id in &ids {
        let row = reopened.database.peek_row(epoch, "Message".into(), id.clone()).unwrap().unwrap();
        let upload = row.uploads.iter().find(|entry| entry.action == "send_files").unwrap();
        assert_eq!(upload.upload.status, crate::ffi::wire::WireUploadStatus::Failed);
        assert_eq!(upload.upload.error.as_ref().unwrap().kind, "missing_file");
    }
    reopened.database.set_online(false).unwrap();
    let source_path = directory.path().join("source");
    std::fs::write(&source_path, b"image").unwrap();
    let source = WireUploadSource { path: source_path.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into() };
    bound(reopened.database.retry(epoch, "Message".into(), ids[0].clone(), Some("send_files".into()), Some(source))).await.unwrap();
    let row = reopened.database.peek_row(epoch, "Message".into(), ids[0].clone()).unwrap().unwrap();
    let url = row.uploads.iter().find(|entry| entry.action == "send_files").and_then(|entry| entry.upload.local_url.as_ref()).unwrap();
    let copy = std::path::PathBuf::from(url.strip_prefix("file://").unwrap());
    assert!(copy.exists());
    bound(reopened.database.session_reset()).await.unwrap();
    for id in ids { assert!(reopened.database.db.read(&["Message"], |reader| reader.point("Message", &id)).unwrap().is_none()); }
    assert!(!copy.exists());
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_restored_transfer_origins_mixed() {
    use std::sync::Arc;
    use super::stand::moment;
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let epoch = stand.database.session_epoch();
    let source = directory.path().join("source.jpg");
    std::fs::write(&source, b"image").unwrap();
    let file = || WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None };
    let items = (0..2).map(|index| WireUploadItem {
        values_json: json!({"chatId":"c1", "kind":"TEXT", "clientKey":format!("key-{index}"), "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
        source: file(), variables_json: "{}".into(),
    }).collect();
    let batch = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    let ids: Vec<_> = batch.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    let single = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: file(), variables_json: "{}".into() } }),
    })).await.unwrap();
    let moment_id = single.optimistic_value.unwrap().id;
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials, None, false).await;
    reopened.settle().await;
    let epoch = reopened.database.session_epoch();
    let mut receipts = Vec::new();
    for id in &ids {
        receipts.push(bound(reopened.database.retry(epoch, "Message".into(), id.clone(), Some("send_files".into()), None)).await.unwrap());
    }
    let receipt = bound(reopened.database.retry(epoch, "Moment".into(), moment_id, Some("create_file".into()), None)).await.unwrap();
    reopened.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
    for _ in 0..3 { reopened.script_put(200, Some("etag")); }
    let mut first = message("m0", "c1", "");
    first["clientKey"] = json!("key-0");
    let mut second = message("m1", "c1", "");
    second["clientKey"] = json!("key-1");
    reopened.answer("MessageAlbumSend", json!({"messageAlbumSend":{"messages":[first, second]}}));
    reopened.answer("MomentCreateFile", json!({"momentCreate":{"moment":moment("mo1", 0)}}));
    reopened.database.set_online(true).unwrap();
    for (index, receipt) in receipts.into_iter().enumerate() {
        let value = bound(reopened.database.confirmed(receipt, false)).await.unwrap().value.unwrap();
        assert_eq!(value.id, format!("m{index}"));
    }
    assert_eq!(bound(reopened.database.confirmed(receipt, false)).await.unwrap().value.unwrap().id, "mo1");
    assert_eq!((reopened.requests("MessageAlbumSend"), reopened.requests("MomentCreateFile"), reopened.puts()), (1, 1, 3));
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_abort_debt_survives_restart() {
    use std::sync::Arc;
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("abort.sqlite").to_string_lossy().into_owned();
    let source = directory.path().join("multipart.jpg");
    std::fs::File::create(&source).unwrap().set_len(11 * crate::owners::write::upload::MEBIBYTE).unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    first.answer_multipart();
    first.hold(&["MultipartUploadPresignParts"]);
    let mark = first.request_mark();
    let epoch = first.database.session_epoch();
    let receipt = bound(first.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    first.receive_after(mark, "MultipartUploadPresignParts", None, 1).await;
    let presign = first.asked().into_iter().find(|(name, _)| name == "MultipartUploadPresignParts").unwrap().1;
    assert_eq!(presign["uploadId"], json!("upload-1"));
    first.database.set_online(false).unwrap();
    first.settle().await;
    bound(first.database.discard(epoch, "Moment".into(), id, Some("create_file".into()))).await.unwrap();
    first.settle().await;
    let orphans = || {
        let db = rusqlite::Connection::open(&path).unwrap();
        let mut statement = db.prepare("SELECT upload_id, key FROM upload_orphan").unwrap();
        statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    };
    let debt = orphans();
    assert_eq!(debt.len(), 1, "the discarded upload retains its abort debt");
    assert_eq!(first.requests("MultipartUploadAbort"), 0);
    first.release(&["MultipartUploadPresignParts"]);
    drop(receipt);
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    assert_eq!(orphans(), debt);

    let reopened = Stand::prepare_online(Some(path.clone()), credentials, None, false).await;
    reopened.answer_multipart();
    reopened.hold(&["MultipartUploadAbort"]);
    let mark = reopened.request_mark();
    reopened.database.set_online(true).unwrap();
    reopened.receive_after(mark, "MultipartUploadAbort", None, 1).await;
    let abort = reopened.asked().into_iter().find(|(name, _)| name == "MultipartUploadAbort").unwrap().1;
    assert_eq!((abort["uploadId"].as_str(), abort["key"].as_str()), (Some("upload-1"), Some("key-1")));
    reopened.release(&["MultipartUploadAbort"]);
    reopened.settle().await;
    assert!(orphans().is_empty(), "a confirmed abort removes its debt");
    reopened.database.set_online(false).unwrap();
    reopened.settle().await;
    reopened.database.set_online(true).unwrap();
    reopened.settle().await;
    assert_eq!(reopened.requests("MultipartUploadAbort"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_transfer_abort_debt_reset() {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
    use crate::test_wait::settled;

    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("multipart.jpg");
    std::fs::File::create(&source).unwrap().set_len(11 * crate::owners::write::upload::MEBIBYTE).unwrap();
    stand.answer_multipart();
    stand.hold(&["MultipartUploadPresignParts", "MultipartUploadAbort"]);
    let receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    assert!(receipt.optimistic_value.is_some());
    settled(&stand, "multipart init reaches presign", || stand.requests("MultipartUploadPresignParts") == 1).await;
    bound(stand.database.session_reset()).await.unwrap();
    stand.release(&["MultipartUploadPresignParts"]);
    stand.sign_in().await;
    settled(&stand, "the new session starts the owed abort", || stand.requests("MultipartUploadAbort") == 1).await;
    let requests = stand.asked();
    let aborts: Vec<_> = requests.iter().filter(|(name, _)| name == "MultipartUploadAbort").collect();
    assert_eq!(aborts.len(), 1);
    assert_eq!(aborts[0].1["uploadId"], json!("upload-1"));
    assert_eq!(aborts[0].1["key"], json!("key-1"));
    stand.release(&["MultipartUploadAbort"]);
    stand.settle().await;
    assert_eq!(stand.requests("MultipartUploadAbort"), 1);
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn out_restore_removed_group_member() {
    use std::sync::Arc;
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::upload_wire::WireSource;

    for removed in [&[0usize][..], &[1usize][..], &[0usize, 1][..]] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("group.sqlite").to_string_lossy().into_owned();
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
        stand.sign_in().await;
        stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
        stand.script_put(200, Some("etag"));
        stand.script_put(200, Some("etag"));
        stand.hold(&["MessageAlbumSend"]);
        let file = directory.path().join("source");
        std::fs::write(&file, b"image").unwrap();
        let items = (0..2).map(|index| WireUploadItem {
            values_json: json!({"chatId":"c1", "kind":"TEXT", "clientKey":format!("key-{index}"), "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
            source: WireSource::File { path: file.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into(),
        }).collect();
        let epoch = stand.database.session_epoch();
        let mark = stand.request_mark();
        let receipt = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
        let ids: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
        stand.receive_after(mark, "MessageAlbumSend", None, 1).await;
        let sql = rusqlite::Connection::open(&path).unwrap();
        let members: Vec<(i64, i64)> = sql.prepare("SELECT position, transfer_id FROM op_transfer ORDER BY position").unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(members.len(), 2);
        drop(sql);
        for index in removed {
            stand.database.db.mark_tombstone("Message", &ids[*index], 100, i64::MAX).unwrap();
        }
        bound(stand.database.close_database()).await.unwrap();
        drop(stand);
        let reopened = Stand::prepare_online(Some(path.clone()), credentials, None, false).await;
        reopened.settle().await;
        let sql = rusqlite::Connection::open(&path).unwrap();
        let left: Vec<i64> = sql.prepare("SELECT transfer_id FROM op_transfer ORDER BY position").unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
        let expected: Vec<i64> = members.iter().enumerate().filter(|(index, _)| !removed.contains(index)).map(|(_, (_, id))| *id).collect();
        assert_eq!(left, expected, "only removed members leave the restored intent: {removed:?}");
        for index in removed {
            let outcome: String = sql.query_row("SELECT kind FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = 1", [members[*index].1], |row| row.get(0)).unwrap();
            assert_eq!(outcome, "discarded");
        }
        drop(sql);
        reopened.database.close_now();
    }
}
