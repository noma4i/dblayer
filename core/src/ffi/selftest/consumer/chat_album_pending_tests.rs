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
    assert_eq!((stand.asked().len(), stand.puts()), (before, 0));
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
