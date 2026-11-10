use std::sync::Arc;
use serde_json::{json, Value};
use super::stand::{message, moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireSource, WireUploadBatch, WireUploadItem};

fn items(directory: &tempfile::TempDir, prefix: &str) -> Vec<WireUploadItem> {
    (0..2).map(|index| {
        let name = format!("{prefix}-{index}");
        let path = directory.path().join(&name);
        std::fs::write(&path, name.as_bytes()).unwrap();
        WireUploadItem {
            values_json: json!({"chatId":"c1","kind":"TEXT","clientKey":name,"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","mediaGroup":{"id":prefix,"position":index,"size":2}}).to_string(),
            source: WireSource::File { path: path.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into(),
        }
    }).collect()
}

fn answers(stand: &Stand, prefix: &str) {
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"blob"}}}));
    stand.script_put(200, Some("etag"));
    stand.script_put(200, Some("etag"));
    let messages: Vec<Value> = (0..2).map(|index| {
        let mut row = message(&format!("{prefix}-server-{index}"), "c1", "");
        row["clientKey"] = json!(format!("{prefix}-{index}"));
        row
    }).collect();
    stand.answer("MessageAlbumSend", json!({"messageAlbumSend":{"messages":messages}}));
}

async fn batch(stand: &Stand, directory: &tempfile::TempDir, prefix: &str) -> crate::ffi::WireBatchReceipt {
    bound(stand.database.upload_create_batch(stand.database.session_epoch(), "Message".into(), items(directory, prefix), WireUploadBatch { operation:"send_files".into(), variables_json:r#"{"chatId":"c1"}"#.into() })).await.unwrap()
}

async fn scenario(single: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    if single {
        use crate::ffi::command::{CommandRequest, CommandUpload};
        use crate::ffi::upload_wire::WireUploadRequest;
        first.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"blob"}}}));
        first.answer("MomentCreateFile", json!({"momentCreate":{"moment":moment("first-moment", 0)}}));
        first.script_put(200, Some("etag"));
        let source = directory.path().join("single");
        std::fs::write(&source, b"single").unwrap();
        let epoch = first.database.session_epoch();
        let _receipt = bound(first.database.send(CommandRequest { epoch, model:"Moment".into(), values_json:json!({"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json:"{}".into(), upload:Some(CommandUpload { action:"create_file".into(), request:WireUploadRequest { source:WireSource::File { path:source.to_string_lossy().into_owned(), name:None, mime:None }, variables_json:"{}".into() } }) })).await.unwrap();
    } else {
        answers(&first, "old");
        let _receipt = batch(&first, &directory, "old").await;
    }
    first.settle().await;
    assert_eq!(first.requests(if single { "MomentCreateFile" } else { "MessageAlbumSend" }), 1);
    let previous = first.request_ids();
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let second = Stand::prepare_on(Some(path), credentials).await;
    answers(&second, "new");
    second.hold_put();
    let receipt = batch(&second, &directory, "new").await;
    second.put_started().await;
    for (index, item) in receipt.items.iter().enumerate() {
        let row = second.database.peek_row(second.database.session_epoch(), "Message".into(), item.optimistic_value.id.clone()).unwrap().unwrap();
        let url = row.uploads.iter().find(|upload| upload.action == "send_files").unwrap().upload.local_url.as_ref().unwrap();
        assert_eq!(std::fs::read(url.strip_prefix("file://").unwrap()).unwrap(), format!("new-{index}").as_bytes());
    }
    second.release_put();
    for (index, item) in receipt.items.into_iter().enumerate() {
        let outcome = bound(second.database.confirmed(item.receipt, false)).await.unwrap();
        assert_eq!(outcome.value.unwrap().id, format!("new-server-{index}"));
    }
    let current = second.request_ids();
    for (_, id) in current.iter().filter(|(name, _)| name == "CreateBlob" || name == "MessageAlbumSend") {
        assert!(!previous.iter().any(|(_, old)| old == id), "request id reused: previous={previous:?}, current={current:?}");
    }
    assert_eq!((second.requests("CreateBlob"), second.puts(), second.requests("MessageAlbumSend")), (2, 2, 1));
    bound(second.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_after_group_reopen() { scenario(false).await; }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_after_single_reopen() { scenario(true).await; }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_after_failed_group_reopen() {
    use crate::ffi::{DbError, ReadRequest, ReadValue};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    answers(&first, "old-failed");
    first.answer_next("CreateBlob", json!({"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}));
    let first_receipt = batch(&first, &directory, "old-failed").await;
    first.settle().await;
    let failed = first.database.peek_row(first.database.session_epoch(), "Message".into(), first_receipt.items[0].optimistic_value.id.clone()).unwrap().unwrap();
    assert_eq!(failed.uploads[0].upload.status, crate::ffi::wire::WireUploadStatus::Failed);
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let reopened = Stand::prepare_on(Some(path), credentials).await;
    answers(&reopened, "new-failed");
    reopened.answer_next("CreateBlob", json!({"errors":[{"message":"refused again","extensions":{"code":"FORBIDDEN"}}]}));
    let next = batch(&reopened, &directory, "new-failed").await;
    let id = next.items[0].optimistic_value.id.clone();
    let result = bound(reopened.database.confirmed(next.items[0].receipt.clone(), true)).await;
    assert!(matches!(result, Err(DbError::Rejected { kept:false, .. })), "new failure: {result:?}, requests={:?}", reopened.asked());
    let stream = reopened.database.subscribe_values(reopened.database.session_epoch(), ReadRequest::Operations);
    let Some(ReadValue::Operations { items, .. }) = bound(stream.pull_next()).await else { panic!("the queue did not open") };
    assert!(!items.iter().any(|item| item.row_id == id && item.action == "send_files"), "discard must remove only the refused new key: {items:?}");
    bound(reopened.database.close_database()).await.unwrap();
}

async fn partially_uploaded_group(failed: bool) {
    use crate::ffi::upload_wire::WireUploadSource;
    use crate::ffi::wire::WireUploadStatus;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("partial.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    answers(&first, "partial");
    let epoch = first.database.session_epoch();
    let pending = items(&directory, "partial").into_iter().map(|mut item| { item.source = WireSource::Pending; item }).collect();
    let batch = bound(first.database.upload_create_batch(epoch, "Message".into(), pending, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    let ids: Vec<_> = batch.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    let source = |index| WireUploadSource { path: directory.path().join(format!("partial-{index}")).to_string_lossy().into_owned(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into() };
    if failed {
        bound(first.database.fail_source(epoch, "Message".into(), ids[0].clone(), "send_files".into(), "preparation".into())).await.unwrap();
    } else {
        bound(first.database.supply_source(epoch, "Message".into(), ids[0].clone(), "send_files".into(), source(0))).await.unwrap();
        first.settle().await;
        assert_eq!(first.puts(), 1);
    }
    first.hold_put();
    bound(first.database.supply_source(epoch, "Message".into(), ids[1].clone(), "send_files".into(), source(1))).await.unwrap();
    first.puts_started(if failed { 1 } else { 2 }).await;
    assert_eq!(first.requests("MessageAlbumSend"), 0);
    first.database.close_now();
    drop(first);
    let reopened = Stand::prepare_online(Some(path), credentials, None, false).await;
    answers(&reopened, "partial");
    reopened.database.set_online(true).unwrap();
    if failed {
        reopened.settle().await;
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), ids[0].clone()).unwrap().unwrap();
        assert_eq!(row.uploads[0].upload.status, WireUploadStatus::Failed);
        assert_eq!(reopened.requests("MessageAlbumSend"), 0);
        bound(reopened.database.retry(reopened.database.session_epoch(), "Message".into(), ids[0].clone(), Some("send_files".into()), Some(source(0)))).await.unwrap();
    }
    crate::test_wait::settled(&reopened, "album mutation after reopen", || reopened.requests("MessageAlbumSend") == 1).await;
    for index in 0..2 {
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), format!("partial-server-{index}")).unwrap();
        assert!(row.is_some(), "album row {index} is missing: {:?}", reopened.asked());
    }
    assert_eq!(reopened.requests("MessageAlbumSend"), 1);
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_partial_success_reopen() { partially_uploaded_group(false).await; }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_partial_failure_reopen() { partially_uploaded_group(true).await; }
