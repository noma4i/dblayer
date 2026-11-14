use super::crash_image_support::{advancing, drained, reopen_timed, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{clip, BLOB, CREATE_MOMENT, STEPS};
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::DbError;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// The subscriptions the library opens by itself: a complete registration declares them.
const SUBSCRIPTIONS: [(&str, &str); 2] = [("UserTyping", "subscription UserTyping { userTyping { id } }"), ("MomentEvents", "subscription MomentEvents { momentEvents { id } }")];

/// xfer_register_complete_fails_undeclared, xfer_failed_retry_undeclared, life_register_ok: an app that updates and registers its complete set without the operation of a saved upload gets the upload `failed` with the refusal and a refused `retry`, and the upload goes after a registration that declares the operation.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_saved_upload_without_its_operation_fails_and_goes_after_declaring_it() {
    let (stand, clock) = timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    database.set_online(false).await.unwrap();
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    crate::ffi::settled(&database).await.unwrap();
    super::crash_image_support::checkpoint_live(&database, &live).await;
    let image = directory.path().join("image.sqlite");
    super::crash_image_call_tests::copy_image(&live, &image);
    let secret = stand.rig.credentials.inner.value().unwrap();
    database.close_now();
    let (fresh, reopened, clock) = reopen_timed(&image, &secret, &SUBSCRIPTIONS, |fresh| fresh.complete.store(true, std::sync::atomic::Ordering::SeqCst), false).await;
    let epoch = reopened.session_epoch();
    let upload = || reopened.peek_row_now(epoch, "Moment".into(), temporary.clone()).unwrap().and_then(|row| row.uploads.into_iter().find(|entry| entry.action == "upload_clip")).map(|entry| entry.upload);
    advancing(&clock, crate::test_wait::settled(&reopened, "the upload failed", || upload().is_some_and(|upload| upload.status == WireUploadStatus::Failed))).await;
    let error = upload().unwrap().error.unwrap();
    assert!(error.message.contains("is not declared"), "{error:?}");
    let refused = bound(reopened.retry(epoch, "Moment".into(), temporary.clone(), Some("upload_clip".into()), None)).await;
    assert!(matches!(refused, Err(DbError::Invalid { .. })), "{refused:?}");
    assert_eq!(fresh.script.count("CreateBlob"), 0);
    let named: Vec<(&str, &str)> = STEPS.iter().copied().chain(SUBSCRIPTIONS).chain([CREATE_MOMENT]).collect();
    fresh.script.register_documents(&named);
    fresh.script.data("CreateBlob", BLOB);
    fresh.script.data("CreateMoment", LANDED);
    let defs: Vec<serde_json::Value> = named.iter().map(|(name, document)| serde_json::json!({"name": name, "document": document})).collect();
    reopened.register_operations(serde_json::Value::Array(defs).to_string()).await.unwrap();
    bound(reopened.retry(epoch, "Moment".into(), temporary.clone(), Some("upload_clip".into()), None)).await.unwrap();
    advancing(&clock, async { crate::test_wait::settled(&reopened, "the upload landed", || reopened.peek_row_now(epoch, "Moment".into(), "m77".into()).unwrap().is_some()).await; drained(&reopened).await; }).await;
    assert_eq!(fresh.script.count("CreateBlob"), 1);
}
