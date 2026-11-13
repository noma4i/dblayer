use super::crash_image_support::{advancing, drained, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{clip, plain, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;

const A: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const B: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// xfer_step_behind_a_held_pair_keeps_attempts: a step that waits for the Session slot of another write does not burn attempts or fail while it waits; it goes out when the slot is free.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_step_behind_a_held_pair_keeps_attempts() {
    let (stand, clock) = timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    let epoch = database.session_epoch();
    let held = stand.script.hold("CreateMoment", A);
    bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    held.entered().await;
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", B);
    let sent = bound(database.send(clip("Moment", r#"{"text":"B"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    let upload = || database.peek_row_now(epoch, "Moment".into(), temporary.clone()).unwrap().and_then(|row| row.uploads.into_iter().find(|entry| entry.action == "upload_clip")).map(|entry| entry.upload);
    crate::test_wait::settled(&database, "the step started", || upload().is_some_and(|upload| upload.attempts == 1)).await;
    clock.advance(3 * 120_000);
    let waiting = upload().unwrap();
    assert_eq!((stand.script.count("CreateBlob"), waiting.attempts, waiting.status != WireUploadStatus::Failed, waiting.error.is_none()), (0, 1, true, true), "the step behind the held pair neither failed nor burned an attempt: {waiting:?}");
    held.release();
    advancing(&clock, async {
        crate::test_wait::settled(&database, "the upload landed", || database.peek_row_now(epoch, "Moment".into(), "m78".into()).unwrap().is_some()).await;
        drained(&database).await;
    }).await;
    assert_eq!(stand.script.count("CreateBlob"), 1, "the step went out once after the slot was free");
}
