use super::crash_image_support::{advancing, reopen_timed, saved_pair, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{clip, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;

/// xfer_restored_step_fails_when_session_lost: the image of a process killed while an upload step was in flight opens with a secret of another digest: the upload ends `failed` with the lost state, it does not stay queued.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_restored_step_fails_when_session_lost() {
    let (stand, clock) = timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    let epoch = database.session_epoch();
    let held = stand.script.hold("CreateBlob", BLOB);
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    held.entered().await;
    super::crash_image_support::checkpoint_live(&database, &live).await;
    let image = directory.path().join("image.sqlite");
    super::crash_image_call_tests::copy_image(&live, &image);
    assert!(saved_pair(&image).is_some(), "the image holds the pair of the step");
    database.close_now();
    let (fresh, reopened, clock) = reopen_timed(&image, "a secret of another digest", &[CREATE_MOMENT], |_| {}, false).await;
    let epoch = reopened.session_epoch();
    let upload = || reopened.peek_row_now(epoch, "Moment".into(), temporary.clone()).unwrap().and_then(|row| row.uploads.into_iter().find(|entry| entry.action == "upload_clip")).map(|entry| entry.upload);
    advancing(&clock, crate::test_wait::settled(&reopened, "the upload failed", || upload().is_some_and(|upload| upload.status == WireUploadStatus::Failed))).await;
    let code = upload().unwrap().error.and_then(|error| error.session_code);
    assert_eq!(code, Some(crate::transport::SessionCode::StateLost), "the upload failed with the lost state");
    assert_eq!(fresh.script.count("CreateBlob"), 0, "nothing was sent with the other secret");
}
