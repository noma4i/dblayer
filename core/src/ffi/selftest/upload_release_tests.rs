use super::rig_tests::bound;
use super::upload_stand::{clip, plain, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::WireOperationOutcome;

const ANSWER: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_with_the_saved_step_of_an_exhausted_upload_does_not_hold_the_queue() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    let lost = stand.script.hold("CreateBlob", BLOB);
    let epoch = database.session_epoch();
    bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    lost.entered().await;
    database.close_now();
    lost.release();
    rusqlite::Connection::open(&path).unwrap().execute("UPDATE sync_upload SET attempts = 50", []).unwrap();

    let reopened = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    stand.script.data("CreateMoment", ANSWER);
    let epoch = reopened.session_epoch();
    let later = bound(reopened.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();

    assert!(matches!(bound(reopened.outcome(later)).await.unwrap(), WireOperationOutcome::Confirmed { .. }), "the mutation of another row is sent");
    assert_eq!((stand.script.count("CreateBlob"), stand.puts.seen.lock().unwrap().len()), (2, 0), "Session replays the saved pair once and the exhausted upload sends nothing else");
    reopened.close_now();
}
