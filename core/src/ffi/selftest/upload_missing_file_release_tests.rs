use super::rig_tests::bound;
use super::upload_stand::{clip, plain, Stand, BLOB, CREATE_MOMENT};

const ANSWER: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_with_the_saved_step_of_an_upload_whose_file_is_gone_does_not_hold_the_queue() {
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
    std::fs::remove_file(&file).unwrap();
    for copy in std::fs::read_dir(directory.path().join("db.sqlite.files/transfers")).unwrap() { std::fs::remove_file(copy.unwrap().path()).unwrap(); }

    let reopened = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    stand.script.data("CreateMoment", ANSWER);
    let epoch = reopened.session_epoch();
    let later = bound(reopened.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();

    assert!(bound(reopened.confirmed(later, false)).await.is_ok(), "the mutation of another row is sent");
    let puts = stand.puts.seen.lock().unwrap().clone();
    assert_eq!(puts.len(), 0, "the upload whose file is gone sends no bytes: {puts:?} calls {:?}", stand.script.names());
    reopened.close_now();
}
