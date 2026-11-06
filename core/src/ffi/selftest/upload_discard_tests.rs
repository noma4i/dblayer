use super::rig_tests::bound;
use super::upload_stand::{clip, watched, Stand, CREATE_MOMENT};
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::yield_until;

const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_discard_offline_leaves_only_the_replay_of_the_unanswered_step_after_the_reopen() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("photo");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&path), &operations).await;
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    let presign = stand.script.hold("PresignParts", PRESIGN);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"group"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    database.set_online(false).unwrap();

    bound(database.discard(epoch, "Moment".into(), id.clone(), None)).await.unwrap();

    let rows = watched(&database, "Moment", &id);
    yield_until("the row is gone", || rows.len() == 0).await;
    database.close_now();
    stand.script.data("PresignParts", PRESIGN);
    let abort = stand.script.hold("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    let reopened = stand.open(Some(&path), &operations).await;
    abort.entered().await;
    abort.release();
    yield_until("the abort was answered", || stand.script.count("AbortMultipart") == 1).await;
    let asked = stand.script.variables("PresignParts");
    assert_eq!(asked.len(), 2, "the discarded upload asked for parts more than once after the reopen: {asked:?}");
    assert_eq!(asked[0], asked[1], "the second presign is the replay of the first");
    reopened.close_now();
}
