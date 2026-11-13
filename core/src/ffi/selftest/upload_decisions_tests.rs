use super::rig_tests::bound;
use super::upload_stand::{clip, shows, watched, Answer, Stand, CREATE_MOMENT};
use crate::ffi::wire::WireSyncState;
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::settled;

const ROW: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// Abort multipart: after every part is confirmed a refused `complete` keeps the upload id and the parts. A database opened again sends no abort for it, and `retry` goes straight to `complete`.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_complete_survives_reopen_without_abort_and_retry_completes() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let database = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#);
    for _ in 0..3 {
        stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#);
    }
    stand.script.enqueue("CompleteMultipart", Answer::Status(422));
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    let rows = watched(&database, "Moment", &id);
    settled(&database, "the refused complete fails the row", || shows(&rows, WireSyncState::Failed)).await;
    assert_eq!(stand.puts.seen.lock().unwrap().len(), 3, "every part was confirmed before complete");
    database.close_now();

    stand.script.data("CompleteMultipart", r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#);
    stand.script.data("CreateMoment", ROW);
    let reopened = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.count("AbortMultipart"), 0, "the reopened database does not abort an upload whose parts are all confirmed");

    bound(reopened.retry(reopened.session_epoch(), "Moment".into(), id, Some("upload_clip".into()), None)).await.unwrap();
    settled(&reopened, "the retry reached complete and create", || stand.script.count("CompleteMultipart") == 2 && stand.script.count("CreateMoment") == 1).await;
    let names = stand.script.names();
    let count = |name: &str| names.iter().filter(|seen| *seen == name).count();
    assert_eq!((count("InitMultipart"), count("PresignParts"), count("AbortMultipart"), stand.puts.seen.lock().unwrap().len()), (1, 3, 0, 3), "retry sends neither init, presign nor a part again: {names:?}");
    reopened.close_now();
}
