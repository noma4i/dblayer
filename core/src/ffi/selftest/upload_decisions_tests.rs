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
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
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

/// P-XFER-77: `writes_quiet` answers only when the writes have no work that runs without a new external event, and always answers.
/// Observable: when the answer comes against the held `Complete` and final mutation, the landed row at the answer, the answer with the network away, and at `close`. Catches: an answer before the end of the upload, an answer that never comes.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_writes_quiet() {
    const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
    const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#;
    const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
    const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
    let scripted = || {
        let stand = Stand::new().multipart_only();
        stand.script.data("InitMultipart", INIT);
        for _ in 0..6 { stand.script.data("PresignParts", PRESIGN); }
        let held = (stand.script.hold("CompleteMultipart", COMPLETE), stand.script.hold("CreateMoment", LANDED));
        (stand, held)
    };
    let file = |directory: &std::path::Path| {
        let source = directory.join("video");
        std::fs::File::create(&source).unwrap().set_len(MEBIBYTE).unwrap();
        source
    };

    let (stand, (complete, final_mutation)) = scripted();
    let directory = tempfile::tempdir().unwrap();
    let database = stand.open(Some(&directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file(directory.path()), epoch))).await.unwrap();
    let waiting = database.clone();
    let quiet = tokio::spawn(async move { waiting.writes_quiet(epoch).await });
    complete.entered().await;
    assert!(!quiet.is_finished(), "Complete is on its way");
    complete.release();
    final_mutation.entered().await;
    assert!(!quiet.is_finished(), "the final mutation is on its way");
    final_mutation.release();
    bound(quiet).await.unwrap();
    assert!(database.peek_row(database.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some(), "the answer comes after the final mutation landed");
    database.close_now();

    let (away_stand, _held) = scripted();
    let away_directory = tempfile::tempdir().unwrap();
    let away = away_stand.open(Some(&away_directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    let away_epoch = away.session_epoch();
    away.set_online(false).unwrap();
    bound(away.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file(away_directory.path()), away_epoch))).await.unwrap();
    bound(away.writes_quiet(away_epoch)).await;
    assert_eq!(away_stand.script.count("InitMultipart"), 0, "the step waits for the network");
    away.close_now();

    let (closing_stand, (closing_complete, _final)) = scripted();
    let closing_directory = tempfile::tempdir().unwrap();
    let closing = closing_stand.open(Some(&closing_directory.path().join("db.sqlite")), &[CREATE_MOMENT]).await;
    let closing_epoch = closing.session_epoch();
    bound(closing.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &file(closing_directory.path()), closing_epoch))).await.unwrap();
    closing_complete.entered().await;
    let database = closing.clone();
    let waits = tokio::spawn(async move { database.writes_quiet(closing_epoch).await });
    closing.close_now();
    bound(waits).await.unwrap();
}
