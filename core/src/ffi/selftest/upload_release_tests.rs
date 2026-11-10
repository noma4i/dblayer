use super::rig_tests::bound;
use super::upload_stand::{clip, plain, Stand, BLOB, CREATE_MOMENT};

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

    bound(reopened.confirmed(later, false)).await.expect("the mutation of another row is sent");
    assert_eq!((stand.script.count("CreateBlob"), stand.puts.seen.lock().unwrap().len()), (2, 0), "Session replays the saved pair once and the exhausted upload sends nothing else");
    reopened.close_now();
}

/// sess_request_identity_on_wire: a request in flight when the network drops goes out again with the same request id and a new attempt.
#[tokio::test(flavor = "multi_thread")]
async fn sess_request_identity_on_wire() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let held = stand.script.hold("CreateMoment", ANSWER);
    let epoch = database.session_epoch();
    let sent = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    held.entered().await;
    database.set_online(false).unwrap();
    held.dropped().await;
    database.set_online(true).unwrap();

    bound(database.confirmed(sent, false)).await.unwrap();
    let wires = stand.script.wires();
    let [.., (first_author, first_request, first_attempt), (second_author, second_request, second_attempt)] = wires[..] else { panic!("{wires:?}") };
    assert_eq!((first_author, second_author), (crate::transport::Author::Outbox, crate::transport::Author::Outbox), "{wires:?}");
    assert_eq!(first_request, second_request, "the same request goes again: {wires:?}");
    assert!(second_attempt > first_attempt, "the repeat is a new attempt: {wires:?}");
    database.close_now();
}
