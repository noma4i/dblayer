use super::rig_tests::bound;
use super::upload_stand::{clip, request, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::session_guards_tests::SOCIAL_BODY;
use crate::ffi::{DbError, WireOperationOutcome};
use crate::test_wait::yield_until;

const PROFILE: (&str, &str) = ("UserProfile", "query UserProfile { me { id name } }");
const PROFILE_ANSWER: &str = r#"{"data":{"me":{"__typename":"User","id":"1","name":"B"}}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn an_upload_keeps_the_epoch_it_was_sent_in() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT, PROFILE]).await;
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let first = database.session_epoch();
    let sent_first = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, first))).await.unwrap();

    bound(database.session_reset()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    stand.script.data("UserProfile", PROFILE_ANSWER);
    let load = database.begin_load(database.session_epoch(), "User".into(), Some("profile".into()), "{}".into()).unwrap();
    bound(load.result()).await.unwrap();
    database.set_online(false).unwrap();
    let second = database.session_epoch();
    let sent_second = bound(database.send(clip("Moment", r#"{"text":"B"}"#, "upload_clip", &file, second))).await.unwrap();

    assert_ne!(first, second);
    assert_eq!((sent_first.epoch, sent_second.epoch), (first, second));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_during_a_put_ends_the_wait_and_removes_the_old_upload() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT, PROFILE]).await;
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    stand.script.data("CreateBlob", BLOB);
    let put = stand.puts.hold_next();
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"hello"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let id = sent.optimistic_value.clone().unwrap().id;
    put.entered().await;
    let waiting = tokio::spawn({ let database = database.clone(); async move { database.outcome(sent).await } });

    bound(database.session_reset()).await.unwrap();

    assert!(matches!(bound(waiting).await.unwrap().unwrap(), WireOperationOutcome::Interrupted));
    let refused = bound(database.upload(database.session_epoch(), "Moment".into(), id.clone(), "upload_clip".into(), request(&file))).await.unwrap_err();
    assert!(matches!(refused, DbError::NoSession { .. }), "{refused:?}");
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    put.release();
    yield_until("the old row is gone", || stand.puts.seen.lock().unwrap().len() == 1).await;
    assert!(bound(database.read_row(database.session_epoch(), "Moment".into(), id)).await.unwrap().is_none());
    assert_eq!(stand.script.count("CreateMoment"), 0);
    database.close_now();
}

/// xfer_prop_reset_during_put_no_stray_work: cells EpochEnded x Uploading (xfer_epoch_ended, xfer_put_cancelled). The PUT of the old epoch is cancelled before the reset answers, and no second PUT appears before or after the held one ends.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_reset_during_put_no_stray_work() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT, PROFILE]).await;
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    stand.script.data("CreateBlob", BLOB);
    let put = stand.puts.hold_next();
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"hello"}"#, "upload_clip", &file, epoch))).await.unwrap();
    put.entered().await;
    let token = stand.puts.tokens.lock().unwrap()[0].clone();
    assert!(!token.is_cancelled());
    let waiting = tokio::spawn({ let database = database.clone(); async move { database.outcome(sent).await } });

    bound(database.session_reset()).await.unwrap();

    assert!(token.is_cancelled(), "the PUT of the ended epoch is cancelled when the reset answers");
    assert!(matches!(bound(waiting).await.unwrap().unwrap(), WireOperationOutcome::Interrupted));
    assert_eq!(stand.puts.seen.lock().unwrap().len(), 1);
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    stand.script.data("UserProfile", PROFILE_ANSWER);
    let load = database.begin_load(database.session_epoch(), "User".into(), Some("profile".into()), "{}".into()).unwrap();
    bound(load.result()).await.unwrap();
    assert_eq!(stand.puts.seen.lock().unwrap().len(), 1, "no second PUT before the held one is released");
    put.release();
    bound(crate::ffi::settled(&database)).await.unwrap();
    assert_eq!((stand.puts.seen.lock().unwrap().len(), stand.script.count("CreateBlob"), stand.script.count("CreateMoment")), (1, 1, 0), "no second PUT or step after the release");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_after_a_reset_without_a_sign_in_is_refused_with_no_session() {
    let stand = Stand::new();
    let database = stand.open(None, &[PROFILE]).await;
    bound(database.session_reset()).await.unwrap();
    database.set_online(true).unwrap();
    stand.script.data("UserProfile", PROFILE_ANSWER);

    let refused = match database.begin_load(database.session_epoch(), "User".into(), Some("profile".into()), "{}".into()) {
        Err(error) => error,
        Ok(load) => bound(load.result()).await.unwrap_err(),
    };

    assert!(matches!(refused, DbError::NoSession { .. }), "{refused:?}");
    assert_eq!(stand.script.count("UserProfile"), 0);
    database.close_now();
}
