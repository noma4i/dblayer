use super::rig_tests::bound;
use super::upload_stand::{clip, request, watched, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::tape::Rows;
use crate::ffi::tests::session_guards_tests::SOCIAL_BODY;
use crate::ffi::{DbError, WireOperationOutcome};
use crate::test_wait::settled;

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
    let waiting = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), { let database = database.clone(); async move { database.outcome(sent).await } }));

    bound(database.session_reset()).await.unwrap();

    assert!(matches!(bound(waiting).await.unwrap().unwrap(), WireOperationOutcome::Ended { cause: crate::ffi::EndCause::SessionEnded }));
    let refused = bound(database.upload(database.session_epoch(), "Moment".into(), id.clone(), "upload_clip".into(), request(&file))).await.unwrap_err();
    assert!(matches!(refused, DbError::NoSession { .. }), "{refused:?}");
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    put.release();
    settled(&database, "the old row is gone", || stand.puts.seen.lock().unwrap().len() == 1).await;
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
    let waiting = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), { let database = database.clone(); async move { database.outcome(sent).await } }));

    bound(database.session_reset()).await.unwrap();

    assert!(token.is_cancelled(), "the PUT of the ended epoch is cancelled when the reset answers");
    assert!(matches!(bound(waiting).await.unwrap().unwrap(), WireOperationOutcome::Ended { cause: crate::ffi::EndCause::SessionEnded }));
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

/// xfer_prop_put_cancel_pushes (P-XFER-34, D-140): a PUT double that never reads the flag is cancelled by the core dropping its future, for the cancel of the upload and for the end of the epoch. The wait ends and `settled` returns before the double is released, and the release brings no second PUT or step.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_put_cancel_pushes() {
    for cause in ["cancel_upload", "session_reset"] {
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
        let waiting = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), { let database = database.clone(); async move { database.outcome(sent).await } }));

        if cause == "cancel_upload" {
            assert!(bound(database.cancel_upload(epoch, "Moment".into(), id, "upload_clip".into())).await.unwrap(), "{cause}");
        } else {
            bound(database.session_reset()).await.unwrap();
            assert!(matches!(bound(waiting).await.unwrap().unwrap(), WireOperationOutcome::Ended { cause: crate::ffi::EndCause::SessionEnded }), "{cause}");
        }

        put.dropped().await;
        bound(crate::ffi::settled(&database)).await.unwrap();
        put.release();
        bound(crate::ffi::settled(&database)).await.unwrap();
        assert_eq!((stand.puts.seen.lock().unwrap().len(), stand.script.count("CreateBlob"), stand.script.count("CreateMoment")), (1, 1, 0), "{cause}: no second PUT or step after the release");
        database.close_now();
    }
}

const MOMENT_ROW: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn local_url(rows: &Rows) -> Option<String> {
    rows.0.lock().unwrap().first().and_then(|row| row.uploads.iter().find(|entry| entry.action == "upload_clip").and_then(|entry| entry.upload.local_url.clone()))
}

fn names(directory: &std::path::Path) -> Vec<String> {
    let mut found: Vec<String> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned()).collect();
    found.sort();
    found
}

/// P-XFER-38 xfer_prop_reply_after_delete (D-142): for a database on disk and one in memory, the answer of `discard` and of `cancel_upload` comes after the removal of the copy of the same step, and a confirmed transfer has no copy once the work settled.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_reply_after_delete() {
    for on_disk in [true, false] {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let database = stand.open(on_disk.then_some(path.as_path()), &[CREATE_MOMENT, PROFILE]).await;
        let file_of = |name: &str| {
            let file = directory.path().join(format!("{name}.jpg"));
            std::fs::write(&file, name.as_bytes()).unwrap();
            file
        };
        let epoch = database.session_epoch();

        stand.script.data("CreateBlob", BLOB);
        stand.script.enqueue("CreateMoment", Answer::Status(422));
        let failed = bound(database.send(clip("Moment", r#"{"text":"failed"}"#, "upload_clip", &file_of("failed"), epoch))).await.unwrap();
        let failed_id = failed.optimistic_value.clone().unwrap().id;
        let failed_rows = watched(&database, "Moment", &failed_id);
        assert!(matches!(bound(database.outcome(failed)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        settled(&database, "the failed row shows its copy", || local_url(&failed_rows).is_some()).await;
        let url = local_url(&failed_rows).unwrap();
        let copies = std::path::Path::new(&url["file://".len()..]).parent().unwrap().to_path_buf();
        assert_eq!(names(&copies).len(), 1, "{on_disk}");
        bound(database.discard(epoch, "Moment".into(), failed_id, None)).await.unwrap();
        assert_eq!(names(&copies), Vec::<String>::new(), "{on_disk}: no copy after the answer of discard");
        settled(&database, "localUrl is cleared", || local_url(&failed_rows).is_none()).await;

        stand.script.data("CreateBlob", BLOB);
        let put = stand.puts.hold_next();
        let held = bound(database.send(clip("Moment", r#"{"text":"held"}"#, "upload_clip", &file_of("held"), epoch))).await.unwrap();
        let held_id = held.optimistic_value.clone().unwrap().id;
        let held_rows = watched(&database, "Moment", &held_id);
        put.entered().await;
        settled(&database, "the held row shows its copy", || local_url(&held_rows).is_some()).await;
        assert_eq!(names(&copies).len(), 1, "{on_disk}");
        assert!(bound(database.cancel_upload(epoch, "Moment".into(), held_id, "upload_clip".into())).await.unwrap());
        assert_eq!(names(&copies), Vec::<String>::new(), "{on_disk}: no copy after the answer of cancel_upload");
        settled(&database, "localUrl is cleared", || local_url(&held_rows).is_none()).await;
        put.release();

        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", MOMENT_ROW);
        let confirmed = bound(database.send(clip("Moment", r#"{"text":"confirmed"}"#, "upload_clip", &file_of("confirmed"), epoch))).await.unwrap();
        assert!(matches!(bound(database.outcome(confirmed)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
        bound(crate::ffi::settled(&database)).await.unwrap();
        assert_eq!(names(&copies), Vec::<String>::new(), "{on_disk}: no copy of a confirmed transfer once the work settled");
        database.close_now();
    }
}
