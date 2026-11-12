use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::crash_image_support::{over_images, queue_now, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::Stand;
use crate::ffi::tests::session_guards_tests::{WatchedHeld, SOCIAL_BODY};
use crate::ffi::{DbError, SessionStanding};

/// A process killed after any commit of a sign-in whose credential save the app refused leaves a file that opens as no session, with no credential, no work and no second sign-in.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_sign_in_with_a_refused_secret_opens_as_no_session() {
    let stand = Stand::new().signed_out();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let database = stand.open(Some(&live), &OPERATIONS).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    stand.rig.credentials.fail_next_set.store(true, Ordering::SeqCst);
    let refused = bound(database.session_call("social".into(), SOCIAL_BODY.into())).await;
    assert!(matches!(refused, Err(DbError::Storage { .. })), "the refused save fails the sign-in: {refused:?}");
    assert_eq!(database.session_status().unwrap().phase, SessionStanding::None);
    let (capture, files) = base.end(&live);

    over_images(&capture, &files, |index, image| async move {
        let mut fresh = Stand::new().signed_out();
        fresh.rig.credentials = Arc::new(WatchedHeld::default());
        let reopened = fresh.open(Some(&image), &OPERATIONS).await;
        crate::test_wait::settled(&reopened, "opened", || true).await;
        let status = reopened.session_status().unwrap();
        assert_eq!((status.phase, status.identity), (SessionStanding::None, None), "cut {index}: the refused sign-in leaves no session");
        assert!(fresh.rig.credentials.inner.value().is_none(), "cut {index}: no credential appears");
        assert_eq!(queue_now(&reopened).await, 0, "cut {index}: no work");
        assert_eq!(fresh.rig.server.seen().len(), 0, "cut {index}: the open asks the server nothing");
        bound(reopened.close_database()).await.unwrap();
    }).await;
    database.close_now();
}
