use std::sync::Arc;

use super::crash_image_support::{over_images, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::Stand;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::SessionStanding;

/// A process killed after any commit of the sign-in leaves a file that opens as no session or as the signed-in session of the same identity, never as a half session, and the commits only move from one to the other.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_the_sign_in_opens_as_no_session_or_the_whole_session() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let signed = database.session_status().unwrap();
    assert_eq!(signed.phase, SessionStanding::Active);
    let secret = stand.rig.credentials.inner.value().expect("the sign-in saved the credential");
    let (capture, files) = Capture::begin(&live).end(&live);

    let phases = over_images(&capture, &files, |index, image| {
        let secret = secret.clone();
        async move {
            let mut fresh = Stand::new();
            fresh.rig.credentials = Arc::new(WatchedHeld::default());
            crate::ffi::CredentialStore::set(fresh.rig.credentials.as_ref(), secret).unwrap();
            let reopened = fresh.open(Some(&image), &OPERATIONS).await;
            let status = reopened.session_status().unwrap();
            let active = status.phase == SessionStanding::Active;
            assert!(active || status.phase == SessionStanding::None, "cut {index}: {:?}", status.phase);
            assert_eq!(status.identity, if active { signed.identity } else { None }, "cut {index}: the session is whole or absent");
            bound(reopened.close_database()).await.unwrap();
            active
        }
    }).await;
    assert!(!phases[0] && *phases.last().unwrap(), "the first cut has no session and the last has it");
    assert!(phases.windows(2).all(|pair| !pair[0] || pair[1]), "a session never goes back to none: {phases:?}");
    database.close_now();
}
