use super::crash_image_support::{drained, over_images, queue_now, reopen, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::SessionStanding;

const ENDED: &str = r#"{"error":"SESSION_ENDED"}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// A process killed after any commit of the local end of a session or of the end the server answers with `SESSION_ENDED` leaves a file that opens as the signed-in session with its offline work intact and sent once, or as no session with the work gone, the old credential cleared and nothing of the old work sent, and the commits only move from the first to the second.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_the_end_of_a_session_opens_whole_or_ended() {
    for server_end in [false, true] {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT];
        let database = stand.open(Some(&live), &operations).await;
        let signed = database.session_status().unwrap();
        let secret = stand.rig.credentials.inner.value().unwrap();
        database.set_online(false).await.unwrap();
        let epoch = database.session_epoch();
        let created = bound(database.send(plain("Moment", r#"{"text":"offline"}"#, epoch))).await.unwrap();
        let temporary = created.optimistic_value.as_ref().unwrap().id.clone();
        assert_eq!(queue_now(&database).await, 1, "the offline create waits in the queue");
        super::crash_image_support::checkpoint_live(&database, &live).await;
        let base = Capture::begin(&live);
        if server_end {
            stand.script.data("CreateMoment", ENDED);
            database.set_online(true).await.unwrap();
            crate::test_wait::settled(&database, "the server ended the session", || database.session_status().is_ok_and(|status| status.phase == SessionStanding::None)).await;
        } else {
            bound(database.session_reset()).await.unwrap();
        }
        assert_eq!(database.session_status().unwrap().phase, SessionStanding::None);
        let (capture, files) = base.end(&live);

        let ended = over_images(&capture, &files, |index, image| {
            let (secret, temporary) = (secret.clone(), temporary.clone());
            async move {
                let mut held_slot = None;
                let (fresh, reopened) = reopen(&image, &secret, &operations, |fresh| { held_slot = Some(fresh.script.hold("CreateMoment", LANDED)); }).await;
                let held = held_slot.take().unwrap();
                crate::test_wait::settled(&reopened, "opened", || true).await;
                let status = reopened.session_status().unwrap();
                let epoch = reopened.session_epoch();
                let ended = status.phase == SessionStanding::None;
                assert!(ended || status.phase == SessionStanding::Active, "server_end {server_end} cut {index}: {:?}", status.phase);
                if ended {
                    assert_eq!(queue_now(&reopened).await, 0, "server_end {server_end} cut {index}: the ended session has no work");
                    assert!(!matches!(reopened.peek_row(epoch, "Moment".into(), temporary.clone()).await, Ok(Some(_))), "server_end {server_end} cut {index}: the ended session has no optimistic row");
                    assert!(fresh.rig.credentials.inner.value().is_none(), "server_end {server_end} cut {index}: the open clears the old credential");
                    assert_eq!(fresh.script.count("CreateMoment"), 0, "server_end {server_end} cut {index}: the old work is not sent");
                } else {
                    assert_eq!(status.identity, signed.identity, "server_end {server_end} cut {index}: the session is the same");
                    assert_eq!(queue_now(&reopened).await, 1, "server_end {server_end} cut {index}: the offline work is intact");
                    assert!(reopened.peek_row(epoch, "Moment".into(), temporary.clone()).await.unwrap().is_some(), "server_end {server_end} cut {index}: the optimistic row is intact");
                    held.release();
                    drained(&reopened).await;
                    assert_eq!(fresh.script.count("CreateMoment"), 1, "server_end {server_end} cut {index}: the work is sent once");
                }
                bound(reopened.close_database()).await.unwrap();
                ended
            }
        }).await;
        assert!(!ended[0] && *ended.last().unwrap(), "server_end {server_end}: the first cut is the whole session and the last is the ended one: {ended:?}");
        assert!(ended.windows(2).all(|pair| !pair[0] || pair[1]), "server_end {server_end}: an ended session never comes back: {ended:?}");
        database.close_now();
    }
}
