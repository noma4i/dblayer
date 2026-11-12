use super::crash_image_support::{advancing, drained, over_images, reopen_timed, saved_work, timed_stand, Capture};
use super::rig_tests::bound;
use super::upload_stand::{plain, shows, watched, CREATE_MOMENT};
use crate::ffi::wire::WireSyncState;
use crate::ffi::DbError;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;

fn saved_status(image: &std::path::Path) -> Option<String> {
    let connection = rusqlite::Connection::open(image).unwrap();
    rusqlite::OptionalExtension::optional(connection.query_row("SELECT status FROM op", [], |row| row.get::<_, String>(0))).unwrap()
}

/// A process killed before or after any commit of the `retry` of a kept refusal leaves a file that sends nothing while the refusal is kept, sends the operation once after the `retry` committed or is run, and lands the row once.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_around_the_retry_of_a_kept_refusal_sends_only_after_the_retry() {
    let (stand, clock) = timed_stand();
    let stand = stand.keep_moment();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", REFUSED);
    let sent = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    let refusal = advancing(&clock, bound(database.confirmed(sent, false))).await;
    assert!(matches!(refusal, Err(DbError::Rejected { kept: true, .. })), "{refusal:?}");
    let rows = watched(&database, "Moment", &temporary);
    crate::test_wait::settled(&database, "the refusal is shown", || shows(&rows, WireSyncState::Failed)).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    stand.script.data("CreateMoment", LANDED);
    bound(database.retry(epoch, "Moment".into(), temporary.clone(), None, None)).await.unwrap();
    advancing(&clock, drained(&database)).await;
    assert!(database.peek_row(epoch, "Moment".into(), "m77".into()).unwrap().is_some(), "the live retry lands the row");
    let (capture, files) = base.end(&live);

    let kept = over_images(&capture, &files, |index, image| {
        let (secret, temporary) = (secret.clone(), temporary.clone());
        async move {
            let status = saved_status(&image);
            let (saved, answered) = saved_work(&image);
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &operations, |fresh| fresh.script.data("CreateMoment", LANDED), true).await;
            crate::test_wait::settled(&reopened, "opened", || true).await;
            let epoch = reopened.session_epoch();
            let kept = status.as_deref() == Some("failed");
            if kept {
                assert_eq!(fresh.script.count("CreateMoment"), 0, "cut {index}: a kept refusal sends nothing until the retry");
                bound(reopened.retry(epoch, "Moment".into(), temporary.clone(), None, None)).await.unwrap();
            }
            advancing(&clock, drained(&reopened)).await;
            assert_eq!(fresh.script.count("CreateMoment"), usize::from(kept || (saved == 1 && answered == 0)), "cut {index}: the operation is sent once, never when its answer was saved or it had landed");
            assert!(reopened.peek_row(epoch, "Moment".into(), "m77".into()).unwrap().is_some(), "cut {index}: the row landed");
            bound(reopened.close_database()).await.unwrap();
            kept
        }
    }).await;
    assert!(kept[0] && !kept.last().unwrap(), "the first cut holds the kept refusal and the last holds the landed row: {kept:?}");
    database.close_now();
}
