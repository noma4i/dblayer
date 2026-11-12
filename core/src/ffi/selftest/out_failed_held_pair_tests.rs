use super::crash_image_support::advancing;
use super::rig_tests::bound;
use super::upload_stand::{plain, shows, watched, Answer, Stand, CREATE_MOMENT};
use crate::ffi::wire::WireSyncState;
use crate::ffi::{Database, TestClock};
use std::sync::Arc;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const OTHER: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;

fn pending_rows(image: &std::path::Path) -> i64 {
    rusqlite::Connection::open(image).unwrap().query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap()
}

async fn until_failed(database: &Database) {
    let stream = database.subscribe_values(database.session_epoch(), crate::ffi::ReadRequest::Operations);
    loop {
        match bound(stream.pull_next()).await {
            Some(crate::ffi::ReadValue::Operations { items, .. }) if items.iter().any(|item| item.status == crate::ffi::OperationQueueStatus::Failed) => return,
            Some(crate::ffi::ReadValue::Operations { .. }) => {}
            other => panic!("the operation queue ended: {other:?}"),
        }
    }
}

/// Waits on the queue events, with the clock moving over every wait armed before and after the call, until the row is read.
async fn row_present(database: &Database, clock: &TestClock, id: &'static str, what: &str) {
    let epoch = database.session_epoch();
    let stream = database.subscribe_values(epoch, crate::ffi::ReadRequest::Operations);
    let wait = async {
        loop {
            if database.peek_row(epoch, "Moment".into(), id.into()).unwrap().is_some() { return; }
            match bound(stream.pull_next()).await {
                Some(crate::ffi::ReadValue::Operations { .. }) => {}
                other => panic!("{what}: the operation queue ended: {other:?}"),
            }
        }
    };
    clock.advance(60_000);
    advancing(clock, wait).await;
}

#[derive(Clone, Copy, Debug)]
enum Case {
    /// The pair released by `Failed` is refused at last: A stays failed and its `retry` takes a new request id.
    ReleasedRefused,
    /// The lost answer of the released pair comes as a success: A lands by itself.
    ReleasedLands,
    /// `retry` of A while the released pair waits for its replay: the server takes one write of A.
    RetryWhileReleased,
    /// A permanent refusal without a sequence: nothing is held.
    NoPair,
}

fn open_stand(clock_stand: (Stand, Arc<TestClock>)) -> (Stand, Arc<TestClock>) { (clock_stand.0.keep_moment(), clock_stand.1) }

async fn scenario(case: Case) {
    let (stand, clock) = open_stand(super::crash_image_support::timed_stand());
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let epoch = database.session_epoch();
    match case {
        Case::NoPair => stand.script.data("CreateMoment", REFUSED),
        _ => for _ in 0..40 { stand.script.enqueue("CreateMoment", Answer::Lost); },
    }
    match case {
        Case::ReleasedRefused => stand.script.data("CreateMoment", REFUSED),
        Case::ReleasedLands => stand.script.data("CreateMoment", LANDED),
        Case::RetryWhileReleased => { stand.script.enqueue("CreateMoment", Answer::Lost); stand.script.data("CreateMoment", LANDED); }
        Case::NoPair => {}
    }
    let first = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    let temporary = first.optimistic_value.as_ref().unwrap().id.clone();
    let rows = watched(&database, "Moment", &temporary);
    advancing(&clock, until_failed(&database)).await;
    if matches!(case, Case::RetryWhileReleased) {
        bound(database.retry(epoch, "Moment".into(), temporary.clone(), None, None)).await.unwrap();
    }
    stand.script.data("CreateMoment", OTHER);
    bound(database.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();
    row_present(&database, &clock, "m78", &format!("{case:?}: the second row landed without a retry or discard of the first")).await;
    match case {
        Case::ReleasedLands | Case::RetryWhileReleased => {
            row_present(&database, &clock, "m77", "the first row landed from the released pair").await;
            let pairs: std::collections::BTreeSet<_> = stand.script.operation_pairs("CreateMoment").into_iter().collect();
            assert_eq!(pairs.len(), 2, "{case:?}: the server took one write of A and one of B: {pairs:?}");
        }
        Case::ReleasedRefused | Case::NoPair => {
            crate::test_wait::settled(&database, "the first operation stays failed", || shows(&rows, WireSyncState::Failed)).await;
            let before = stand.script.operation_pairs("CreateMoment");
            stand.script.data("CreateMoment", LANDED);
            bound(database.retry(epoch, "Moment".into(), temporary.clone(), None, None)).await.unwrap();
            row_present(&database, &clock, "m77", "the retry landed").await;
            let pairs = stand.script.operation_pairs("CreateMoment");
            let (released, retried) = (&before[before.len() - 2], pairs.last().unwrap());
            assert_ne!(retried.0, released.0, "{case:?}: the retry after the answer of the pair takes a new request id");
        }
    }
    bound(database.close_database()).await.unwrap();
    assert_eq!(pending_rows(&live), 0, "{case:?}: no pair is left");
}

/// out_failed_releases_held_pair: an operation that enters `Failed` with a lost pair lets Session replay it, the writes behind it go out, and the pair is written once on the server.
#[tokio::test(flavor = "multi_thread")]
async fn out_failed_releases_held_pair() {
    for case in [Case::ReleasedRefused, Case::ReleasedLands, Case::RetryWhileReleased, Case::NoPair] { scenario(case).await; }
}

/// A process that died after the operation ended `Failed` with its pair held opens, sends the pair again once, and the write of another row goes out without a `retry` or `discard`.
#[tokio::test(flavor = "multi_thread")]
async fn out_failed_held_pair_restart_frees_the_queue() {
    let (stand, clock) = open_stand(super::crash_image_support::timed_stand());
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    for _ in 0..40 { stand.script.enqueue("CreateMoment", Answer::Lost); }
    stand.script.data("CreateMoment", REFUSED);
    let first = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    let temporary = first.optimistic_value.as_ref().unwrap().id.clone();
    advancing(&clock, until_failed(&database)).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let image = directory.path().join("image.sqlite");
    super::crash_image_call_tests::copy_image(&live, &image);
    let held = super::crash_image_support::saved_pair(&image).is_some();
    database.close_now();
    let (_fresh, reopened, clock) = super::crash_image_support::reopen_timed(&image, &secret, &operations, |fresh| {
        if held { fresh.script.data("CreateMoment", REFUSED); }
        fresh.script.data("CreateMoment", OTHER);
    }, true).await;
    let epoch = reopened.session_epoch();
    let rows = watched(&reopened, "Moment", &temporary);
    advancing(&clock, until_failed(&reopened)).await;
    bound(reopened.send(plain("Moment", r#"{"text":"B"}"#, epoch))).await.unwrap();
    row_present(&reopened, &clock, "m78", "the second row landed without a retry or discard of the first").await;
    crate::test_wait::settled(&reopened, "the first operation stays failed", || shows(&rows, WireSyncState::Failed)).await;
    bound(reopened.close_database()).await.unwrap();
}
