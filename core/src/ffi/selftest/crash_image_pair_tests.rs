use super::crash_image_support::{advancing, drained, over_images, reopen_timed, saved_pair, saved_work, timed_stand, Capture};
use super::rig_tests::bound;
use super::upload_stand::{plain, Answer, CREATE_MOMENT};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const NEXT: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// The answer of the first send is lost or too large; the pair of that send is held and sent again until an answer comes. A process killed at any commit of the resend leaves a file that sends the held pair once (nothing when the answer was saved), and the next mutation takes the next sequence and a new request id.
async fn pair_held_at_every_commit(fault: fn() -> Answer) {
    let (stand, clock) = timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap();
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    stand.script.enqueue("CreateMoment", fault());
    stand.script.data("CreateMoment", LANDED);
    database.set_online(true).unwrap();
    advancing(&clock, drained(&database)).await;
    let held = stand.script.operation_pairs("CreateMoment");
    assert!(held.len() >= 2 && held.iter().all(|pair| pair == &held[0]), "the live run sends the held pair again: {held:?}");
    let (capture, files) = base.end(&live);

    over_images(&capture, &files, |index, image| {
        let (secret, held) = (secret.clone(), held[0].clone());
        async move {
            let saved = saved_pair(&image);
            let (saved_operations, answered) = saved_work(&image);
            let sends_first = saved_operations == 1 && answered == 0;
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &operations, |fresh| {
                if sends_first { fresh.script.data("CreateMoment", LANDED); }
                fresh.script.data("CreateMoment", NEXT);
            }, false).await;
            advancing(&clock, drained(&reopened)).await;
            let first = fresh.script.operation_pairs("CreateMoment");
            assert_eq!(first.len(), usize::from(sends_first), "cut {index}: the first send goes out once, never when its answer was saved: {first:?}");
            if let (Some(saved), Some(sent)) = (saved, first.first()) { assert_eq!(sent.0, saved, "cut {index}: the held pair is sent"); assert_eq!(sent, &held, "cut {index}: the pair is the live one"); }
            let epoch = reopened.session_epoch();
            bound(reopened.send(plain("Moment", r#"{"text":"second"}"#, epoch))).await.unwrap();
            advancing(&clock, drained(&reopened)).await;
            let pairs = fresh.script.operation_pairs("CreateMoment");
            assert_eq!(pairs.len(), usize::from(sends_first) + 1, "cut {index}: one request for the next mutation: {pairs:?}");
            let next = pairs.last().unwrap();
            assert_eq!(next.1, held.1.map(|sequence| sequence + 1), "cut {index}: the next mutation takes the next sequence");
            assert_ne!(next.0, held.0, "cut {index}: the next mutation has a new request id");
            bound(reopened.close_database()).await.unwrap();
        }
    }).await;
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_during_the_resend_after_a_lost_answer_resends_the_held_pair_once() { pair_held_at_every_commit(|| Answer::Lost).await; }

#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_during_the_resend_after_a_too_large_answer_resends_the_held_pair_once() { pair_held_at_every_commit(|| Answer::ResponseTooLarge(1024)).await; }
