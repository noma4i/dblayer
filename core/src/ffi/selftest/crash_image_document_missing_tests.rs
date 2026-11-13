use std::sync::Arc;

use super::crash_image_call_tests::copy_image;
use super::crash_image_support::{advancing, drained, over_images, reopen_timed, saved_pair, Capture};
use super::rig_tests::bound;
use super::upload_stand::{plain, Answer, Hold, Stand, CREATE_MOMENT};
use crate::ffi::{Database, TestClock};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const NEXT: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

#[derive(Clone, Copy, Debug)]
enum PairPath {
    LostAnswerRetry,
    AfterOffline,
    AfterRestart,
}

fn pending_rows(image: &std::path::Path) -> i64 {
    rusqlite::Connection::open(image).unwrap().query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap()
}

async fn first_send(database: &Database) {
    let epoch = database.session_epoch();
    bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap();
}

/// The server forgot the hash after the pair was sent once: the next send of the same pair carries the full document under the same number, whatever way the pair came back.
async fn forgotten_hash(way: PairPath) {
    let (stand, clock) = super::crash_image_support::timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    database.set_online(false).unwrap();
    first_send(&database).await;
    let (served, process, clock, image): (Stand, Arc<Database>, Arc<TestClock>, std::path::PathBuf) = match way {
        PairPath::LostAnswerRetry => {
            let hold = Arc::new(Hold::default());
            stand.script.enqueue("CreateMoment", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
            database.set_online(true).unwrap();
            hold.entered().await;
            stand.script.forget_documents();
            stand.script.data("CreateMoment", LANDED);
            advancing(&clock, async { hold.release(); drained(&database).await }).await;
            stand.script.data("CreateMoment", NEXT);
            (stand, database, clock, live)
        }
        PairPath::AfterOffline => {
            let hold = Arc::new(Hold::default());
            stand.script.enqueue("CreateMoment", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
            database.set_online(true).unwrap();
            hold.entered().await;
            stand.script.forget_documents();
            database.set_online(false).unwrap();
            hold.dropped().await;
            stand.script.data("CreateMoment", LANDED);
            stand.script.data("CreateMoment", NEXT);
            database.set_online(true).unwrap();
            advancing(&clock, drained(&database)).await;
            (stand, database, clock, live)
        }
        PairPath::AfterRestart => {
            let hold = Arc::new(Hold::default());
            stand.script.enqueue("CreateMoment", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
            database.set_online(true).unwrap();
            hold.entered().await;
            let image = directory.path().join("image.sqlite");
            copy_image(&live, &image);
            assert!(saved_pair(&image).is_some(), "the image holds the sent pair");
            database.close_now();
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &operations, |fresh| {
                fresh.script.forget_documents();
                fresh.script.data("CreateMoment", LANDED);
                fresh.script.data("CreateMoment", NEXT);
            }, false).await;
            advancing(&clock, drained(&reopened)).await;
            (fresh, reopened, clock, image)
        }
    };
    let own = served.script.operation_pairs("CreateMoment");
    let own_full = served.script.full_pairs("CreateMoment");
    let tail = own.len() - 1;
    assert!(own.len() >= 2, "{way:?}: the pair went out again: {own:?}");
    assert_eq!(own[tail].1, own[0].1, "{way:?}: the full pair keeps the number: {own:?}");
    assert!(!own_full[0] && own_full[tail], "{way:?}: hash-only first, then the full document: {own_full:?}");
    assert_eq!(own_full.iter().filter(|full| **full).count(), 1, "{way:?}: the full document goes once: {own_full:?}");
    process.set_online(true).unwrap();
    let epoch = process.session_epoch();
    bound(process.send(plain("Moment", r#"{"text":"second"}"#, epoch))).await.unwrap();
    advancing(&clock, drained(&process)).await;
    let pairs = served.script.operation_pairs("CreateMoment");
    assert_eq!(pairs.last().unwrap().1, own[0].1.map(|number| number + 1), "{way:?}: the next mutation takes the next number");
    bound(process.close_database()).await.unwrap();
    assert_eq!(pending_rows(&image), 0, "{way:?}: no pair is left");
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_held_pair_document_missing_any_origin() {
    for way in [PairPath::LostAnswerRetry, PairPath::AfterOffline, PairPath::AfterRestart] { forgotten_hash(way).await; }
}

const INVALID_INPUT: &str = r#"{"errors":[{"extensions":{"code":"INVALID_INPUT"},"message":"INVALID_INPUT"}]}"#;

#[tokio::test(flavor = "multi_thread")]
async fn sess_refused_held_pair_is_not_sent_again_and_spends_no_number() {
    let (stand, clock) = super::crash_image_support::timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [CREATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    database.set_online(false).unwrap();
    first_send(&database).await;
    let hold = Arc::new(Hold::default());
    stand.script.enqueue("CreateMoment", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
    database.set_online(true).unwrap();
    hold.entered().await;
    let image = directory.path().join("image.sqlite");
    copy_image(&live, &image);
    database.close_now();
    let (fresh, reopened, clock) = reopen_timed(&image, &secret, &operations, |fresh| {
        fresh.script.enqueue("CreateMoment", Answer::Unconfirmed(INVALID_INPUT.into()));
        fresh.script.data("CreateMoment", NEXT);
    }, false).await;
    advancing(&clock, drained(&reopened)).await;
    let epoch = reopened.session_epoch();
    bound(reopened.send(plain("Moment", r#"{"text":"second"}"#, epoch))).await.unwrap();
    advancing(&clock, drained(&reopened)).await;
    let pairs = fresh.script.operation_pairs("CreateMoment");
    assert_eq!(pairs.len(), 2, "the refused pair is not sent again: {pairs:?}");
    assert_eq!(pairs[1].1, pairs[0].1, "the refusal spent no number: {pairs:?}");
    bound(reopened.close_database()).await.unwrap();
    assert_eq!(pending_rows(&image), 0, "no pair is left");
}

const CREATED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","text":"first","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATED: &str = r#"{"data":{"updateMoment":{"__typename":"Moment","id":"m77","uuid":"u","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATE_MOMENT: (&str, &str) = ("UpdateMoment", "mutation UpdateMoment { updateMoment { id uuid text likedByMe media createdAt } }");

const CREATED_OTHER: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m78","uuid":"v","text":"other","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// Update A of a row (answer lost), update B of the same row, create C of another row: a process killed after any commit of this run leaves a file that opens, ends all three operations with the updates of one row in the order they were made, and leaves no pair.
#[tokio::test(flavor = "multi_thread")]
async fn sess_held_pair_author_moves_it_after_restart() {
    let (stand, clock) = super::crash_image_support::timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [super::upload_batch_shift_tests::OPERATIONS[0], super::upload_batch_shift_tests::OPERATIONS[1], super::upload_batch_shift_tests::OPERATIONS[2], CREATE_MOMENT, UPDATE_MOMENT];
    let database = stand.open_with(Some(&live), &operations, Some(clock.clone())).await;
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", CREATED);
    let created = bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap();
    bound(database.confirmed(created, false)).await.unwrap();
    advancing(&clock, drained(&database)).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    let hold = Arc::new(Hold::default());
    stand.script.enqueue("UpdateMoment", Answer::Held(hold.clone(), Box::new(Answer::Lost)));
    stand.script.data("UpdateMoment", UPDATED);
    stand.script.data("UpdateMoment", UPDATED);
    stand.script.data("CreateMoment", CREATED_OTHER);
    bound(database.update(epoch, "Moment".into(), "m77".into(), r#"{"text":"edited"}"#.into(), "{}".into())).await.unwrap();
    hold.entered().await;
    bound(database.update(epoch, "Moment".into(), "m77".into(), r#"{"text":"edited again"}"#.into(), "{}".into())).await.unwrap();
    bound(database.send(plain("Moment", r#"{"text":"other"}"#, epoch))).await.unwrap();
    advancing(&clock, async { hold.release(); drained(&database).await }).await;
    let (capture, files) = base.end(&live);

    let held = over_images(&capture, &files, |index, image| {
        let (secret, operations) = (secret.clone(), operations);
        async move {
            let saved = saved_pair(&image).is_some();
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &operations, |fresh| {
                fresh.script.data("UpdateMoment", UPDATED);
                fresh.script.data("UpdateMoment", UPDATED);
                fresh.script.data("CreateMoment", CREATED_OTHER);
            }, false).await;
            if tokio::time::timeout(std::time::Duration::from_secs(8), advancing(&clock, drained(&reopened))).await.is_err() { { let connection = rusqlite::Connection::open(&image).unwrap(); let ops: Vec<String> = connection.prepare("SELECT seq || ':' || status || ':' || attempts || ':' || action_key FROM op").unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap().map(|row| row.unwrap()).collect(); panic!("cut {index}: the queue stands: sent {:?} ops {ops:?} pair {:?} owner {:?}", fresh.script.names(), saved_pair(&image), connection.query_row("SELECT owner_kind || owner_operation_id || sequence FROM encrypted_pending", [], |row| row.get::<_, String>(0)).ok()); } }
            let updates: Vec<String> = fresh.script.variables("UpdateMoment").iter().map(|variables| variables["text"].as_str().unwrap_or_default().to_string()).collect();
            assert!(updates.windows(2).all(|pair| pair[0] == "edited" && pair[1] == "edited again") && updates.len() <= 2, "cut {index}: the updates go in the order they were made: {updates:?}");
            bound(reopened.close_database()).await.unwrap();
            assert_eq!(pending_rows(&image), 0, "cut {index}: no pair is left");
            saved
        }
    }).await;
    assert!(held.iter().any(|saved| *saved), "some cut holds the pair of the first update: {held:?}");
    database.close_now();
}
