use super::crash_image_support::{drained, queue_now, reopen};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::SessionStanding;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// An end-journal record of a kind this version never writes (a file of a newer version) -> reopen the file -> the open succeeds as the same signed-in session, the cached server rows are gone, the offline work waits in the queue and is sent once, and the record is removed. Defect caught: `Boot` refused every open with `Storage` and the app never started.
#[tokio::test(flavor = "multi_thread")]
async fn an_end_journal_record_of_an_unknown_kind_opens_the_saved_session_with_a_clean_cache() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let signed = database.session_status().unwrap();
    let secret = stand.rig.credentials.inner.value().unwrap();
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", LANDED);
    bound(database.send(plain("Moment", r#"{"text":"online"}"#, epoch))).await.unwrap();
    drained(&database).await;
    assert!(database.peek_row(epoch, "Moment".into(), "m77".into()).await.unwrap().is_some(), "the server row is cached");
    database.set_online(false).await.unwrap();
    bound(database.send(plain("Moment", r#"{"text":"offline"}"#, epoch))).await.unwrap();
    assert_eq!(queue_now(&database).await, 1, "the offline create waits in the queue");
    bound(database.close_database()).await.unwrap();
    database.close_now();
    rusqlite::Connection::open(&live).unwrap().execute("INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'written-by-a-newer-version', 1)", []).unwrap();

    let mut held_slot = None;
    let (fresh, reopened) = reopen(&live, &secret, &operations, |fresh| { held_slot = Some(fresh.script.hold("CreateMoment", LANDED)); }).await;
    let held = held_slot.take().unwrap();
    let status = reopened.session_status().unwrap();
    let epoch = reopened.session_epoch();
    assert_eq!((status.phase, status.identity), (SessionStanding::Active, signed.identity), "the user stays signed in");
    assert!(reopened.peek_row(epoch, "Moment".into(), "m77".into()).await.unwrap().is_none(), "the server cache is wiped");
    assert_eq!(queue_now(&reopened).await, 1, "the offline work is kept");
    held.release();
    drained(&reopened).await;
    assert_eq!(fresh.script.count("CreateMoment"), 1, "the kept work is sent once");
    bound(reopened.close_database()).await.unwrap();
    let journal: i64 = rusqlite::Connection::open(&live).unwrap().query_row("SELECT count(*) FROM reset_intent", [], |row| row.get(0)).unwrap();
    assert_eq!(journal, 0, "the unknown record is removed");
}
