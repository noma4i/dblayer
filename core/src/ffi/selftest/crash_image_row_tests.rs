use super::crash_image_support::{over_images, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{ReadRequest, ReadValue};

const CREATED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// A process killed after the commit that saved the server answer of a row create and before the landing leaves a file that reopens with the row landed, the optimistic row gone, the operation queue empty and no second request.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_with_the_saved_create_answer_reopens_with_the_row_landed() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let connection = rusqlite::Connection::open(&live).unwrap();
    connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    drop(connection);
    let base = Capture::begin(&live);
    stand.script.data("CreateMoment", CREATED);
    let epoch = database.session_epoch();
    let created = bound(database.send(plain("Moment", r#"{"text":"hello"}"#, epoch))).await.unwrap();
    let temporary = created.optimistic_value.as_ref().unwrap().id.clone();
    bound(database.confirmed(created, false)).await.unwrap();
    let (capture, files) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let answered = over_images(&capture, &files, |index, image| {
        let secret = secret.clone();
        let temporary = temporary.clone();
        async move {
            let saved: i64 = rusqlite::Connection::open(&image).unwrap().query_row("SELECT count(*) FROM encrypted_answer", [], |row| row.get(0)).unwrap();
            if saved == 0 { return false }
            let mut fresh = Stand::new();
            fresh.rig.credentials = std::sync::Arc::new(WatchedHeld::default());
            crate::ffi::CredentialStore::set(fresh.rig.credentials.as_ref(), secret).unwrap();
            let held = fresh.script.hold("CreateMoment", CREATED);
            let reopened = fresh.open(Some(&image), &operations).await;
            let epoch = reopened.session_epoch();
            crate::test_wait::settled(&reopened, "quiet", || true).await;
            let stream = reopened.subscribe_values(epoch, ReadRequest::Operations);
            let queue = match bound(stream.pull_next()).await {
                Some(ReadValue::Operations { items, .. }) => items.len(),
                other => panic!("the operation queue ended: {other:?}"),
            };
            let landed = reopened.peek_row(epoch, "Moment".into(), "m77".into()).unwrap().is_some();
            let optimistic = reopened.peek_row(epoch, "Moment".into(), temporary).unwrap().is_some();
            assert_eq!((landed, optimistic, fresh.script.count("CreateMoment")), (true, false, 0), "cut {index}: the saved answer lands the row once, with no second request");
            assert_eq!(queue, 0, "cut {index}: the landed create leaves the queue");
            held.release();
            bound(reopened.close_database()).await.unwrap();
            true
        }
    }).await;
    assert!(answered.contains(&true), "the capture holds a cut with the saved answer");
    database.close_now();
}
