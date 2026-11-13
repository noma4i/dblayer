use super::crash_image_support::{over_images, reopen, Capture};
use super::rig_tests::bound;
use super::upload_stand::{plain, request, Stand, BLOB, CREATE_MOMENT};

const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;

fn holds_only_an_upload(image: &std::path::Path) -> bool {
    let connection = rusqlite::Connection::open(image).unwrap();
    let count = |table: &str| connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get::<_, i64>(0)).unwrap();
    count("op") == 0 && count("sync_upload") > 0
}

/// A process killed after the rollback of a refused `create` and before its waiting upload ended leaves a file that ends the upload as refused and never sends its final mutation with the temporary id.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_after_a_refused_create_ends_the_upload_on_its_row_without_a_request() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let epoch = database.session_epoch();
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    stand.script.data("CreateBlob", BLOB);
    let refused = stand.script.hold("CreateMoment", REFUSED);
    let created = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    let temporary = created.optimistic_value.as_ref().unwrap().id.clone();
    refused.entered().await;
    let upload = bound(database.upload(epoch, "Moment".into(), temporary.clone(), "upload_clip".into(), request(&file))).await.unwrap();
    refused.release();
    let ended = bound(database.confirmed(upload, false)).await;
    assert!(matches!(ended, Err(crate::ffi::DbError::Rejected { kept: false, .. })), "the live upload ends refused: {ended:?}");
    let (capture, files) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &files, |index, image| {
        let (secret, temporary) = (secret.clone(), temporary.clone());
        async move {
            if !holds_only_an_upload(&image) { return false; }
            let (fresh, reopened) = reopen(&image, &secret, &operations, |fresh| fresh.script.data("CreateBlob", BLOB)).await;
            let epoch = reopened.session_epoch();
            crate::test_wait::settled(&reopened, "the upload on the refused row ended", || reopened.peek_row_now(epoch, "Moment".into(), temporary.clone()).unwrap().is_none()).await;
            assert_eq!(fresh.script.count("CreateMoment"), 0, "cut {index}: the final mutation of the upload is not sent");
            bound(reopened.close_database()).await.unwrap();
            true
        }
    }).await;
    assert!(cuts.iter().any(|cut| *cut), "a cut holds the upload after the rollback of its create: {cuts:?}");
    database.close_now();
}
