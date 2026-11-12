use super::crash_image_support::{copy_tree, drained, files_of, over_images, saved_work, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, items, OPERATIONS};
use super::upload_stand::Stand;
use crate::ffi::upload_wire::WireUploadBatch;

const ASSETS: &str = r#"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":3,"position":0},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a2","clientKey":"key-2","size":3,"position":2}]}}}"#;

fn landed(database: &crate::ffi::Database) -> usize {
    ["a0", "a1", "a2"].iter().filter(|id| database.peek_row(database.session_epoch(), "Asset".into(), id.to_string()).unwrap().is_some()).count()
}

/// A process killed after any commit of a three file batch upload, also while the final request is held, leaves a file that finishes the batch with every step sent at most once and all three rows landed, or finds nothing begun, and never lands a part of the batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_group_upload_reopens_with_the_group_finished() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let sources = tempfile::tempdir().unwrap();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    for index in 0..3 { stand.script.data("CreateBlob", &blob(index)); }
    let held = stand.script.hold("CreateAssets", ASSETS);
    let kept_files = directory.path().join("kept.files");
    bound(database.upload_create_batch(database.session_epoch(), "Asset".into(), items(sources.path()), WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    held.entered().await;
    copy_tree(&files_of(&live), &kept_files);
    held.release();
    crate::test_wait::settled(&database, "the batch landed", || landed(&database) == 3).await;
    drained(&database).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &kept_files, |index, image| {
        let secret = secret.clone();
        async move {
            let (saved, _) = saved_work(&image);
            let mut fresh = Stand::new();
            fresh.rig.credentials = std::sync::Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default());
            crate::ffi::CredentialStore::set(fresh.rig.credentials.as_ref(), secret.clone()).unwrap();
            let clock = crate::ffi::TestClock::new(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);
            for blob_index in 0..3 { fresh.script.data("CreateBlob", &blob(blob_index)); }
            fresh.script.data("CreateAssets", ASSETS);
            let reopened = fresh.open_with(Some(&image), &OPERATIONS, Some(clock.clone())).await;
            drained(&reopened).await;
            let whole = landed(&reopened);
            assert!(whole == 0 || whole == 3, "cut {index}: the batch lands whole or not at all: {whole}");
            assert!(whole == 3 || saved == 0, "cut {index}: a saved batch is finished");
            let pairs: std::collections::HashSet<_> = fresh.script.operation_pairs("CreateAssets").into_iter().map(|pair| pair.0).collect();
            assert!(pairs.len() <= 1, "cut {index}: the rows are created by one request: {pairs:?}");
            for key in 0..3 {
                let name = format!("{key}.jpg");
                let sent: std::collections::HashSet<_> = fresh.script.pairs("CreateBlob", &name).into_iter().map(|pair| pair.0).collect();
                assert!(sent.len() <= 1, "cut {index}: the blob of {name} is asked once: {sent:?}");
            }
            bound(reopened.close_database()).await.unwrap();
            (whole == 3, saved)
        }
    }).await;
    let whole: Vec<bool> = cuts.iter().map(|cut| cut.0).collect();
    assert!(!whole[0] && *whole.last().unwrap() && whole.windows(2).all(|pair| !pair[0] || pair[1]), "{whole:?}");
    assert!(cuts.iter().any(|cut| cut.1 > 0 && cut.0), "a cut holds the batch unfinished");
    database.close_now();
}
