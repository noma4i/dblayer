use std::path::PathBuf;
use std::sync::Arc;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen_preparing, saved_work, Capture};
use super::rig_tests::bound;
use super::upload_prepare_tests::{probed_image, send, shrink, PREPARED, SOURCE};
use super::upload_stand::{ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn landed(database: &crate::ffi::Database) -> bool { database.peek_row(database.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some() }

/// A process killed after any commit of an upload with a prepare step leaves a file that runs the step again from the copy, finishes the upload with the final mutation sent once and a body that is the prepared file, and never goes back from a landed row.
#[tokio::test(flavor = "multi_thread")]
async fn crash_image_prepare() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.jpg");
    std::fs::write(&file, SOURCE).unwrap();
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
    let stand = Stand::new().with_preparer(preparer.clone());
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let epoch = database.session_epoch();
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    let run_hold = preparer.hold_next();
    let put_hold = stand.puts.hold_next();
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(epoch, &file, shrink()))).await.unwrap();
    let kept_files = directory.path().join("kept.files");
    run_hold.entered().await;
    copy_tree(&files_of(&live), &kept_files);
    run_hold.release();
    put_hold.entered().await;
    copy_tree(&files_of(&live), &kept_files);
    put_hold.release();
    bound(database.confirmed(receipt, false)).await.unwrap();
    crate::test_wait::settled(&database, "the upload landed", || landed(&database)).await;
    drained(&database).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &kept_files, |index, image: PathBuf| {
        let secret = secret.clone();
        async move {
            let (saved, _) = saved_work(&image);
            let fresh_preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[0.5]);
            let (fresh, reopened) = reopen_preparing(&image, &secret, &operations, Some(fresh_preparer.clone() as Arc<dyn crate::ffi::Preparer>), |fresh| {
                fresh.script.data("CreateBlob", BLOB);
                fresh.script.data("CreateMoment", LANDED);
            }).await;
            crate::test_wait::settled(&reopened, "opened", || true).await;
            drained(&reopened).await;
            let pairs: std::collections::HashSet<_> = fresh.script.operation_pairs("CreateMoment").into_iter().map(|pair| pair.0).collect();
            assert!(pairs.len() <= 1, "cut {index}: the row is created by one request: {pairs:?}");
            let is_landed = landed(&reopened);
            assert!(is_landed || saved == 0, "cut {index}: a saved upload is finished");
            assert!(fresh.puts.bodies.lock().unwrap().iter().all(|body| body.as_slice() == PREPARED), "cut {index}: no byte of the source goes");
            assert!(fresh_preparer.runs().len() <= 1, "cut {index}: the step runs at most once");
            let runs = fresh_preparer.runs().len();
            bound(reopened.close_database()).await.unwrap();
            (is_landed, saved, runs)
        }
    }).await;
    let landed_cuts: Vec<bool> = cuts.iter().map(|cut| cut.0).collect();
    assert!(!landed_cuts[0] && *landed_cuts.last().unwrap() && landed_cuts.windows(2).all(|pair| !pair[0] || pair[1]), "{landed_cuts:?}");
    assert!(cuts.iter().any(|cut| cut.1 > 0 && cut.0 && cut.2 == 1), "a cut holds the upload before the step ends and the step runs again from the copy");
    database.close_now();
}
