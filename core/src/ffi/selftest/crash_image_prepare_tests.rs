use std::path::PathBuf;
use std::sync::Arc;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen_preparing, saved_work, unaccepted, Capture};
use super::rig_tests::bound;
use super::upload_prepare_tests::{probed_image, send, send_limited, shrink, upload_of, PREPARED, SOURCE};
use crate::ffi::wire::WireUploadStatus;
use super::upload_stand::{ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn landed(database: &crate::ffi::Database) -> bool { database.peek_row_now(database.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some() }

/// A process killed after any commit of an upload with a prepare step leaves a file that runs the step again from the copy, finishes the upload with the final mutation sent once and a body that is the prepared file, and never goes back from a landed row.
#[tokio::test(flavor = "multi_thread")]
async fn crash_image_prepare() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.jpg");
    std::fs::write(&file, SOURCE).unwrap();
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
    let stand = Stand::new().with_preparer(preparer.clone());
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let epoch = database.session_epoch();
    super::crash_image_support::checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    let run_hold = preparer.hold_next();
    let put_hold = stand.puts.hold_next();
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(epoch, &file, shrink()))).await.unwrap();
    let kept_files = directory.path().join("kept.files");
    assert_eq!(upload_of(&database, &receipt.optimistic_value.as_ref().unwrap().id).map(|upload| upload.status), Some(WireUploadStatus::Preparing), "the accepted transfer waits in the prepare step");
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
            let unaccepted = unaccepted(&image);
            let fresh_preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
            let (fresh, reopened) = reopen_preparing(&image, &secret, &operations, Some(fresh_preparer.clone() as Arc<dyn crate::ffi::Preparer>), |fresh| {
                fresh.script.data("CreateBlob", BLOB);
                fresh.script.data("CreateMoment", LANDED);
            }).await;
            crate::test_wait::settled(&reopened, "opened", || true).await;
            drained(&reopened).await;
            let pairs: std::collections::HashSet<_> = fresh.script.operation_pairs("CreateMoment").into_iter().map(|pair| pair.0).collect();
            assert!(pairs.len() <= 1, "cut {index}: the row is created by one request: {pairs:?}");
            let is_landed = landed(&reopened);
            assert!(is_landed || saved == 0 || unaccepted, "cut {index}: a saved upload is finished");
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

/// The `max_bytes` of the source survives a process killed while the step runs, or after the copy and before the refusal: the fresh process ends the upload with `too_large` and no transfer request.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_too_large_after_restart() {
    for prepared in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let file = directory.path().join("clip.jpg");
        std::fs::write(&file, SOURCE).unwrap();
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        let stand = if prepared { Stand::new().with_preparer(preparer.clone()) } else { Stand::new() };
        let operations = [CREATE_MOMENT];
        let database = stand.open(Some(&live), &operations).await;
        super::crash_image_support::checkpoint_live(&database, &live).await;
        let base = Capture::begin(&live);
        let run_hold = prepared.then(|| preparer.hold_next());
        let (prepare, limit) = if prepared { (Some(shrink()), PREPARED.len() as u64 - 1) } else { (None, SOURCE.len() as u64 - 1) };
        let receipt = bound(database.send(send_limited(database.session_epoch(), &file, prepare, Some(limit)))).await.unwrap();
        let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
        let kept_files = directory.path().join("kept.files");
        if let Some(hold) = &run_hold { hold.entered().await; }
        else { crate::test_wait::settled(&database, "the refused size is shown", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await; }
        copy_tree(&files_of(&live), &kept_files);
        let (capture, _) = base.end(&live);
        let secret = stand.rig.credentials.inner.value().unwrap();
        if let Some(hold) = &run_hold { hold.release(); }
        database.close_now();

        let cuts = over_images(&capture, &kept_files, |_, image: PathBuf| {
            let secret = secret.clone();
            let id = id.clone();
            async move {
                let accepted = !unaccepted(&image) && saved_work(&image).0 > 0;
                let fresh_preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
                let (fresh, reopened) = reopen_preparing(&image, &secret, &[CREATE_MOMENT], Some(fresh_preparer as Arc<dyn crate::ffi::Preparer>), |fresh| {
                    fresh.script.data("CreateBlob", BLOB);
                }).await;
                let kind = if accepted {
                    crate::test_wait::settled(&reopened, "the refused size is shown", || upload_of(&reopened, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
                    upload_of(&reopened, &id).and_then(|upload| upload.error).map(|error| error.kind)
                } else { None };
                let requests = (fresh.script.count("CreateBlob"), fresh.puts.bodies.lock().unwrap().len());
                bound(reopened.close_database()).await.unwrap();
                (accepted, kind, requests)
            }
        }).await;
        assert!(cuts.iter().any(|cut| cut.0), "prepared={prepared}: a cut holds the accepted upload");
        assert!(cuts.iter().filter(|cut| cut.0).all(|cut| cut.1.as_deref() == Some("too_large") && cut.2 == (0, 0)), "prepared={prepared}: {cuts:?}");
    }
}
