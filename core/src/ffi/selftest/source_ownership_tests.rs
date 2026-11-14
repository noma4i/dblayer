use std::path::{Path, PathBuf};

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen, reopen_preparing, unaccepted, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, items, OPERATIONS};
use super::upload_prepare_tests::{probed_image, send, shrink, PREPARED, SOURCE};
use super::upload_stand::{clip, ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireSource, WireUploadBatch, WireUploadItem};

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn count(image: &Path, table: &str) -> i64 {
    rusqlite::Connection::open(image).unwrap().query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get(0)).unwrap()
}

fn transfer_files(image: &Path) -> usize {
    std::fs::read_dir(files_of(image).join("transfers")).map_or(0, |entries| entries.count())
}

/// Requirement D-376 (after the receipt): the file of the caller is read only during the call. Input: `send` with prepare settings, the receipt, the file of the caller deleted, the process killed before the prepare step ends. Observable: the upload after the open. Assert: it finishes, the final mutation goes once, and the body of the put is the prepared file. Catches: the restored transfer or the step reading the path of the caller again.
#[tokio::test(flavor = "multi_thread")]
async fn source_file_of_the_caller_is_not_read_after_the_receipt() {
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
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let receipt = bound(database.send(send(epoch, &file, shrink()))).await.unwrap();
    run_hold.entered().await;
    std::fs::remove_file(&file).unwrap();
    let kept_files = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept_files);
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();
    let last = capture.cuts() - 1;
    run_hold.release();
    drop(receipt);
    let results = over_images(&capture, &kept_files, |index, image: PathBuf| {
        let secret = secret.clone();
        async move {
            if index != last { return None; }
            let fresh_preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
            let (fresh, reopened) = reopen_preparing(&image, &secret, &operations, Some(fresh_preparer as std::sync::Arc<dyn crate::ffi::Preparer>), |fresh| {
                fresh.script.data("CreateBlob", BLOB);
                fresh.script.data("CreateMoment", LANDED);
            }).await;
            drained(&reopened).await;
            let landed = reopened.peek_row(reopened.session_epoch(), "Moment".into(), "m77".into()).await.unwrap().is_some();
            let bodies = fresh.puts.bodies.lock().unwrap().clone();
            bound(reopened.close_database()).await.unwrap();
            Some((landed, fresh.script.count("CreateMoment"), bodies))
        }
    }).await;
    let (landed, mutations, bodies) = results[last].clone().unwrap();
    assert!(landed && mutations == 1, "the upload finishes with one final mutation: {landed} {mutations}");
    assert!(!bodies.is_empty() && bodies.iter().all(|body| body.as_slice() == PREPARED), "the put carries the prepared file");
    database.close_now();
}

/// Requirement D-376 (before the receipt): a transfer whose copy was not durable when the process died is removed at the open, the file of the caller is not read. Input: crash images of every commit of a single upload and of a batch of 3, the files of the caller deleted. Observable: the image after the open. Assert: no transfer, no operation, no row of the model, no file of a transfer, no request. Catches: the restored `copying` row reading the path of the caller and ending as `missing_file`.
#[tokio::test(flavor = "multi_thread")]
async fn source_unaccepted_copy_is_removed_at_the_open() {
    for batch in [false, true] {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let sources = tempfile::tempdir().unwrap();
        let file = sources.path().join("clip.mp4");
        std::fs::write(&file, b"clip").unwrap();
        let operations: Vec<(&str, &str)> = if batch { OPERATIONS.to_vec() } else { vec![CREATE_MOMENT] };
        let database = stand.open(Some(&live), &operations).await;
        let epoch = database.session_epoch();
        super::crash_image_support::checkpoint_live(&database, &live).await;
        let base = Capture::begin(&live);
        let kept_files = directory.path().join("kept.files");
        if batch {
            for index in 0..3 { stand.script.data("CreateBlob", &blob(index)); }
            let received: Vec<WireUploadItem> = items(sources.path());
            bound(database.upload_create_batch(epoch, "Asset".into(), received, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.ok();
        } else {
            stand.script.data("CreateBlob", BLOB);
            stand.script.data("CreateMoment", LANDED);
            bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
        }
        crate::test_wait::settled(&database, "the work is quiet", || true).await;
        let (capture, _) = base.end(&live);
        let secret = stand.rig.credentials.inner.value().unwrap();
        for entry in std::fs::read_dir(sources.path()).unwrap() { std::fs::remove_file(entry.unwrap().path()).unwrap(); }
        let table = if batch { "row_Asset" } else { "row_Moment" };
        let results = over_images(&capture, &kept_files, |index, image: PathBuf| {
            let (secret, operations) = (secret.clone(), operations.clone());
            async move {
                if !unaccepted(&image) { return None; }
                let (fresh, reopened) = reopen(&image, &secret, &operations, |_| {}).await;
                crate::test_wait::settled(&reopened, "opened", || true).await;
                let state = (count(&image, "sync_upload"), count(&image, "op"), count(&image, table), transfer_files(&image), fresh.script.names().len());
                bound(reopened.close_database()).await.unwrap();
                Some((index, state))
            }
        }).await;
        let seen: Vec<_> = results.into_iter().flatten().collect();
        assert!(!seen.is_empty(), "batch {batch}: an image holds a copying transfer");
        for (index, state) in seen {
            assert_eq!(state, (0, 0, 0, 0, 0), "batch {batch} image {index}: transfers, operations, rows, files, requests after the open");
        }
        database.close_now();
    }
}

/// Requirement D-376 (deferred source): a transfer that waits for its source when the process dies reopens as `failed` with the kind `missing_file`, `discard` removes it, `retry` without a new source is refused. Input: a deferred upload, close, open. Observable: the upload status of the row, the answers of `discard` and `retry`. Assert: failed with `missing_file`, a retry without a source refused, the row gone after `discard`. Catches: a deferred transfer that reads a path it never had or stays waiting.
#[tokio::test(flavor = "multi_thread")]
async fn source_deferred_without_attach_reopens_failed_missing_file() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let epoch = database.session_epoch();
    let pending: Vec<WireUploadItem> = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: WireSource::Pending, variables_json: "{}".into() }).collect();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), pending, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let id = receipt.items[0].optimistic_value.id.clone();
    database.close_now();
    let secret = stand.rig.credentials.inner.value().unwrap();
    let (fresh, reopened) = reopen(&live, &secret, &OPERATIONS, |_| {}).await;
    let epoch = reopened.session_epoch();
    let status = || reopened.peek_row_now(reopened.session_epoch(), "Asset".into(), id.clone()).unwrap().and_then(|row| row.uploads.iter().find(|entry| entry.action == "create_files").map(|entry| format!("{:?} {:?}", entry.upload.status, entry.upload.error)));
    crate::test_wait::settled(&reopened, "the transfer failed", || status().is_some_and(|text| text.contains("Failed") && text.contains("missing_file"))).await;
    assert!(bound(reopened.retry(epoch, "Asset".into(), id.clone(), Some("create_files".into()), None)).await.is_err(), "a retry needs a new source");
    bound(reopened.discard(epoch, "Asset".into(), id.clone(), Some("create_files".into()))).await.unwrap();
    crate::test_wait::settled(&reopened, "the row ended", || reopened.peek_row_now(reopened.session_epoch(), "Asset".into(), id.clone()).unwrap().is_none()).await;
    assert_eq!(fresh.script.names().len(), 0, "no request");
    reopened.close_now();
}
