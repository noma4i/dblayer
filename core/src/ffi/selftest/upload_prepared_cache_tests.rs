//! D-496 (prepared file survives a cancelled upload, discard and forget clear it) through the public entry.

use std::path::Path;

use super::crash_image_support::{files_of, timed_stand};
use super::rig_tests::bound;
use super::upload_prepare_tests::{descriptor, files_left, probed_image, send, PREPARED, SOURCE};
use super::upload_stage_tests::{answers, LANDED, OPERATIONS};
use super::upload_stand::{ScriptedPreparer, Stand, BLOB};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
use crate::ffi::Database;

const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;
const WEEK_MS: u64 = 7 * 24 * 3600 * 1000;

fn prepared_files(database: &Path) -> usize { std::fs::read_dir(files_of(database).join("prepared")).map_or(0, |found| found.count()) }

fn cache_rows(database: &Database) -> i64 { database.db.lock().connection.query_row("SELECT count(*) FROM prepared_cache", [], |row| row.get(0)).unwrap() }

fn prepared_request(source: &Path) -> WireUploadRequest {
    WireUploadRequest { source: WireSource::File { path: source.to_str().unwrap().into(), name: None, mime: None, prepare: Some(descriptor()), max_bytes: None }, variables_json: "{}".into() }
}

/// Sends the source with prepare settings, waits for its first put and cancels it: the prepared result is written before the put starts.
async fn cancelled_after_prepare(stand: &Stand, database: &Database, source: &Path) {
    let epoch = database.session_epoch();
    stand.script.data("CreateBlob", BLOB);
    let put = stand.puts.hold_next();
    let sent = bound(database.send(send(epoch, source, descriptor()))).await.unwrap();
    put.entered().await;
    let id = sent.optimistic_value.unwrap().id;
    assert!(bound(database.cancel_upload(epoch, "Moment".into(), id, "upload_clip".into())).await.unwrap());
    put.release();
}

/// P-XFER-84: the upload of the same content with the same settings after a `cancel_upload` takes the kept result, in the same process and after a restart: the preparer runs once, the network gets the prepared bytes, the cache gives its file away.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_cancel_keeps_the_prepared_file() {
    for restart in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
        std::fs::write(&source, SOURCE).unwrap();
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        let stand = Stand::new().with_preparer(preparer.clone());
        let mut database = stand.open(Some(&path), &[super::upload_stand::CREATE_MOMENT]).await;
        cancelled_after_prepare(&stand, &database, &source).await;
        assert_eq!(preparer.runs().len(), 1, "{restart}");
        assert_eq!((prepared_files(&path), cache_rows(&database)), (1, 1), "{restart}: the cancel keeps the prepared file");
        if restart {
            database.close_now();
            database = stand.open(Some(&path), &[super::upload_stand::CREATE_MOMENT]).await;
            assert_eq!((prepared_files(&path), cache_rows(&database)), (1, 1), "the kept file and its row survive the restart");
        }
        stand.script.data("CreateBlob", BLOB);
        stand.script.data("CreateMoment", LANDED);
        let bodies_before = stand.puts.bodies.lock().unwrap().len();
        let again = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.unwrap();
        bound(database.confirmed(again, false)).await.unwrap();
        assert_eq!(preparer.runs().len(), 1, "{restart}: the second upload is not prepared again");
        assert_eq!(stand.puts.bodies.lock().unwrap()[bodies_before..].to_vec(), vec![PREPARED.to_vec()], "{restart}: the network gets the kept result");
        assert_eq!((prepared_files(&path), cache_rows(&database)), (0, 0), "{restart}: the kept file left the cache");
        database.close_now();
    }
}

/// P-XFER-85: `discard_staged` (what `StagedUpload.discard()` calls) leaves no file and no cache row, `cancel_upload` of the same staged upload keeps 1 prepared file.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_discard_drops_the_prepared_file() {
    for discard in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
        std::fs::write(&source, SOURCE).unwrap();
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        let stand = Stand::new().with_preparer(preparer.clone());
        let database = stand.open(Some(&path), &OPERATIONS).await;
        answers(&stand);
        stand.script.data("AbortMultipart", ABORTED);
        let epoch = database.session_epoch();
        let staged = bound(database.stage(epoch, "Moment".into(), "upload_clip".into(), prepared_request(&source))).await.unwrap();
        crate::test_wait::settled(&database, "the prepared file is on the server", || stand.puts.seen.lock().unwrap().len() == 1).await;
        let answer = if discard {
            bound(database.discard_staged(epoch, "Moment".into(), staged.row_id.clone(), "upload_clip".into())).await
        } else {
            bound(database.cancel_upload(epoch, "Moment".into(), staged.row_id.clone(), "upload_clip".into())).await
        };
        assert!(matches!(answer, Ok(true)), "discard {discard}: {answer:?}");
        let kept = usize::from(!discard);
        assert_eq!((prepared_files(&path), cache_rows(&database), files_left(&path)), (kept, i64::from(!discard), kept), "discard {discard}");
        database.close_now();
    }
}

/// P-XFER-86: a kept result older than 7 days is not taken: the source is prepared again and the old file and row are gone.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prepared_cache_expires() {
    let directory = tempfile::tempdir().unwrap();
    let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
    std::fs::write(&source, SOURCE).unwrap();
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
    let (stand, clock) = timed_stand();
    let stand = stand.with_preparer(preparer.clone());
    let database = stand.open_with(Some(&path), &[super::upload_stand::CREATE_MOMENT], Some(clock.clone())).await;
    cancelled_after_prepare(&stand, &database, &source).await;
    assert_eq!(prepared_files(&path), 1);
    clock.advance(WEEK_MS);
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", LANDED);
    let again = bound(database.send(send(database.session_epoch(), &source, descriptor()))).await.unwrap();
    bound(database.confirmed(again, false)).await.unwrap();
    assert_eq!(preparer.runs().len(), 2, "the expired result is prepared again");
    assert_eq!((prepared_files(&path), cache_rows(&database)), (0, 0), "the expired file and row are gone");
    database.close_now();
}

/// P-XFER-90: leaving the create screen and starting again at once: `discard_staged` of a live staged upload, or `cancel_upload` then `forget_prepared`, directly followed by `stage` of the same file with the same settings prepares again; after the second transfer is discarded there is no row, no transfer file, no cache row and no cache file.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_discard_then_stage_prepares_again() {
    for forget in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
        std::fs::write(&source, SOURCE).unwrap();
        let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
        let stand = Stand::new().with_preparer(preparer.clone());
        let database = stand.open(Some(&path), &OPERATIONS).await;
        answers(&stand);
        for _ in 0..3 { stand.script.data("AbortMultipart", ABORTED); }
        let epoch = database.session_epoch();
        let first = bound(database.stage(epoch, "Moment".into(), "upload_clip".into(), prepared_request(&source))).await.unwrap();
        crate::test_wait::settled(&database, "the first prepared file is on the server", || stand.puts.seen.lock().unwrap().len() == 1).await;
        if forget {
            assert!(bound(database.cancel_upload(epoch, "Moment".into(), first.row_id.clone(), "upload_clip".into())).await.unwrap());
            assert!(bound(database.forget_prepared(epoch, prepared_request(&source))).await.unwrap(), "the kept file is forgotten");
        } else {
            assert!(bound(database.discard_staged(epoch, "Moment".into(), first.row_id.clone(), "upload_clip".into())).await.unwrap());
        }
        answers(&stand);
        let second = bound(database.stage(epoch, "Moment".into(), "upload_clip".into(), prepared_request(&source))).await.unwrap();
        crate::test_wait::settled(&database, "the second prepared file is on the server", || stand.puts.seen.lock().unwrap().len() == 2).await;
        assert_eq!(preparer.runs().len(), 2, "forget {forget}: the second stage prepares again");
        let rows = || database.db.lock().connection.query_row("SELECT count(*) FROM sync_upload", [], |found| found.get::<_, i64>(0)).unwrap();
        assert_eq!((rows(), cache_rows(&database), prepared_files(&path)), (1, 0, 0), "forget {forget}: exactly one transfer, nothing cached");
        assert!(bound(database.discard_staged(epoch, "Moment".into(), second.row_id, "upload_clip".into())).await.unwrap());
        assert_eq!((rows(), cache_rows(&database), prepared_files(&path), files_left(&path)), (0, 0, 0, 0), "forget {forget}: no leftovers");
        database.close_now();
    }
}
