//! A process killed after any commit of a cancel, a replaced source or a deferred source leaves a file that reopens and settles. Requirement -> observed value -> assert.

use std::collections::HashSet;
use std::path::Path;

use super::crash_image_support::{advancing, checkpoint_live, copy_tree, drained, files_of, over_images, reopen_timed, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, OPERATIONS};
use super::upload_stand::{clip, shows, watched, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadSource};
use crate::ffi::wire::WireSyncState;

const MOMENT: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const ASSETS: &str = r#"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":3,"position":0},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a2","clientKey":"key-2","size":3,"position":2}]}}}"#;

/// What a process opened on one image found after it settled.
struct Settled {
    temporary: Vec<String>,
    landed: Vec<String>,
    repeated: Vec<String>,
}

fn rows(image: &Path, table: &str) -> Vec<String> {
    let connection = rusqlite::Connection::open(image).unwrap();
    let mut statement = connection.prepare(&format!("SELECT id FROM {table}")).unwrap();
    statement.query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<_, _>>().unwrap()
}

/// Opens a fresh process on the image of every cut, lets its work end, and reads the rows and the requests it made.
async fn settle_at_every_cut(capture: &Capture, kept: &Path, secret: String, operations: &'static [(&'static str, &'static str)], table: &'static str, script: fn(&Stand), writes: &'static [&'static str], drains: bool) -> Vec<Settled> {
    over_images(capture, kept, move |_, image| {
        let secret = secret.clone();
        async move {
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, operations, script, false).await;
            if drains { advancing(&clock, drained(&reopened)).await; } else { advancing(&clock, crate::test_wait::settled(&reopened, "the work of the image is quiet", || true)).await; }
            bound(reopened.close_database()).await.unwrap();
            let (temporary, landed) = rows(&image, table).into_iter().partition(|id| id.starts_with("temp-"));
            let repeated = writes.iter().filter(|operation| fresh.script.operation_pairs(operation).into_iter().map(|pair| pair.0).collect::<HashSet<_>>().len() > 1).map(|operation| operation.to_string()).collect();
            Settled { temporary, landed, repeated }
        }
    }).await
}

/// The cuts flip from one outcome to the other at most the number of times the scenario names, and never back.
fn flips(outcome: &[bool]) -> usize { outcome.windows(2).filter(|pair| pair[0] != pair[1]).count() }

fn moment_script(fresh: &Stand) {
    for _ in 0..2 { fresh.script.data("CreateBlob", BLOB); }
    fresh.script.data("CreateMoment", MOMENT);
}

fn assets_script(fresh: &Stand) {
    for index in 0..3 { fresh.script.data("CreateBlob", &blob(index)); }
    fresh.script.data("CreateAssets", ASSETS);
}

fn source(path: &Path) -> WireUploadSource { WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None } }

/// A cancel of a single upload killed at any commit: the image either finishes the upload or finds it ended, never both and never a row without a holder, and the final request leaves at most once.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_cancelled_upload_reopens_ended_or_finished() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.mp4");
    std::fs::write(&file, b"clip").unwrap();
    let database = stand.open(Some(&live), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    let held = stand.script.hold("CreateBlob", BLOB);
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
    let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
    held.entered().await;
    let kept = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept);
    bound(database.cancel_upload(epoch, "Moment".into(), temporary.clone(), "upload_clip".into())).await.unwrap();
    held.release();
    crate::test_wait::settled(&database, "the row ended", || database.peek_row_now(epoch, "Moment".into(), temporary.clone()).unwrap().is_none()).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = settle_at_every_cut(&capture, &kept, secret, &[CREATE_MOMENT], "row_Moment", moment_script, &["CreateMoment"], true).await;
    for (index, cut) in cuts.iter().enumerate() {
        assert!(cut.temporary.is_empty() && cut.repeated.is_empty(), "cut {index}: rows without a holder {:?}, requests sent more than once {:?}", cut.temporary, cut.repeated);
        assert!(cut.landed.iter().all(|id| id == "m77"), "cut {index}: a row that is not the answer of the server {:?}", cut.landed);
    }
    let landed: Vec<bool> = cuts.iter().map(|cut| !cut.landed.is_empty()).collect();
    assert!(!landed[0] && !landed.last().unwrap() && flips(&landed) <= 2, "the upload is begun, then cancelled, never begun again: {landed:?}");
    assert!(landed.iter().any(|landed| *landed), "a cut holds the upload begun");
    database.close_now();
}

/// A failed upload that takes a new source, killed at any commit of the retry: the image still shows the failed upload or finishes the new one, and the last cut finishes.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_replaced_source_reopens_failed_or_finished() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let first = directory.path().join("first.mp4");
    let second = directory.path().join("second.mp4");
    std::fs::write(&first, b"first").unwrap();
    std::fs::write(&second, b"second").unwrap();
    let database = stand.open(Some(&live), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    stand.script.enqueue("CreateBlob", Answer::Status(422));
    let id = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &first, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let shown = watched(&database, "Moment", &id);
    crate::test_wait::settled(&database, "the upload failed", || shows(&shown, WireSyncState::Failed)).await;
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    let kept = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept);
    moment_script(&stand);
    bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), Some(source(&second)))).await.unwrap();
    crate::test_wait::settled(&database, "the new source landed", || database.peek_row_now(epoch, "Moment".into(), "m77".into()).unwrap().is_some()).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = settle_at_every_cut(&capture, &kept, secret, &[CREATE_MOMENT], "row_Moment", moment_script, &["CreateMoment"], false).await;
    for (index, cut) in cuts.iter().enumerate() {
        let finished = cut.landed == ["m77"] && cut.temporary.is_empty();
        let failed = cut.landed.is_empty() && cut.temporary == [id.clone()];
        assert!(finished || failed, "cut {index}: neither failed nor finished: temporary {:?}, landed {:?}", cut.temporary, cut.landed);
        assert!(cut.repeated.is_empty(), "cut {index}: requests sent more than once {:?}", cut.repeated);
    }
    let finished: Vec<bool> = cuts.iter().map(|cut| !cut.landed.is_empty()).collect();
    assert!(!finished[0] && *finished.last().unwrap(), "the replaced source ends finished and starts failed: {finished:?}");
    database.close_now();
}

/// A deferred source supplied to every item of a batch, killed at any commit: the image lands the three rows whole or none, sends the final request at most once, and holds no row without a holder once finished.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_deferred_source_reopens_whole_or_none() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let sources = tempfile::tempdir().unwrap();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let epoch = database.session_epoch();
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    let pending: Vec<WireUploadItem> = (0..3).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), pending, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    for index in 0..3 { stand.script.data("CreateBlob", &blob(index)); }
    let held = stand.script.hold("CreateAssets", ASSETS);
    for (index, item) in receipt.items.iter().enumerate() {
        let path = sources.path().join(format!("{index}.jpg"));
        std::fs::write(&path, format!("image-{index}")).unwrap();
        bound(database.supply_source(epoch, "Asset".into(), item.optimistic_value.id.clone(), "create_files".into(), source(&path))).await.unwrap();
    }
    held.entered().await;
    let kept = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept);
    held.release();
    crate::test_wait::settled(&database, "the batch landed", || ["a0", "a1", "a2"].iter().all(|id| database.peek_row_now(epoch, "Asset".into(), id.to_string()).unwrap().is_some())).await;
    drained(&database).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = settle_at_every_cut(&capture, &kept, secret, &OPERATIONS, "row_Asset", assets_script, &["CreateAssets"], false).await;
    for (index, cut) in cuts.iter().enumerate() {
        assert!(cut.landed.is_empty() || cut.landed.len() == 3, "cut {index}: the batch lands whole or not at all: {:?}", cut.landed);
        assert!(cut.repeated.is_empty(), "cut {index}: the final request left more than once {:?}", cut.repeated);
        assert!(cut.landed.len() == 3 || cut.temporary.len() == 3 || cut.temporary.is_empty(), "cut {index}: a part of the rows is left: {:?}", cut.temporary);
    }
    let whole: Vec<bool> = cuts.iter().map(|cut| cut.landed.len() == 3).collect();
    assert!(!whole[0] && *whole.last().unwrap() && whole.windows(2).all(|pair| !pair[0] || pair[1]), "the supplied sources finish the batch and never flip back: {whole:?}");
    database.close_now();
}
