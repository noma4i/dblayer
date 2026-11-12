use std::path::{Path, PathBuf};

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen_preparing, Capture};
use super::rig_tests::bound;
use super::upload_prepare_tests::{probed_image, send, shrink, PREPARED};
use super::upload_stand::{clip, plain, ScriptedPreparer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;

/// The rows of `Moment` with a temporary id that no operation of the queue and no transfer holds.
fn unheld(image: &Path) -> Vec<String> {
    let connection = rusqlite::Connection::open(image).unwrap();
    let mut statement = connection.prepare("SELECT id FROM row_Moment WHERE id LIKE 'temp-%' AND id NOT IN (SELECT temp_id FROM op WHERE temp_id IS NOT NULL) AND id NOT IN (SELECT row_id FROM op_row) AND id NOT IN (SELECT row_id FROM sync_upload)").unwrap();
    statement.query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<_, _>>().unwrap()
}

/// Every row of `Moment` with a temporary id, for a queue that ended whole.
fn temporary_rows(image: &Path) -> Vec<String> {
    let connection = rusqlite::Connection::open(image).unwrap();
    let mut statement = connection.prepare("SELECT id FROM row_Moment WHERE id LIKE 'temp-%'").unwrap();
    statement.query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<_, _>>().unwrap()
}

fn upload_status(database: &crate::ffi::Database, id: &str) -> Option<WireUploadStatus> {
    let row = database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap()?;
    row.uploads.iter().find(|entry| entry.action == "upload_clip").map(|entry| entry.upload.status)
}

fn present(database: &crate::ffi::Database, id: &str) -> bool { database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap().is_some() }

#[derive(Clone, Copy, Debug)]
enum Case { RefusedCreate, DiscardedFailedCreate, DiscardedFailedUpload, CancelledUpload, LandedUpload, CancelledDuringSend, OfflineSend, PreparedLanded }

/// Requirement P-STORE-32: after any commit and after every open, each row of a server model with a temporary id has a holder, an operation `create` or a transfer. Observable: the file of every commit of a scenario and the file after the open on it. Assert: no row with a temporary id without a holder, in any scenario that ends its holder. Catches: an end of the holder that leaves the optimistic row behind.
#[tokio::test(flavor = "multi_thread")]
async fn store_prop_temp_row_has_a_holder() {
    for case in [Case::RefusedCreate, Case::DiscardedFailedCreate, Case::DiscardedFailedUpload, Case::CancelledUpload, Case::LandedUpload, Case::CancelledDuringSend, Case::OfflineSend, Case::PreparedLanded] {
        let stand = match case {
            Case::DiscardedFailedCreate => Stand::new().keep_moment(),
            Case::PreparedLanded => Stand::new().with_preparer(ScriptedPreparer::new(probed_image(), PREPARED, &[])),
            _ => Stand::new(),
        };
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let file = directory.path().join("clip.mp4");
        std::fs::write(&file, b"clip").unwrap();
        let operations = [CREATE_MOMENT];
        let database = stand.open(Some(&live), &operations).await;
        let epoch = database.session_epoch();
        rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
        let base = Capture::begin(&live);
        let kept_files = directory.path().join("kept.files");
        match case {
            Case::RefusedCreate | Case::DiscardedFailedCreate => {
                stand.script.data("CreateMoment", REFUSED);
                let sent = bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
                let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
                let _ = bound(database.confirmed(sent, false)).await;
                if matches!(case, Case::DiscardedFailedCreate) {
                    bound(database.discard(epoch, "Moment".into(), temporary.clone(), None)).await.unwrap();
                }
                crate::test_wait::settled(&database, "the row ended", || matches!(case, Case::DiscardedFailedCreate) == !present(&database, &temporary) || !present(&database, &temporary)).await;
            }
            Case::DiscardedFailedUpload => {
                stand.script.data("CreateBlob", REFUSED);
                let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
                let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
                let _ = bound(database.confirmed(sent, false)).await;
                crate::test_wait::settled(&database, "the refusal is shown", || upload_status(&database, &temporary) == Some(WireUploadStatus::Failed) || !present(&database, &temporary)).await;
                if present(&database, &temporary) {
                    bound(database.discard(epoch, "Moment".into(), temporary.clone(), Some("upload_clip".into()))).await.unwrap();
                }
                crate::test_wait::settled(&database, "the row ended", || !present(&database, &temporary)).await;
            }
            Case::CancelledUpload => {
                let held = stand.script.hold("CreateBlob", BLOB);
                let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
                let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
                held.entered().await;
                copy_tree(&files_of(&live), &kept_files);
                let _ = bound(database.cancel_upload(epoch, "Moment".into(), temporary.clone(), "upload_clip".into())).await;
                held.release();
                crate::test_wait::settled(&database, "the row ended", || !present(&database, &temporary)).await;
            }
            Case::LandedUpload => {
                let held = stand.script.hold("CreateBlob", BLOB);
                stand.script.data("CreateMoment", LANDED);
                bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
                held.entered().await;
                copy_tree(&files_of(&live), &kept_files);
                held.release();
                crate::test_wait::settled(&database, "the row landed", || present(&database, "m77")).await;
            }
            Case::CancelledDuringSend => {
                stand.script.data("CreateBlob", BLOB);
                let held = stand.script.hold("CreateMoment", LANDED);
                let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
                let temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
                held.entered().await;
                copy_tree(&files_of(&live), &kept_files);
                let _ = bound(database.cancel_upload(epoch, "Moment".into(), temporary, "upload_clip".into())).await;
                held.release();
                crate::test_wait::settled(&database, "the row landed", || present(&database, "m77")).await;
            }
            Case::OfflineSend => {
                database.set_online(false).unwrap();
                stand.script.data("CreateBlob", BLOB);
                stand.script.data("CreateMoment", LANDED);
                bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
                copy_tree(&files_of(&live), &kept_files);
                database.set_online(true).unwrap();
                crate::test_wait::settled(&database, "the row landed", || present(&database, "m77")).await;
            }
            Case::PreparedLanded => {
                let image = directory.path().join("clip.jpg");
                std::fs::write(&image, super::upload_prepare_tests::SOURCE).unwrap();
                stand.script.data("CreateBlob", BLOB);
                let held = stand.script.hold("CreateMoment", LANDED);
                bound(database.send(send(epoch, &image, shrink()))).await.unwrap();
                held.entered().await;
                copy_tree(&files_of(&live), &kept_files);
                held.release();
                crate::test_wait::settled(&database, "the row landed", || present(&database, "m77")).await;
            }
        }
        crate::test_wait::settled(&database, "the scenario is quiet", || true).await;
        let (capture, _) = base.end(&live);
        let secret = stand.rig.credentials.inner.value().unwrap();
        let prepared = matches!(case, Case::PreparedLanded);
        let drains = !matches!(case, Case::DiscardedFailedCreate | Case::DiscardedFailedUpload);
        let checks = over_images(&capture, &kept_files, move |index, image: PathBuf| {
            let secret = secret.clone();
            let preparer: Option<std::sync::Arc<dyn crate::ffi::Preparer>> = prepared.then(|| ScriptedPreparer::new(probed_image(), PREPARED, &[]) as std::sync::Arc<dyn crate::ffi::Preparer>);
            async move {
                let before = unheld(&image);
                let (_fresh, reopened) = reopen_preparing(&image, &secret, &operations, preparer, |fresh| { fresh.script.data("CreateBlob", BLOB); fresh.script.data("CreateMoment", LANDED); }).await;
                crate::test_wait::settled(&reopened, "opened", || true).await;
                if drains { crate::test_wait::within_named(5, &format!("{case:?} image {index} drains"), drained(&reopened)).await; }
                let after = if drains { temporary_rows(&image) } else { unheld(&image) };
                bound(reopened.close_database()).await.unwrap();
                (index, before, after)
            }
        }).await;
        for (index, before, after) in checks {
            assert!(before.is_empty(), "{case:?} commit {index}: a row without a holder after a commit: {before:?}");
            assert!(after.is_empty(), "{case:?} commit {index}: a row without a holder after the open: {after:?}");
        }
        database.close_now();
    }
}
