use std::path::PathBuf;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen, saved_work, unaccepted, Capture};
use super::rig_tests::bound;
use super::upload_stand::{clip, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;

fn status(database: &crate::ffi::Database, id: &str) -> Option<WireUploadStatus> {
    let row = database.peek_row(database.session_epoch(), "Moment".into(), id.into()).unwrap()?;
    row.uploads.iter().find(|entry| entry.action == "upload_clip").map(|entry| entry.upload.status)
}

fn landed(database: &crate::ffi::Database) -> bool { database.peek_row(database.session_epoch(), "Moment".into(), "m77".into()).unwrap().is_some() }

/// A process killed after any commit of a single file upload, from its first run or from the retry of a refused one, leaves a file that finishes the upload with every step sent at most once and the row landed once, that keeps a refusal as a refusal until the retry commits, and never goes back from a landed row.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_single_upload_finishes_it_once_or_keeps_its_refusal() {
    for retried in [false, true] {
        let stand = if retried { Stand::new().keep_moment() } else { Stand::new() };
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let file = directory.path().join("clip.mp4");
        std::fs::write(&file, b"clip").unwrap();
        let operations = [CREATE_MOMENT];
        let database = stand.open(Some(&live), &operations).await;
        let epoch = database.session_epoch();
        let kept_files = directory.path().join("kept.files");
        let temporary;
        let base;
        if retried {
            stand.script.data("CreateBlob", REFUSED);
            let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
            temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
            let _ = bound(database.confirmed(sent, false)).await;
            crate::test_wait::settled(&database, "the refusal is shown", || status(&database, &temporary) == Some(WireUploadStatus::Failed)).await;
            rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
            copy_tree(&files_of(&live), &kept_files);
            base = Capture::begin(&live);
            stand.script.data("CreateBlob", BLOB);
            stand.script.data("CreateMoment", LANDED);
            bound(database.retry(epoch, "Moment".into(), temporary.clone(), Some("upload_clip".into()), None)).await.unwrap();
        } else {
            rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
            base = Capture::begin(&live);
            let held = stand.script.hold("CreateBlob", BLOB);
            stand.script.data("CreateMoment", LANDED);
            let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, epoch))).await.unwrap();
            temporary = sent.optimistic_value.as_ref().unwrap().id.clone();
            held.entered().await;
            copy_tree(&files_of(&live), &kept_files);
            held.release();
        }
        crate::test_wait::settled(&database, "the upload landed", || landed(&database)).await;
        drained(&database).await;
        let (capture, _) = base.end(&live);
        let secret = stand.rig.credentials.inner.value().unwrap();

        let cuts = over_images(&capture, &kept_files, |index, image: PathBuf| {
            let (secret, temporary) = (secret.clone(), temporary.clone());
            async move {
                let (saved, _) = saved_work(&image);
                let unaccepted = unaccepted(&image);
                let (fresh, reopened) = reopen(&image, &secret, &operations, |fresh| {
                    fresh.script.data("CreateBlob", BLOB);
                    fresh.script.data("CreateMoment", LANDED);
                }).await;
                crate::test_wait::settled(&reopened, "opened", || true).await;
                if status(&reopened, &temporary) == Some(WireUploadStatus::Failed) {
                    assert_eq!(fresh.script.names().len(), 0, "cut {index}: a refusal that was kept sends nothing until the retry");
                    bound(reopened.close_database()).await.unwrap();
                    return (false, saved);
                }
                drained(&reopened).await;
                let pairs: std::collections::HashSet<_> = fresh.script.operation_pairs("CreateMoment").into_iter().map(|pair| pair.0).collect();
                assert!(pairs.len() <= 1, "cut {index}: the row is created by one request: {pairs:?}");
                let is_landed = landed(&reopened);
                assert!(is_landed || saved == 0 || unaccepted, "cut {index}: a saved upload is finished");
                if is_landed { assert_eq!(status(&reopened, "m77"), None, "cut {index}: the landed row has no upload left"); }
                bound(reopened.close_database()).await.unwrap();
                (is_landed, saved)
            }
        }).await;
        let landed_cuts: Vec<bool> = cuts.iter().map(|cut| cut.0).collect();
        assert!(!landed_cuts[0] && *landed_cuts.last().unwrap() && landed_cuts.windows(2).all(|pair| !pair[0] || pair[1]), "retried {retried}: {landed_cuts:?}");
        assert!(cuts.iter().any(|cut| cut.1 > 0 && cut.0), "retried {retried}: a cut holds the upload unfinished");
        database.close_now();
    }
}
