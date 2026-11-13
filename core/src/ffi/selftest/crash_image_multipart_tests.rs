use std::collections::HashSet;

use super::crash_image_support::{copy_tree, drained, files_of, over_images, reopen, saved_work, unaccepted, Capture};
use super::rig_tests::bound;
use super::upload_stand::{clip, Stand, CREATE_MOMENT};
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const PRESIGN: &str = r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#;
const COMPLETE: &str = r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#;
const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

fn script(stand: &Stand) {
    stand.script.data("InitMultipart", INIT);
    for _ in 0..40 { stand.script.data("PresignParts", PRESIGN); }
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
}

fn confirmed_parts(image: &std::path::Path) -> usize {
    let connection = rusqlite::Connection::open(image).unwrap();
    connection.query_row("SELECT count(*) FROM sync_upload_part WHERE etag IS NOT NULL", [], |row| row.get::<_, i64>(0)).unwrap() as usize
}

/// A process killed after any commit of a three part upload leaves a file that finishes it with the parts the live run had confirmed not sent again, no abort for a live upload and the row landed once, or finds nothing begun.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_multipart_upload_reopens_with_the_upload_finished() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&live), &operations).await;
    let epoch = database.session_epoch();
    rusqlite::Connection::open(&live).unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    let base = Capture::begin(&live);
    script(&stand);
    let held = stand.puts.hold_next();
    bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    held.entered().await;
    let kept_files = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept_files);
    held.release();
    crate::test_wait::settled(&database, "the upload landed", || database.peek_row_now(epoch, "Moment".into(), "m77".into()).unwrap().is_some()).await;
    drained(&database).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &kept_files, |index, image| {
        let secret = secret.clone();
        async move {
            let (saved, _) = saved_work(&image);
            let unaccepted = unaccepted(&image);
            let confirmed = confirmed_parts(&image);
            let (fresh, reopened) = reopen(&image, &secret, &operations, script).await;
            if saved > 0 { drained(&reopened).await; } else { crate::test_wait::settled(&reopened, "opened", || true).await; }
            let landed = reopened.peek_row(reopened.session_epoch(), "Moment".into(), "m77".into()).await.unwrap().is_some();
            assert!(landed || saved == 0 || unaccepted, "cut {index}: a saved upload is finished");
            let offsets: Vec<u64> = fresh.puts.seen.lock().unwrap().iter().map(|put| put.offset).collect();
            assert_eq!(offsets.iter().collect::<HashSet<_>>().len(), offsets.len(), "cut {index}: no part goes out twice: {offsets:?}");
            assert!(offsets.len() <= 3 - confirmed, "cut {index}: parts confirmed before the cut ({confirmed}) are not sent again: {offsets:?}");
            let presigned: HashSet<_> = fresh.script.operation_pairs("PresignParts").into_iter().map(|pair| pair.0).collect();
            assert!(presigned.len() <= 3 - confirmed, "cut {index}: one presign per part not confirmed before the cut ({confirmed}): {presigned:?}, calls {:?}", fresh.script.names());
            assert_eq!(fresh.script.count("AbortMultipart"), 0, "cut {index}: a live upload is not aborted");
            assert!(fresh.script.count("CreateMoment") <= 1 && fresh.script.count("CompleteMultipart") <= 1, "cut {index}: complete and create go out at most once");
            bound(reopened.close_database()).await.unwrap();
            (landed, saved, confirmed)
        }
    }).await;
    let landed: Vec<bool> = cuts.iter().map(|cut| cut.0).collect();
    assert!(!landed[0] && *landed.last().unwrap() && landed.windows(2).all(|pair| !pair[0] || pair[1]), "{landed:?}");
    assert!(cuts.iter().any(|cut| cut.2 > 0 && cut.0), "a cut holds confirmed parts of an unfinished upload: {cuts:?}");
    database.close_now();
}
