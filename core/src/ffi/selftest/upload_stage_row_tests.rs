//! D-497 (staged upload on an existing row), D-498 (commit of a staged upload after the server blob lifetime) through the public entry.

use super::crash_image_support::timed_stand;
use super::rig_tests::bound;
use super::upload_prepare_tests::{descriptor, probed_image, PREPARED, SOURCE};
use super::upload_stage_tests::{answers, COMPLETE, LANDED, OPERATIONS};
use super::upload_stand::{plain, request, ScriptedPreparer, Stand};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;
const WEEK_MS: u64 = 7 * 24 * 3600 * 1000;

fn prepared_request(source: &std::path::Path) -> WireUploadRequest {
    WireUploadRequest { source: WireSource::File { path: source.to_str().unwrap().into(), name: None, mime: None, prepare: Some(descriptor()), max_bytes: None }, variables_json: "{}".into() }
}

/// P-XFER-87, P-XFER-88: `stage_on_row` + `commit_staged` runs the action on the existing row: no new row, the id stays, the mutation goes once after `complete` and its typed answer reaches the caller; a missing row is `NotFound` and creates no transfer.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_on_existing_row_commits_the_action() {
    let directory = tempfile::tempdir().unwrap();
    let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("intro"));
    std::fs::write(&source, b"intro").unwrap();
    let stand = Stand::new();
    let database = stand.open(Some(&path), &OPERATIONS).await;
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", LANDED);
    let created = bound(database.send(plain("Moment", r#"{"text":"me"}"#, epoch))).await.unwrap();
    let row = bound(database.confirmed(created, false)).await.unwrap().value.unwrap().id;
    let rows = || database.db.lock().connection.query_row("SELECT count(*) FROM sync_upload", [], |found| found.get::<_, i64>(0)).unwrap();

    let missing = bound(database.stage_on_row(epoch, "Moment".into(), "absent".into(), "upload_clip".into(), request(&source))).await;
    assert!(matches!(missing, Err(crate::ffi::DbError::NotFound { .. })), "{missing:?}");
    assert_eq!(rows(), 0, "a missing row creates no transfer");

    answers(&stand);
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
    let staged = bound(database.stage_on_row(epoch, "Moment".into(), row.clone(), "upload_clip".into(), request(&source))).await.unwrap();
    assert_eq!(staged.row_id, row, "the key is the existing row");
    crate::test_wait::settled(&database, "the file is on the server", || stand.puts.seen.lock().unwrap().len() == 1).await;
    let committed = bound(database.commit_staged(epoch, "Moment".into(), row.clone(), "upload_clip".into(), "{}".into(), "{}".into())).await.unwrap();
    let confirmed = bound(database.confirmed(committed, false)).await.unwrap();
    assert_eq!(confirmed.value.map(|value| value.id), Some(row.clone()), "the row keeps its id");
    assert!(confirmed.answer_json.is_some(), "the typed answer of the action reaches the caller");
    let names = stand.script.names();
    assert_eq!(names.iter().filter(|name| *name == "CreateMoment").count(), 2, "the create of the row and the action, once each: {names:?}");
    assert_eq!(names.last().map(String::as_str), Some("CreateMoment"), "the action goes after complete: {names:?}");
    database.close_now();
}

/// P-XFER-89: `commit_staged` 7 days after `stage`: the old multipart is aborted, a new one goes from part 1 out of the stored file, then `complete` and the mutation; the preparer does not run again.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_stage_commit_after_blob_lifetime_restarts_the_upload() {
    let directory = tempfile::tempdir().unwrap();
    let (path, source) = (directory.path().join("db.sqlite"), directory.path().join("clip"));
    std::fs::write(&source, SOURCE).unwrap();
    let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
    let (stand, clock) = timed_stand();
    let stand = stand.with_preparer(preparer.clone());
    let database = stand.open_with(Some(&path), &OPERATIONS, Some(clock.clone())).await;
    answers(&stand);
    stand.script.data("AbortMultipart", ABORTED);
    stand.script.data("CompleteMultipart", COMPLETE);
    stand.script.data("CreateMoment", LANDED);
    let epoch = database.session_epoch();
    let staged = bound(database.stage(epoch, "Moment".into(), "upload_clip".into(), prepared_request(&source))).await.unwrap();
    crate::test_wait::settled(&database, "the prepared file is on the server", || stand.puts.seen.lock().unwrap().len() == 1).await;
    clock.advance(WEEK_MS);
    answers(&stand);
    let committed = bound(database.commit_staged(epoch, "Moment".into(), staged.row_id, "upload_clip".into(), r#"{"text":"late"}"#.into(), "{}".into())).await.unwrap();
    bound(database.confirmed(committed, false)).await.unwrap();
    let names = stand.script.names();
    let count = |name: &str| names.iter().filter(|seen| *seen == name).count();
    assert_eq!((count("AbortMultipart"), count("InitMultipart"), count("CompleteMultipart"), count("CreateMoment")), (1, 2, 1, 1), "{names:?}");
    assert_eq!(names.last().map(String::as_str), Some("CreateMoment"), "{names:?}");
    assert_eq!(stand.puts.bodies.lock().unwrap().clone(), vec![PREPARED.to_vec(), PREPARED.to_vec()], "the new upload sends the stored prepared file");
    assert_eq!(preparer.runs().len(), 1, "the file is not prepared again");
    database.close_now();
}
