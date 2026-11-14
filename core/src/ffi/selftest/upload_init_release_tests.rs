use std::sync::Arc;

use super::rig_tests::bound;
use super::upload_stand::{clip, Hold, Stand, CREATE_MOMENT};
use crate::ffi::Database;
use crate::test_wait::settled;

const INIT: &str = r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#;
const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;

fn owed(path: &std::path::Path) -> i64 {
    rusqlite::Connection::open(path).unwrap().query_row("SELECT count(*) FROM upload_orphan", [], |row| row.get(0)).unwrap()
}

/// A clip sent with the answer of its `Init` held on the server: the transfer waits for the multipart.
async fn init_held(stand: &Stand, path: &std::path::Path, directory: &std::path::Path) -> (Arc<Database>, Arc<Hold>, String) {
    let database = stand.open(Some(path), &[CREATE_MOMENT]).await;
    let file = directory.join("clip.mp4");
    std::fs::File::create(&file).unwrap().set_len(11 * crate::owners::write::upload::MEBIBYTE).unwrap();
    let held = stand.script.hold("InitMultipart", INIT);
    stand.script.data("AbortMultipart", ABORTED);
    let sent = bound(database.send(clip("Moment", r#"{"text":"A"}"#, "upload_clip", &file, database.session_epoch()))).await.unwrap();
    held.entered().await;
    (database, held, sent.optimistic_value.unwrap().id)
}

/// The abort of the multipart that the server created for a removed transfer goes once, with the id of that multipart, and no debt stays in the base.
fn aborted_once(stand: &Stand) {
    assert_eq!(stand.script.count("AbortMultipart"), 1);
    assert_eq!(stand.script.variables("AbortMultipart")[0]["uploadId"], "upload");
}

/// xfer_released_init_answer_owes_abort (D-388): a discard or a cancel while `Init` is in flight, the answer comes after the removal.
/// Observable: the calls to the server, the debts in the base. Catches: a multipart that stays on the server until its timeout.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_init_answer_after_removal_aborts_the_multipart() {
    for cancel in [false, true] {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let (database, held, id) = init_held(&stand, &path, directory.path()).await;
        if cancel {
            bound(database.cancel_upload(database.session_epoch(), "Moment".into(), id, "upload_clip".into())).await.unwrap();
        } else {
            bound(database.discard(database.session_epoch(), "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();
        }
        held.release();
        settled(&database, "the abort went", || stand.script.count("AbortMultipart") == 1).await;
        aborted_once(&stand);
        assert_eq!((stand.script.count("PresignParts"), owed(&path)), (0, 0), "cancel {cancel}");
        database.close_now();
    }
}

/// The process ends after the discard, before the answer of `Init`: the next opening sends the saved pair, the answer is the debt, the abort goes once, and a further opening sends nothing.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_init_answer_after_restart_aborts_the_multipart_once() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let (database, held, id) = init_held(&stand, &path, directory.path()).await;
    bound(database.discard(database.session_epoch(), "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();
    database.close_now();
    held.release();

    stand.script.data("InitMultipart", INIT);
    let reopened = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    settled(&reopened, "the abort went", || stand.script.count("AbortMultipart") == 1).await;
    super::crash_image_support::drained(&reopened).await;
    aborted_once(&stand);
    assert_eq!(owed(&path), 0);
    reopened.close_now();

    let again = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    super::crash_image_support::drained(&again).await;
    assert_eq!((stand.script.count("AbortMultipart"), stand.script.count("InitMultipart"), owed(&path)), (1, 2, 0), "the held call and the replay of the saved pair are the only Init calls");
    again.close_now();
}
