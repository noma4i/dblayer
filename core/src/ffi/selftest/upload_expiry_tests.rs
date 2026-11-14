use super::rig_tests::bound;
use super::upload_stand::{clip, shows, watched, Answer, Stand};
use crate::ffi::wire::WireSyncState;
use crate::ffi::{Database, TestClock};
use crate::test_wait::settled;

const WEEK_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;
const OPERATIONS: [(&str, &str); 2] = [("CreateBlob", "mutation CreateBlob { createBlob { signedId } }"), ("CreateMoment", "mutation CreateMoment { createMoment { id } }")];

fn now_millis() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 }

async fn failed_upload(stand: &Stand, database: &Database, source: &std::path::Path) -> String {
    stand.script.enqueue("CreateBlob", Answer::Status(422));
    let id = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", source, database.session_epoch()))).await.unwrap().optimistic_value.unwrap().id;
    let rows = watched(database, "Moment", &id);
    settled(&database, "the upload failed", || shows(&rows, WireSyncState::Failed)).await;
    id
}

async fn until_gone(database: &Database, id: &str) {
    let rows = watched(database, "Moment", id);
    settled(&database, "the expired row is gone", || rows.emptied()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_blob_older_than_seven_days_is_gone_after_the_reopen() {
    let (stand, start) = (Stand::new(), now_millis());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    let database = stand.open_with(Some(&path), &OPERATIONS, Some(TestClock::new(start))).await;
    let id = failed_upload(&stand, &database, &source).await;
    database.close_now();

    let reopened = stand.open_with(Some(&path), &OPERATIONS, Some(TestClock::new(start + WEEK_MILLIS + 1))).await;

    until_gone(&reopened, &id).await;
    reopened.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_blob_older_than_seven_days_is_gone_when_the_network_returns() {
    let (stand, start) = (Stand::new(), now_millis());
    let clock = TestClock::new(start);
    let database = stand.open_with(None, &OPERATIONS, Some(clock.clone())).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    let id = failed_upload(&stand, &database, &source).await;
    database.set_online(false).await.unwrap();
    clock.advance(WEEK_MILLIS + 1);

    database.set_online(true).await.unwrap();

    until_gone(&database, &id).await;
    database.close_now();
}
