use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, items, ASSETS, OPERATIONS};
use super::upload_stand::Stand;
use crate::ffi::upload_wire::WireUploadBatch;
use crate::test_wait::yield_until;

#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_after_a_lost_answer_of_a_batch_member_replays_its_step_once() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let database = stand.open(Some(&path), &OPERATIONS).await;
    stand.script.data("CreateBlob", &blob(0));
    let lost = stand.script.hold("CreateBlob", &blob(1));
    let epoch = database.session_epoch();
    bound(database.upload_create_batch(epoch, "Asset".into(), items(directory.path()), WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    lost.entered().await;
    database.close_now();
    lost.release();

    stand.script.data("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", ASSETS);
    let reopened = stand.open(Some(&path), &OPERATIONS).await;
    yield_until("the batch reaches CreateAssets", || stand.script.count("CreateAssets") == 1).await;

    let files: Vec<String> = stand.script.variables("CreateBlob").iter().map(|variables| variables["filename"].as_str().unwrap().to_string()).collect();
    assert_eq!(files, ["0.jpg", "1.jpg", "1.jpg", "2.jpg"], "the second member went out once and once more as the replay of its saved pair");
    assert_eq!(stand.script.count("CreateAssets"), 1);
    reopened.close_now();
}
