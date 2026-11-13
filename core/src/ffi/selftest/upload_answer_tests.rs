use serde_json::Value;

use super::rig_tests::bound;
use super::upload_stand::{clip, plain, request, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::DbError;

const ROW: &str = r#""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#;

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_upload_reports_the_copied_file_size() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    database.set_online(false).await.unwrap();
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("intro.mp4");
    std::fs::write(&source, b"video").unwrap();
    let local = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap().optimistic_value.unwrap();

    bound(database.upload(epoch, "Moment".into(), local.id.clone(), "upload_clip".into(), request(&source))).await.unwrap();

    let row = bound(database.read_row(epoch, "Moment".into(), local.id)).await.unwrap().unwrap();
    let waiting = &row.uploads.iter().find(|entry| entry.action == "upload_clip").expect("the row shows the upload").upload;
    assert_eq!((waiting.status, waiting.total_bytes, waiting.sent_bytes), (WireUploadStatus::Waiting, 5, 0));
    assert!(stand.script.names().is_empty(), "{:?}", stand.script.names());
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upload_answers_with_the_server_text_and_null_when_the_server_left_it_out() {
    let stand = Stand::new();
    let document = ("CreateMoment", "mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }");
    let database = stand.open(None, &[document]).await;
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("intro.mp4");
    std::fs::write(&source, b"video").unwrap();
    stand.script.data("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}}}}}}}"#));
    let created = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap();
    assert!(bound(database.confirmed(created, false)).await.is_ok());

    stand.script.data("CreateBlob", BLOB);
    for expected in [Some("Intro received"), None] {
        let field = expected.map(|text| format!(r#","successMessage":"{text}""#)).unwrap_or_default();
        stand.script.data("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}{field}}}}}}}"#));
        let uploaded = bound(database.upload(epoch, "Moment".into(), "m77".into(), "upload_clip".into(), request(&source))).await.unwrap();
        assert_eq!(uploaded.optimistic_value.as_ref().map(|row| row.id.as_str()), Some("m77"));
        let answer_json = bound(database.confirmed(uploaded, false)).await.unwrap().answer_json;
        let answer: Value = serde_json::from_str(&answer_json.expect("the upload carries its answer")).unwrap();
        assert_eq!(answer["successMessage"].as_str(), expected);
    }
    assert_eq!(stand.script.names(), ["CreateMoment", "CreateBlob", "CreateMoment", "CreateMoment"], "the second upload of the same content finds its blob in the cache");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_send_with_a_file_confirms_through_the_declared_steps() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("photo.jpg");
    std::fs::write(&source, b"image").unwrap();
    stand.script.data("CreateBlob", BLOB);
    stand.script.data("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}}}}}}}"#));

    let sent = bound(database.send(clip("Moment", r#"{"text":"group"}"#, "upload_clip", &source, epoch))).await.unwrap();

    assert!(bound(database.confirmed(sent, false)).await.is_ok());
    assert_eq!(stand.script.names(), ["CreateBlob", "CreateMoment"]);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_accept_check_order() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("intro.mp4");
    std::fs::write(&source, b"video").unwrap();
    let missing = directory.path().join("missing.mp4");
    for (action, file) in [("undeclared", &source), ("upload_clip", &missing)] {
        let refused = bound(database.upload(database.session_epoch(), "Moment".into(), "absent".into(), action.into(), request(file))).await;
        assert!(matches!(refused, Err(DbError::Invalid { .. })), "{action}: {refused:?}");
    }
    assert!(stand.script.names().is_empty(), "{:?}", stand.script.names());
    database.close_now();
}

/// K2.02 (outcome.md OUTCOME, xfer_removing_committed_confirmed): the answer of the upload lands no row because the row was destroyed while the final mutation was in the network; the receipt shows the optimistic row, both reads of the outcome carry no value.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_upload_without_a_landed_row_answers_no_value_on_both_reads() {
    let stand = Stand::new();
    let delete = ("DeleteMoment", "mutation DeleteMoment($id: ID!) { deleteMoment(id: $id) { id } }");
    let database = stand.open(None, &[CREATE_MOMENT, delete]).await;
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("intro.mp4");
    std::fs::write(&source, b"video").unwrap();
    stand.script.data("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}}}}}}}"#));
    let created = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap();
    assert!(bound(database.confirmed(created, false)).await.is_ok());
    stand.script.data("CreateBlob", BLOB);
    let final_mutation = stand.script.hold("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}}}}}}}"#));
    stand.script.data("DeleteMoment", r#"{"data":{"deleteMoment":{"id":"m77"}}}"#);

    let uploaded = bound(database.upload(epoch, "Moment".into(), "m77".into(), "upload_clip".into(), request(&source))).await.unwrap();
    assert!(uploaded.optimistic_value.is_some(), "the receipt shows the optimistic row");
    final_mutation.entered().await;
    bound(database.destroy(epoch, "Moment".into(), "m77".into())).await.unwrap();
    final_mutation.release();

    for read in ["first", "second"] {
        let outcome = bound(database.confirmed(uploaded.clone(), false)).await.unwrap();
        assert!(outcome.value.is_none(), "{read} read: {outcome:?}");
    }
    database.close_now();
}
