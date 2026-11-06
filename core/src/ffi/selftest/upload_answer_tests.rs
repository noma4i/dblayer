use serde_json::Value;

use super::rig_tests::bound;
use super::upload_stand::{clip, plain, request, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::WireOperationOutcome;

const ROW: &str = r#""__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z""#;

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_upload_reports_the_copied_file_size() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    database.set_online(false).unwrap();
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
    assert!(matches!(bound(database.outcome(created)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));

    stand.script.data("CreateBlob", BLOB);
    for expected in [Some("Intro received"), None] {
        let field = expected.map(|text| format!(r#","successMessage":"{text}""#)).unwrap_or_default();
        stand.script.data("CreateMoment", &format!(r#"{{"data":{{"createMoment":{{{ROW}{field}}}}}}}"#));
        let uploaded = bound(database.upload(epoch, "Moment".into(), "m77".into(), "upload_clip".into(), request(&source))).await.unwrap();
        assert_eq!(uploaded.optimistic_value.as_ref().map(|row| row.id.as_str()), Some("m77"));
        let WireOperationOutcome::Confirmed { answer_json, .. } = bound(database.outcome(uploaded)).await.unwrap() else { panic!("the upload was not confirmed") };
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

    assert!(matches!(bound(database.outcome(sent)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    assert_eq!(stand.script.names(), ["CreateBlob", "CreateMoment"]);
    database.close_now();
}
