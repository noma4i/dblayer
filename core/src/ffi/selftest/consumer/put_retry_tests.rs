use serde_json::json;

use super::stand::{message, Stand};
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::upload_wire::{WireUploadRequest, WireUploadSource};

/// transfer.md Uploading x StepError temporary (xfer_step_temp) then StepOk (xfer_step_ok): a PUT answered 503 is retried by the Backoff, the next PUT answers 200, and the upload confirms after exactly two PUTs.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_scripted_put_failure() {
    let stand = Stand::open().await;
    stand.script_put(503, None);
    stand.script_put(200, Some("etag"));
    let database = &stand.database;
    let epoch = database.session_epoch();
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"b"}}}));
    let mut sent = message("m0", "c1", "");
    sent["clientKey"] = json!("k1");
    stand.answer("MessageSendFile", json!({"messageSend":{"message": sent}}));
    let request = WireUploadRequest { source: crate::ffi::upload_wire::WireSource::Pending, variables_json: r#"{"chatId":"c1","kind":"VIDEO"}"#.into() };
    let command = CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","kind":"VIDEO","clientKey":"k1","media":{"kind":"VIDEO"}}).to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "send_file".into(), request }) };
    let receipt = bound(database.send(command)).await.unwrap();
    let id = receipt.optimistic_value.clone().expect("the optimistic row").id;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("image");
    std::fs::write(&path, b"image").unwrap();
    let source = WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: r#"{"media":{"width":100,"height":80}}"#.into(), variables_json: "{}".into() };
    let mut armed = stand.clock.armed();
    bound(database.supply_source(epoch, "Message".into(), id.clone(), "send_file".into(), source)).await.unwrap();
    let mut waiting = Box::pin(database.confirmed(receipt));
    let outcome = loop {
        tokio::select! {
            result = &mut waiting => break result.unwrap(),
            () = stand.clock.await_armed(armed) => { armed = stand.clock.armed(); stand.clock.advance(16_000); }
        }
    };
    assert!(matches!(outcome.value, Some(ref row) if row.id == "m0"), "xfer_step_temp then xfer_step_ok confirm the upload: {outcome:?}");
    assert_eq!(stand.puts(), 2, "xfer_step_temp retries the 503 PUT once, xfer_step_ok ends the 200 PUT");
    database.close_now();
}
