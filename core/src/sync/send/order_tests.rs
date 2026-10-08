use serde_json::json;

use crate::identity::RowId;
use crate::transport::{Reply, TransportError};
use crate::upload::{UploadInput, UploadState, UploadSteps};

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn updated(text: &str) -> Reply {
    Reply::data(json!({"updateMoment": {"id": "m1", "text": text, "userId": null}}))
}

#[tokio::test(start_paused = true)]
async fn an_unknown_delivery_holds_the_later_updates_of_the_row_when_an_upload_is_the_oldest_operation() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let input = UploadInput::new("/nonexistent/file.bin", UploadSteps::new("CreateMoment").single("CreateBlob"));
    rig.sync.inner.begin_upload("moments", &rid("m1"), &input, UploadState::queued()).unwrap();
    rig.db.update("moments", &rid("m1"), params(json!({"text": "first"}))).unwrap();
    rig.db.update("moments", &rid("m1"), params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("connection lost".to_string())));
    rig.transport.enqueue("UpdateMoment", updated("second"));
    rig.sync.flush().await.unwrap();
    let texts: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].clone()).collect();
    assert_eq!(texts, [json!("first")], "the second update must wait for the unknown delivery of the first");
}

#[tokio::test(start_paused = true)]
async fn inv_op_02_a_retry_during_a_flush_does_not_let_a_later_write_overtake_the_retried_one() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    put(&rig, "moments", moment("m2", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "first"}))).unwrap();
    rig.db.update("moments", &rid("m2"), params(json!({"text": "other"}))).unwrap();
    rig.db.update("moments", &rid("m1"), params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("connection lost".to_string())));
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m2", "text": "other", "userId": null}})).after(std::time::Duration::from_secs(5)));
    rig.transport.enqueue("UpdateMoment", updated("second"));
    let retry = async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        rig.db.retry_all().unwrap();
    };
    let (flushed, ()) = tokio::join!(rig.sync.flush(), retry);
    flushed.unwrap();
    let texts: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].clone()).collect();
    assert_eq!(texts, [json!("first"), json!("other")]);
}
