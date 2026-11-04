use serde_json::json;

use crate::identity::RowId;
use crate::transport::{Reply, TransportError};
use crate::upload::{UploadFile, UploadOperations, UploadState};

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn updated(text: &str) -> Reply {
    Reply::data(json!({"updateMoment": {"id": "m1", "text": text, "userId": null}}))
}

#[test]
fn an_unknown_delivery_holds_the_later_updates_of_the_row_when_an_upload_is_the_oldest_operation() { crate::test_wait::paused(async {
    let rig = rig_with(crate::sync::SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..Default::default() });
    rig.sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    put(&rig, "moments", moment("m1", "old"));
    let input = UploadFile::new("/nonexistent/file.bin");
    (rig.sync.inner.begin_upload(&rig.sync.inner.db.admit().unwrap(), rig.db.generation(), "moments", &rid("m1"), "upload", &input, UploadState::queued())).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "first"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("connection lost".to_string())));
    rig.transport.enqueue("UpdateMoment", updated("second"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    let texts: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].clone()).collect();
    assert_eq!(texts, [json!("first")], "the second update must wait for the unknown delivery of the first");
}) }

#[test]
fn inv_op_02_a_retry_during_a_flush_does_not_let_a_later_write_overtake_the_retried_one() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    put(&rig, "moments", moment("m2", "old"));
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "first"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m2"), params(json!({"text": "other"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("connection lost".to_string())));
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m2", "text": "other", "userId": null}})).after(std::time::Duration::from_secs(5)));
    rig.transport.enqueue("UpdateMoment", updated("second"));
    let retry = async {
        crate::test_wait::pass(std::time::Duration::from_secs(1)).await;
        rig.db.retry_all(&rig.db.admit().unwrap()).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, async { rig.sync.flush(&rig.sync.inner.db.admit().unwrap()).await }), retry);
    flushed.unwrap();
    let texts: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].clone()).collect();
    assert_eq!(texts, [json!("first"), json!("other")]);
}) }

#[test]
fn a_second_change_of_a_row_is_sent_only_after_the_first_one_ends() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.transport.enqueue("UpdateMoment", updated("one").after(std::time::Duration::from_secs(10)));
    rig.transport.enqueue("UpdateMoment", updated("two"));
    for text in ["one", "two"] {
        rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": text}))).unwrap();
    }
    let sync = rig.sync.clone();
    let flushing = tokio::spawn(async move { let admitted = sync.inner.db.admit().unwrap(); crate::sync::rig::app(&sync, sync.flush(&admitted)).await.map(drop) });
    crate::test_wait::pass(std::time::Duration::from_secs(5)).await;
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1, "the second change left while the first was in flight");
    crate::test_wait::within(flushing).await.unwrap().unwrap();
    let texts: Vec<_> = rig.transport.calls().iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].clone()).collect();
    assert_eq!(texts, [json!("one"), json!("two")]);
}) }
