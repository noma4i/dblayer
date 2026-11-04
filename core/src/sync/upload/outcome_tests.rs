use super::super::rig::*;
use super::upload_tests::{blob, created, file, input, rid, small, upload_of, upload_rig};
use crate::syncstate::SyncState;
use crate::transport::{Reply, TransportError};
use crate::upload::UploadStatus;
use serde_json::json;

async fn uploaded_with_answer(answer: Reply) -> (Rig, tempfile::NamedTempFile) {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", answer);
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    (rig, source)
}

#[test]
fn inv_upload_03_an_event_showing_the_applied_effect_does_not_lift_failed_or_delivery_unknown() { crate::test_wait::paused(async {
    for (answer, state, status) in [
        (Reply::fail(TransportError::Interrupted("reset".into())), SyncState::DeliveryUnknown, UploadStatus::DeliveryUnknown),
        (Reply::graphql_error("nope", "FORBIDDEN"), SyncState::Failed, UploadStatus::Failed),
    ] {
        let (rig, _source) = uploaded_with_answer(answer).await;
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        rig.sync.wait_uploads().await;
        assert_eq!((rig.db.sync_info("moments", &rid("m1")).unwrap().state, upload_of(&rig, "m1").unwrap().status), (state, status));
        put(&rig, "moments", moment("m1", "clip"));
        rig.db.await_revision(rig.db.revision_seq().unwrap());
        assert_eq!((rig.db.sync_info("moments", &rid("m1")).unwrap().state, upload_of(&rig, "m1").unwrap().status), (state, status));
        assert_eq!(rig.db.outbox().unwrap().len(), 1);
        if status == UploadStatus::Failed {
            assert_eq!(rig.db.discard(&rig.db.admit().unwrap(), "moments", &rid("m1"), None).unwrap(), 1);
        } else {
            crate::sync::rig::app(&rig.sync, rig.sync.retry(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), None)).await.unwrap();
            rig.sync.wait_uploads().await;
        }
        assert!(rig.db.outbox().unwrap().is_empty(), "{status:?} left {:?}", rig.db.outbox().unwrap());
    }
}) }

#[test]
fn inv_upload_13_the_content_mutation_meets_each_transport_outcome_like_an_ordinary_mutation() { crate::test_wait::paused(async {
    for (answer, expected) in [
        (Reply::fail(TransportError::Interrupted("reset".into())), vec![SyncState::DeliveryUnknown]),
        (Reply::fail(TransportError::Network("no connection".into())), vec![SyncState::Pending]),
        (Reply::graphql_error("nope", "FORBIDDEN"), vec![SyncState::Failed]),
    ] {
        let (rig, _source) = uploaded_with_answer(answer).await;
        let state = rig.db.sync_info("moments", &rid("m1")).unwrap().state;
        assert!(expected.contains(&state), "{state:?} is not one of {expected:?}");
        assert_eq!(rig.db.outbox().unwrap().len(), 1, "an unconfirmed mutation must keep its operation");
        assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    }
}) }

#[test]
fn inv_op_05_an_unreadable_content_answer_keeps_the_upload_operation_and_the_row() { crate::test_wait::paused(async {
    let (rig, _source) = uploaded_with_answer(Reply::data(json!({"createMoment":{"text":"unreadable without id"}}))).await;
    let pending = rig.db.outbox().unwrap();
    assert_eq!(pending.len(), 1, "normalization must precede closing the upload operation");
    assert_eq!(pending[0].operation.status, crate::operation::OperationStatus::DeliveryUnknown);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("x".into()));
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::DeliveryUnknown);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}) }
