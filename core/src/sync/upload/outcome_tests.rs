use super::super::rig::*;
use super::upload_tests::{blob, created, file, input, rid, small, upload_of, upload_rig};
use crate::syncstate::SyncState;
use crate::transport::{Reply, TransportError};
use crate::upload::UploadStatus;

async fn uploaded_with_answer(answer: Reply) -> (Rig, tempfile::NamedTempFile) {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", answer);
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    (rig, source)
}

#[tokio::test(start_paused = true)]
async fn inv_upload_03_an_event_showing_the_applied_effect_does_not_lift_failed_or_delivery_unknown() {
    for (answer, state, status) in [
        (Reply::fail(TransportError::Interrupted("reset".into())), SyncState::DeliveryUnknown, UploadStatus::DeliveryUnknown),
        (Reply::graphql_error("nope", "FORBIDDEN"), SyncState::Failed, UploadStatus::Failed),
    ] {
        let (rig, _source) = uploaded_with_answer(answer).await;
        rig.sync.flush().await.unwrap();
        rig.sync.wait_uploads().await;
        assert_eq!((rig.db.sync_info("moments", &rid("m1")).unwrap().state, upload_of(&rig, "m1").unwrap().status), (state, status));
        put(&rig, "moments", moment("m1", "clip"));
        rig.db.await_revision(rig.db.revision_seq().unwrap());
        assert_eq!((rig.db.sync_info("moments", &rid("m1")).unwrap().state, upload_of(&rig, "m1").unwrap().status), (state, status));
        assert_eq!(rig.db.outbox().unwrap().len(), 1);
        if status == UploadStatus::Failed {
            assert_eq!(rig.db.discard("moments", &rid("m1"), None).unwrap(), 1);
        } else {
            rig.sync.retry("moments", &rid("m1"), None).await.unwrap();
            rig.sync.wait_uploads().await;
        }
        assert!(rig.db.outbox().unwrap().is_empty(), "{status:?} left {:?}", rig.db.outbox().unwrap());
    }
}

#[tokio::test(start_paused = true)]
async fn inv_upload_13_the_content_mutation_meets_each_transport_outcome_like_an_ordinary_mutation() {
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
}
