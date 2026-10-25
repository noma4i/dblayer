use super::*;
use serde_json::json;
use crate::transport::{Reply, Response};
use crate::upload::{UploadRequest, UploadStage};
use super::upload_tests::{file, input, rid, small, upload_rig};

#[tokio::test(start_paused = true)]
async fn a_resumed_presign_for_one_expired_part_keeps_the_other_unfinished_parts() {
    let rig = upload_rig(small());
    let source = file(b"0123456789ab");
    let request = UploadRequest {
        document: "operation x { x }".into(), name: "PresignParts".into(),
        variables: json!({"key": "key", "uploadId": "upload", "partNumbers": [2]}),
    };
    let state = UploadState {
        stage: UploadStage::Presign, total_bytes: 12, key: Some("key".into()), upload_id: Some("upload".into()), request: Some(request.clone()),
        ..UploadState::queued()
    };
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), state).unwrap();
    rig.transport.enqueue("PresignParts", Reply::data(json!({"presignParts": {"parts": [
        {"partNumber": 1, "url": "https://s3/p1"}, {"partNumber": 3, "url": "https://s3/p3"}
    ]}})));
    rig.transport.enqueue("CompleteMultipart", Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "blob"}})));
    rig.transport.enqueue("CreateMoment", upload_tests::created("m9"));
    let resumed = Response::data(json!({"presignParts": {"parts": [{"partNumber": 2, "url": "https://s3/p2"}]}}));
    let serial = rig.sync.inner.outbox_gate.lock().await;
    let owner = crate::transport::RequestOwner { seq, item_position: 0, stage: "presign".into() };
    let upload = rig.sync.inner.claim_upload_resume(&owner).await.unwrap();
    rig.sync.inner.settle_resumed(&serial, upload, owner, Ok(resumed)).await.unwrap();
    drop(serial);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 3);
    let calls: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "PresignParts").collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].variables["partNumbers"], json!([1, 3]));
    assert!(rig.db.outbox().unwrap().is_empty());
}
