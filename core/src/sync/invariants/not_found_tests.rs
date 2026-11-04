use super::super::rig::*;
use super::fixtures::*;
use crate::syncstate::SyncState;
use crate::transport::Reply;

#[tokio::test(start_paused = true)]
async fn inv_op_22_a_destroy_answered_not_found_is_a_success() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.as_app().destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("gone", "NOT_FOUND"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Synced);
}

#[tokio::test(start_paused = true)]
async fn inv_op_22_a_destroy_answered_with_another_code_is_still_a_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.as_app().destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("no", "FORBIDDEN"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Failed);
}
