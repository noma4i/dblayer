use std::time::Duration;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use crate::syncstate::SyncState;

#[tokio::test(start_paused = true)]
async fn inv_op_21_destroy_of_a_temp_row_with_an_unsent_create_removes_the_pair_without_the_network() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.db.destroy("moments", &local.id).unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
    assert_eq!(rig.db.sync_info("moments", &local.id).unwrap().state, SyncState::Synced);
    rig.sync.flush().await.unwrap();
    assert!(names(&rig).is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_op_21_destroy_while_the_create_is_in_flight_waits_and_then_destroys_the_real_id() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello").after(Duration::from_secs(5)));
    rig.transport.enqueue("DeleteMoment", deleted("m77"));
    let destroy = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        rig.db.destroy("moments", &local.id).unwrap();
    };
    let (flushed, ()) = tokio::join!(rig.sync.flush(), destroy);
    flushed.unwrap();
    assert_eq!(names(&rig), ["CreateMoment", "DeleteMoment"]);
    assert_eq!(rig.transport.calls()[1].variables["id"], json!("m77"));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}

