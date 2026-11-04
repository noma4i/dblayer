use std::time::Duration;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use crate::syncstate::SyncState;

#[test]
fn inv_op_21_destroy_of_a_temp_row_with_an_unsent_create_removes_the_pair_without_the_network() { crate::test_wait::paused(async {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
    assert_eq!(rig.db.sync_info("moments", &local.id).unwrap().state, SyncState::Synced);
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(names(&rig).is_empty());
}) }

#[test]
fn inv_op_21_destroy_while_the_create_is_in_flight_waits_and_then_destroys_the_real_id() { crate::test_wait::paused(async {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello").after(Duration::from_secs(5)));
    rig.transport.enqueue("DeleteMoment", deleted("m77"));
    let destroy = async {
        crate::test_wait::pass(Duration::from_secs(1)).await;
        rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, async { rig.sync.flush(&rig.sync.inner.db.admit().unwrap()).await }), destroy);
    flushed.unwrap();
    assert_eq!(names(&rig), ["CreateMoment", "DeleteMoment"]);
    assert_eq!(rig.transport.calls()[1].variables["id"], json!("m77"));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}) }

