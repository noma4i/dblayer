use std::time::Duration;

use serde_json::json;

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::transport::{Reply, TransportError};

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

async fn flush_while_resetting(rig: &Rig, reply: Reply) {
    rig.transport.enqueue("UpdateMoment", reply);
    reset_during_flush(rig).await;
}

async fn reset_during_flush(rig: &Rig) {
    let reset = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        rig.db.reset(&["moments"]).unwrap();
    };
    let (flushed, ()) = tokio::join!(rig.sync.flush(), reset);
    flushed.unwrap();
}

fn rig_with_pending_update() -> Rig {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig
}

#[tokio::test(start_paused = true)]
async fn a_reset_during_a_slow_answer_leaves_no_operation_stuck_in_sending() {
    let rig = rig_with_pending_update();
    flush_while_resetting(&rig, Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})).after(Duration::from_secs(5))).await;
    let stuck: Vec<_> = rig.db.outbox().unwrap().into_iter().filter(|entry| entry.operation.status == OperationStatus::Pending && entry.operation.transport_started).collect();
    assert!(stuck.is_empty(), "an operation stays started although no request is in flight: {stuck:?}");
}

#[tokio::test(start_paused = true)]
async fn a_reset_during_a_slow_network_failure_leaves_no_operation_stuck_in_sending() {
    let rig = rig_with_pending_update();
    flush_while_resetting(&rig, Reply::fail(TransportError::Network("down".to_string())).after(Duration::from_secs(5))).await;
    let stuck: Vec<_> = rig.db.outbox().unwrap().into_iter().filter(|entry| entry.operation.status == OperationStatus::Pending && entry.operation.transport_started).collect();
    assert!(stuck.is_empty(), "an operation stays started although no request is in flight: {stuck:?}");
}

#[tokio::test(start_paused = true)]
async fn a_success_answered_after_a_reset_closes_the_operation_and_does_not_bring_the_row_back() {
    let rig = rig_with_pending_update();
    flush_while_resetting(&rig, Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})).after(Duration::from_secs(5))).await;
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_network_failure_after_a_reset_defers_the_operation_and_it_is_sent_again() {
    let rig = rig_with_pending_update();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Network("down".to_string())).after(Duration::from_secs(5)));
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})));
    reset_during_flush(&rig).await;
    rig.clock.advance(Duration::from_secs(3600));
    rig.sync.flush().await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_refusal_answered_after_a_reset_rolls_the_operation_back_like_any_refusal() {
    let rig = rig_with_pending_update();
    flush_while_resetting(&rig, Reply::graphql_error("not allowed", "FORBIDDEN").after(Duration::from_secs(5))).await;
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_create_answered_after_a_reset_of_another_model_leaves_no_temp_row_behind() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})).after(Duration::from_secs(5)));
    let reset = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        rig.db.reset(&["vibes"]).unwrap();
    };
    let (flushed, ()) = tokio::join!(rig.sync.flush(), reset);
    flushed.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
}
