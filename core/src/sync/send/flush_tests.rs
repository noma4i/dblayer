use serde_json::json;

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::operation::{quarantine_counts, QuarantineReason};
use crate::query::Query;
use crate::transport::{Reply, TransportError};

use super::super::support::*;
use super::{SendResult, SkipReason};

use std::sync::{Arc, Mutex};

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn created(id: &str, text: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": text, "userId": null}}))
}

fn updated(id: &str, text: &str) -> Reply {
    Reply::data(json!({"updateMoment": {"id": id, "text": text, "userId": null}}))
}

fn call_variables(rig: &Rig, operation: &str) -> Vec<serde_json::Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == operation).map(|call| call.variables).collect()
}

fn statuses(rig: &Rig) -> Vec<OperationStatus> {
    rig.db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect()
}

#[tokio::test(start_paused = true)]
async fn create_success_swaps_the_temp_id_for_the_real_one_and_observers_see_it() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    let seen: Arc<Mutex<Vec<Vec<String>>>> = Arc::default();
    let sink = seen.clone();
    let _observer = rig
        .db
        .observe(Query::new("moments"), move |change| {
            sink.lock().unwrap().push(change.rows.iter().map(|row| row.id.to_string()).collect());
        })
        .unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.committed(), 1);
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
    assert_eq!(text_of(&rig, "moments", "m77"), Some("hello".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(call_variables(&rig, "CreateMoment"), [json!({"text": "hello"})]);
    rig.db.settle();
    let last = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last, ["m77"]);
}

#[tokio::test(start_paused = true)]
async fn rejected_update_restores_the_row_and_reports_the_error() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.rejections(), ["not allowed"]);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn rejected_create_removes_the_temp_row_and_rejected_destroy_restores_the_row() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    put(&rig, "moments", moment("m2", "keep"));
    rig.db.destroy("moments", &rid("m2")).unwrap();
    assert_eq!(text_of(&rig, "moments", "m2"), None);
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("bad", "BAD_USER_INPUT"));
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("gone", "FORBIDDEN"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.rejections(), ["bad", "gone"]);
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
    assert_eq!(text_of(&rig, "moments", "m2"), Some("keep".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn network_error_is_retried_within_the_budget() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Network("offline".into())));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.committed(), 1);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
}

#[tokio::test(start_paused = true)]
async fn exhausted_network_budget_leaves_the_operation_pending_for_the_next_flush() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Network("offline".into())));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.operations[0].result, SendResult::Deferred);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 4);
    let entry = rig.db.outbox().unwrap().remove(0);
    assert_eq!((entry.operation.status, entry.operation.transport_started), (OperationStatus::Pending, false));
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
}

#[tokio::test(start_paused = true)]
async fn interrupted_request_is_delivery_unknown_and_blocks_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "a"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.operations[0].result, SendResult::DeliveryUnknown);
    assert_eq!(statuses(&rig), [OperationStatus::DeliveryUnknown]);
    rig.db.update("moments", &rid("m1"), params(json!({"text": "b"}))).unwrap();
    let again = rig.sync.flush().await.unwrap();
    assert_eq!(again.operations[0].result, SendResult::Skipped(SkipReason::BlockedByHead));
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn concurrent_flushes_send_each_operation_once() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    let (first, second) = tokio::join!(rig.sync.flush(), rig.sync.flush());
    assert_eq!(first.unwrap().committed() + second.unwrap().committed(), 1);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn operations_leave_in_the_order_they_were_queued() {
    let rig = rig();
    rig.db.create("moments", params(json!({"text": "first"}))).unwrap();
    rig.db.create("moments", params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m1", "first"));
    rig.transport.enqueue("CreateMoment", created("m2", "second"));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 2);
    assert_eq!(call_variables(&rig, "CreateMoment"), [json!({"text": "first"}), json!({"text": "second"})]);
}

#[tokio::test(start_paused = true)]
async fn follower_of_a_temp_row_is_retargeted_to_the_real_id() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "draft"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m9", "draft"));
    rig.transport.enqueue("UpdateMoment", updated("m9", "edited"));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 2);
    assert_eq!(call_variables(&rig, "UpdateMoment"), [json!({"id": "m9", "text": "edited"})]);
}

#[tokio::test(start_paused = true)]
async fn rejected_create_drops_the_updates_queued_on_its_temp_row() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "draft"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("bad", "BAD_USER_INPUT"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.rejections(), ["bad"]);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.transport.call_count("UpdateMoment"), 0);
}

#[tokio::test(start_paused = true)]
async fn unusable_create_response_is_quarantined_and_the_operation_stays_open() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"bogus": true}})));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.operations[0].result, SendResult::Quarantined);
    assert_eq!(statuses(&rig), [OperationStatus::DeliveryUnknown]);
    assert!(rig.db.find("moments", &local.id).unwrap().is_some());
    let counts = quarantine_counts(&rig.db.shared.store.lock().connection).unwrap();
    assert_eq!(counts.get(QuarantineReason::ReplaceNormalizeRejected.as_str()), Some(&1));
}

#[tokio::test(start_paused = true)]
async fn operations_survive_a_restart_and_leave_on_the_next_flush() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.db");
    {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
        rig.db.create("moments", params(json!({"text": "offline"}))).unwrap();
        rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Network("offline".into())));
        assert_eq!(rig.sync.flush().await.unwrap().operations[0].result, SendResult::Deferred);
    }
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
    assert_eq!(rig.db.outbox().unwrap().len(), 1);
    rig.transport.enqueue("CreateMoment", created("m5", "offline"));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
    assert_eq!(text_of(&rig, "moments", "m5"), Some("offline".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn custom_action_sends_mapped_variables_and_rolls_its_patch_back_on_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.act("moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
    rig.transport.enqueue("LikeMoment", Reply::graphql_error("nope", "FORBIDDEN"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(call_variables(&rig, "LikeMoment"), [json!({"momentId": "m1"})]);
    assert_eq!(report.rejections(), ["nope"]);
    let row = rig.db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_ne!(row.data.get("likedByMe"), Some(&json!(true)));
}
