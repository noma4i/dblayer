use serde_json::json;

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::operation::{quarantine_counts, QuarantineReason};
use crate::query::Query;
use crate::syncstate::{FailureKind, OperationFailure, SyncState};
use crate::transport::{Reply, TransportError};

use super::super::rig::*;
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
async fn rejected_create_removes_the_temp_row_and_rejected_destroy_restores_the_row_as_failed() {
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
    assert_eq!(statuses(&rig), [OperationStatus::Failed]);
    let failed = info(&rig, "m2");
    assert_eq!((failed.state, failed.actions, failed.error.map(|error| error.message)), (SyncState::Failed, vec!["destroy".to_string()], Some("gone".to_string())));
}

#[tokio::test(start_paused = true)]
async fn retry_of_a_failed_destroy_hides_the_record_again_and_deletes_it() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "keep"));
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("gone", "FORBIDDEN"));
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"id": "m1"}})));
    rig.sync.flush().await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("keep".to_string()));
    assert_eq!(rig.db.retry("moments", &rid("m1")).unwrap(), 1);
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn discard_of_a_failed_destroy_keeps_the_record_and_clears_the_error() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "keep"));
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("gone", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.db.discard("moments", &rid("m1")).unwrap(), 1);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("keep".to_string()));
    assert!(info(&rig, "m1").is_synced());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn interrupted_transfer_is_not_retried_and_leaves_delivery_unknown() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.operations[0].result, SendResult::DeliveryUnknown);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
    assert_eq!(statuses(&rig), [OperationStatus::DeliveryUnknown]);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 0);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn server_error_status_is_not_retried_and_leaves_delivery_unknown() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Http { status: 503, body: "busy".into() }));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.operations[0].result, SendResult::DeliveryUnknown);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
    assert_eq!(statuses(&rig), [OperationStatus::DeliveryUnknown]);
}

#[tokio::test(start_paused = true)]
async fn retriable_graphql_code_is_a_refusal_and_is_not_retried() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("busy", "INTERNAL_SERVER_ERROR"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.rejections(), ["busy"]);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
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
async fn unsent_operations_survive_a_restart_and_leave_on_the_next_flush() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.db");
    {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
        rig.db.create("moments", params(json!({"text": "offline"}))).unwrap();
    }
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
    assert_eq!(rig.db.outbox().unwrap().len(), 1);
    assert_eq!(rig.db.outbox().unwrap()[0].operation.status, OperationStatus::Pending);
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

fn info(rig: &Rig, id: &str) -> crate::syncstate::SyncInfo {
    rig.db.sync_info("moments", &rid(id)).unwrap()
}

#[tokio::test(start_paused = true)]
async fn record_is_sending_while_the_request_is_in_flight_and_synced_after_the_answer() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    assert_eq!(info(&rig, "m1").state, SyncState::Pending);
    rig.transport.enqueue("UpdateMoment", updated("m1", "new").after(std::time::Duration::from_secs(5)));
    let flight = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.flush().await }
    });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let sending = info(&rig, "m1");
    assert_eq!((sending.state, sending.attempts), (SyncState::Sending, 1));
    assert_eq!(flight.await.unwrap().unwrap().committed(), 1);
    assert_eq!(info(&rig, "m1"), crate::syncstate::SyncInfo::default());
}

#[tokio::test(start_paused = true)]
async fn interrupted_transfer_records_the_kind_and_attempt_and_retry_sends_the_same_operation_again() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    rig.sync.flush().await.unwrap();
    let unknown = info(&rig, "m1");
    assert_eq!((unknown.state, unknown.attempts), (SyncState::DeliveryUnknown, 1));
    assert_eq!(unknown.error.map(|error| error.kind), Some(FailureKind::Network));
    assert_eq!(rig.sync.retry("moments", &rid("m1")).await.unwrap().committed(), 1);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
    assert!(info(&rig, "m1").is_synced());
}

#[tokio::test(start_paused = true)]
async fn kept_update_stays_failed_with_the_error_and_blocks_only_its_own_record() {
    let rig = keeping_rig();
    put(&rig, "moments", moment("m1", "old"));
    put(&rig, "moments", moment("m2", "other"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.db.update("moments", &rid("m2"), params(json!({"text": "next"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.transport.enqueue("UpdateMoment", updated("m2", "next"));
    let report = rig.sync.flush().await.unwrap();
    assert_eq!(report.rejections(), ["not allowed"]);
    let failed = info(&rig, "m1");
    assert_eq!((failed.state, failed.attempts, failed.error), (SyncState::Failed, 1, Some(OperationFailure::new(FailureKind::Rejected, "not allowed"))));
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    assert!(info(&rig, "m2").is_synced());
    rig.db.update("moments", &rid("m1"), params(json!({"text": "newer"}))).unwrap();
    let again = rig.sync.flush().await.unwrap();
    assert_eq!(again.operations[0].result, SendResult::Skipped(SkipReason::BlockedByHead));
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
    assert_eq!(statuses(&rig), [OperationStatus::Failed, OperationStatus::Pending]);
}

// mutation: drop the retire_operations call from Store::reset_incompatible
#[tokio::test(start_paused = true)]
async fn logout_leaves_no_refused_operation_able_to_block_the_next_one_of_the_row() {
    let rig = keeping_rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "later"));
    rig.sync.flush().await.unwrap();
    assert_eq!(statuses(&rig), [OperationStatus::Failed]);
    rig.db.reset_all().unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "later"}))).unwrap();
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
}

#[tokio::test(start_paused = true)]
async fn retry_of_a_kept_failure_resets_attempts_and_confirms_on_success() {
    let rig = keeping_rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    rig.sync.flush().await.unwrap();
    assert_eq!(info(&rig, "m1").state, SyncState::Failed);
    assert_eq!(rig.db.retry("moments", &rid("m1")).unwrap(), 1);
    let pending = info(&rig, "m1");
    assert_eq!((pending.state, pending.attempts, pending.error), (SyncState::Pending, 0, None));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 1);
    assert!(info(&rig, "m1").is_synced());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn discard_of_a_kept_failure_restores_the_old_values() {
    let rig = keeping_rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.db.discard("moments", &rid("m1")).unwrap(), 1);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
    assert!(info(&rig, "m1").is_synced());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn kept_create_stays_in_lists_as_failed_and_retry_swaps_the_temp_id() {
    let rig = keeping_rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("bad", "BAD_USER_INPUT"));
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    rig.sync.flush().await.unwrap();
    let failed = rig.db.query(&crate::query::Query::failed("moments")).unwrap();
    assert_eq!(failed.iter().map(|row| row.id.clone()).collect::<Vec<_>>(), std::slice::from_ref(&local.id));
    assert!(failed[0].is_local());
    assert_eq!(rig.sync.retry("moments", &local.id).await.unwrap().committed(), 1);
    assert_eq!(text_of(&rig, "moments", "m77"), Some("hello".to_string()));
    assert!(rig.db.query(&crate::query::Query::unsynced("moments")).unwrap().is_empty());
    assert_eq!(info(&rig, local.id.as_str()), crate::syncstate::SyncInfo::default());
}

#[tokio::test(start_paused = true)]
async fn follower_of_a_temp_row_moves_its_sync_state_to_the_real_id() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "draft"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m9", "draft").after(std::time::Duration::ZERO));
    rig.transport.enqueue("UpdateMoment", updated("m9", "edited").after(std::time::Duration::from_secs(5)));
    let flight = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.flush().await }
    });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(info(&rig, local.id.as_str()), crate::syncstate::SyncInfo::default());
    let moved = info(&rig, "m9");
    assert_eq!((moved.state, moved.actions), (SyncState::Sending, vec!["update".to_string()]));
    flight.await.unwrap().unwrap();
    assert!(info(&rig, "m9").is_synced());
}

#[tokio::test(start_paused = true)]
async fn kinds_follow_the_failure_class() {
    for (id, reply, state, kind) in [
        ("m1", Reply::fail(TransportError::Http { status: 502, body: "bad gateway".into() }), SyncState::DeliveryUnknown, FailureKind::Server),
        ("m2", Reply::graphql_error("busy", "SERVICE_UNAVAILABLE"), SyncState::Failed, FailureKind::Server),
        ("m3", Reply::fail(TransportError::Http { status: 400, body: "bad".into() }), SyncState::Failed, FailureKind::Rejected),
        ("m4", Reply::fail(TransportError::Decode("junk".into())), SyncState::DeliveryUnknown, FailureKind::Unrecognized),
        ("m5", Reply::fail(TransportError::Interrupted("reset".into())), SyncState::DeliveryUnknown, FailureKind::Network),
        ("m6", Reply::fail(TransportError::Graphql { messages: vec!["no".into()] }), SyncState::Failed, FailureKind::Rejected),
    ] {
        let rig = keeping_rig();
        put(&rig, "moments", moment(id, "old"));
        rig.db.update("moments", &rid(id), params(json!({"text": "new"}))).unwrap();
        rig.transport.enqueue("UpdateMoment", reply);
        rig.sync.flush().await.unwrap();
        let seen = info(&rig, id);
        assert_eq!((seen.state, seen.error.map(|error| error.kind)), (state, Some(kind)), "{id}");
    }
}

#[tokio::test(start_paused = true)]
async fn unusable_create_response_marks_the_record_unrecognized() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"bogus": true}})));
    rig.sync.flush().await.unwrap();
    let seen = info(&rig, local.id.as_str());
    assert_eq!((seen.state, seen.error.map(|error| error.kind)), (SyncState::DeliveryUnknown, Some(FailureKind::Unrecognized)));
}

#[tokio::test(start_paused = true)]
async fn custom_action_queued_before_a_restart_is_kept_and_sent_afterwards() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.db");
    {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
        put(&rig, "moments", moment("m1", "t"));
        rig.db.act("moments", "like", &rid("m1"), params(json!({"likedByMe": true})), params(json!({}))).unwrap();
        rig.db.destroy("moments", &rid("m1")).unwrap();
    }
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
    assert_eq!(statuses(&rig), [OperationStatus::Pending, OperationStatus::Pending]);
    rig.transport.enqueue("LikeMoment", Reply::data(json!({"likeMoment": {"id": "m1", "text": "t", "userId": null}})));
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"id": "m1"}})));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 2);
}

#[tokio::test(start_paused = true)]
async fn wrapped_action_nests_its_variables_and_reads_the_row_at_its_root() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::data(json!({"starMoment": {"moment": {"id": "m1", "text": "starred", "userId": null}}})));
    rig.sync.flush().await.unwrap();
    assert_eq!(call_variables(&rig, "StarMoment"), [json!({"input": {"momentId": "m1"}})]);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("starred".to_string()));
}

#[tokio::test(start_paused = true)]
async fn action_lands_the_rows_it_declares_outside_its_root() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.act("moments", "star", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("StarMoment", Reply::data(json!({"starMoment": {"moment": {"id": "m1", "text": "starred", "userId": null}, "viewer": {"id": "u9", "name": "Zed"}}})));
    rig.sync.flush().await.unwrap();
    let viewer = rig.db.find("users", &rid("u9")).unwrap().expect("the viewer row lands");
    assert_eq!(viewer.text("name"), Some("Zed"));
}

#[tokio::test(start_paused = true)]
async fn action_that_ignores_its_response_keeps_the_local_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.act("moments", "poke", &rid("m1"), params(json!({})), params(json!({}))).unwrap();
    rig.transport.enqueue("PokeMoment", Reply::data(json!({"pokeMoment": {"id": "m1", "text": "SERVER", "userId": null}})));
    rig.sync.flush().await.unwrap();
    assert_eq!(call_variables(&rig, "PokeMoment"), [json!({"momentId": "m1"})]);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("t".to_string()));
}

#[tokio::test(start_paused = true)]
async fn confirmed_waits_for_the_server_id_of_a_local_create() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    let real = rig.sync.confirmed("moments", &local.id).await.unwrap();
    assert_eq!(real.as_str(), "m77");
    assert_eq!(rig.sync.confirmed("moments", &real).await.unwrap(), real);
}

#[tokio::test(start_paused = true)]
async fn confirmed_fails_when_the_create_was_refused() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("nope", "FORBIDDEN"));
    assert!(rig.sync.confirmed("moments", &local.id).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn settled_waits_for_every_change_of_the_record() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    rig.sync.settled("moments", &rid("m1")).await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn settled_fails_with_the_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    let error = rig.sync.settled("moments", &rid("m1")).await.unwrap_err();
    assert!(error.to_string().contains("not allowed"));
}

#[tokio::test(start_paused = true)]
async fn settled_reports_a_refusal_answered_before_it_started_and_reports_it_once() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    let error = rig.sync.settled("moments", &rid("m1")).await.unwrap_err();
    assert!(error.to_string().contains("not allowed"));
    rig.sync.settled("moments", &rid("m1")).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn confirmed_reports_a_create_refused_before_it_started() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("nope", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    assert!(rig.sync.confirmed("moments", &local.id).await.unwrap_err().to_string().contains("nope"));
}

#[tokio::test(start_paused = true)]
async fn settled_reports_a_destroy_and_a_kept_update_refused_before_it_started() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("no destroy", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    assert!(rig.sync.settled("moments", &rid("m1")).await.unwrap_err().to_string().contains("no destroy"));
    let keeping = keeping_rig();
    put(&keeping, "moments", moment("m2", "t"));
    keeping.db.update("moments", &rid("m2"), params(json!({"text": "new"}))).unwrap();
    keeping.transport.enqueue("UpdateMoment", Reply::graphql_error("kept", "FORBIDDEN"));
    keeping.sync.flush().await.unwrap();
    assert!(keeping.sync.settled("moments", &rid("m2")).await.unwrap_err().to_string().contains("kept"));
}

#[tokio::test(start_paused = true)]
async fn settled_forgets_a_refusal_that_a_retry_has_since_committed() {
    let rig = rig_without_operations(crate::db::Db::open_in_memory(specs(), None).unwrap(), crate::transport::MockTransport::new(), crate::sync::SyncConfig::default());
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.sync.flush().await.unwrap();
    rig.sync.register(crate::sync::OperationDef::new("UpdateMoment", "operation UpdateMoment { x }", "updateMoment"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
    rig.sync.settled("moments", &rid("m1")).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn update_whose_mutation_is_not_registered_fails_visibly_and_sends_after_registering() {
    let rig = rig_without_operations(crate::db::Db::open_in_memory(specs(), None).unwrap(), crate::transport::MockTransport::new(), crate::sync::SyncConfig::default());
    put(&rig, "moments", moment("m1", "old"));
    rig.db.update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    let report = rig.sync.flush().await.unwrap();
    let message = "operation UpdateMoment is not registered";
    assert_eq!(report.operations[0].result, SendResult::Rejected(message.to_string()));
    let failed = info(&rig, "m1");
    assert_eq!((failed.state, failed.error), (SyncState::Failed, Some(OperationFailure::new(FailureKind::Rejected, message))));
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    assert_eq!(rig.transport.calls().len(), 0);
    rig.sync.register(crate::sync::OperationDef::new("UpdateMoment", "operation UpdateMoment { x }", "updateMoment"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    assert_eq!(rig.sync.retry("moments", &rid("m1")).await.unwrap().committed(), 1);
    assert!(info(&rig, "m1").is_synced());
}
