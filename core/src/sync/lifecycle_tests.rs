use serde_json::json;

use super::rig::*;
use crate::operation::OperationStatus;
use crate::transport::Reply;

#[test]
fn an_ended_session_parks_the_outbox_until_the_next_session_starts() { crate::test_wait::paused(async {
    let rig = rig();
    let row = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "unsent"}))).unwrap();
    rig.sync.lose_session_for_test();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m1", "text": "unsent", "userId": null}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert_eq!(rig.db.outbox().unwrap()[0].operation.status, OperationStatus::Pending);
    rig.sync.commit_session_for_test();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.find("moments", &row.id).unwrap().is_none());
}) }

#[test]
fn repeating_a_session_online_or_listening_event_adds_no_request() { crate::test_wait::paused(async {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &Default::default(), Default::default()).await.unwrap();
    let _watch = rig.sync.watch("moments", "feed", &Default::default()).unwrap();
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.sync.settled_for_test().await;
    let settled = (rig.transport.call_count("MomentsFeed"), rig.transport.call_count("MomentListen"));
    for _ in 0..2 {
        rig.sync.commit_session_for_test();
        rig.sync.set_online(true);
        rig.sync.set_listening(true);
    }
    rig.sync.settled_for_test().await;
    assert_eq!((rig.transport.call_count("MomentsFeed"), rig.transport.call_count("MomentListen")), settled);
}) }

#[tokio::test]
async fn ending_the_session_cancels_a_send_in_flight_and_frees_the_outbox() {
    let rig = rig();
    rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "unsent"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m1", "text": "unsent", "userId": null}})).after(std::time::Duration::from_secs(3600)));
    let sync = rig.sync.clone();
    let flushing = tokio::spawn(async move { crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await });
    rig.transport.await_calls("CreateMoment", 1).await;
    rig.sync.lose_session_for_test();
    assert!(matches!(crate::test_wait::within(flushing).await.unwrap(), Err(crate::sync::SyncError::Transport(crate::transport::TransportError::Interrupted(_)))));
    crate::test_wait::yield_until("the outbox gate is free", || rig.sync.inner.outbox_gate.try_lock().is_ok()).await;
    assert_ne!(rig.db.outbox().unwrap()[0].operation.status, OperationStatus::Committed);
}

#[tokio::test]
async fn closing_the_database_ends_the_outcome_wait_of_a_pending_operation() {
    let rig = rig();
    rig.sync.lose_session_for_test();
    let (_, ticket) = create_claimed(&rig.sync, "moments", params(json!({"text": "unsent"})));
    let admitted = rig.sync.inner.db.admit().unwrap();
    let waiting = tokio::spawn(async move { admitted.run(ticket.result()).await.map(drop) });
    crate::test_wait::pass(std::time::Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    rig.sync.close();
    assert!(crate::test_wait::within(waiting).await.unwrap().is_err(), "an outcome wait outlived the close");
}

#[tokio::test]
async fn a_flush_woken_during_a_model_reset_sends_the_queued_write_once_after_the_reset() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.sync.set_online(false);
    rig.db.update(&rig.db.admit().unwrap(), "moments", &crate::identity::RowId::new("m1").unwrap(), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})));
    let lifecycle = rig.db.shared.store.lifecycle.clone();
    let permit = lifecycle.begin_blocking(crate::ffi::legacy::Boundary::Models, None).unwrap();
    rig.sync.set_online(true);
    rig.db.reset_in(&permit, &["users"]).unwrap();
    permit.finish();
    crate::test_wait::within(rig.transport.await_calls("UpdateMoment", 1)).await;
    crate::test_wait::yield_until("the update left the outbox", || rig.db.outbox().unwrap().is_empty()).await;
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}
