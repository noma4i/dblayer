use std::time::Duration;

use serde_json::json;

use crate::operation::OperationStatus;
use crate::identity::RowId;
use crate::pacing::Clock;
use crate::syncstate::{FailureKind, SyncInfo, SyncState};
use crate::transport::{Reply, TransportError};

use super::super::rig::*;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn updated(id: &str, text: &str) -> Reply {
    Reply::data(json!({"updateMoment": {"id": id, "text": text, "userId": null}}))
}

fn unreachable() -> Reply {
    Reply::fail(TransportError::Network("no connection".into()))
}

fn info(rig: &Rig, id: &str) -> SyncInfo {
    rig.db.sync_info("moments", &rid(id)).unwrap()
}

fn queue_update(rig: &Rig, text: &str) {
    put(rig, "moments", moment("m1", "old"));
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": text}))).unwrap();
}

async fn exhaust(rig: &Rig, operation: &str) {
    for _ in 0..40 {
        rig.transport.enqueue(operation, unreachable());
    }
    for _ in 0..40 {
        rig.clock.advance(Duration::from_secs(31));
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    }
}

#[test]
fn unreached_mutation_stays_pending_and_leaves_again_after_the_pause() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "new");
    rig.transport.enqueue("UpdateMoment", unreachable());
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    assert_eq!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap().committed(), 0);
    let waiting = info(&rig, "m1");
    assert_eq!((waiting.state, waiting.attempts), (SyncState::Pending, 1));
    assert_eq!(waiting.error.map(|error| error.kind), Some(FailureKind::Network));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
    rig.clock.advance(Duration::from_secs(2));
    assert_eq!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap().committed(), 1);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
    assert!(info(&rig, "m1").is_synced());
}) }

#[test]
fn interrupted_transfer_is_delivery_unknown_and_never_resent_by_itself() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "new");
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(info(&rig, "m1").state, SyncState::DeliveryUnknown);
    rig.clock.advance(Duration::from_secs(3600));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}) }

#[test]
fn inv_op_17_offline_flag_holds_the_mutation_without_a_call_or_an_attempt_and_online_sends_it() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "new");
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    rig.sync.set_online(false);
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("UpdateMoment"), 0);
    let held = info(&rig, "m1");
    assert_eq!((held.state, held.attempts), (SyncState::Pending, 0));
    let mut outbox = rig.db.outbox_changes();
    rig.sync.set_online(true);
    await_outbox(&mut outbox, || info(&rig, "m1").is_synced()).await;
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
}) }

#[test]
fn the_pause_ends_by_itself_without_another_flush() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "new");
    rig.transport.enqueue("UpdateMoment", unreachable());
    rig.transport.enqueue("UpdateMoment", updated("m1", "new"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    rig.clock.advance(Duration::from_secs(2));
    crate::test_wait::pass(Duration::from_secs(2)).await;
    assert_eq!(rig.transport.call_count("UpdateMoment"), 2);
    assert!(info(&rig, "m1").is_synced());
}) }

#[test]
fn forty_unreached_attempts_keep_the_edit_failed_and_own_pending() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "new");
    let seq = rig.db.outbox().unwrap()[0].operation.id.0;
    rig.db.shared.store.lock().connection.execute(
        "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, owner_kind, owner_operation_id, purpose) VALUES (1, 'digest', 'request', 'key', 1, '/graphql', '{}', 'operation', ?1, 0)",
        [seq],
    ).unwrap();
    exhaust(&rig, "UpdateMoment").await;
    assert_eq!(rig.transport.call_count("UpdateMoment"), 40);
    assert_eq!(text_of(&rig, "moments", "m1"), Some("new".to_string()));
    let failed = info(&rig, "m1");
    assert_eq!((failed.state, failed.attempts), (SyncState::Failed, 40));
    assert_eq!(rig.db.outbox().unwrap()[0].operation.id.0, seq);
    assert!(crate::transport::encrypted::pending_matches(&rig.db, &crate::transport::RequestOwner::Operation { seq }).unwrap());
}) }

#[test]
fn forty_unreached_attempts_leave_a_kept_update_failed() { crate::test_wait::paused(async {
    let rig = keeping_rig();
    queue_update(&rig, "new");
    exhaust(&rig, "UpdateMoment").await;
    let failed = info(&rig, "m1");
    assert_eq!((failed.state, failed.attempts), (SyncState::Failed, 40));
    assert_eq!(failed.error.map(|error| error.kind), Some(FailureKind::Network));
    assert_eq!(rig.transport.call_count("UpdateMoment"), 40);
}) }

#[test]
fn inv_op_08_forty_unreached_attempts_bring_a_destroyed_record_back_as_failed() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    assert_eq!(rig.db.find("moments", &rid("m1")).unwrap(), None);
    exhaust(&rig, "DeleteMoment").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
    let failed = info(&rig, "m1");
    assert_eq!((failed.state, failed.attempts, failed.error.map(|error| error.kind)), (SyncState::Failed, 40, Some(FailureKind::Network)));
}) }

#[test]
fn a_waiting_mutation_holds_back_later_ones_on_the_same_record() { crate::test_wait::paused(async {
    let rig = rig();
    queue_update(&rig, "first");
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", unreachable());
    rig.transport.enqueue("UpdateMoment", updated("m1", "first"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "second"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.transport.call_count("UpdateMoment"), 1);
    rig.clock.advance(Duration::from_secs(2));
    assert_eq!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap().committed(), 2);
    let sent: Vec<_> = rig.transport.calls().into_iter().map(|call| call.variables).collect();
    assert_eq!(sent.len(), 3);
    assert!(rig.db.outbox().unwrap().is_empty());
}) }

#[test]
fn the_count_continues_after_a_restart() { crate::test_wait::paused(async {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
    queue_update(&rig, "new");
    rig.transport.enqueue("UpdateMoment", unreachable());
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(info(&rig, "m1").attempts, 1);
    let stopped_at = Clock::now(&rig.clock);
    rig.db.close().unwrap();
    drop(rig);
    let again = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), Default::default());
    again.transport.enqueue("UpdateMoment", unreachable());
    again.clock.set(stopped_at + Duration::from_secs(2));
    crate::sync::rig::app(&again.sync, again.sync.flush(&again.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(again.transport.call_count("UpdateMoment"), 1);
    assert_eq!(info(&again, "m1").attempts, 2);
    assert_eq!(again.db.outbox().unwrap()[0].operation.status, OperationStatus::Pending);
}) }

#[test]
fn retry_all_sends_every_failed_and_unknown_record_again() { crate::test_wait::paused(async {
    let rig = keeping_rig();
    put(&rig, "moments", moment("m1", "a"));
    put(&rig, "moments", moment("m2", "b"));
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m1"), params(json!({"text": "a2"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &rid("m2"), params(json!({"text": "b2"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("no", "FORBIDDEN"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "a2"));
    rig.transport.enqueue("UpdateMoment", updated("m2", "b2"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!((info(&rig, "m1").state, info(&rig, "m2").state), (SyncState::DeliveryUnknown, SyncState::Failed));
    let report = crate::sync::rig::app(&rig.sync, rig.sync.retry_all(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(report.committed(), 2);
    assert_eq!(rig.transport.call_count("UpdateMoment"), 4);
}) }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_of_the_network_waits_for_the_commit_of_a_start_that_passed_its_admission() {
    use crate::commit::{CommitEnvelope, EntityOp, NetworkAdmission, WriteOrigin};
    use crate::identity::ModelKey;
    let rig = rig();
    let (inner, shared) = (rig.sync.inner.clone(), rig.db.shared.clone());
    let store = &shared.store;
    let (lost, account, connectivity) = (inner.network_lost.clone(), inner.account.clone(), inner.connectivity.clone());
    let state = { let (lost, account, connectivity) = (lost.clone(), account.clone(), connectivity.clone()); move || (*super::super::locked(&lost), super::super::locked(&account).1, connectivity.is_online()) };
    let expected = state();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m1"), row: moment("m1", "start").as_object().unwrap().clone() });
    envelope.network = Some(NetworkAdmission::new(expected, std::sync::Arc::new(state), inner.network_line.clone()));
    let waiting_before = *inner.network_line.waiting.0.lock().unwrap();
    let changing = std::sync::Arc::new(std::sync::Mutex::new(None));
    let (sync, line, slot, online, handle) = (rig.sync.clone(), inner.network_line.clone(), changing.clone(), connectivity.clone(), tokio::runtime::Handle::current());
    *store.after_network_check.lock().unwrap() = Some(Box::new(move || {
        *slot.lock().unwrap() = Some(std::thread::spawn(move || { let _runtime = handle.enter(); sync.set_online(false) }));
        crate::test_wait::spin_until("the change reached the network line", || *line.waiting.0.lock().unwrap() >= waiting_before + 2);
        assert!(!slot.lock().unwrap().as_ref().unwrap().is_finished(), "the change of the network did not wait for the commit");
        assert!(online.is_online(), "the network moved while the start held the line");
    }));
    let admitted = store.lifecycle.admit(crate::ffi::legacy::Origin::Explicit, "test commit").unwrap();
    let committed = store.commit_as(&admitted.authority(), envelope);
    assert!(committed.is_ok(), "{:?}", committed.err());
    crate::test_wait::join_within(changing.lock().unwrap().take().unwrap());
    assert!(!connectivity.is_online());
}

#[tokio::test]
async fn the_state_of_the_network_and_its_publication_change_in_one_step() {
    use std::{future::Future, task::{Context, Waker}};
    let rig = rig();
    for online in [false, true] {
        let connectivity = rig.sync.inner.connectivity.clone();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = seen.clone();
        *super::super::locked(&rig.sync.inner.after_network_change) = Some(Box::new(move || {
            let mut wait = Box::pin(async { if online { connectivity.online().await } else { connectivity.offline().await } });
            let published = wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready();
            *slot.lock().unwrap() = Some((connectivity.is_online(), published));
        }));
        rig.sync.set_online(online);
        assert_eq!(*seen.lock().unwrap(), Some((online, true)), "online={online}: the state read inside the change and the published state differ");
    }
}
