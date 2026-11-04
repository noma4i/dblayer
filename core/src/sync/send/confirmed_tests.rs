use serde_json::json;

use crate::identity::RowId;
use crate::transport::Reply;

use super::super::error::SyncError;
use super::super::rig::*;
use super::super::{locked, Sync};

pub(in crate::sync) fn confirm_while_landing(sync: &Sync, targets: Vec<(String, RowId)>) -> std::sync::mpsc::Receiver<Vec<Result<RowId, SyncError>>> {
    let (results, received) = std::sync::mpsc::channel();
    let (runtime, waiter) = (tokio::runtime::Handle::current(), sync.clone());
    *locked(&sync.inner.landed_probe) = Some(Box::new(move || {
        let (finished, finish) = std::sync::mpsc::channel();
        for (index, (model, id)) in targets.iter().cloned().enumerate() {
            let (waiter, finished) = (waiter.clone(), finished.clone());
            drop(runtime.spawn(async move { let _ = finished.send((index, waiter.confirmed(&model, &id).await)); }));
        }
        let mut settled: Vec<_> = (0..targets.len()).map(|_| crate::test_wait::recv_within(&finish)).collect();
        settled.sort_by_key(|(index, _)| *index);
        let _ = results.send(settled.into_iter().map(|(_, result)| result).collect());
    }));
    received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiter_woken_by_the_commit_of_a_create_gets_the_real_id() {
    let rig = rig();
    let temp = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap().id.clone();
    let received = confirm_while_landing(&rig.sync, vec![("moments".into(), temp)]);
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})));
    rig.sync.flush().await.unwrap();
    let confirmed = crate::test_wait::recv_within(&received);
    *locked(&rig.sync.inner.landed_probe) = None;
    assert_eq!(confirmed.into_iter().map(|result| result.unwrap().to_string()).collect::<Vec<_>>(), ["m77"]);
}

fn temp_create_reply(id: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": "hello", "userId": null}}))
}

/// F03: the wait of a create belongs to the operation queued with it, also after that operation left the queue and another operation of the same row has a refusal waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_wait_for_a_confirmed_create_ignores_the_refusal_of_another_operation_of_its_row() {
    let rig = rig();
    let temp = rig.db.create("moments", params(json!({"id": "temp-late", "text": "hello"}))).unwrap().id.clone();
    rig.transport.enqueue("CreateMoment", temp_create_reply("m78"));
    rig.sync.flush().await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty(), "the create is still queued");
    let refusal = super::super::SendResult::Rejected(crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "late update refusal"));
    locked(&rig.sync.inner.outcomes).keep(9_999, "moments", "moments:update", vec![temp.clone()], &refusal);
    let confirmed = crate::test_wait::within(rig.sync.confirmed("moments", &temp)).await;
    assert_eq!(confirmed.unwrap().to_string(), "m78");
}

/// F04: a temporary id that two models hold is told apart by the model: the wait of one model never takes the operation of the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_for_a_create_never_takes_the_operation_of_another_model_with_the_same_temporary_id() {
    let mut vibes = vibes_spec();
    vibes.actions = vec![moments_spec().actions.into_iter().find(|action| action.name == "create").unwrap()];
    vibes.actions[0].mutation = Some("StarMoment".into());
    vibes.actions[0].variables.clear();
    let mut moments = moments_spec();
    moments.actions.iter_mut().filter(|action| action.name != "destroy").for_each(|action| action.on_failure = crate::schema::OnFailure::Keep);
    let rig = rig_on(crate::db::Db::open_in_memory(vec![users_spec(), moments, vibes], None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig::default());
    rig.db.create("moments", params(json!({"id": "temp-shared", "text": "a"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("not allowed", "BAD_USER_INPUT"));
    rig.sync.flush().await.unwrap();
    let temp = rig.db.create("vibes", params(json!({"id": "temp-shared", "kind": "b"}))).unwrap().id.clone();
    rig.transport.enqueue("StarMoment", Reply::data(json!({"starMoment": {"id": "v1", "kind": "b"}})));
    let confirmed = crate::test_wait::within(rig.sync.confirmed("vibes", &temp)).await;
    assert_eq!(confirmed.unwrap().to_string(), "v1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_with_an_id_of_the_caller_is_confirmed_with_that_id() {
    let rig = rig();
    let (row, operation) = rig.db.create_queued(None, "moments", params(json!({"id": "known-id", "text": "hello"})), params(json!({}))).unwrap();
    rig.transport.enqueue("CreateMoment", temp_create_reply("known-id"));
    let confirmed = crate::test_wait::within(rig.sync.confirmed_by("moments", &row.id, operation.unwrap())).await;
    assert_eq!(confirmed.unwrap().to_string(), "known-id");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_restored_after_open_is_waited_for_by_its_own_operation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("restored.sqlite");
    let temp = {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig::default());
        rig.db.create("moments", params(json!({"text": "hello"}))).unwrap().id.clone()
    };
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig::default());
    let refusal = super::super::SendResult::Rejected(crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "update refusal"));
    locked(&rig.sync.inner.outcomes).keep(9_999, "moments", "moments:update", vec![temp.clone()], &refusal);
    rig.transport.enqueue("CreateMoment", temp_create_reply("m91"));
    let confirmed = crate::test_wait::within(rig.sync.confirmed("moments", &temp)).await;
    assert_eq!(confirmed.unwrap().to_string(), "m91");
}
