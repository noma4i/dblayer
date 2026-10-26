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
