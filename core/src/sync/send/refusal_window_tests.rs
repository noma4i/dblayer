use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::Receiver;

use serde_json::json;

use crate::identity::RowId;
use crate::transport::Reply;

use super::super::error::SyncError;
use super::super::rig::*;
use super::super::{locked, Sync};
use super::confirmed_tests::confirm_while_landing;

type Asked = Pin<Box<dyn Future<Output = Result<(), SyncError>> + Send>>;

fn ask_when_refused(sync: &Sync, ask: impl Fn(Sync) -> Asked + Send + std::marker::Sync + 'static) -> Receiver<Result<(), SyncError>> {
    let (results, received) = std::sync::mpsc::channel();
    let (runtime, waiter) = (tokio::runtime::Handle::current(), sync.clone());
    *locked(&sync.inner.refused_probe) = Some(Box::new(move || {
        let (finished, finish) = std::sync::mpsc::channel();
        let asked = ask(waiter.clone());
        drop(runtime.spawn(async move { let _ = finished.send(asked.await); }));
        let _ = results.send(crate::test_wait::recv_within(&finish));
    }));
    received
}

async fn flush_and_read(rig: &Rig, received: &Receiver<Result<(), SyncError>>) -> Result<(), SyncError> {
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    let asked = crate::test_wait::recv_within(received);
    *locked(&rig.sync.inner.refused_probe) = None;
    asked
}

fn settled_of(id: RowId) -> impl Fn(Sync) -> Asked + Send + std::marker::Sync + 'static {
    move |sync| {
        let id = id.clone();
        Box::pin(async move { sync.settled("moments", &id, None).await })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_called_right_after_a_rolled_back_action_leaves_the_queue_is_the_refusal() {
    let rig = rig();
    let id = rig.db.as_app().create("moments", params(json!({"text": "hello"}))).unwrap().id.clone();
    let received = ask_when_refused(&rig.sync, settled_of(id));
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    let asked = flush_and_read(&rig, &received).await;
    assert!(matches!(&asked, Err(SyncError::Graphql { messages, .. }) if messages == &["not allowed".to_string()]), "{asked:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_called_right_after_a_refused_action_stays_failed_is_the_refusal() {
    let rig = keeping_rig();
    let id = rig.db.as_app().create("moments", params(json!({"text": "hello"}))).unwrap().id.clone();
    let received = ask_when_refused(&rig.sync, settled_of(id));
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    let asked = flush_and_read(&rig, &received).await;
    assert!(matches!(&asked, Err(SyncError::Graphql { messages, .. }) if messages == &["not allowed".to_string()]), "{asked:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answer_called_right_after_a_rolled_back_action_leaves_the_queue_is_the_refusal() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let acted = rig.db.as_app().act("moments", "star", &RowId::new("m1").unwrap(), params(json!({})), params(json!({}))).unwrap();
    let received = ask_when_refused(&rig.sync, move |sync| Box::pin(async move { sync.answer("moments", acted.operation).await.map(|_| ()) }));
    rig.transport.enqueue("StarMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    let asked = flush_and_read(&rig, &received).await;
    assert!(matches!(&asked, Err(SyncError::Graphql { messages, .. }) if messages == &["not allowed".to_string()]), "{asked:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_action_that_lands_does_not_settle_with_its_earlier_refusal() {
    let rig = keeping_rig();
    let temp = rig.db.as_app().create("moments", params(json!({"text": "hello"}))).unwrap().id.clone();
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("not allowed", "BAD_USER_INPUT"));
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})));
    let received = confirm_while_landing(&rig.sync, vec![("moments".into(), temp.clone())]);
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    assert_eq!(crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &temp, None)).await.unwrap().committed(), 1);
    let confirmed = crate::test_wait::recv_within(&received);
    *locked(&rig.sync.inner.landed_probe) = None;
    assert_eq!(confirmed.into_iter().map(|result| result.unwrap().to_string()).collect::<Vec<_>>(), ["m77"]);
}
