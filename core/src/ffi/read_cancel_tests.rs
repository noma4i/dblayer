//! dlv_wait_pull, D-54: `cancel` of a stream ends the `pull` that waits on it, and both return.

use std::sync::Arc;

use super::{Config, Database, ReadEnd, ReadRequest, ReadValue, TestClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::test_wait::within;
use crate::value::ScalarKind;

async fn open() -> Arc<Database> {
    let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    note.storage = Storage::Local;
    Database::open_with_clock(Config { path: None, data_version: None }, serde_json::to_string(&vec![note]).unwrap(), crate::ffi::OpenPorts::default(), TestClock::new(1_000_000)).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_from_another_task_ends_the_pull_that_waits() {
    let db = open().await;
    let stream = Arc::new(db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) }));
    assert!(matches!(within(stream.pull()).await, Some(ReadValue::Snapshot { .. })));
    let mut pending = Box::pin(stream.pull());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx).is_pending())).await);

    let canceller = tokio::spawn({
        let stream = stream.clone();
        async move { stream.cancel().await }
    });

    assert!(matches!(within(pending).await, Some(ReadValue::Ended { cause: ReadEnd::Cancelled })));
    within(canceller).await.unwrap();
    assert!(within(stream.pull()).await.is_none());
    within(db.close_database()).await.unwrap();
}
