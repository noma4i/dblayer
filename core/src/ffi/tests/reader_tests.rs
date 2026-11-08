//! boundary_reader_orders, boundary_reader_early_exit (D-175, P-BND-07): one reader per stream, and a reader that leaves ends the interest while the handle is held.

use std::sync::Arc;

use super::{Config, Database, DbError, ReadEnd, ReadRequest, ReadValue, TestClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::test_wait::within;
use crate::value::ScalarKind;

async fn open() -> Arc<Database> {
    let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    note.storage = Storage::Local;
    Database::open_with_clock(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![note]).unwrap()), crate::ffi::OpenPorts::default(), TestClock::new(1_000_000)).await.unwrap()
}

fn notes(db: &Database) -> Arc<super::ReadStream> {
    db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) })
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_orders_the_second_reader_is_invalid_and_the_first_keeps_its_stream() {
    let db = open().await;
    let stream = notes(&db);
    let first = within(stream.claim_reader()).await.unwrap();
    assert!(matches!(within(stream.claim_reader()).await, Err(DbError::Invalid { .. })));
    stream.release_reader(first + 1);
    assert!(matches!(within(stream.pull()).await, Some(ReadValue::Snapshot { .. })));
    let mut pending = Box::pin(stream.pull());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx).is_pending())).await, "a release of another reader leaves the stream open");
    drop(pending);
    within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_early_exit_a_reader_that_leaves_ends_the_stream_while_the_handle_is_held() {
    let db = open().await;
    let stream = notes(&db);
    let reader = within(stream.claim_reader()).await.unwrap();
    assert!(matches!(within(stream.pull()).await, Some(ReadValue::Snapshot { .. })));
    stream.release_reader(reader);
    assert!(matches!(within(stream.pull()).await, Some(ReadValue::Ended { cause: ReadEnd::Closed })));
    assert!(within(stream.pull()).await.is_none());
    assert!(matches!(within(stream.claim_reader()).await, Err(DbError::Invalid { .. })));
    within(db.close_database()).await.unwrap();
}
