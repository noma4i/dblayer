//! boundary_reader_orders, boundary_reader_early_exit (D-175, P-BND-07): one reader per stream, and a reader that leaves ends the interest while the handle is held.

use std::sync::Arc;

use super::{Config, Database, EndCause, ReadClaim, ReadRequest, ReadValue, TestClock};
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

fn claimed(value: ReadClaim) -> u64 { let ReadClaim::Claimed { reader } = value else { panic!("the reader claim ended: {value:?}") }; reader }

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_orders() {
    let db = open().await;
    let stream = notes(&db);
    let first = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.claim_reader()).await, ReadClaim::Ended { cause: EndCause::Invalid { .. } }));
    stream.release_reader(first + 1);
    assert!(matches!(within(stream.pull(first)).await, Some(ReadValue::Snapshot { .. })));
    let mut pending = Box::pin(stream.pull(first));
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx).is_pending())).await, "a release of another reader leaves the stream open");
    drop(pending);
    within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_early_exit() {
    let db = open().await;
    let stream = notes(&db);
    let reader = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.pull(reader)).await, Some(ReadValue::Snapshot { .. })));
    stream.release_reader(reader);
    assert!(matches!(within(stream.pull(reader)).await, Some(ReadValue::Ended { cause: EndCause::Closed })));
    assert!(matches!(within(stream.pull(reader)).await, Some(ReadValue::Ended { cause: EndCause::Closed })));
    let again = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.pull(again)).await, Some(ReadValue::Ended { cause: EndCause::Closed })));
    within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_orders_a_pull_of_another_reader_is_invalid_and_leaves_the_stream_to_its_reader() {
    let db = open().await;
    let stream = notes(&db);
    let first = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.pull(first + 1)).await, Some(ReadValue::Ended { cause: EndCause::Invalid { .. } })));
    assert!(matches!(within(stream.pull(first)).await, Some(ReadValue::Snapshot { .. })));
    within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_reader_orders_a_claim_after_the_stream_ended_and_left_gets_ended_not_invalid() {
    let db = open().await;
    let stream = notes(&db);
    within(db.session_reset()).await.unwrap();
    within(stream.cancel()).await;
    let reader = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.pull(reader)).await, Some(ReadValue::Ended { .. })));
    within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_released_reader_after_two_database_lives() {
    let first = open().await;
    within(first.close_database()).await.unwrap();
    let db = open().await;
    let stream = notes(&db);
    let reader = claimed(within(stream.claim_reader()).await);
    assert!(matches!(within(stream.pull(reader)).await, Some(ReadValue::Snapshot { .. })));
    stream.release_reader(reader);
    within(crate::ffi::settled(&db)).await.unwrap();
    let ended = within(stream.pull(reader)).await;
    assert!(matches!(ended, Some(ReadValue::Ended { cause: EndCause::Closed })), "released reader: {ended:?}");
    within(db.close_database()).await.unwrap();
}
