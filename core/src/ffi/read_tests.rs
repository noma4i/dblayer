//! The values of reading through `Database`: every value is the value of the owner `Deliveries`, the bridge adds the full rows.
//! dlv_open_rows, dlv_live_read_changed (P-DLV-01, P-DLV-14): the first value is a snapshot, the next is a delta with the order of the core.

use std::sync::Arc;
use std::time::Duration;

use super::{Config, Database, ReadEnd, ReadPhase, ReadRequest, ReadStream, ReadValue, TestClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

const LIMIT: Duration = Duration::from_secs(5);

async fn open() -> Arc<Database> {
    let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    note.storage = Storage::Local;
    Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![note]).unwrap(), None, TestClock::new(1_000_000), None, None, None, None).await.unwrap()
}

async fn next(stream: &ReadStream) -> ReadValue { tokio::time::timeout(LIMIT, stream.pull()).await.expect("a value within 5 s").unwrap().expect("a value") }

fn ids(rows: &[super::WireRow]) -> Vec<&str> { rows.iter().map(|row| row.id.as_str()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_first_value_off_caller() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1"}"#.into(), "{}".into()).await.unwrap();
    assert_eq!(db.db.shared.store.published_revision().load(std::sync::atomic::Ordering::Acquire), db.db.revision_seq().unwrap());
    let caller = std::thread::current().id();
    db.db.shared.store.read_threads.lock().unwrap().clear();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let reads = db.db.shared.store.read_threads.lock().unwrap().clone();
    assert!(!reads.contains(&caller), "SQLite reads must run outside the caller");
    assert!(matches!(crate::test_wait::within(stream.pull()).await.unwrap(), Some(ReadValue::Snapshot { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_resync_ends() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let ReadValue::Snapshot { revision, .. } = crate::test_wait::within(stream.pull()).await.unwrap().unwrap() else { panic!("first snapshot") };
    stream.resync();
    let ReadValue::Snapshot { revision: recovered, .. } = crate::test_wait::within(stream.pull()).await.unwrap().unwrap() else { panic!("resync snapshot") };
    assert!(recovered >= revision);
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view() {
    let db = open().await;
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query_json: r#"{"model":"Note"}"#.into() });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Note".into(), id: "n1".into(), fetch: super::ReadFetch::Never });
    for stream in [&list, &count, &entity] { crate::test_wait::within(stream.pull()).await.unwrap().unwrap(); }
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let list_revision = match crate::test_wait::within(list.pull()).await.unwrap().unwrap() { ReadValue::Delta { revision, .. } | ReadValue::Snapshot { revision, .. } => revision, value => panic!("{value:?}") };
    let count_revision = match crate::test_wait::within(count.pull()).await.unwrap().unwrap() { ReadValue::Count { revision, count: 1, .. } => revision, value => panic!("{value:?}") };
    let entity_revision = match crate::test_wait::within(entity.pull()).await.unwrap().unwrap() { ReadValue::Delta { revision, .. } | ReadValue::Snapshot { revision, .. } => revision, value => panic!("{value:?}") };
    assert_eq!((count_revision, entity_revision), (list_revision, list_revision));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_stream_gives_a_snapshot_then_a_delta_with_the_final_order() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let ReadValue::Snapshot { revision, rows, state } = next(&stream).await else { panic!("the first value is a snapshot") };
    assert_eq!(ids(&rows), ["n1"]);
    assert_eq!(state.phase, ReadPhase::Loaded, "a local list has no window to load: it is loaded at once");
    db.create(epoch, "Note".into(), r#"{"id":"n2","text":"two"}"#.into(), "{}".into()).await.unwrap();
    let ReadValue::Delta { base, size, rows, indexes, .. } = next(&stream).await else { panic!("the next value is a delta") };
    assert_eq!((base, size), (revision, 2));
    assert_eq!((ids(&rows), indexes), (vec!["n2"], vec![1]), "only the new row travels, with its place in the final list");
    stream.cancel().await;
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Cancelled }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_cannot_be_read_ends_as_invalid() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Entity { model: "Ghost".into(), id: "g1".into(), fetch: super::ReadFetch::Never });
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Invalid }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delta_after_a_removal_carries_the_rows_of_its_own_snapshot() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let ReadValue::Snapshot { .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    db.destroy(epoch, "Note".into(), "n1".into()).await.unwrap();
    db.create(epoch, "Note".into(), r#"{"id":"n2","text":"two"}"#.into(), "{}".into()).await.unwrap();
    loop {
        match next(&stream).await {
            ReadValue::Delta { size, rows, indexes, .. } => {
                assert_eq!(rows.len(), indexes.len(), "every index has the values of its row");
                if size == 1 { break; }
            }
            ReadValue::Snapshot { rows, .. } if rows.len() == 1 => break,
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_held_across_close_gets_one_ended_then_nothing() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let ReadValue::Snapshot { .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    db.close_database().await.unwrap();
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Closed }));
    assert!(tokio::time::timeout(LIMIT, stream.pull()).await.expect("an answer within 5 s").unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn inv_api_06_an_events_stream_that_cannot_open_its_subscription_reports_the_failure() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Events { model: "Note".into() });
    let value = next(&stream).await;
    assert!(matches!(value, ReadValue::Ended { .. }), "no subscription can open here: the stream ends with its cause instead of waiting: {value:?}");
}
