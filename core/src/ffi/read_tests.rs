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
    Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![note]).unwrap(), None, None, TestClock::new(1_000_000), None, None, None, None).await.unwrap()
}

async fn next(stream: &ReadStream) -> ReadValue { tokio::time::timeout(LIMIT, stream.pull()).await.expect("a value within 5 s").unwrap().expect("a value") }

fn ids(rows: &[super::WireRow]) -> Vec<&str> { rows.iter().map(|row| row.id.as_str()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn a_list_stream_gives_a_snapshot_then_a_delta_with_the_final_order() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query_json: r#"{"model":"Note"}"#.into() });
    let ReadValue::Snapshot { revision, rows, state } = next(&stream).await else { panic!("the first value is a snapshot") };
    assert_eq!(ids(&rows), ["n1"]);
    assert_eq!(state.phase, ReadPhase::Idle, "a local list has no window to load");
    db.create(epoch, "Note".into(), r#"{"id":"n2","text":"two"}"#.into(), "{}".into()).await.unwrap();
    let ReadValue::Delta { base, size, rows, indexes, .. } = next(&stream).await else { panic!("the next value is a delta") };
    assert_eq!((base, size), (revision, 2));
    assert_eq!((ids(&rows), indexes), (vec!["n2"], vec![1]), "only the new row travels, with its place in the final list");
    stream.cancel().await;
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Cancelled }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_cannot_be_read_reports_the_refusal_then_ends_as_invalid() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Entity { model: "Ghost".into(), id: "g1".into(), fetch: super::ReadFetch::Never });
    let refusal = tokio::time::timeout(LIMIT, stream.pull()).await.expect("an answer within 5 s");
    assert!(matches!(refusal, Err(super::DbError::Database { .. })), "{refusal:?}");
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
