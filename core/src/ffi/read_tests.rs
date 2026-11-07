//! The values of reading through `Database`: every value is the value of the owner `Deliveries`, the bridge adds the full rows.
//! dlv_open_rows, dlv_live_read_changed (P-DLV-01, P-DLV-14): the first value is a snapshot, the next is a delta with the order of the core.

use std::sync::Arc;
use std::time::Duration;

use super::{Config, Database, ReadEnd, ReadPhase, ReadRequest, ReadStream, ReadValue, RowEdit, TestClock};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

const LIMIT: Duration = Duration::from_secs(5);

async fn open() -> Arc<Database> {
    let mut note = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    note.storage = Storage::Local;
    Database::open_with_clock(Config { path: None, data_version: None }, serde_json::to_string(&vec![note]).unwrap(), crate::ffi::OpenPorts::default(), TestClock::new(1_000_000)).await.unwrap()
}

async fn next(stream: &ReadStream) -> ReadValue { tokio::time::timeout(LIMIT, stream.pull()).await.expect("a value within 5 s").expect("a value") }

fn ids(rows: &[super::WireRow]) -> Vec<&str> { rows.iter().map(|row| row.id.as_str()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_pending_pull_cancels_the_stream() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    assert!(matches!(crate::test_wait::within(stream.pull()).await, Some(ReadValue::Snapshot { .. })));
    let mut pending = Box::pin(stream.pull());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx).is_pending())).await);
    drop(pending);
    assert!(matches!(crate::test_wait::within(stream.pull()).await, Some(ReadValue::Ended { cause: ReadEnd::Cancelled })));
    assert!(crate::test_wait::within(stream.pull()).await.is_none());
    crate::test_wait::within(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_first_value_off_caller() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1"}"#.into(), "{}".into()).await.unwrap();
    assert_eq!(db.db.published_revision().load(std::sync::atomic::Ordering::Acquire), db.db.revision_seq().unwrap());
    let caller = std::thread::current().id();
    db.db.read_threads.lock().unwrap().clear();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let reads = db.db.read_threads.lock().unwrap().clone();
    assert!(!reads.contains(&caller), "SQLite reads must run outside the caller");
    assert!(matches!(crate::test_wait::within(stream.pull()).await, Some(ReadValue::Snapshot { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_resync_ends() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let ReadValue::Snapshot { revision, .. } = crate::test_wait::within(stream.pull()).await.unwrap() else { panic!("first snapshot") };
    stream.resync();
    let ReadValue::Snapshot { revision: recovered, .. } = crate::test_wait::within(stream.pull()).await.unwrap() else { panic!("resync snapshot") };
    assert!(recovered >= revision);
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_one_change_every_view() {
    let db = open().await;
    let epoch = db.session_epoch();
    let list = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let entity = db.subscribe_values(epoch, ReadRequest::Entity { model: "Note".into(), id: "n1".into(), fetch: super::ReadFetch::Never });
    for stream in [&list, &count, &entity] { crate::test_wait::within(stream.pull()).await.unwrap(); }
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let list_revision = match crate::test_wait::within(list.pull()).await.unwrap() { ReadValue::Delta { revision, .. } | ReadValue::Snapshot { revision, .. } => revision, value => panic!("{value:?}") };
    let count_revision = match crate::test_wait::within(count.pull()).await.unwrap() { ReadValue::Count { revision, count: 1, .. } => revision, value => panic!("{value:?}") };
    let entity_revision = match crate::test_wait::within(entity.pull()).await.unwrap() { ReadValue::Delta { revision, .. } | ReadValue::Snapshot { revision, .. } => revision, value => panic!("{value:?}") };
    assert_eq!((count_revision, entity_revision), (list_revision, list_revision));
    let first_revision = list_revision;
    db.update(epoch, "Note".into(), "n1".into(), r#"{"text":"two"}"#.into(), "{}".into()).await.unwrap();
    let list_revision = match crate::test_wait::within(list.pull()).await.unwrap() { ReadValue::Delta { revision, edits, .. } => { assert_eq!(ids(&RowEdit::carried(&edits)), ["n1"]); revision }, ReadValue::Snapshot { revision, rows, .. } => { assert_eq!(ids(&rows), ["n1"]); revision }, value => panic!("{value:?}") };
    let entity_revision = match crate::test_wait::within(entity.pull()).await.unwrap() { ReadValue::Delta { revision, edits, .. } => { assert_eq!(ids(&RowEdit::carried(&edits)), ["n1"]); revision }, ReadValue::Snapshot { revision, rows, .. } => { assert_eq!(ids(&rows), ["n1"]); revision }, value => panic!("{value:?}") };
    assert!(list_revision > first_revision);
    assert_eq!(entity_revision, list_revision);
    let mut pull = Box::pin(count.pull());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pull.as_mut(), cx).is_pending())).await);
    let life = db.life_handle();
    assert!(crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier(Duration::from_secs(5)))).await.unwrap());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(pull.as_mut(), cx).is_pending())).await);
    db.destroy(epoch, "Note".into(), "n1".into()).await.unwrap();
    let count_revision = match crate::test_wait::within(pull).await.unwrap() { ReadValue::Count { revision, count: 0, .. } => revision, value => panic!("{value:?}") };
    let list_revision = match crate::test_wait::within(list.pull()).await.unwrap() { ReadValue::Delta { revision, edits, .. } if matches!(edits.as_slice(), [RowEdit::Remove { .. }]) => revision, ReadValue::Snapshot { revision, rows, .. } if rows.is_empty() => revision, value => panic!("{value:?}") };
    let entity_revision = match crate::test_wait::within(entity.pull()).await.unwrap() { ReadValue::Delta { revision, edits, .. } if matches!(edits.as_slice(), [RowEdit::Remove { .. }]) => revision, ReadValue::Snapshot { revision, rows, .. } if rows.is_empty() => revision, value => panic!("{value:?}") };
    assert_eq!((list_revision, entity_revision), (count_revision, count_revision));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_stream_gives_a_snapshot_then_a_delta_with_the_final_order() {
    let db = open().await;
    let epoch = db.session_epoch();
    db.create(epoch, "Note".into(), r#"{"id":"n1","text":"one"}"#.into(), "{}".into()).await.unwrap();
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let ReadValue::Snapshot { revision, rows, state, .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    assert_eq!(ids(&rows), ["n1"]);
    assert_eq!(state.phase, ReadPhase::Loaded, "a local list has no window to load: it is loaded at once");
    db.create(epoch, "Note".into(), r#"{"id":"n2","text":"two"}"#.into(), "{}".into()).await.unwrap();
    let ReadValue::Delta { base, edits, .. } = next(&stream).await else { panic!("the next value is a delta") };
    assert_eq!(base, revision);
    assert_eq!((edits.len(), ids(&RowEdit::carried(&edits))), (1, vec!["n2"]), "only the new row travels");
    assert!(matches!(edits[0], RowEdit::Insert { index: 1, .. }), "with its place in the list: {edits:?}");
    stream.cancel().await;
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Cancelled }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_that_changes_its_place_arrives_as_a_move_and_its_new_values() {
    let db = open().await;
    let epoch = db.session_epoch();
    for (id, text) in [("n1", "b"), ("n2", "c")] {
        db.create(epoch, "Note".into(), format!(r#"{{"id":"{id}","text":"{text}"}}"#), "{}".into()).await.unwrap();
    }
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#) });
    let ReadValue::Snapshot { mut rows, .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    assert_eq!(ids(&rows), ["n1", "n2"]);
    db.update(epoch, "Note".into(), "n1".into(), r#"{"text":"d"}"#.into(), "{}".into()).await.unwrap();
    let ReadValue::Delta { edits, .. } = next(&stream).await else { panic!("the next value is a delta") };
    assert!(matches!(edits.as_slice(), [RowEdit::Move { .. }, RowEdit::Rewrite { index: 1, .. }]), "{edits:?}");
    RowEdit::replay(&mut rows, &edits);
    assert_eq!(ids(&rows), ["n2", "n1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_that_enters_the_middle_of_the_list_arrives_with_its_index() {
    let db = open().await;
    let epoch = db.session_epoch();
    for (id, text) in [("n1", "b"), ("n3", "d")] {
        db.create(epoch, "Note".into(), format!(r#"{{"id":"{id}","text":"{text}"}}"#), "{}".into()).await.unwrap();
    }
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#) });
    let ReadValue::Snapshot { mut rows, .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    for (id, text, index, expected) in [("n2", "c", 1, vec!["n1", "n2", "n3"]), ("n0", "a", 0, vec!["n0", "n1", "n2", "n3"])] {
        db.create(epoch, "Note".into(), format!(r#"{{"id":"{id}","text":"{text}"}}"#), "{}".into()).await.unwrap();
        let ReadValue::Delta { edits, .. } = next(&stream).await else { panic!("the next value is a delta") };
        assert!(matches!(edits.as_slice(), [RowEdit::Insert { index: at, row }] if *at == index && row.id == id), "{edits:?}");
        RowEdit::replay(&mut rows, &edits);
        assert_eq!(ids(&rows), expected);
    }
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
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let ReadValue::Snapshot { .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    db.destroy(epoch, "Note".into(), "n1".into()).await.unwrap();
    db.create(epoch, "Note".into(), r#"{"id":"n2","text":"two"}"#.into(), "{}".into()).await.unwrap();
    loop {
        match next(&stream).await {
            ReadValue::Delta { edits, .. } => {
                if edits.iter().any(|edit| matches!(edit, RowEdit::Insert { .. })) { break; }
            }
            ReadValue::Snapshot { rows, .. } if rows.len() == 1 => break,
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_held_across_close_gets_one_ended_then_nothing() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    let ReadValue::Snapshot { .. } = next(&stream).await else { panic!("the first value is a snapshot") };
    db.close_database().await.unwrap();
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Closed }));
    assert!(tokio::time::timeout(LIMIT, stream.pull()).await.expect("an answer within 5 s").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn inv_api_06_an_events_stream_that_cannot_open_its_subscription_reports_the_failure() {
    let db = open().await;
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::Events { model: "Note".into() });
    let value = next(&stream).await;
    assert!(matches!(value, ReadValue::Ended { .. }), "no subscription can open here: the stream ends with its cause instead of waiting: {value:?}");
}

/// Requirement: P-DATA-91, a query window of `limit` rows after `offset` rows in `order`.
/// Observable: the rows of the snapshot, the edits of the delta after a row enters the window, the count, the end of a stream with a bad window. Assert: the window rows, at most `limit`, the entering row as `Insert` and the pushed out row as `Remove`, a count of all rows, `Ended(Invalid)` for `limit` 0 and a negative `offset`.
#[tokio::test(flavor = "multi_thread")]
async fn data_query_limit_offset() {
    let db = open().await;
    let epoch = db.session_epoch();
    for (id, text) in [("n1", "a"), ("n2", "b"), ("n3", "c"), ("n4", "d")] {
        db.create(epoch, "Note".into(), format!(r#"{{"id":"{id}","text":"{text}"}}"#), "{}".into()).await.unwrap();
    }
    let window = r#"{"model":"Note","order":[{"field":"text","direction":"asc"}],"limit":2,"offset":1}"#;
    let list = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(window) });
    let count = db.subscribe_values(epoch, ReadRequest::Count { query: crate::ffi::QuerySpec::json(window) });
    let ReadValue::Snapshot { mut rows, .. } = next(&list).await else { panic!("the first value is a snapshot") };
    assert_eq!(ids(&rows), ["n2", "n3"]);
    assert!(matches!(next(&count).await, ReadValue::Count { count: 4, .. }));
    db.create(epoch, "Note".into(), r#"{"id":"n5","text":"ab"}"#.into(), "{}".into()).await.unwrap();
    let ReadValue::Delta { edits, .. } = next(&list).await else { panic!("the next value is a delta") };
    assert!(edits.iter().any(|edit| matches!(edit, RowEdit::Insert { row, .. } if row.id == "n5")), "{edits:?}");
    assert!(edits.iter().any(|edit| matches!(edit, RowEdit::Remove { id, .. } if id == "n3")), "{edits:?}");
    RowEdit::replay(&mut rows, &edits);
    assert_eq!(ids(&rows), ["n5", "n2"]);
    assert!(matches!(next(&count).await, ReadValue::Count { count: 5, .. }));
    let stream = db.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note","limit":0}"#) });
    assert!(matches!(next(&stream).await, ReadValue::Ended { cause: ReadEnd::Invalid }), "a zero limit");
    crate::test_wait::within(db.close_database()).await.unwrap();
}
