use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::trace::{TraceEvent, TraceEventCodes};
use serde_json::json;

use crate::commit::EntityOp;
use super::fixtures::*;
use super::*;

static OLD_ROW_READS: AtomicUsize = AtomicUsize::new(0);

fn count_old_row_reads(event: TraceEvent<'_>) {
    if let TraceEvent::Stmt(_, sql) = event {
        if sql.starts_with("SELECT") && sql.contains("FROM row_messages WHERE id IN") {
            OLD_ROW_READS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn messages(count: usize, body: &str) -> Vec<EntityOp> {
    (0..count).map(|n| upsert("messages", json!({"id": format!("m{n}"), "chatId": "c1", "body": body}))).collect()
}

#[test]
fn an_envelope_reads_the_old_rows_of_all_its_upserts_in_one_statement() {
    let (store, _) = booted_store();
    store.commit(envelope(&store, messages(50, "one"), vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_old_row_reads));
    OLD_ROW_READS.store(0, Ordering::SeqCst);
    let result = store.commit(envelope(&store, messages(50, "two"), vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert_eq!((result.rows.len(), OLD_ROW_READS.load(Ordering::SeqCst)), (50, 1));
}

static NODE_WRITES: AtomicUsize = AtomicUsize::new(0);
static NOTE_WRITES: AtomicUsize = AtomicUsize::new(0);
static LIST_WRITES: AtomicUsize = AtomicUsize::new(0);

fn count_note_writes(event: TraceEvent<'_>) {
    if let TraceEvent::Stmt(_, sql) = event {
        if sql.starts_with("INSERT INTO row_notes__tags") {
            NOTE_WRITES.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn count_child_writes(event: TraceEvent<'_>) {
    if let TraceEvent::Stmt(_, sql) = event {
        if sql.starts_with("INSERT INTO row_docs__value") {
            NODE_WRITES.fetch_add(1, Ordering::SeqCst);
        }
        if sql.starts_with("INSERT INTO row_docs__tags") {
            LIST_WRITES.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn one_row_writes_its_free_value_nodes_and_its_list_items_in_one_statement_each() {
    use crate::schema::{FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let spec = ModelSpec::new("docs", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::raw("data").nullable(), FieldSpec::list("tags", ScalarKind::Str)]);
    let store = Store::open_in_memory().unwrap();
    store.boot(&[spec], None).unwrap();
    store.mark_ready();
    store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_child_writes));
    let data = json!({"a": 1, "b": [true, null, "x"], "c": {"d": 2.5, "e": "y"}});
    store.commit(envelope(&store, vec![upsert("docs", json!({"id": "d1", "data": data, "tags": ["p", "q", "r", "s", "t"]}))], vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert_eq!((NODE_WRITES.load(Ordering::SeqCst), LIST_WRITES.load(Ordering::SeqCst)), (1, 1));
    assert_eq!(store.point("docs", "d1").unwrap().unwrap()["data"], data);
}

#[test]
fn a_long_list_is_written_in_chunks_and_reads_back_in_order() {
    use crate::schema::{FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let spec = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::raw("data").nullable(), FieldSpec::list("tags", ScalarKind::Str)]);
    let store = Store::open_in_memory().unwrap();
    store.boot(&[spec], None).unwrap();
    store.mark_ready();
    NOTE_WRITES.store(0, Ordering::SeqCst);
    store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_note_writes));
    let tags: Vec<String> = (0..450).map(|n| format!("t{n}")).collect();
    store.commit(envelope(&store, vec![upsert("notes", json!({"id": "d1", "tags": tags}))], vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert_eq!(NOTE_WRITES.load(Ordering::SeqCst), 3);
    assert_eq!(store.point("notes", "d1").unwrap().unwrap()["tags"], json!(tags));
}
