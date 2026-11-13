use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::trace::{TraceEvent, TraceEventCodes};
use serde_json::json;

use crate::store::EntityOp;
use super::fixtures::*;

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
    store.commit(envelope(messages(50, "one"), vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count_old_row_reads));
    OLD_ROW_READS.store(0, Ordering::SeqCst);
    let result = store.commit(envelope(messages(50, "two"), vec![])).unwrap();
    store.lock().connection.trace_v2(TraceEventCodes::empty(), None);
    assert_eq!((result.rows.len(), OLD_ROW_READS.load(Ordering::SeqCst)), (50, 1));
}
