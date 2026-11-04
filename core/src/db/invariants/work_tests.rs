use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::fixtures::*;
use crate::commit::{OperationRecord, OperationTransition};
use crate::operation::OperationStatus;
use crate::db::fixtures::*;
use crate::db::Db;
use crate::upload::{UploadStatus, UploadState};

fn with_open_operations(count: usize) -> (Db, i64) {
    let db = booted();
    for turn in 0..count {
        put(&db, "moments", json!({"id": format!("x{turn}"), "userId": "u1", "text": "one"}));
        db.update(&db.admit().unwrap(), "moments", &rid(&format!("x{turn}")), obj(json!({"text": "two"}))).unwrap();
    }
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let record = OperationRecord {
        seq: None,
        action_key: "moments:upload:upload".to_string(),
        status: OperationStatus::Pending,
        row_ids: vec![rid("m1")],
        temp_id: None,
        input: json!({}),
        transport_started: false,
        base_revision: None,
        created_at: 0,
        dedupe_key: None,
        rollback: None,
        upload: Some(UploadState::queued()),
    };
    apply(&db, OperationTransition::Begin(Box::new(record)));
    let seq = db.outbox().unwrap().iter().find(|entry| entry.operation.upload.is_some()).unwrap().operation.id.0;
    (db, seq)
}

fn steps_of_one_tick(open: usize) -> usize {
    let (db, seq) = with_open_operations(open);
    let steps = Arc::new(AtomicUsize::new(0));
    let counter = steps.clone();
    db.shared.store.lock().connection.progress_handler(1, Some(move || counter.fetch_add(1, Ordering::SeqCst) == usize::MAX)).unwrap();
    let state = UploadState { phase: UploadStatus::Uploading, sent_bytes: 5, total_bytes: 10, ..UploadState::queued() };
    apply(&db, OperationTransition::Progress { seq, item_position: 0, state: Box::new(state) });
    steps.load(Ordering::SeqCst)
}

#[test]
fn inv_sync_05_the_work_of_a_progress_tick_does_not_grow_with_the_number_of_open_operations() {
    let (few, many) = (steps_of_one_tick(5), steps_of_one_tick(400));
    assert!(many < few * 3, "one tick took {few} steps beside 5 open operations and {many} beside 400");
}
