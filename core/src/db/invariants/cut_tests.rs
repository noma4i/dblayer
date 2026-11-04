use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use crate::db::fixtures::*;
use crate::db::Db;
use crate::syncstate::SyncState;

fn interrupt_at(db: &Db, limit: usize) -> Arc<AtomicUsize> {
    let steps = Arc::new(AtomicUsize::new(0));
    let counter = steps.clone();
    db.shared.store.lock().connection.progress_handler(1, Some(move || counter.fetch_add(1, Ordering::SeqCst) + 1 >= limit)).unwrap();
    steps
}

fn write(db: &Db) -> bool {
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).is_ok()
}

#[test]
fn inv_op_01_a_local_write_cut_at_any_step_leaves_the_row_the_operation_and_row_sync_together() {
    let total = {
        let root = tempfile::tempdir().unwrap();
        let db = Db::open(&root.path().join("1.sqlite"), all_specs(), None).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        let steps = interrupt_at(&db, usize::MAX);
        assert!(write(&db));
        steps.load(Ordering::SeqCst)
    };
    assert!(total > 50, "{total} steps counted");
    let (mut cut, mut written) = (0, 0);
    for limit in (1..=total).step_by((total / 120).max(1)).chain([total + 1]) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("1.sqlite");
        let db = Db::open(&path, all_specs(), None).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        interrupt_at(&db, limit);
        let ok = write(&db);
        db.close().unwrap();
        drop(db);
        let db = Db::open(&path, all_specs(), None).unwrap();
        let landed = db.find("moments", &rid("m1")).unwrap().unwrap().text("text") == Some("two");
        let operations = db.outbox().unwrap();
        let state = db.sync_info("moments", &rid("m1")).unwrap().state;
        let prior = operations.first().is_some_and(|entry| entry.rollback.is_some());
        assert_eq!((landed, operations.len(), prior, state == SyncState::Pending), (landed, usize::from(landed), landed, landed), "limit {limit}: write returned {ok}");
        if landed {
            written += 1;
        } else {
            cut += 1;
        }
    }
    assert!(cut > 0 && written > 0, "{cut} cut, {written} written");
}
