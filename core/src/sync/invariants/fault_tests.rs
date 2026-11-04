use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use super::spec_fixtures::*;

const MOMENT_ROW: &str = "m77";

fn prepare(path: &Path) -> Rig {
    let rig = open_rig(path);
    let parent = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "parent"}))).unwrap();
    rig.db.create(&rig.db.admit().unwrap(), "vibes", params(json!({"id": "v1", "momentId": parent.id.as_str()}))).unwrap();
    rig.db.attach(&rig.db.admit().unwrap(), "moments", &parent.id, "media", b"bytes", "a.bin", "a/b").unwrap();
    rig.transport.enqueue("CreateMoment", created(MOMENT_ROW, "parent"));
    rig
}

fn interrupt_at(rig: &Rig, limit: usize) -> Arc<AtomicUsize> {
    let steps = Arc::new(AtomicUsize::new(0));
    let counter = steps.clone();
    let inner = rig.db.shared.store.lock();
    inner.connection.progress_handler(1, Some(move || counter.fetch_add(1, Ordering::SeqCst) + 1 >= limit)).unwrap();
    steps
}

fn check_all_or_nothing(path: &Path, limit: usize) -> bool {
    let rig = open_rig(path);
    let confirmed = rig.db.find("moments", &rid(MOMENT_ROW)).unwrap().is_some();
    let open_create = rig.db.outbox().unwrap().iter().any(|entry| entry.operation.action_key == "moments:create");
    let temp_rows = rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().filter(|row| row.id.is_temp()).count();
    assert_ne!(confirmed, open_create, "limit {limit}: confirmed {confirmed}, create open {open_create}");
    if confirmed {
        assert_eq!(temp_rows, 0, "limit {limit}");
        assert_eq!(mentions_in_ops(&rig, "temp-"), Vec::<String>::new(), "limit {limit}");
        assert_eq!(rig.db.find("vibes", &rid("v1")).unwrap().unwrap().get("momentId"), Some(&json!(MOMENT_ROW)), "limit {limit}");
        assert_eq!(rig.db.read("moments", &rid(MOMENT_ROW), "media").unwrap(), Some(b"bytes".to_vec()), "limit {limit}");
    } else {
        assert_eq!(temp_rows, 1, "limit {limit}");
        assert_eq!(rig.db.find("vibes", &rid("v1")).unwrap().unwrap().get("momentId").and_then(|id| id.as_str()).map(|id| id.starts_with("temp-")), Some(true), "limit {limit}");
        let state = rig.db.query(&crate::query::Query::new("moments")).unwrap()[0].sync.state;
        assert!(matches!(state, crate::syncstate::SyncState::Pending | crate::syncstate::SyncState::DeliveryUnknown), "limit {limit}: {state:?}");
    }
    confirmed
}

fn mentions_in_ops(rig: &Rig, needle: &str) -> Vec<String> {
    let rows = rig.db.outbox().unwrap();
    rows.iter()
        .filter(|entry| format!("{} {:?} {:?}", entry.input, entry.rollback, entry.operation.row_ids).contains(needle))
        .map(|entry| entry.operation.action_key.clone())
        .collect()
}

#[tokio::test(start_paused = true)]
async fn inv_op_16_inv_row_01_a_failure_at_any_point_of_the_confirmation_leaves_all_or_nothing() {
    let (flushed, total) = {
        let root = tempfile::tempdir().unwrap();
        let rig = prepare(&root.path().join("1.sqlite"));
        let steps = interrupt_at(&rig, usize::MAX);
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        let flushed = steps.load(Ordering::SeqCst);
        rig.sync.wait_uploads().await;
        (flushed, steps.load(Ordering::SeqCst))
    };
    assert!(total > 100, "{total} steps counted");
    let (mut faulted, mut confirmed, mut unconfirmed) = (0, 0, 0);
    let tail = flushed.saturating_sub(16).max(1);
    let limits: std::collections::BTreeSet<usize> = (1..tail).step_by((total / 150).max(1)).chain(tail..=total).collect();
    for limit in limits {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("1.sqlite");
        let rig = prepare(&path);
        interrupt_at(&rig, limit);
        faulted += usize::from(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.is_err());
        rig.sync.wait_uploads().await;
        drop(rig);
        match check_all_or_nothing(&path, limit) {
            true => confirmed += 1,
            false => unconfirmed += 1,
        }
    }
    assert!(faulted > 20, "only {faulted} runs hit a failure");
    assert!(confirmed > 0 && unconfirmed > 0, "{confirmed} confirmed, {unconfirmed} unconfirmed, {faulted} faulted, flush ended at step {flushed} of {total}");
}
