use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use crate::identity::RowId;
use crate::transport::{Reply, TransportError};

use super::super::rig::*;

fn update_texts(rig: &Rig) -> Vec<String> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].as_str().unwrap().to_string()).collect()
}

#[test]
fn a_flush_reads_the_queue_once_and_visits_each_candidate_once() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m1", "plain"));
    let count = 8;
    for number in 0..count {
        rig.db.update(&rig.db.admit().unwrap(), "moments", &RowId::new("m1").unwrap(), params(json!({"text": number.to_string()}))).unwrap();
    }
    rig.transport.respond_with(|request| Some(Reply::data(json!({"updateMoment": {"id": request.variables["id"], "text": request.variables["text"], "userId": null}}))));
    let before = rig.db.counters();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    let after = rig.db.counters();
    let scans = after.outbox_reads - before.outbox_reads;
    let rows = after.outbox_rows_loaded - before.outbox_rows_loaded;
    let candidates = rig.sync.counters().outbox_candidates;
    assert!(scans <= 1 && rows <= count * 3 && candidates <= count, "scans={scans}, rows={rows}, candidates={candidates}, operations={count}");
    assert_eq!(update_texts(&rig), (0..count).map(|number| number.to_string()).collect::<Vec<_>>());
}) }

#[test]
fn a_create_that_is_retried_during_a_pass_does_not_let_its_later_updates_overtake_the_earlier_ones() { crate::test_wait::paused(async {
    let rig = rig();
    put(&rig, "moments", moment("m9", "plain"));
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &local.id, params(json!({"text": "one"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &RowId::new("m9").unwrap(), params(json!({"text": "x"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &local.id, params(json!({"text": "two"}))).unwrap();
    let creates = Arc::new(AtomicUsize::new(0));
    let (db, seen) = (rig.db.clone(), creates.clone());
    rig.transport.respond_with(move |request| match request.operation_name.as_str() {
        "CreateMoment" if seen.fetch_add(1, Ordering::SeqCst) == 0 => Some(Reply::fail(TransportError::Interrupted("lost".to_string()))),
        "CreateMoment" => Some(Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}}))),
        "UpdateMoment" => {
            if request.variables["id"] == json!("m9") {
                db.retry_all(&db.admit().unwrap()).unwrap();
            }
            Some(Reply::data(json!({"updateMoment": {"id": request.variables["id"], "text": request.variables["text"], "userId": null}})))
        }
        _ => None,
    });
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(update_texts(&rig), ["x"]);
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(update_texts(&rig), ["x", "one", "two"]);
}) }
