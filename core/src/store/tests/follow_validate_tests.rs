use std::sync::Arc;

use serde_json::json;

use crate::owners::contract::BatchAuthor;
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};
use crate::schema::RemovedBy;
use crate::store::fixtures::*;
use crate::store::{DestroyReason, Store, StoreError};

fn open(removed_by: bool) -> Arc<Store> {
    let mut specs = all_specs();
    if removed_by {
        let vibes = specs.iter_mut().find(|spec| spec.key.as_str() == "vibes").unwrap();
        vibes.removed_by.push(RemovedBy { model: "moments".into(), matches: [("momentId".to_string(), "id".to_string())].into() });
    }
    Arc::new(Store::open_models(None, specs, None, Arc::new(crate::runtime::SystemClock)).unwrap())
}

fn snapshot(store: &Store) -> (i64, i64, i64, i64) {
    (store.revision_seq().unwrap(), count(store, "SELECT COUNT(*) FROM row_chats"), count(store, "SELECT COUNT(*) FROM row_messages"), count(store, "SELECT COUNT(*) FROM row_vibes"))
}

fn nodes_batch(store: &Store, roots: &[&str]) -> StoreBatch {
    let key = WindowKey::nodes("moments", roots);
    let roots = roots.iter().map(|id| RowValue { row_id: (*id).into(), values: json!({"id": id, "userId": "u1", "text": "served"}).to_string() }).collect();
    let page = StoreOp::Window { landed_at: 0, key: key.name.clone(), model: key.model.clone(), page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: store.revision_seq().unwrap() };
    StoreBatch { author: BatchAuthor::Window, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![page] }
}

fn check(failures: &mut Vec<String>, case: &str, outcome: Result<(), StoreError>, before: (i64, i64, i64, i64), after: (i64, i64, i64, i64)) {
    if !matches!(outcome, Err(StoreError::Plan(_))) { failures.push(format!("{case}: expected Plan fault, got {outcome:?}")); }
    if before != after { failures.push(format!("{case}: database changed {before:?} -> {after:?}")); }
}

#[test]
fn store_follow_ops_are_validated() {
    let mut failures = Vec::new();

    let cascade = open(false);
    put(&cascade, "chats", json!({"id": "c1", "title": "t"}));
    put(&cascade, "messages", message("m1", "c1"));
    let before = snapshot(&cascade);
    let plan = envelope(&cascade, vec![upsert("messages", message("m1", "c1")), destroy("chats", "c1", DestroyReason::Local)], vec![]);
    let outcome = cascade.commit(plan).map(|_| ());
    check(&mut failures, "cascade", outcome, before, snapshot(&cascade));

    let removed = open(true);
    put(&removed, "vibes", json!({"id": "v1", "momentId": "m9", "kind": "k"}));
    let before = snapshot(&removed);
    let plan = envelope(&removed, vec![upsert("moments", json!({"id": "m9", "userId": "u1"})), upsert("vibes", json!({"id": "v1", "momentId": "m9", "kind": "k2"}))], vec![]);
    let outcome = removed.commit(plan).map(|_| ());
    check(&mut failures, "removed_by", outcome, before, snapshot(&removed));

    let nodes = open(false);
    let before = snapshot(&nodes);
    let batch = nodes_batch(&nodes, &["m1", "m1"]);
    let outcome = nodes.write_batch(&batch, nodes.files(), nodes.pointer_guards(), None).map(|_| ());
    check(&mut failures, "nodes window", outcome, before, snapshot(&nodes));

    assert!(failures.is_empty(), "{failures:#?}");
}
