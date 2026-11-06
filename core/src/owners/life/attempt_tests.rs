//! The durable attempt counter of an operation: `StoreOp::Attempt` reaches the column the restore reads.

use serde_json::json;

use crate::db::fixtures::{booted, put};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{EffectRow, IntentEffects, StoreBatch, StoreOp};

fn attempts_of(db: &crate::db::Db, op_id: i64) -> i64 {
    db.shared.store.lock().connection.query_row("SELECT attempts FROM op WHERE seq = ?1", [op_id], |row| row.get(0)).unwrap()
}

/// out_attempt_persisted: the attempt counter written before a `Mutate` is in the base when the app restarts.
#[test]
fn the_attempt_counter_is_stored_with_the_operation() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u0", "text": "t0"}));
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "userId": "u0", "text": "a"}).to_string() }], ..Default::default() };
    db.write_queue_ops(&[StoreOp::Intent { op_id: 1, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects }]).unwrap();
    assert_eq!(attempts_of(&db, 1), 0);
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Attempt { op_id: 1, attempts: 3, cycle_id: 2 }] };
    db.shared.store.write_batch(&batch, &db.shared.files, &db.shared.pointer_guards, None).unwrap();
    assert_eq!(attempts_of(&db, 1), 3);
}
