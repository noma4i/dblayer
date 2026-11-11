//! The durable attempt counter of an operation: `StoreOp::Attempt` reaches the column the restore reads.

use serde_json::json;

use crate::store::Store;

use crate::store::fixtures::{booted, put};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{EffectRow, IntentEffects, StoreBatch, StoreOp};

fn attempts_of(db: &Store, op_id: i64) -> i64 {
    db.lock().connection.query_row("SELECT attempts FROM op WHERE seq = ?1", [op_id], |row| row.get(0)).unwrap()
}

/// out_attempt_persisted: the attempt counter written before a `Mutate` is in the base when the app restarts.
#[test]
fn the_attempt_counter_is_stored_with_the_operation() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u0", "text": "t0"}));
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "userId": "u0", "text": "a"}).to_string() }], ..Default::default() };
    db.write_queue_ops(&[StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1, op_id: 1, model: "moments".into(), row_id: "m1".into(), action: "update".into(), payload: "{}".into(), effects, origin: crate::owners::store::IntentOrigin::Api }]).unwrap();
    assert_eq!(attempts_of(&db, 1), 1);
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Attempt { op_id: 1, attempts: 3, cycle_id: 2, cause: None }] };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(attempts_of(&db, 1), 3);
}
