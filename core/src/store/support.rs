use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, RowId, ScopeKey};
use crate::pacing::ManualClock;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

use super::Store;

pub fn messages_spec() -> ModelSpec {
    ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str).optional(),
            FieldSpec::scalar("seen", ScalarKind::Bool).optional().indexed(),
        ],
    )
}

pub fn items_spec() -> ModelSpec {
    ModelSpec::new(
        "items",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("createdAt", ScalarKind::Date).optional().indexed()],
    )
}

pub fn booted_store() -> (Store, Arc<ManualClock>) {
    let store = Store::open_in_memory().unwrap();
    let clock = Arc::new(ManualClock::new(Duration::from_millis(1_000_000)));
    store.set_clock(clock.clone());
    store.boot(&[messages_spec(), items_spec()], None).unwrap();
    store.mark_ready();
    (store, clock)
}

pub fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

pub fn id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

pub fn model(name: &str) -> ModelKey {
    ModelKey::new(name)
}

pub fn scope(model_name: &str, name: &str) -> ScopeRef {
    ScopeRef::new(model(model_name), ScopeKey::compose(&[model_name, name, "1"]))
}

pub fn upsert(model_name: &str, payload: Value) -> EntityOp {
    let row = row(payload);
    let row_id = id(row["id"].as_str().unwrap());
    EntityOp::Upsert { model: model(model_name), id: row_id, row }
}

pub fn destroy(model_name: &str, row_id: &str, reason: DestroyReason) -> EntityOp {
    EntityOp::Destroy { model: model(model_name), id: id(row_id), reason }
}

pub fn append(target: &ScopeRef, row_id: &str, order_key: &str) -> ScopeOp {
    ScopeOp::Append { scope: target.clone(), row: id(row_id), order_key: order_key.to_string() }
}

pub fn envelope(store: &Store, entity_ops: Vec<EntityOp>, scope_ops: Vec<ScopeOp>) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, store.generation());
    envelope.entity_ops = entity_ops;
    envelope.scope_ops = scope_ops;
    envelope
}

pub fn message(row_id: &str, chat: &str) -> Value {
    json!({"id": row_id, "chatId": chat})
}

pub fn count(store: &Store, sql: &str) -> i64 {
    store.lock().connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

pub fn text(store: &Store, sql: &str) -> Option<String> {
    rusqlite::OptionalExtension::optional(store.lock().connection.query_row(sql, [], |row| row.get(0))).unwrap()
}

pub fn exec(store: &Store, sql: &str) {
    store.lock().connection.execute_batch(sql).unwrap();
}
