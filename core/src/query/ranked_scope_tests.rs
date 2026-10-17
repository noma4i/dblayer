use rusqlite::StatementStatus;
use serde_json::json;

use super::*;
use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::sync::stable_order::rig::{messages_spec, CHAT};

fn steps_to_read_30_members(size: usize) -> i32 {
    let db = Db::open_in_memory(vec![messages_spec(true)], None).unwrap();
    let query = db.remote_relation_query("messages", "thread", json!({"chatId": CHAT}).as_object().unwrap()).unwrap().unwrap();
    let scope = ScopeRef::new(ModelKey::new("messages"), query.scope.clone().unwrap());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..size {
        let id = RowId::new(&format!("m{index:05}")).unwrap();
        let row = json!({"id": id.as_str(), "chatId": CHAT, "sequenceNumber": index, "createdAt": "2026-01-01T00:00:00.000Z"}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: id.clone(), row });
        envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: id, order_key: String::new() });
    }
    db.land(envelope).unwrap();
    let ids: Vec<String> = (0..30).map(|index| format!("m{:05}", index * size / 30)).collect();
    let statement = compile_ranked(&query, &db.spec("messages").unwrap(), Some(&ids)).unwrap().statement;
    let inner = db.shared.store.lock();
    let mut prepared = inner.connection.prepare(&statement.sql).unwrap();
    let found = prepared.query_map(rusqlite::params_from_iter(statement.bindings.iter()), |row| row.get::<_, String>(0)).unwrap().count();
    assert_eq!(found, 30);
    prepared.get_status(StatementStatus::VmStep)
}

#[test]
fn reading_chosen_members_of_a_scope_costs_the_same_steps_at_every_scope_size() {
    let (small, large) = (steps_to_read_30_members(200), steps_to_read_30_members(2000));
    assert!(large <= small + small / 10, "{small} steps at 200 members, {large} at 2000");
}
