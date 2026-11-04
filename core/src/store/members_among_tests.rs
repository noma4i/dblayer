use rusqlite::StatementStatus;
use serde_json::json;

use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::sync::stable_order::rig::{messages_spec, CHAT};

use super::rows::MEMBER_EXISTS_SQL as MEMBERS_SQL;

fn steps_to_find_stored_members(size: usize) -> i32 {
    let db = Db::open_in_memory(vec![messages_spec(false)], None).unwrap();
    let query = db.remote_relation_query("messages", "thread", json!({"chatId": CHAT}).as_object().unwrap()).unwrap().unwrap();
    let key = query.scope.clone().unwrap();
    let scope = ScopeRef::new(ModelKey::new("messages"), key.clone());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..size {
        let id = RowId::new(&format!("m{index:05}")).unwrap();
        let row = json!({"id": id.as_str(), "chatId": CHAT, "sequenceNumber": index, "createdAt": "2026-01-01T00:00:00.000Z"}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: id.clone(), row });
        envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: id, order_key: format!("k{index:06}") });
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
    let ids: Vec<String> = (size - 30..size).map(|index| format!("m{index:05}")).collect();
    let wanted: Vec<&str> = ids.iter().map(String::as_str).collect();
    db.shared.store
        .read(&["messages"], |reader| {
            reader.connection().prepare_cached(MEMBERS_SQL)?.reset_status(StatementStatus::VmStep);
            let found = reader.scope_members_among("messages", key.as_str(), &wanted)?;
            assert_eq!(found.len(), 30);
            Ok(reader.connection().prepare_cached(MEMBERS_SQL)?.get_status(StatementStatus::VmStep))
        })
        .unwrap()
}

#[test]
fn finding_thirty_stored_members_costs_the_same_steps_at_every_scope_size() {
    let (small, large) = (steps_to_find_stored_members(200), steps_to_find_stored_members(2000));
    assert!(large <= small + small / 10, "{small} steps at 200 members, {large} at 2000");
}
