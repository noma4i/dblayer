use super::*;
use crate::commit::{EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId, ScopeKey};
use serde_json::{json, Map, Value};

fn person() -> ModelKey {
    ModelKey::new("Person")
}

fn scope(name: &str) -> ScopeRef {
    ScopeRef::new(person(), ScopeKey::compose(&["Person", name]))
}

fn row(id: &str) -> Map<String, Value> {
    json!({"id": id, "name": id}).as_object().unwrap().clone()
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn member(id: &str, order_key: &str) -> StoredMember {
    StoredMember { id: id.into(), order_key: order_key.into() }
}

fn land(name: &str, current: &[StoredMember], incoming: &[Map<String, Value>], first: bool) -> LandingPlan {
    plan_landing(&scope(name), &person(), current, incoming, first).unwrap()
}

fn upserted(plan: &LandingPlan) -> Vec<(String, Map<String, Value>)> {
    plan.entity_ops
        .iter()
        .map(|operation| match operation {
            EntityOp::Upsert { id, row, .. } => (id.as_str().to_string(), row.clone()),
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

fn appended(plan: &LandingPlan) -> Vec<(String, String)> {
    plan.scope_ops
        .iter()
        .filter_map(|operation| match operation {
            ScopeOp::Append { row, order_key, .. } => Some((row.as_str().to_string(), order_key.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn first_page_appends_in_server_order_with_increasing_keys() {
    let plan = land("order", &[], &[row("b"), row("a")], true);
    let members = appended(&plan);
    assert_eq!(members.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["b", "a"]);
    assert!(members[0].1 < members[1].1);
    assert_eq!(plan.scope_ops.len(), 2);
}

#[test]
fn duplicate_ids_keep_the_first_row() {
    let first = object(json!({"id": "a", "name": "A"}));
    let second = object(json!({"id": "a", "name": "B"}));
    let rows = upserted(&land("duplicate", &[], &[first, second], true));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1["name"], json!("A"));
}

#[test]
fn numeric_ids_become_string_ids() {
    let rows = upserted(&land("numeric", &[], &[object(json!({"id": 42, "name": "n"}))], true));
    assert_eq!(rows[0].0, "42");
    assert_eq!(rows[0].1["id"], json!("42"));
}

#[test]
fn rows_without_a_usable_id_are_dropped() {
    let missing = object(json!({"name": "missing"}));
    let empty = object(json!({"id": "", "name": "empty"}));
    let fractional = object(json!({"id": 1.5}));
    let plan = land("withoutId", &[], &[missing, empty, fractional, row("valid")], true);
    assert_eq!(upserted(&plan).iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["valid"]);
    assert_eq!(appended(&plan).len(), 1);
}

#[test]
fn entity_ops_target_the_scope_model() {
    let plan = land("model", &[], &[row("a")], true);
    assert_eq!(
        plan.entity_ops[0],
        EntityOp::Upsert { model: person(), id: RowId::new("a").unwrap(), row: row("a") }
    );
}

#[test]
fn first_page_detaches_members_missing_from_the_page() {
    let plan = land("replace", &[member("x", "U")], &[row("y")], true);
    assert!(plan.scope_ops.contains(&ScopeOp::Detach { scope: scope("replace"), row: RowId::new("x").unwrap() }));
    assert_eq!(appended(&plan).iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["y"]);
}

#[test]
fn next_page_appends_after_the_largest_key_and_keeps_existing_keys() {
    let current = [member("a", "K1"), member("b", "K2")];
    let plan = land("append", &current, &[row("c"), row("a")], false);
    let added = appended(&plan);
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].0, "c");
    assert!(added[0].1.as_str() > "K2");
    assert!(plan.scope_ops.contains(&ScopeOp::Move { scope: scope("append"), row: RowId::new("a").unwrap(), order_key: "K1".into() }));
    assert!(!plan.scope_ops.iter().any(|operation| matches!(operation, ScopeOp::Detach { .. })));
}

#[test]
fn first_page_reorder_moves_existing_members() {
    let current = [member("a", "K1"), member("b", "K2")];
    let plan = land("reorder", &current, &[row("b"), row("a")], true);
    let moved: Vec<&RowId> = plan
        .scope_ops
        .iter()
        .filter_map(|operation| match operation {
            ScopeOp::Move { row, .. } => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(moved.len(), 2);
    assert!(appended(&plan).is_empty());
}

#[test]
fn foreign_scope_is_rejected() {
    let foreign = ScopeRef::new(ModelKey::new("other"), ScopeKey::compose(&["other", "items"]));
    assert_eq!(plan_landing(&foreign, &person(), &[], &[row("x")], true), Err(LandingError::ScopeModelMismatch));
}
