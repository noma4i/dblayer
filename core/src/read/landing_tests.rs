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

fn stored(last_key: &str, incoming: Vec<&str>) -> StoredScope {
    StoredScope { last_key: Some(last_key.into()), incoming: incoming.into_iter().map(String::from).collect() }
}

fn land(name: &str, stored: &StoredScope, incoming: &[Map<String, Value>]) -> LandingPlan {
    plan_landing(&scope(name), &person(), stored, incoming).unwrap()
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
fn a_page_without_stored_members_appends_in_server_order_with_increasing_keys() {
    let plan = land("order", &StoredScope::default(), &[row("b"), row("a")]);
    let members = appended(&plan);
    assert_eq!(members.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["b", "a"]);
    assert!(members[0].1 < members[1].1);
    assert_eq!(plan.scope_ops.len(), 2);
}

#[test]
fn duplicate_ids_keep_the_first_row() {
    let first = object(json!({"id": "a", "name": "A"}));
    let second = object(json!({"id": "a", "name": "B"}));
    let rows = upserted(&land("duplicate", &StoredScope::default(), &[first, second]));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1["name"], json!("A"));
}

#[test]
fn numeric_ids_become_string_ids() {
    let rows = upserted(&land("numeric", &StoredScope::default(), &[object(json!({"id": 42, "name": "n"}))]));
    assert_eq!(rows[0].0, "42");
    assert_eq!(rows[0].1["id"], json!("42"));
}

#[test]
fn rows_without_a_usable_id_are_dropped() {
    let missing = object(json!({"name": "missing"}));
    let empty = object(json!({"id": "", "name": "empty"}));
    let fractional = object(json!({"id": 1.5}));
    let plan = land("withoutId", &StoredScope::default(), &[missing, empty, fractional, row("valid")]);
    assert_eq!(upserted(&plan).iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["valid"]);
    assert_eq!(appended(&plan).len(), 1);
}

#[test]
fn entity_ops_target_the_scope_model() {
    let plan = land("model", &StoredScope::default(), &[row("a")]);
    assert_eq!(
        plan.entity_ops[0],
        EntityOp::Upsert { model: person(), id: RowId::new("a").unwrap(), row: row("a") }
    );
}

#[test]
fn next_page_appends_after_the_largest_key_and_keeps_existing_keys() {
    let current = stored("a2", vec!["a"]);
    let plan = land("append", &current, &[row("c"), row("a")]);
    let added = appended(&plan);
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].0, "c");
    assert!(added[0].1.as_str() > "a2");
    assert!(!plan.scope_ops.iter().any(|operation| matches!(operation, ScopeOp::Move { .. })));
    assert!(!plan.scope_ops.iter().any(|operation| matches!(operation, ScopeOp::Detach { .. })));
}

#[test]
fn foreign_scope_is_rejected() {
    let foreign = ScopeRef::new(ModelKey::new("other"), ScopeKey::compose(&["other", "items"]));
    assert_eq!(plan_landing(&foreign, &person(), &StoredScope::default(), &[row("x")]), Err(LandingError::ScopeModelMismatch));
}


#[test]
fn a_page_of_stored_members_in_another_order_writes_no_scope_operation() {
    let current = stored("K2", vec!["a", "b"]);
    let plan = land("reorder", &current, &[row("b"), row("a")]);
    assert!(plan.scope_ops.is_empty());
}
