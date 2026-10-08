use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId};
use crate::store::{MembershipChange, MembershipPlan};
use crate::value::{fractional_between, read_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LandingError {
    #[error("scope belongs to another model")]
    ScopeModelMismatch,
    #[error("fractional order bounds are invalid")]
    InvalidOrderBounds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMember {
    pub id: String,
    pub order_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LandingPlan {
    pub entity_ops: Vec<EntityOp>,
    pub scope_ops: Vec<ScopeOp>,
}

pub fn plan_landing(
    scope: &ScopeRef,
    model: &ModelKey,
    current: &[StoredMember],
    incoming: &[Map<String, Value>],
    is_first_page: bool,
) -> Result<LandingPlan, LandingError> {
    if &scope.model != model {
        return Err(LandingError::ScopeModelMismatch);
    }
    let rows = identified_rows(incoming);
    let existing: BTreeMap<&str, &StoredMember> = current.iter().map(|member| (member.id.as_str(), member)).collect();
    let mut previous_key: Option<String> =
        if is_first_page { None } else { current.iter().map(|member| member.order_key.clone()).max() };
    let mut members = Vec::with_capacity(rows.len());
    for (id, _) in &rows {
        if let Some(member) = existing.get(id.as_str()).filter(|_| !is_first_page) {
            members.push((*member).clone());
            continue;
        }
        let order_key = fractional_between(previous_key.as_deref(), None).map_err(|_| LandingError::InvalidOrderBounds)?;
        previous_key = Some(order_key.clone());
        members.push(StoredMember { id: id.as_str().to_string(), order_key });
    }
    let current_ids: Vec<String> = current.iter().map(|member| member.id.clone()).collect();
    let incoming_ids: Vec<String> = members.iter().map(|member| member.id.clone()).collect();
    let change = if is_first_page {
        MembershipPlan::complete(&current_ids, &incoming_ids)
    } else {
        MembershipPlan::page(&current_ids, &incoming_ids)
    };
    let order_keys: BTreeMap<&str, &str> = members.iter().map(|member| (member.id.as_str(), member.order_key.as_str())).collect();
    let entity_ops = rows
        .into_iter()
        .map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row })
        .collect();
    Ok(LandingPlan { entity_ops, scope_ops: scope_operations(scope, &change, &order_keys) })
}

fn identified_rows(incoming: &[Map<String, Value>]) -> Vec<(RowId, Map<String, Value>)> {
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for row in incoming {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        if !seen.insert(id.clone()) {
            continue;
        }
        let mut normalized = row.clone();
        normalized.insert("id".to_string(), Value::String(id.as_str().to_string()));
        rows.push((id, normalized));
    }
    rows
}

fn scope_operations(scope: &ScopeRef, change: &MembershipChange, order_keys: &BTreeMap<&str, &str>) -> Vec<ScopeOp> {
    let keyed = |ids: &[String]| -> Vec<(RowId, String)> {
        ids.iter()
            .filter_map(|id| Some((RowId::new(id).ok()?, order_keys.get(id.as_str())?.to_string())))
            .collect()
    };
    let inserted = keyed(&change.inserted)
        .into_iter()
        .map(|(row, order_key)| ScopeOp::Append { scope: scope.clone(), row, order_key });
    let moved = keyed(&change.moved)
        .into_iter()
        .map(|(row, order_key)| ScopeOp::Move { scope: scope.clone(), row, order_key });
    let removed = change
        .removed
        .iter()
        .filter_map(|id| RowId::new(id).ok())
        .map(|row| ScopeOp::Detach { scope: scope.clone(), row });
    removed.chain(inserted).chain(moved).collect()
}
