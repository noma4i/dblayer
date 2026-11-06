use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::store::{EntityOp, ScopeOp, ScopeRef};
use crate::read::longest_increasing;
use crate::store::DbError;
use crate::identity::RowId;
use crate::read::LandingPlan;
use crate::schema::Coverage;
use crate::value::{fractional_between, read_id};

use super::Reader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplaceRequest {
    pub scope: ScopeRef,
    pub coverage: Coverage,
    pub replace_all: bool,
}

impl ReplaceRequest {
    pub fn new(scope: ScopeRef, coverage: Coverage) -> Self { Self { scope, coverage, replace_all: false } }
}

struct Member {
    id: String,
    order_key: String,
}

pub(crate) fn scope_membership(reader: &Reader<'_>, model: &str, scope_key: &str, base_revision: i64) -> Result<HashMap<String, bool>, DbError> {
    let mut membership: HashMap<String, bool> = reader.scope_changes_since(model, scope_key, base_revision)?.into_iter().map(|(id, order)| (id, order.is_some())).collect();
    for (id, joined) in crate::store::plan::op_effect::open_scope_effects(reader.connection(), model, scope_key)? { membership.insert(id, joined); }
    Ok(membership)
}

pub(crate) fn replace_plan(reader: &Reader<'_>, request: &ReplaceRequest, rows: &[Map<String, Value>], base_revision: i64) -> Result<LandingPlan, DbError> {
    let model = &request.scope.model;
    let scope_key = request.scope.key.as_str();
    let current: Vec<Member> = reader.scope_members(model.as_str(), scope_key)?.into_iter().map(|(id, order_key)| Member { id, order_key }).collect();
    let mut incoming: Vec<(RowId, Map<String, Value>)> = Vec::new();
    let mut incoming_ids: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let Some(id) = row.get("id").and_then(read_id) else { continue };
        if seen.insert(id.to_string()) {
            incoming_ids.push(id.to_string());
            incoming.push((id, row.clone()));
        }
    }
    let membership = scope_membership(reader, model.as_str(), scope_key, base_revision)?;
    incoming_ids.retain(|id| membership.get(id) != Some(&false));
    let anchor = (request.coverage == Coverage::Page && !request.replace_all)
        .then(|| incoming_ids.last().and_then(|last| current.iter().position(|member| &member.id == last)))
        .flatten();
    let current_ids: HashSet<&str> = current.iter().map(|member| member.id.as_str()).collect();
    incoming_ids.retain(|id| membership.get(id) != Some(&true) || !current_ids.contains(id.as_str()));
    let mut result = incoming_ids;
    if let Some(position) = anchor {
        result.extend(current.iter().skip(position + 1).filter(|member| !seen.contains(&member.id)).map(|member| member.id.clone()));
    }
    let mut present: HashSet<String> = result.iter().cloned().collect();
    for (index, member) in current.iter().enumerate() {
        if membership.get(&member.id) == Some(&true) && present.insert(member.id.clone()) {
            result.insert(index.min(result.len()), member.id.clone());
        }
    }
    let removed = current.iter().filter(|member| !present.contains(&member.id));
    let entity_ops = incoming.into_iter().map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row }).collect();
    let mut scope_ops = Vec::new();
    for member in removed { scope_ops.push(ScopeOp::Detach { scope: request.scope.clone(), row: RowId::new(&member.id)? }); }
    scope_ops.extend(order_ops(&request.scope, &current, &result)?);
    Ok(LandingPlan { entity_ops, scope_ops })
}

fn order_ops(scope: &ScopeRef, current: &[Member], result: &[String]) -> Result<Vec<ScopeOp>, DbError> {
    let rank: HashMap<&str, usize> = current.iter().enumerate().map(|(index, member)| (member.id.as_str(), index)).collect();
    let existing: Vec<(usize, usize)> = result.iter().enumerate().filter_map(|(position, id)| rank.get(id.as_str()).map(|rank| (position, *rank))).collect();
    let ranks: Vec<usize> = existing.iter().map(|(_, rank)| *rank).collect();
    let stable_positions: HashSet<usize> = longest_increasing(&ranks).into_iter().map(|index| existing[index].0).collect();
    let mut keys: Vec<Option<String>> = result.iter().enumerate().map(|(position, id)| stable_positions.contains(&position).then(|| current[rank[id.as_str()]].order_key.clone())).collect();
    let mut uppers: Vec<Option<String>> = vec![None; result.len()];
    let mut next: Option<String> = None;
    for position in (0..result.len()).rev() {
        uppers[position] = next.clone();
        if keys[position].is_some() { next = keys[position].clone(); }
    }
    let mut ops = Vec::new();
    for position in 0..result.len() {
        if keys[position].is_some() { continue; }
        let lower = position.checked_sub(1).and_then(|before| keys[before].clone());
        let upper = uppers[position].take();
        crate::identity::touches::touch(1);
        let key = fractional_between(lower.as_deref(), upper.as_deref()).map_err(|_| crate::read::LandingError::InvalidOrderBounds)?;
        let row = RowId::new(&result[position])?;
        ops.push(if rank.contains_key(result[position].as_str()) {
            ScopeOp::Move { scope: scope.clone(), row, order_key: key.clone() }
        } else {
            ScopeOp::Append { scope: scope.clone(), row, order_key: key.clone() }
        });
        keys[position] = Some(key);
    }
    Ok(ops)
}
