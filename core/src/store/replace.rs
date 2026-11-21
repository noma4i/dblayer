use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::store::{EntityOp, ScopeOp, ScopeRef};
use crate::owners::read::diff::longest_increasing;
use crate::store::CoreError;
use crate::identity::RowId;
use crate::owners::read::codec::LandingPlan;
use crate::schema::{Coverage, SortField};
use crate::value::{fractional_between, read_id, SortDirection};
use rusqlite::types::Value as SqlValue;

use super::Reader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplaceRequest {
    pub scope: ScopeRef,
    pub coverage: Coverage,
    pub replace_all: bool,
    pub ordered_complete: bool,
    pub first_page: bool,
    pub start_after: Option<String>,
    pub local_order: Option<Vec<SortField>>,
}

impl ReplaceRequest {
    pub fn new(scope: ScopeRef, coverage: Coverage) -> Self { Self { scope, coverage, replace_all: false, ordered_complete: false, first_page: false, start_after: None, local_order: None } }
}

struct Member {
    id: String,
    order_key: String,
}

pub(crate) fn scope_membership(reader: &Reader<'_>, model: &str, scope_key: &str, base_revision: i64) -> Result<HashMap<String, bool>, CoreError> {
    let mut membership: HashMap<String, bool> = reader.scope_changes_since(model, scope_key, base_revision)?.into_iter().map(|(id, order)| (id, order.is_some())).collect();
    for (id, joined) in crate::store::plan::op_effect::open_scope_effects(reader.connection(), model, scope_key)? { membership.insert(id, joined); }
    Ok(membership)
}

pub(crate) fn replace_plan(reader: &Reader<'_>, request: &ReplaceRequest, rows: &[Map<String, Value>], base_revision: i64) -> Result<LandingPlan, CoreError> {
    let model = &request.scope.model;
    let scope_key = request.scope.key.as_str();
    let whole: Vec<Member> = reader.scope_members(model.as_str(), scope_key)?.into_iter().map(|(id, order_key)| Member { id, order_key }).collect();
    let split = request.start_after.as_ref().and_then(|start| whole.iter().position(|member| &member.id == start)).map_or(0, |at| at + 1);
    let (before, current) = whole.split_at(split);
    let before_ids: HashSet<&str> = before.iter().map(|member| member.id.as_str()).collect();
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
    if let Some(fields) = &request.local_order {
        return ordered_plan(reader, request, fields, &whole, incoming, &membership, base_revision);
    }
    incoming_ids.retain(|id| membership.get(id) != Some(&false) && !before_ids.contains(id.as_str()));
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
    let full: Vec<String> = before.iter().map(|member| member.id.clone()).chain(result).collect();
    scope_ops.extend(order_ops(&request.scope, &whole, &full)?);
    Ok(LandingPlan { entity_ops, scope_ops })
}

fn ordered_key(spec: &crate::schema::ModelSpec, fields: &[SortField], row: &Map<String, Value>) -> Result<String, CoreError> {
    let mut cells = Vec::new();
    let mut descending = Vec::new();
    for field in fields {
        let value = if field.column == "id" {
            row.get("id").and_then(read_id).map_or(SqlValue::Null, |id| SqlValue::Text(id.to_string()))
        } else {
            let declared = spec.field(&field.column).ok_or_else(|| CoreError::UnknownField { model: spec.key.to_string(), field: field.column.clone() })?;
            let kind = crate::store::ddl::column_type(&declared.kind).ok_or_else(|| CoreError::Internal(format!("{} is not a scalar", field.column)))?;
            super::rows::column_value(kind, row.get(&field.column))
        };
        match field.direction {
            SortDirection::AscNullsLast => { cells.push(SqlValue::Integer(i64::from(value == SqlValue::Null))); descending.push(false); }
            SortDirection::DescNullsFirst => { cells.push(SqlValue::Integer(i64::from(value != SqlValue::Null))); descending.push(false); }
            _ => {}
        }
        cells.push(value);
        descending.push(matches!(field.direction, SortDirection::Desc | SortDirection::DescNullsFirst));
    }
    Ok(crate::store::query::place_of(&cells, &descending))
}

fn ordered_plan(reader: &Reader<'_>, request: &ReplaceRequest, fields: &[SortField], whole: &[Member], incoming: Vec<(RowId, Map<String, Value>)>, membership: &HashMap<String, bool>, base_revision: i64) -> Result<LandingPlan, CoreError> {
    let model = &request.scope.model;
    let info = reader.models().get(model.as_str()).ok_or_else(|| CoreError::UnknownModel(model.to_string()))?;
    let mut keys = incoming.iter().map(|(_, row)| ordered_key(&info.spec, fields, row)).collect::<Result<Vec<_>, _>>()?;
    let direction = keys.first().zip(keys.last()).map(|(first, last)| first.cmp(last));
    keys.sort();
    let bounds = keys.first().zip(keys.last());
    let orders = fields.iter().map(|field| crate::store::query::Order::new(&field.column, field.direction)).collect();
    let query = crate::store::query::Query::new(model.as_str()).ordered(orders).in_scope(request.scope.key.clone());
    let ranked = crate::store::query::compile_ranked(&query, &info.spec, None)?;
    let rows = crate::store::query::fetch_ranked(reader.connection(), &ranked.statement)?;
    let seen: HashSet<String> = incoming.iter().map(|(id, _)| id.to_string()).collect();
    let mut removed = HashSet::new();
    for row in rows {
        let key = crate::store::query::place_of(&row.cells, &ranked.descending);
        let covered = bounds.is_some_and(|(low, high)| match (request.first_page, direction) {
            (true, Some(std::cmp::Ordering::Less)) => &key < high,
            (true, Some(std::cmp::Ordering::Greater)) => &key > low,
            _ => &key > low && &key < high,
        });
        if (request.ordered_complete || covered) && !seen.contains(&row.id) && row.revision <= base_revision && membership.get(&row.id) != Some(&true) {
            removed.insert(row.id);
        }
    }
    let mut full: Vec<String> = whole.iter().filter(|member| !removed.contains(&member.id)).map(|member| member.id.clone()).collect();
    let mut present: HashSet<String> = full.iter().cloned().collect();
    for (id, _) in &incoming { let id = id.to_string(); if membership.get(&id) != Some(&false) && present.insert(id.clone()) { full.push(id); } }
    let mut scope_ops = whole.iter().filter(|member| removed.contains(&member.id)).map(|member| Ok(ScopeOp::Detach { scope: request.scope.clone(), row: RowId::new(&member.id)? })).collect::<Result<Vec<_>, CoreError>>()?;
    scope_ops.extend(order_ops(&request.scope, whole, &full)?);
    let entity_ops = incoming.into_iter().map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row }).collect();
    Ok(LandingPlan { entity_ops, scope_ops })
}

fn order_ops(scope: &ScopeRef, current: &[Member], result: &[String]) -> Result<Vec<ScopeOp>, CoreError> {
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
        let key = fractional_between(lower.as_deref(), upper.as_deref()).map_err(|_| crate::owners::read::codec::LandingError::InvalidOrderBounds)?;
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
