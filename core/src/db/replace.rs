use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{RowId, RowRef};
use crate::operation::{open_operations, OutboxQueue};
use crate::read::LandingPlan;
use crate::schema::Coverage;
use crate::store::Reader;
use crate::value::{fractional_between, read_id};

use super::diff::longest_increasing;
use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnAbsent {
    #[default]
    Detach,
    Destroy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceRequest {
    pub scope: ScopeRef,
    pub coverage: Coverage,
    pub on_absent: OnAbsent,
    pub replace_all: bool,
}

impl ReplaceRequest {
    pub fn new(scope: ScopeRef, coverage: Coverage) -> Self {
        Self { scope, coverage, on_absent: OnAbsent::Detach, replace_all: false }
    }
}

struct Member {
    id: String,
    order_key: String,
}

impl Db {
    pub fn replace(&self, request: &ReplaceRequest, rows: Vec<Map<String, Value>>) -> Result<CommitResult, DbError> {
        let spec = self.shared.spec(request.scope.model.as_str())?;
        let rows = rows.iter().map(|row| {
            let mut patch = spec.patch(row);
            self.shared.store.decode_values(request.scope.model.as_str(), &mut patch, crate::schema::read_path)?;
            Ok(patch)
        }).collect::<Result<Vec<_>, DbError>>()?;
        let rows = crate::store::coalesce_rows(rows.into_iter().map(|row| (request.scope.model.clone(), row)));
        let generation = self.generation();
        self.commit_following(generation, |reader| {
                let model = request.scope.model.as_str();
                let rows = rows
                    .iter()
                    .map(|(_, row)| match row.get("id").and_then(read_id) {
                        Some(id) => Ok(self.merged(model, reader.point(model, id.as_str())?.as_ref(), row)?.unwrap_or_else(|| row.clone())),
                        None => Ok(row.clone()),
                    })
                    .collect::<Result<Vec<_>, DbError>>()?;
                let plan = self.replace_plan(reader, request, &rows, None)?;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Replace, generation);
                envelope.entity_ops = plan.entity_ops;
                envelope.scope_ops = plan.scope_ops;
                Ok(Some(envelope))
            })
            .map_err(plan_fault)
    }

    pub(crate) fn replace_plan(&self, reader: &Reader<'_>, request: &ReplaceRequest, rows: &[Map<String, Value>], base_revision: Option<i64>) -> Result<LandingPlan, DbError> {
        let model = &request.scope.model;
        let scope_key = request.scope.key.as_str();
        let current: Vec<Member> =
            reader.scope_members(model.as_str(), scope_key)?.into_iter().map(|(id, order_key)| Member { id, order_key }).collect();
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
        let busy: HashSet<String> = OutboxQueue::new(open_operations(reader.connection())?).open_row_ids(model).into_iter().map(|id| id.to_string()).collect();
        let names = current.iter().map(|member| member.id.clone()).collect::<Vec<_>>();
        let newer: HashSet<String> = match base_revision {
            Some(base) => reader.stored_rows(model.as_str(), &names)?.into_iter().filter(|(_, row)| row.revision > base).map(|(id, _)| id).collect(),
            None => HashSet::new(),
        };
        let is_local = |id: &str| crate::identity::is_temp_id(id) || busy.contains(id) || newer.contains(id);

        let anchor = (request.coverage == Coverage::Page && !request.replace_all)
            .then(|| incoming_ids.last().and_then(|last| current.iter().position(|member| &member.id == last)))
            .flatten();
        let mut result: Vec<String> = incoming_ids.clone();
        if let Some(position) = anchor {
            result.extend(current.iter().skip(position + 1).filter(|member| !seen.contains(&member.id)).map(|member| member.id.clone()));
        }
        let mut present: HashSet<String> = result.iter().cloned().collect();
        for (index, member) in current.iter().enumerate() {
            if is_local(&member.id) && present.insert(member.id.clone()) {
                result.insert(index.min(result.len()), member.id.clone());
            }
        }

        let removed: Vec<usize> = current.iter().enumerate().filter(|(_, member)| !present.contains(&member.id)).map(|(index, _)| index).collect();
        let destroyable: Vec<usize> = match request.on_absent {
            OnAbsent::Detach => Vec::new(),
            OnAbsent::Destroy if request.coverage == Coverage::Complete || request.replace_all => removed.clone(),
            OnAbsent::Destroy => anchor.map_or_else(Vec::new, |position| removed.iter().copied().filter(|index| *index < position).collect()),
        };

        let mut entity_ops: Vec<EntityOp> = incoming.into_iter().map(|(id, row)| EntityOp::Upsert { model: model.clone(), id, row }).collect();

        let mut destroyed: HashSet<RowRef> = HashSet::new();
        for index in destroyable.iter().filter(|index| !is_local(&current[**index].id)) {
            let own = RowRef::new(model.clone(), RowId::new(&current[*index].id)?);
            for target in self.destroy_targets(reader, &own, WriteOrigin::Event)? {
                let upserted = target.model == *model && seen.contains(target.id.as_str());
                if !upserted && destroyed.insert(target.clone()) {
                    entity_ops.push(EntityOp::Destroy { model: target.model, id: target.id, reason: DestroyReason::Server });
                }
            }
        }

        let mut scope_ops: Vec<ScopeOp> = Vec::new();
        for index in removed {
            let id = RowId::new(&current[index].id)?;
            if !destroyed.contains(&RowRef::new(model.clone(), id.clone())) {
                scope_ops.push(ScopeOp::Detach { scope: request.scope.clone(), row: id });
            }
        }
        scope_ops.extend(order_ops(&request.scope, &current, &result)?);
        Ok(LandingPlan { entity_ops, scope_ops })
    }
}

fn order_ops(scope: &ScopeRef, current: &[Member], result: &[String]) -> Result<Vec<ScopeOp>, DbError> {
    let rank: HashMap<&str, usize> = current.iter().enumerate().map(|(index, member)| (member.id.as_str(), index)).collect();
    let existing: Vec<(usize, usize)> = result.iter().enumerate().filter_map(|(position, id)| rank.get(id.as_str()).map(|rank| (position, *rank))).collect();
    let ranks: Vec<usize> = existing.iter().map(|(_, rank)| *rank).collect();
    let stable_positions: HashSet<usize> = longest_increasing(&ranks).into_iter().map(|index| existing[index].0).collect();
    let mut keys: Vec<Option<String>> = result
        .iter()
        .enumerate()
        .map(|(position, id)| stable_positions.contains(&position).then(|| current[rank[id.as_str()]].order_key.clone()))
        .collect();
    let mut uppers: Vec<Option<String>> = vec![None; result.len()];
    let mut next: Option<String> = None;
    for position in (0..result.len()).rev() {
        uppers[position] = next.clone();
        if keys[position].is_some() {
            next = keys[position].clone();
        }
    }
    let mut ops = Vec::new();
    for position in 0..result.len() {
        if keys[position].is_some() {
            continue;
        }
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
