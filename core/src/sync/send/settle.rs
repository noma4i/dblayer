use std::collections::HashSet;

use crate::commit::{EntityOp, OperationTransition};
use crate::db::{swap_id, OutboxOperation, Prior};
use crate::identity::{ModelKey, RowId};
use crate::operation::Operation;
use crate::store::{Reader, StoreError};
use crate::value::read_id;

use super::super::land::admit;
use super::super::normalize::Normalized;

pub(crate) fn response_ops(reader: &Reader<'_>, base: i64, closing: i64, model: &str, normalized: &Normalized, drops: &mut usize) -> Result<Vec<EntityOp>, StoreError> {
    let roots = admit(reader, base, Some(closing), model, &normalized.roots, drops)?;
    let root_ids: HashSet<RowId> = roots.iter().filter_map(|row| row.get("id").and_then(read_id)).collect();
    let mut ops = Vec::new();
    for (related_model, row) in &normalized.related {
        for row in admit(reader, base, Some(closing), related_model.as_str(), std::slice::from_ref(row), drops)? {
            let Some(id) = row.get("id").and_then(read_id) else { continue };
            if related_model.as_str() == model && root_ids.contains(&id) {
                continue;
            }
            ops.push(EntityOp::Upsert { model: related_model.clone(), id, row });
        }
    }
    for row in roots {
        if let Some(id) = row.get("id").and_then(read_id) {
            ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        }
    }
    Ok(ops)
}

pub(crate) struct Swap<'a> {
    pub model: &'a str,
    pub temp: &'a RowId,
    pub real: &'a RowId,
    pub references: &'a [(String, String)],
}

pub(crate) fn retarget(entry: &OutboxOperation, swap: &Swap<'_>) -> Option<OperationTransition> {
    let Swap { model, temp, real, references } = *swap;
    let owner = Operation::split_action_key(&entry.operation.action_key).map(|(owner, _)| owner).unwrap_or_default();
    let mut input = entry.input.clone();
    let mut changed = false;
    if let Some(object) = input.as_object_mut() {
        let own_id = (owner == model).then_some("id");
        let fields = references.iter().filter(|(child, _)| *child == owner).map(|(_, field)| field.as_str());
        for field in own_id.into_iter().chain(fields) {
            changed |= object.get_mut(field).is_some_and(|value| swap_id(value, temp, real));
        }
    }
    let row_ids: Vec<RowId> = entry.operation.row_ids.iter().map(|id| if id == temp { real.clone() } else { id.clone() }).collect();
    changed |= row_ids != entry.operation.row_ids;
    let mut rollback = entry.rollback.clone();
    if let Some(mut prior) = Prior::of(entry) {
        if prior.swap(temp, real, model, references) {
            rollback = Some(prior.to_value());
            changed = true;
        }
    }
    changed.then(|| entry.rewritten(row_ids, input, rollback))
}
