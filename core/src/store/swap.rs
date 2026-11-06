use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, OperationTransition};
use crate::identity::{ModelKey, RowId};
use crate::operation::Operation;
use crate::store::Reader;
use crate::value::read_id;

use crate::store::plan::{swap_id, OutboxOperation, Prior};
use super::references::{references_to, rows_referencing, ReferencingRow};
use super::StoreError;

pub(crate) struct Swap<'a> {
    pub model: &'a str,
    pub temp: &'a RowId,
    pub real: &'a RowId,
    pub references: &'a [(String, String)],
}

pub(crate) fn retarget(entry: &OutboxOperation, swap: &Swap<'_>) -> Result<Option<OperationTransition>, crate::store::StoreError> {
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
    if let Some(mut prior) = Prior::of(entry)? {
        if prior.swap(temp, real, model, references) {
            rollback = Some(prior.to_value());
            changed = true;
        }
    }
    Ok(changed.then(|| entry.rewritten(row_ids, input, rollback)))
}

pub(crate) struct TempReplacement<'a> {
    pub model: &'a str,
    pub temp: &'a RowId,
    pub real: &'a RowId,
}

fn plan(reader: &Reader<'_>, envelope: &mut CommitEnvelope, outbox: &[OutboxOperation], replacement: TempReplacement<'_>, own: i64) -> Result<(), StoreError> {
        let TempReplacement { model, temp, real } = replacement;
        let followed = |other: &&OutboxOperation| other.operation.id.0 != own && other.operation.row_ids.contains(temp);
        if outbox.iter().filter(followed).any(|follower| follower.operation.action_key.ends_with(":destroy")) {
            envelope.entity_ops.retain(|operation| !matches!(operation, EntityOp::Upsert { model: owner, id, .. } if owner.as_str() == model && id == real));
            envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: real.clone(), reason: DestroyReason::Local });
        }
        carry_locals(reader, envelope, model, temp, real)?;
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: temp.clone(), reason: DestroyReason::Replaced { by: real.clone() } });
        let references = references_to(reader.models(), model);
        let swap = Swap { model, temp, real, references: &references };
        envelope.operation_transitions.extend(outbox.iter().filter(|other| other.operation.id.0 != own).filter_map(|other| retarget(other, &swap).transpose()).collect::<Result<Vec<_>, _>>()?);
        for ReferencingRow { model: owner, field, mut row } in rows_referencing(reader, reader.models(), &references, temp)? {
            let Some(child) = row.get("id").and_then(read_id) else { continue };
            if let Some(value) = row.get_mut(&field) { swap_id(value, temp, real); }
            let landing = envelope.entity_ops.iter_mut().find_map(|op| match op {
                EntityOp::Upsert { model: holder, id, row } if holder.as_str() == owner && *id == child => Some(row),
                _ => None,
            });
            match landing {
                Some(landing) => {
                    if let Some(value) = landing.get_mut(&field) {
                        swap_id(value, temp, real);
                    } else if let Some(value) = row.remove(&field) {
                        landing.insert(field, value);
                    }
                }
                None => envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(&owner), id: child, row }),
            }
        }
        Ok(())
}

    /// The `local` fields of the temporary row go to the row of the answer unless the answer brings its own value (P-OUT-22).
fn carry_locals(reader: &Reader<'_>, envelope: &mut CommitEnvelope, model: &str, temp: &RowId, real: &RowId) -> Result<(), StoreError> {
        let Some(held) = reader.point(model, temp.as_str())? else { return Ok(()) };
        let spec = &reader.models().get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        for field in spec.fields.iter().filter(|field| matches!(field.merge, Some(crate::schema::MergeRule::Local))) {
            let Some(value) = held.get(&field.name).filter(|value| !value.is_null()) else { continue };
            let landing = envelope.entity_ops.iter_mut().find_map(|op| match op {
                EntityOp::Upsert { model: owner, id, row } if owner.as_str() == model && id == real => Some(row),
                _ => None,
            });
            match landing {
                Some(row) if row.get(&field.name).is_some_and(|brought| !brought.is_null()) => {}
                Some(row) => { row.insert(field.name.clone(), value.clone()); }
                None => envelope.pending_locals.push(crate::commit::PendingLocal { model: model.to_string(), row_id: real.to_string(), path: field.name.clone(), value: value.clone() }),
            }
        }
        Ok(())
}

pub(super) fn apply(writer: &mut super::row_write::RowWrite<'_>, model: &str, temp: &str, real: &str, own: i64) -> Result<crate::commit::CommitResult, StoreError> {
    let temp = RowId::new(temp).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let real = RowId::new(real).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let reader = Reader::new(writer.connection, writer.models);
    let mut envelope = CommitEnvelope::new(crate::commit::WriteOrigin::Optimistic, writer.generation);
    if reader.point(model, temp.as_str())?.is_some() {
        let outbox = crate::store::plan::read_outbox(&reader)?;
        plan(&reader, &mut envelope, &outbox, TempReplacement { model, temp: &temp, real: &real }, own)?;
    }
    super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals))
}
