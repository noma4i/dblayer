use crate::store::{BatchPlan, DestroyReason, EntityOp, OperationTransition};
use crate::identity::{ModelKey, RowId};
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
    let mut changed = false;
    let row_ids: Vec<RowId> = entry.operation.row_ids.iter().map(|id| if id == temp { real.clone() } else { id.clone() }).collect();
    changed |= row_ids != entry.operation.row_ids;
    let mut rollback = entry.rollback.clone();
    if let Some(mut prior) = Prior::of(entry)? {
        if prior.swap(temp, real, model, references) {
            rollback = Some(prior.to_value());
            changed = true;
        }
    }
    Ok(changed.then(|| entry.rewritten(row_ids, entry.input.clone(), rollback)))
}

pub(crate) struct TempReplacement<'a> {
    pub model: &'a str,
    pub temp: &'a RowId,
    pub real: &'a RowId,
}

pub(super) fn plan(writer: &mut super::row_write::RowWrite<'_>, envelope: &mut BatchPlan, outbox: &[OutboxOperation], replacement: TempReplacement<'_>, closing: &[i64], held: Option<serde_json::Map<String, serde_json::Value>>) -> Result<(), StoreError> {
        let connection = writer.connection;
        let models = writer.models;
        let reader = Reader::new(connection, models);
        let TempReplacement { model, temp, real } = replacement;
        let followed = |other: &&OutboxOperation| !closing.contains(&other.operation.id.0) && other.operation.row_ids.contains(temp);
        if outbox.iter().filter(followed).any(|follower| follower.operation.action_key.ends_with(":destroy")) {
            envelope.entity_ops.retain(|operation| !matches!(operation, EntityOp::Upsert { model: owner, id, .. } if owner.as_str() == model && id == real));
            envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: real.clone(), reason: DestroyReason::Local });
        }
        let ending: Vec<i64> = closing.iter().chain(writer.closing.iter()).copied().collect();
        let open = super::merge::unconfirmed_fields(&reader, &ending, model, &[temp.to_string()])?.remove(temp.as_str()).unwrap_or_default();
        let carried: serde_json::Map<String, serde_json::Value> = held.iter().flat_map(|row| open.iter().filter_map(|field| row.get(field).map(|value| (field.clone(), value.clone())))).collect();
        carry_locals(&reader, envelope, model, real, held)?;
        carry_open(writer, envelope, model, real, carried);
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: temp.clone(), reason: DestroyReason::Replaced { by: real.clone() } });
        let references = references_to(reader.models(), model);
        let swap = Swap { model, temp, real, references: &references };
        envelope.operation_transitions.extend(outbox.iter().filter(|other| !closing.contains(&other.operation.id.0)).filter_map(|other| retarget(other, &swap).transpose()).collect::<Result<Vec<_>, _>>()?);
        for ReferencingRow { model: owner, field, mut row } in rows_referencing(writer, &references, temp)? {
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

/// The values open operations hold on the temporary row lie over the row of the answer: on its landing in this plan, or on the server row of the real id later in the batch.
fn carry_open(writer: &mut super::row_write::RowWrite<'_>, envelope: &mut BatchPlan, model: &str, real: &RowId, carried: serde_json::Map<String, serde_json::Value>) {
        if carried.is_empty() { return; }
        let landing = envelope.entity_ops.iter_mut().find_map(|op| match op {
            EntityOp::Upsert { model: owner, id, row } if owner.as_str() == model && id == real => Some(row),
            _ => None,
        });
        match landing {
            Some(row) => row.extend(carried),
            None => writer.carried.entry((model.to_string(), real.to_string())).or_default().extend(carried),
        }
}

    /// The `local` fields of the temporary row go to the row of the answer unless the answer brings its own value (P-OUT-22).
fn carry_locals(reader: &Reader<'_>, envelope: &mut BatchPlan, model: &str, real: &RowId, held: Option<serde_json::Map<String, serde_json::Value>>) -> Result<(), StoreError> {
        let Some(held) = held else { return Ok(()) };
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
                None => envelope.pending_locals.push(crate::store::PendingLocal { model: model.to_string(), row_id: real.to_string(), path: field.name.clone(), value: value.clone() }),
            }
        }
        Ok(())
}

pub(super) fn apply(writer: &mut super::row_write::RowWrite<'_>, model: &str, temp: &str, real: &str, closing: &[i64]) -> Result<crate::store::CommitResult, StoreError> {
    writer.connection.execute(include_str!("sql/ddl/put_row_alias.sql"), rusqlite::params![model, temp, real])?;
    let temp = RowId::new(temp).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let real = RowId::new(real).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let held = writer.stored(model, temp.as_str())?.map(|stored| stored.row);
    let reader = Reader::new(writer.connection, writer.models);
    let mut envelope = BatchPlan::new(writer.generation);
    if held.is_some() {
        let outbox = crate::store::plan::read_outbox(&reader)?;
        plan(writer, &mut envelope, &outbox, TempReplacement { model, temp: &temp, real: &real }, closing, held)?;
    }
    writer.apply(&envelope)
}
