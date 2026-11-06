use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, ScopeOp, ScopeRef};
use crate::store::plan::{Prior, PriorRow};
use crate::identity::{ListKey, ModelKey, RowId, RowRef};
use crate::owners::write::operation_status::{Operation, OperationStatus};
use crate::owners::store::{EffectRow, IntentEffects};

use super::row_write::RowWrite;
use super::{Reader, StoreError};

fn id(value: &str) -> Result<RowId, StoreError> {
    RowId::new(value).map_err(|error| StoreError::Constraint(error.to_string()))
}

fn values(text: &str) -> Result<Map<String, Value>, StoreError> {
    serde_json::from_str::<Value>(text)?.as_object().cloned().ok_or_else(|| StoreError::Constraint("row values must be an object".into()))
}

fn effects(reader: &Reader<'_>, writes: &IntentEffects, writer: &RowWrite<'_>) -> Result<(BatchPlan, Prior), StoreError> {
    let mut envelope = BatchPlan::new(writer.generation);
    let mut prior = Prior::default();
    for EffectRow { model, row_id, values: text } in &writes.rows {
        let fields = values(text)?;
        let next = match reader.point(model, row_id)? {
            Some(current) => {
                let mut merged = current.clone();
                merged.extend(fields);
                let mut entry = PriorRow::edited(reader, model, row_id, &current, &merged)?;
                if let PriorRow::Edited { owned, .. } = &mut entry { for revision in owned.values_mut() { *revision = writer.revision; } }
                prior.rows.push(entry);
                merged
            }
            None => {
                prior.rows.push(PriorRow::Created { model: model.clone(), id: row_id.clone() });
                let spec = &writer.models.get(model).ok_or_else(|| StoreError::UnknownModel(model.clone()))?.spec;
                for relation in spec.relations.iter().filter(|relation| relation.takes_created_rows()) {
                    for params in relation.row_params(&fields) {
                        let key = spec.selection_key(&relation.name, &params).map_err(|error| StoreError::Constraint(error.to_string()))?;
                        let direction = match relation.remote.as_ref().map(|remote| &remote.shape) {
                            Some(crate::schema::RemoteShape::Connection { direction, .. }) => *direction,
                            _ => crate::schema::PageDirection::Forward,
                        };
                        let window = super::window_key(key.as_str(), direction);
                        let loaded: bool = reader.connection().query_row(include_str!("sql/intent_write/effects.sql"), [&window], |row| row.get(0))?;
                        if !loaded { continue; }
                        let first = reader.scope_first_key(model, key.as_str())?;
                        let order_key = crate::value::fractional_between(None, first.as_deref()).map_err(|error| StoreError::Constraint(error.to_string()))?;
                        envelope.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), key.clone()), row: id(row_id)?, order_key });
                        prior.rows.push(PriorRow::Join { model: model.clone(), id: row_id.clone(), scope_key: key.to_string(), before: None, owned: writer.revision });
                    }
                }
                fields
            }
        };
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id(row_id)?, row: next });
    }
    for tombstone in &writes.tombstones {
        if let Some(row) = PriorRow::destroyed(reader, &tombstone.model, &tombstone.row_id)? { prior.rows.push(row); }
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&tombstone.model), id: id(&tombstone.row_id)?, reason: DestroyReason::Local });
    }
    for touch in &writes.touches {
        envelope.entity_ops.push(EntityOp::Touch { model: ModelKey::new(&touch.model), id: id(&touch.row_id)? });
    }
    for member in &writes.members {
        let scope = ScopeRef::new(ModelKey::new(&member.model), ListKey::from_raw(&member.list));
        let before = reader.scope_member_key(&member.model, &member.list, &member.row_id)?;
        if member.leave {
            if before.is_some() { envelope.scope_ops.push(ScopeOp::Detach { scope, row: id(&member.row_id)? }); }
            prior.rows.push(PriorRow::Leave { model: member.model.clone(), id: member.row_id.clone(), scope_key: member.list.clone(), before, owned: writer.revision });
        } else {
            let present = writes.rows.iter().any(|row| row.model == member.model && row.row_id == member.row_id) || reader.point(&member.model, &member.row_id)?.is_some();
            if before.is_none() && present { envelope.scope_ops.push(ScopeOp::Append { scope, row: id(&member.row_id)?, order_key: member.order_key.clone() }); }
            prior.rows.push(PriorRow::Join { model: member.model.clone(), id: member.row_id.clone(), scope_key: member.list.clone(), before, owned: writer.revision });
        }
    }
    Ok((envelope, prior))
}

pub(super) fn planned(writer: &mut RowWrite<'_>, writes: &IntentEffects) -> Result<CommitResult, StoreError> {
    let reader = Reader::new(writer.connection, writer.models);
    let (mut envelope, _) = effects(&reader, writes, writer)?;
    envelope.writes_local = true;
    envelope.validate(writer.generation)?;
    super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals))
}

pub(super) fn intent(writer: &mut RowWrite<'_>, op_id: u64, model: &str, row_id: &str, action: &str, payload: &str, writes: &IntentEffects) -> Result<CommitResult, StoreError> {
    let reader = Reader::new(writer.connection, writer.models);
    let (mut envelope, prior) = effects(&reader, writes, writer)?;
    let seq = i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
    let prior = crate::store::plan::op_effect::read_prior(writer.connection, seq)?.unwrap_or(serde_json::to_value(prior)?);
    let row_id = id(row_id)?;
    envelope.snapshot = Some(RowRef::new(ModelKey::new(model), row_id.clone()));
    envelope.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
        seq: Some(i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?),
        action_key: Operation::action_key(model, action),
        status: OperationStatus::Pending,
        row_ids: vec![row_id.clone()],
        temp_id: row_id.is_temp().then_some(row_id),
        input: serde_json::from_str(payload)?,
        transport_started: false,
        base_revision: Some(writer.revision - 1),
        created_at: writer.now,
        dedupe_key: None,
        rollback: Some(prior),
        upload: None,
    })));
    envelope.validate(writer.generation)?;
    super::apply::apply_envelope_at(writer.connection, writer.models, &envelope, writer.now, writer.revision, Some(writer.originals))
}
