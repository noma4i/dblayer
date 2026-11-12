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

fn effects(reader: &Reader<'_>, writes: &IntentEffects, writer: &mut RowWrite<'_>) -> Result<(BatchPlan, Prior), StoreError> {
    let mut envelope = BatchPlan::new(writer.generation);
    let mut prior = Prior::default();
    let mut wanted = std::collections::BTreeMap::<String, Vec<String>>::new();
    for row in &writes.rows { wanted.entry(row.model.clone()).or_default().push(row.row_id.clone()); }
    for row in &writes.tombstones { wanted.entry(row.model.clone()).or_default().push(row.row_id.clone()); }
    for row in &writes.members { wanted.entry(row.model.clone()).or_default().push(row.row_id.clone()); }
    for (model, ids) in wanted { writer.prefetch(&model, &ids, writer.closing)?; }
    for EffectRow { model, row_id, values: text } in &writes.rows {
        let fields = values(text)?;
        let spec = &writer.models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        let next = match writer.stored(model, row_id)?.map(|stored| stored.row) {
            Some(current) => {
                let mut merged = current.clone();
                merged.extend(fields);
                super::merge::own_values(spec, Some(&current), &mut merged);
                let mut entry = PriorRow::edited(reader, model, row_id, &current, &merged)?;
                if let PriorRow::Edited { owned, .. } = &mut entry { for revision in owned.values_mut() { *revision = writer.revision; } }
                prior.rows.push(entry);
                merged
            }
            None => {
                prior.rows.push(PriorRow::Created { model: model.clone(), id: row_id.clone() });
                let mut fields = fields;
                super::merge::own_values(spec, None, &mut fields);
                fields
            }
        };
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id(row_id)?, row: next });
    }
    for tombstone in &writes.tombstones {
        if let Some(row) = PriorRow::destroyed(reader, &tombstone.model, &tombstone.row_id, writer.stored(&tombstone.model, &tombstone.row_id)?.map(|stored| stored.row))? { prior.rows.push(row); }
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
            let present = writes.rows.iter().any(|row| row.model == member.model && row.row_id == member.row_id) || writer.stored(&member.model, &member.row_id)?.is_some();
            if before.is_none() && present { envelope.scope_ops.push(ScopeOp::Append { scope, row: id(&member.row_id)?, order_key: member.order_key.clone() }); }
            prior.rows.push(PriorRow::Join { model: member.model.clone(), id: member.row_id.clone(), scope_key: member.list.clone(), before, owned: writer.revision });
        }
    }
    Ok((envelope, prior))
}

pub(super) fn planned(writer: &mut RowWrite<'_>, writes: &IntentEffects) -> Result<CommitResult, StoreError> {
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let (mut envelope, _) = effects(&reader, writes, writer)?;
    envelope.source = crate::schema::RowSource::Local;
    writer.apply(&envelope)
}

pub(super) fn intent(writer: &mut RowWrite<'_>, op_id: u64, (model, row_id, action): (&str, &str, &str), payload: &str, writes: &IntentEffects, (append, created_at): (bool, u64)) -> Result<CommitResult, StoreError> {
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let (mut envelope, prior) = effects(&reader, writes, writer)?;
    envelope.source = crate::schema::RowSource::Create;
    let seq = i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
    let held = crate::store::plan::op_effect::read_prior(writer.connection, seq)?;
    let prior = match (held, append) {
        (Some(held), true) => {
            let mut held = Prior::decode(held)?;
            for next in prior.rows {
                if let PriorRow::Edited { model, id, before, owned } = &next {
                    if let Some(PriorRow::Edited { before: previous, owned: revisions, .. }) = held.rows.iter_mut().find(|row| matches!(row, PriorRow::Edited { model: m, id: i, .. } if m == model && i == id)) {
                        for (field, value) in before { previous.entry(field.clone()).or_insert_with(|| value.clone()); }
                        revisions.extend(owned.clone());
                        continue;
                    }
                }
                if let PriorRow::Join { model, id, scope_key, owned, .. } | PriorRow::Leave { model, id, scope_key, owned, .. } = &next {
                    if let Some(PriorRow::Join { owned: revision, .. } | PriorRow::Leave { owned: revision, .. }) = held.rows.iter_mut().find(|row| matches!(row, PriorRow::Join { model: m, id: i, scope_key: s, .. } | PriorRow::Leave { model: m, id: i, scope_key: s, .. } if m == model && i == id && s == scope_key)) {
                        *revision = *owned;
                        continue;
                    }
                }
                held.rows.push(next);
            }
            serde_json::to_value(held)?
        }
        (Some(held), false) => held,
        (None, _) => serde_json::to_value(prior)?,
    };
    let row_id = id(row_id)?;
    envelope.snapshot = Some(RowRef::new(ModelKey::new(model), row_id.clone()));
    envelope.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
        seq: Some(i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?),
        action_key: Operation::action_key(model, action),
        status: OperationStatus::Pending,
        row_ids: vec![row_id.clone()],
        temp_id: row_id.is_temp().then_some(row_id),
        input: serde_json::from_str(payload)?,
        base_revision: Some(writer.revision - 1),
        created_at: i64::try_from(created_at).map_err(|_| StoreError::Constraint("operation time is too large".into()))?,
        dedupe_key: None,
        rollback: Some(prior),
    })));
    writer.apply(&envelope)
}
