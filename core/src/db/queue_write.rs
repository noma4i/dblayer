//! The executor `Write` for the ops of the queue: `Intent`, `RollBack`, `RemoveIntent`, `MarkError`, `Rows`, `LocalRows`, `RemoveRows`,
//! `Upload`, `RemoveUpload`. The owners Outbox and Transfer decide, this code turns their ops into SQL through the commit path of `Db`.

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, OperationRecord, OperationTransition, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ListKey, ModelKey, RowId};
use crate::operation::{Operation, OperationStatus};
use crate::files::Location;
use crate::owners::store::{EffectRow, IntentEffects, RowValue, StoreOp, UploadKind, UploadRow};
use crate::owners::write::outbox::{NewOp, Origin};
use crate::owners::write::transfer::{Source, Steps, StoredTransfer};
use crate::runtime::Time;
use crate::schema::OnFailure;

use crate::lifecycle::authority::Authority;
use super::error::DbError;
use super::local_write::{plan_fault, read_outbox};
use super::swap::TempReplacement;
use super::open::Db;
use super::prior::{Prior, PriorRow};

fn json_fault(error: serde_json::Error) -> DbError { DbError::Internal(error.to_string()) }

fn row_id(value: &str) -> Result<RowId, DbError> { RowId::new(value).map_err(|error| DbError::InvalidUpload(error.to_string())) }

fn values(text: &str) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(text).map_err(json_fault)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::InvalidUpload("the values of a row are not an object".into())),
    }
}

fn upserts(model: &str, rows: &[RowValue]) -> Result<Vec<EntityOp>, DbError> {
    rows.iter().map(|row| Ok(EntityOp::Upsert { model: ModelKey::new(model), id: row_id(&row.row_id)?, row: values(&row.values)? })).collect()
}

impl Db {
    /// Applies the ops of one batch. The batch of an operation holds the ops of one commit kind, so each op is one commit.
    pub(crate) fn write_queue_ops(&self, ops: &[StoreOp]) -> Result<(), DbError> {
        let generation = self.generation();
        let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
        let mut rolled_back = Vec::new();
        let mut swaps: Vec<(&str, RowId, RowId)> = Vec::new();
        let mut landed_op = -1;
        for op in ops {
            match op {
                StoreOp::RollBack { op_id } => rolled_back.push(i64::try_from(*op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?),
                StoreOp::RemoveIntent { op_id } if rolled_back.contains(&i64::try_from(*op_id).unwrap_or(i64::MAX)) => {}
                StoreOp::RemoveIntent { op_id } => {
                    landed_op = i64::try_from(*op_id).unwrap_or(i64::MAX);
                    envelope.operation_transitions.push(OperationTransition::Close { seq: i64::try_from(*op_id).unwrap_or(i64::MAX), status: OperationStatus::Committed });
                }
                StoreOp::Rows { model, rows } => {
                    envelope.origin = WriteOrigin::Response;
                    envelope.entity_ops.extend(upserts(model, rows)?);
                }
                StoreOp::LocalRows { model, rows } | StoreOp::EphemeralRows { model, rows } | StoreOp::LocalPending { model, rows } => {
                    envelope.writes_local = true;
                    envelope.entity_ops.extend(upserts(model, rows)?);
                }
                StoreOp::RemoveRows { model, ids } => {
                    for id in ids {
                        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: row_id(id)?, reason: DestroyReason::Local });
                    }
                }
                StoreOp::MarkError { op_id, error } => {
                    let seq = i64::try_from(*op_id).unwrap_or(i64::MAX);
                    envelope.operation_transitions.push(match error {
                        Some(message) => OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, message.clone()) },
                        None => OperationTransition::Retry { seq },
                    });
                }
                StoreOp::Swap { model, temp, real } => swaps.push((model.as_str(), row_id(temp)?, row_id(real)?)),
                StoreOp::Upload { row, kind } => self.write_upload_row(row, *kind)?,
                StoreOp::RemoveUpload { key } => self.remove_upload_row(key)?,
                StoreOp::Planned { effects } => return self.write_planned(effects),
                StoreOp::Intent { op_id, model, row_id: id, action, payload, effects } => return self.write_intent(*op_id, (model, id, action), payload, effects),
                StoreOp::Window { key, model, page, replace_all, cursor, roots, related, total, base_revision } => {
                    let key = crate::owners::read::windows::WindowKey { model: model.clone(), name: key.clone() };
                    return self.land_window_page(&super::land_window::WindowPage { key: &key, page: *page, replace_all: *replace_all, cursor: cursor.as_deref(), roots, related, total: *total, base_revision: *base_revision });
                }
                StoreOp::Pending { .. } | StoreOp::RemovePending | StoreOp::EraseSession | StoreOp::Answer { .. } | StoreOp::RemoveAnswer { .. } | StoreOp::Session { .. } | StoreOp::Attempt { .. } => {}
            }
        }
        for seq in rolled_back {
            self.roll_back_gated(&Authority::Owner, seq, generation, None)?;
        }
        if envelope.entity_ops.is_empty() && envelope.operation_transitions.is_empty() && swaps.is_empty() {
            return Ok(());
        }
        self.commit_following(&Authority::Owner, generation, |reader| {
            let mut planned = envelope.clone();
            if !swaps.is_empty() {
                let outbox = read_outbox(reader)?;
                for (model, temp, real) in &swaps {
                    if reader.point(model, temp.as_str())?.is_some() {
                        self.swap_temp(reader, &mut planned, &outbox, TempReplacement { model, temp, real }, landed_op)?;
                    }
                }
            }
            Ok::<_, DbError>(Some(planned))
        }).map_err(plan_fault)?;
        Ok(())
    }

    /// `Intent`: the optimistic rows, the membership, the tombstones and the operation, in one commit. The prior state the rollback restores is read here.
    fn write_intent(&self, op_id: u64, (model, id, action): (&str, &str, &str), payload: &str, effects: &IntentEffects) -> Result<(), DbError> {
        let generation = self.generation();
        let seq = i64::try_from(op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?;
        let created_at = self.shared.store.lock().now_ms();
        let input: Value = serde_json::from_str(payload).map_err(json_fault)?;
        let owner = row_id(id)?;
        let (result, _) = self
            .commit_following_deferred(&Authority::Owner, generation, |reader| {
                let (mut envelope, prior) = effects_envelope(reader, effects, generation)?;
                envelope.snapshot = Some(crate::identity::RowRef::new(ModelKey::new(model), owner.clone()));
                envelope.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
                    seq: Some(seq),
                    action_key: Operation::action_key(model, action),
                    status: OperationStatus::Pending,
                    row_ids: vec![owner.clone()],
                    temp_id: owner.is_temp().then(|| owner.clone()),
                    input: input.clone(),
                    transport_started: false,
                    base_revision: Some(reader.revision_seq()?),
                    created_at,
                    dedupe_key: None,
                    rollback: Some(serde_json::to_value(&prior).map_err(json_fault)?),
                    upload: None,
                })));
                Ok::<_, DbError>(Some(envelope))
            })
            .map_err(plan_fault)?;
        result.publish();
        Ok(())
    }

    /// `Planned`: a write without an operation (a local model, an action without a mutation): the rows are final, nothing is queued and nothing is rolled back.
    fn write_planned(&self, effects: &IntentEffects) -> Result<(), DbError> {
        let generation = self.generation();
        let (result, _) = self
            .commit_following_deferred(&Authority::Owner, generation, |reader| {
                let (mut envelope, _) = effects_envelope(reader, effects, generation)?;
                envelope.writes_local = true;
                Ok::<_, DbError>(Some(envelope))
            })
            .map_err(plan_fault)?;
        result.publish();
        Ok(())
    }
}

/// The envelope of the effects of one call and the prior state of everything it changes. A row that exists takes the fields of the effect over its own.
fn effects_envelope(reader: &crate::store::Reader<'_>, effects: &IntentEffects, generation: crate::identity::Generation) -> Result<(CommitEnvelope, Prior), DbError> {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
    let mut prior = Prior::default();
    let owned = reader.revision_seq()? + 1;
    for EffectRow { model: row_model, row_id: row_key, values: text } in &effects.rows {
        let fields = values(text)?;
        let next = match reader.point(row_model, row_key)? {
            Some(current) => {
                let mut merged = current.clone();
                merged.extend(fields);
                prior.rows.push(PriorRow::edited(reader, row_model, row_key, &current, &merged)?);
                merged
            }
            None => {
                prior.rows.push(PriorRow::Created { model: row_model.clone(), id: row_key.clone() });
                fields
            }
        };
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(row_model), id: row_id(row_key)?, row: next });
    }
    for tombstone in &effects.tombstones {
        if let Some(destroyed) = PriorRow::destroyed(reader, &tombstone.model, &tombstone.row_id)? {
            prior.rows.push(destroyed);
        }
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&tombstone.model), id: row_id(&tombstone.row_id)?, reason: DestroyReason::Local });
    }
    for touch in &effects.touches {
        envelope.entity_ops.push(EntityOp::Touch { model: ModelKey::new(&touch.model), id: row_id(&touch.row_id)? });
    }
    for member in &effects.members {
        let scope = ScopeRef::new(ModelKey::new(&member.model), ListKey::from_raw(&member.list));
        let before = reader.scope_member_key(&member.model, &member.list, &member.row_id)?;
        if member.leave {
            if before.is_some() { envelope.scope_ops.push(ScopeOp::Detach { scope, row: row_id(&member.row_id)? }); }
            prior.rows.push(PriorRow::Leave { model: member.model.clone(), id: member.row_id.clone(), scope_key: member.list.clone(), before, owned });
        } else {
            if before.is_none() { envelope.scope_ops.push(ScopeOp::Append { scope, row: row_id(&member.row_id)?, order_key: member.order_key.clone() }); }
            prior.rows.push(PriorRow::Join { model: member.model.clone(), id: member.row_id.clone(), scope_key: member.list.clone(), before, owned });
        }
    }
    Ok((envelope, prior))
}
