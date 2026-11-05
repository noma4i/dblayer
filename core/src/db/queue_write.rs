//! The executor `Write` of `store.md` for the ops of the queue: `Intent`, `RollBack`, `RemoveIntent`, `MarkError`, `Rows`, `LocalRows`, `RemoveRows`,
//! `Upload`, `RemoveUpload`. The owners Outbox and Transfer decide, this code turns their ops into SQL through the commit path of `Db`.

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, OperationRecord, OperationTransition, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ListKey, ModelKey, RowId};
use crate::operation::{Operation, OperationStatus};
use crate::owners::store::{EffectRow, IntentEffects, RowValue, StoreOp, UploadRow};

use super::error::DbError;
use super::local_write::plan_fault;
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
    pub(crate) fn write_queue_ops(&self, admitted: &crate::ffi::legacy::Admitted, ops: &[StoreOp]) -> Result<(), DbError> {
        let generation = self.generation();
        let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
        let mut rolled_back = Vec::new();
        for op in ops {
            match op {
                StoreOp::RollBack { op_id } => rolled_back.push(i64::try_from(*op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?),
                StoreOp::RemoveIntent { op_id } if rolled_back.contains(&i64::try_from(*op_id).unwrap_or(i64::MAX)) => {}
                StoreOp::RemoveIntent { op_id } => envelope.operation_transitions.push(OperationTransition::Close { seq: i64::try_from(*op_id).unwrap_or(i64::MAX), status: OperationStatus::Committed }),
                StoreOp::Rows { model, rows } => {
                    envelope.origin = WriteOrigin::Response;
                    envelope.entity_ops.extend(upserts(model, rows)?);
                }
                StoreOp::LocalRows { model, rows } => {
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
                        Some(_) => OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, error.clone().unwrap_or_default()) },
                        None => OperationTransition::Retry { seq },
                    });
                }
                StoreOp::Upload { row } => self.write_upload_row(admitted, row)?,
                StoreOp::RemoveUpload { key } => self.remove_upload_row(admitted, key)?,
                StoreOp::Intent { op_id, model, row_id: id, action, payload, effects } => return self.write_intent(admitted, *op_id, (model, id, action), payload, effects),
                StoreOp::Pending { .. } | StoreOp::RemovePending | StoreOp::Session { .. } => {}
            }
        }
        for seq in rolled_back {
            self.roll_back_at(admitted, seq, generation)?;
        }
        if envelope.entity_ops.is_empty() && envelope.operation_transitions.is_empty() {
            return Ok(());
        }
        self.commit_following(&admitted.authority(), generation, |_| Ok::<_, DbError>(Some(envelope.clone()))).map_err(plan_fault)?;
        Ok(())
    }

    /// `Intent`: the optimistic rows, the membership, the tombstones and the operation, in one commit. The prior state the rollback restores is read here.
    fn write_intent(&self, admitted: &crate::ffi::legacy::Admitted, op_id: u64, (model, id, action): (&str, &str, &str), payload: &str, effects: &IntentEffects) -> Result<(), DbError> {
        let generation = self.generation();
        let seq = i64::try_from(op_id).map_err(|_| DbError::UnknownOperation(i64::MAX))?;
        let created_at = self.shared.store.lock().now_ms();
        let input: Value = serde_json::from_str(payload).map_err(json_fault)?;
        let owner = row_id(id)?;
        let (result, _) = self
            .commit_following_deferred(&admitted.authority(), generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                let mut prior = Prior::default();
                for EffectRow { model: row_model, row_id: row_key, values: text } in &effects.rows {
                    let next = values(text)?;
                    prior.rows.push(match reader.point(row_model, row_key)? {
                        Some(current) => PriorRow::edited(reader, row_model, row_key, &current, &next)?,
                        None => PriorRow::Created { model: row_model.clone(), id: row_key.clone() },
                    });
                    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(row_model), id: row_id(row_key)?, row: next });
                }
                for tombstone in &effects.tombstones {
                    if let Some(destroyed) = PriorRow::destroyed(reader, &tombstone.model, &tombstone.row_id)? {
                        prior.rows.push(destroyed);
                    }
                    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&tombstone.model), id: row_id(&tombstone.row_id)?, reason: DestroyReason::Local });
                }
                for member in &effects.members {
                    envelope.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), ListKey::from_raw(&member.list)), row: row_id(&member.row_id)?, order_key: format!("{:020}", member.revision) });
                }
                envelope.snapshot = Some(crate::identity::RowRef::new(ModelKey::new(model), owner.clone()));
                envelope.operation_transitions.push(OperationTransition::Begin(Box::new(OperationRecord {
                    seq: Some(seq),
                    action_key: Operation::action_key(model, action),
                    status: OperationStatus::Pending,
                    row_ids: vec![owner.clone()],
                    temp_id: None,
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

    /// `Upload{row}`: the columns of `sync_upload` that exist for the key `model:row_id:action`.
    fn write_upload_row(&self, admitted: &crate::ffi::legacy::Admitted, row: &UploadRow) -> Result<(), DbError> {
        let [model, id, action] = key_parts(&row.key)?;
        let (status, stage) = match (&row.failed, row.completed) {
            (Some(_), _) => ("failed", "failed"),
            (None, true) => ("uploading", "mutating"),
            (None, false) => ("uploading", "uploading"),
        };
        self.shared
            .store
            .write_store(&admitted.authority(), |stored| {
                stored.connection.execute(
                "INSERT OR REPLACE INTO sync_upload (model, row_id, action, status, stage, sent, total, wait, attempts, error_kind, error_message) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 0, ?8, ?9)",
                rusqlite::params![model, id, action, status, stage, row.confirmed_parts, row.parts, row.failed.as_ref().map(|_| "failed"), row.failed],
                )?;
                Ok(())
            })
            .map_err(DbError::from)
    }

    fn remove_upload_row(&self, admitted: &crate::ffi::legacy::Admitted, key: &str) -> Result<(), DbError> {
        let [model, id, action] = key_parts(key)?;
        self.shared
            .store
            .write_store(&admitted.authority(), |stored| {
                stored.connection.execute("DELETE FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND action = ?3", rusqlite::params![model, id, action])?;
                Ok(())
            })
            .map_err(DbError::from)
    }
}

fn key_parts(key: &str) -> Result<[&str; 3], DbError> {
    let mut parts = key.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(model), Some(id), Some(action)) => Ok([model, id, action]),
        _ => Err(DbError::InvalidUpload(format!("transfer key `{key}` is not model:row_id:action"))),
    }
}
