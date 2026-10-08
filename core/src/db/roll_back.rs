use serde_json::Value;

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::{ModelKey, RowId};

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

impl Db {
    pub fn roll_back(&self, seq: i64) -> Result<CommitResult, DbError> {
        let entry = self.outbox()?.into_iter().find(|entry| entry.operation.id.0 == seq).ok_or(DbError::UnknownOperation(seq))?;
        let (model, action) = entry.operation.action_key.split_once(':').ok_or_else(|| DbError::NotRollbackable(entry.operation.action_key.clone()))?;
        let model = ModelKey::new(model);
        let row_id = entry.operation.row_ids.first().cloned();
        let generation = self.generation();
        self.shared
            .store
            .commit_with(generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                match (action, &entry.rollback, row_id) {
                    ("create", _, Some(id)) => {
                        envelope.entity_ops.push(EntityOp::Destroy { model: model.clone(), id, reason: DestroyReason::Expired });
                    }
                    ("update", Some(Value::Object(patch)), Some(id)) => {
                        if let Some(mut current) = reader.point(model.as_str(), id.as_str())? {
                            current.extend(patch.clone());
                            envelope.entity_ops.push(EntityOp::Upsert { model: model.clone(), id, row: current });
                        }
                    }
                    ("destroy", Some(snapshot), _) => {
                        for stored in snapshot.get("rows").and_then(Value::as_array).into_iter().flatten() {
                            let (Some(name), Some(Value::Object(row))) = (stored.get("model").and_then(Value::as_str), stored.get("row")) else { continue };
                            let Some(id) = row.get("id").and_then(crate::value::read_id) else { continue };
                            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(name), id: RowId::new(id.as_str())?, row: row.clone() });
                        }
                    }
                    _ => return Err(DbError::NotRollbackable(entry.operation.action_key.clone())),
                }
                envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::RolledBack, once: false });
                Ok(Some(envelope))
            })
            .map_err(plan_fault)
    }
}
