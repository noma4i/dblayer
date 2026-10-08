use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId};
use crate::operation::Operation;
use crate::store::{Reader, StoreError};
use crate::value::read_id;

use super::error::DbError;
use super::local_write::{plan_fault, OutboxOperation};
use super::open::Db;

pub(crate) fn rollback_ops(reader: &Reader<'_>, model: &str, action: &str, entry: &OutboxOperation) -> Result<Vec<EntityOp>, StoreError> {
    if let Some(temp) = &entry.operation.temp_id {
        return Ok(vec![EntityOp::Destroy { model: ModelKey::new(model), id: temp.clone(), reason: DestroyReason::Expired }]);
    }
    let Some(snapshot) = &entry.rollback else { return Ok(Vec::new()) };
    if action == "destroy" {
        return Ok(destroyed_rows(snapshot));
    }
    let (Some(fields), Some(id)) = (snapshot.as_object(), entry.operation.row_ids.first()) else { return Ok(Vec::new()) };
    let Some(current) = reader.point(model, id.as_str())? else { return Ok(Vec::new()) };
    let mut restored = current.clone();
    restored.extend(fields.clone());
    if restored == current {
        return Ok(Vec::new());
    }
    Ok(vec![EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row: restored }])
}

fn destroyed_rows(snapshot: &Value) -> Vec<EntityOp> {
    let rows = snapshot.get("rows").and_then(Value::as_array).cloned().unwrap_or_default();
    rows.iter()
        .filter_map(|item| {
            let model = item.get("model")?.as_str()?;
            let row: &Map<String, Value> = item.get("row")?.as_object()?;
            let id = read_id(row.get("id")?)?;
            Some(EntityOp::Upsert { model: ModelKey::new(model), id, row: row.clone() })
        })
        .collect()
}

pub(crate) fn follows(entry: &OutboxOperation, temp: &RowId, own: i64) -> bool {
    entry.operation.id.0 != own && entry.operation.row_ids.contains(temp)
}

impl Db {
    pub fn roll_back(&self, seq: i64) -> Result<CommitResult, DbError> {
        self.roll_back_at(seq, self.generation())
    }

    pub(crate) fn roll_back_at(&self, seq: i64, generation: Generation) -> Result<CommitResult, DbError> {
        let outbox = self.outbox()?;
        let entry = outbox.iter().find(|entry| entry.operation.id.0 == seq).ok_or(DbError::UnknownOperation(seq))?;
        let (model, action) =
            Operation::split_action_key(&entry.operation.action_key).ok_or_else(|| DbError::NotRollbackable(entry.operation.action_key.clone()))?;
        let followers: Vec<&OutboxOperation> = match &entry.operation.temp_id {
            Some(temp) => outbox.iter().filter(|other| follows(other, temp, seq)).collect(),
            None => Vec::new(),
        };
        self.shared
            .store
            .commit_with(generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops = rollback_ops(reader, &model, &action, entry)?;
                envelope
                    .operation_transitions
                    .extend(followers.iter().map(|follower| OperationTransition::Remove { seq: follower.operation.id.0, expected: None }));
                envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::RolledBack, once: false });
                Ok(Some(envelope))
            })
            .map_err(plan_fault)
    }
}
