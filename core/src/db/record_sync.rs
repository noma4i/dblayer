use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::RowId;
use crate::syncstate::{operation_keys, read_sync, SyncInfo};
use crate::upload::UploadInfo;

use super::error::DbError;
use super::local_write::{plan_fault, OutboxOperation};
use super::open::Db;

fn is_stuck(entry: &OutboxOperation) -> bool {
    matches!(entry.operation.status, OperationStatus::Failed | OperationStatus::DeliveryUnknown)
}

impl Db {
    pub fn sync_info(&self, model: &str, id: &RowId) -> Result<SyncInfo, DbError> {
        self.shared.spec(model)?;
        self.shared.read(|reader| Ok(read_sync(reader.connection(), model, &[id.to_string()])?.remove(id.as_str()).unwrap_or_default()))
    }

    pub fn upload_info(&self, model: &str, id: &RowId) -> Result<Option<UploadInfo>, DbError> {
        Ok(self.sync_info(model, id)?.upload)
    }

    pub fn retry(&self, model: &str, id: &RowId) -> Result<usize, DbError> {
        let stuck = self.stuck_operations(model, id)?;
        self.retry_operations(&stuck)
    }

    pub fn retry_all(&self) -> Result<usize, DbError> {
        let stuck: Vec<i64> = self.outbox()?.iter().filter(|entry| is_stuck(entry)).map(|entry| entry.operation.id.0).collect();
        self.retry_operations(&stuck)
    }

    fn retry_operations(&self, stuck: &[i64]) -> Result<usize, DbError> {
        if stuck.is_empty() {
            return Ok(0);
        }
        let generation = self.generation();
        self.shared
            .store
            .commit_with(generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.operation_transitions.extend(stuck.iter().map(|seq| OperationTransition::Retry { seq: *seq }));
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(stuck.len())
    }

    pub fn discard(&self, model: &str, id: &RowId) -> Result<usize, DbError> {
        let mut discarded = 0;
        while let Some(seq) = self.stuck_operations(model, id)?.first() {
            self.roll_back(*seq)?;
            discarded += 1;
        }
        Ok(discarded)
    }

    fn stuck_operations(&self, model: &str, id: &RowId) -> Result<Vec<i64>, DbError> {
        self.shared.spec(model)?;
        let wanted = (model.to_string(), id.to_string());
        Ok(self
            .outbox()?
            .into_iter()
            .filter(is_stuck)
            .filter(|entry| operation_keys(&entry.operation.action_key, &entry.operation.row_ids, entry.operation.temp_id.as_ref()).contains(&wanted))
            .map(|entry| entry.operation.id.0)
            .collect())
    }
}
