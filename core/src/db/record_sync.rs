use rusqlite::OptionalExtension;
use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::RowId;
#[cfg(test)]
use crate::syncstate::{read_sync, SyncInfo};
#[cfg(test)]
use crate::upload::UploadInfo;

use super::error::DbError;
use super::prior::Prior;
use super::local_write::{plan_fault, OutboxOperation};
use super::open::Db;

fn is_stuck(entry: &OutboxOperation) -> bool {
    matches!(entry.operation.status, OperationStatus::Failed | OperationStatus::DeliveryUnknown)
}

fn is_sending(entry: &OutboxOperation) -> bool {
    entry.operation.status == OperationStatus::Pending && entry.operation.transport_started
}

fn is_failed_destroy(entry: &OutboxOperation) -> bool {
    entry.operation.status == OperationStatus::Failed && entry.operation.action_key.ends_with(":destroy")
}

impl Db {
    #[cfg(test)]
    pub fn sync_info(&self, model: &str, id: &RowId) -> Result<SyncInfo, DbError> {
        self.shared.spec(model)?;
        self.shared.read(&[model], |reader| Ok(read_sync(reader.connection(), model, &[id.to_string()])?.remove(id.as_str()).unwrap_or_default()))
    }

    #[cfg(test)]
    pub fn upload_info(&self, model: &str, id: &RowId) -> Result<Option<UploadInfo>, DbError> {
        Ok(self.sync_info(model, id)?.upload)
    }

    pub fn retry(&self, model: &str, id: &RowId) -> Result<usize, DbError> {
        let stuck = self.stuck_operations(model, id)?;
        Ok(self.retry_operations(stuck)? + self.retry_upload_items(Some((model, id)))?)
    }

    pub fn retry_all(&self) -> Result<usize, DbError> {
        let stuck: Vec<OutboxOperation> = self.outbox()?.into_iter().filter(is_stuck).collect();
        Ok(self.retry_operations(stuck)? + self.retry_upload_items(None)?)
    }

    fn retry_upload_items(&self, target: Option<(&str, &RowId)>) -> Result<usize, DbError> {
        let generation = self.generation();
        let mut retried = 0;
        self.commit_following(generation, |reader| {
            let connection = reader.connection();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            let uploads = match target {
                Some((model, id)) => {
                    let keys = [(model.to_string(), id.to_string())].into();
                    let mut uploads = std::collections::HashMap::new();
                    for seq in crate::operation::row_operation_sequences(connection, &keys)? {
                        uploads.extend(crate::operation::read_operation_uploads(connection, seq)?);
                    }
                    uploads
                }
                None => crate::operation::read_upload_items(connection)?,
            };
            for ((seq, position), mut state) in uploads {
                if state.phase != crate::upload::UploadStatus::Failed { continue; }
                let (action, status, row): (String, String, String) = connection.query_row("SELECT op.action_key, op.status, op_item.row_id FROM op JOIN op_item USING (seq) WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
                if status != "pending" || target.is_some_and(|(model, id)| row != id.as_str() || crate::operation::Operation::split_action_key(&action).is_none_or(|(owner, _)| owner != model)) { continue; }
                state.phase = crate::upload::UploadStatus::Queued;
                state.error = None;
                state.failed_at = None;
                state.retries.clear();
                envelope.operation_transitions.push(OperationTransition::Progress { seq, item_position: position, state });
                retried += 1;
            }
            Ok::<_, DbError>(Some(envelope))
        }).map_err(super::local_write::plan_fault)?;
        Ok(retried)
    }

    pub(crate) fn retry_operations(&self, stuck: Vec<OutboxOperation>) -> Result<usize, DbError> {
        if stuck.is_empty() {
            return Ok(0);
        }
        let generation = self.generation();
        self.commit_following(generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops = stuck.iter().filter(|entry| is_failed_destroy(entry)).filter_map(Prior::of).flat_map(|prior| prior.hiding()).collect();
                envelope.operation_transitions.extend(stuck.iter().map(|entry| OperationTransition::Retry { seq: entry.operation.id.0 }));
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(stuck.len())
    }

    pub(crate) fn failed_upload_candidates(&self, due: i64) -> Result<Vec<(i64, String, String, String)>, DbError> {
        Ok(self.shared.read(&[], |reader| {
            let mut statement = reader.connection().prepare_cached(
                "SELECT op.seq, op.action_key, op.status, op_item.row_id FROM op JOIN op_item USING (seq) \
                 WHERE (op.status = 'failed' AND op.since <= ?1) \
                    OR (op.status = 'pending' AND op_item.phase = 'failed' AND op_item.failed_at <= ?1) ORDER BY op.seq, op_item.position",
            )?;
            let rows = statement.query_map([due], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })?)
    }

    pub(crate) fn expire_candidates(&self, candidates: Vec<(i64, String, String, String)>, due: i64, running: &[i64]) -> Result<usize, DbError> {
        let mut removed = 0;
        let mut rolled_back = std::collections::BTreeSet::new();
        for (seq, action_key, status, row) in candidates {
            if running.contains(&seq) { continue; }
            let Some((model, _)) = crate::operation::Operation::split_action_key(&action_key) else { continue };
            let batched = self.spec(&model)?.is_batch_upload(&action_key);
            if batched && status != "delivery_unknown" {
                let id = RowId::new(&row).map_err(|error| DbError::InvalidUpload(error.to_string()))?;
                let item = row.clone();
                let still = move |reader: &crate::store::Reader<'_>, entry: &OutboxOperation| -> Result<bool, crate::store::StoreError> {
                    match entry.operation.status {
                        OperationStatus::Failed => Ok(entry.operation.since <= due),
                        OperationStatus::Pending => {
                            let Some(position) = reader.connection().query_row("SELECT position FROM op_item WHERE seq = ?1 AND row_id = ?2", rusqlite::params![seq, item], |found| found.get::<_, i64>(0)).optional()? else { return Ok(false) };
                            let states = crate::operation::read_operation_uploads(reader.connection(), seq)?;
                            Ok(states.get(&(seq, position)).is_some_and(|state| state.phase == crate::upload::UploadStatus::Failed && state.failed_at.is_some_and(|failed| failed as i64 <= due)))
                        }
                        _ => Ok(false),
                    }
                };
                if self.remove_upload_item_if(&model, &id, seq, true, Some(&still))? { removed += 1; }
            } else if rolled_back.insert(seq) {
                let still = |_: &crate::store::Reader<'_>, entry: &OutboxOperation| Ok(entry.operation.status == OperationStatus::Failed && entry.operation.since <= due);
                if !self.roll_back_if(seq, self.generation(), Some(&still))?.is_noop() { removed += 1; }
            }
        }
        Ok(removed)
    }

    pub fn discard(&self, model: &str, id: &RowId) -> Result<usize, DbError> {
        if self.record_operations(model, id)?.iter().any(is_sending) {
            return Err(DbError::InFlight { model: model.to_string(), id: id.to_string() });
        }
        let mut discarded = 0;
        while let Some(entry) = self.record_operations(model, id)?.first() {
            let batched = self.spec(model)?.is_batch_upload(&entry.operation.action_key);
            if batched && entry.operation.status != OperationStatus::DeliveryUnknown {
                self.remove_upload_item(model, id, entry.operation.id.0, true)?;
            } else {
                self.roll_back(entry.operation.id.0)?;
            }
            discarded += 1;
        }
        Ok(discarded)
    }

    fn stuck_operations(&self, model: &str, id: &RowId) -> Result<Vec<OutboxOperation>, DbError> {
        Ok(self.record_operations(model, id)?.into_iter().filter(is_stuck).collect())
    }

    fn record_operations(&self, model: &str, id: &RowId) -> Result<Vec<OutboxOperation>, DbError> {
        self.shared.spec(model)?;
        self.shared.read(&[], |reader| Ok(OutboxOperation::for_rows(reader, &[(model.to_string(), id.to_string())].into())?))
    }
}
