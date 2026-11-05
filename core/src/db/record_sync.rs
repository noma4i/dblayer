use rusqlite::OptionalExtension;
use crate::commit::{CommitEnvelope, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::identity::RowId;
#[cfg(test)]
use crate::syncstate::{read_sync, SyncInfo};

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

    pub fn retry(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, action: Option<&str>) -> Result<usize, DbError> {
        let key = self.action_operation_key(model, action)?;
        let stuck = self.stuck_operations(model, id, key.as_deref())?;
        Ok(self.retry_operations(admitted, stuck)? + self.retry_upload_items(admitted, Some((model, id, key.as_deref())))?)
    }

    pub fn retry_all(&self, admitted: &crate::ffi::legacy::Admitted) -> Result<usize, DbError> {
        let stuck: Vec<OutboxOperation> = self.outbox()?.into_iter().filter(is_stuck).collect();
        Ok(self.retry_operations(admitted, stuck)? + self.retry_upload_items(admitted, None)?)
    }

    fn retry_upload_items(&self, admitted: &crate::ffi::legacy::Admitted, target: Option<(&str, &RowId, Option<&str>)>) -> Result<usize, DbError> {
        let generation = self.generation();
        let mut retried = 0;
        self.commit_following(&admitted.authority(), generation, |reader| {
            retried = 0;
            let items = retried_upload_items(reader, target)?;
            retried = items.len();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            envelope.operation_transitions.extend(items.into_iter().map(|(seq, item_position, state)| OperationTransition::Progress { seq, item_position, state: Box::new(state) }));
            Ok::<_, DbError>(Some(envelope))
        }).map_err(super::local_write::plan_fault)?;
        Ok(retried)
    }

    pub(crate) fn retry_operations(&self, admitted: &crate::ffi::legacy::Admitted, stuck: Vec<OutboxOperation>) -> Result<usize, DbError> {
        if stuck.is_empty() {
            return Ok(0);
        }
        if stuck.iter().any(|entry| entry.operation.error.as_ref().is_some_and(|error| error.kind == crate::syncstate::FailureKind::MissingFile)) {
            return Err(DbError::InvalidUpload("select the file again".into()));
        }
        let generation = self.generation();
        self.commit_following(&admitted.authority(), generation, |_| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops = stuck.iter().filter(|entry| is_failed_destroy(entry)).filter_map(|entry| Prior::of(entry).transpose()).collect::<Result<Vec<_>, _>>()?.into_iter().flat_map(|prior| prior.hiding()).collect();
                envelope.operation_transitions.extend(stuck.iter().map(|entry| OperationTransition::Retry { seq: entry.operation.id.0 }));
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(stuck.len())
    }

    pub fn discard(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, action: Option<&str>) -> Result<usize, DbError> {
        let key = self.action_operation_key(model, action)?;
        if self.record_operations(model, id, key.as_deref())?.iter().any(is_sending) {
            return Err(DbError::InFlight { model: model.to_string(), id: id.to_string() });
        }
        let mut discarded = 0;
        while let Some(entry) = self.record_operations(model, id, key.as_deref())?.first() {
            let batched = self.spec(model)?.is_batch_upload(&entry.operation.action_key);
            if batched && entry.operation.status != OperationStatus::DeliveryUnknown {
                self.remove_upload_item(admitted, model, id, entry.operation.id.0, true)?;
            } else {
                self.roll_back(admitted, entry.operation.id.0)?;
            }
            discarded += 1;
        }
        Ok(discarded)
    }

    /// The operation key of a declared action: ordinary actions and upload actions are told apart by the declaration. `None` selects the whole record, an unknown action never does.
    pub(crate) fn action_operation_key(&self, model: &str, action: Option<&str>) -> Result<Option<String>, DbError> {
        let spec = self.shared.spec(model)?;
        let Some(action) = action else { return Ok(None) };
        let declared = spec.actions.iter().find(|declared| declared.name == action).ok_or_else(|| DbError::UnknownAction { model: model.to_string(), action: action.to_string() })?;
        Ok(Some(if declared.upload { crate::operation::Operation::upload_action_key(model, action) } else { crate::operation::Operation::action_key(model, action) }))
    }

    fn stuck_operations(&self, model: &str, id: &RowId, key: Option<&str>) -> Result<Vec<OutboxOperation>, DbError> {
        Ok(self.record_operations(model, id, key)?.into_iter().filter(is_stuck).collect())
    }

    fn record_operations(&self, model: &str, id: &RowId, key: Option<&str>) -> Result<Vec<OutboxOperation>, DbError> {
        self.shared.spec(model)?;
        let all = self.shared.read(&[], |reader| Ok(OutboxOperation::for_rows(reader, &[(model.to_string(), id.to_string())].into())?))?;
        Ok(all.into_iter().filter(|entry| key.is_none_or(|key| entry.operation.action_key == key)).collect())
    }
}

/// The failed upload items of a record (or of every record) moved back to sending.
fn retried_upload_items(reader: &crate::store::Reader<'_>, target: Option<(&str, &RowId, Option<&str>)>) -> Result<Vec<(i64, i64, crate::upload::UploadState)>, DbError> {
    let connection = reader.connection();
    let mut retried = Vec::new();
    let uploads = match target {
        Some((model, id, _)) => {
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
        let (action, status, row): (String, String, String) = connection.query_row("SELECT op.action_key, op.status, op_item.row_id FROM op JOIN op_item USING (seq) WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        if status != "pending" || target.is_some_and(|(model, id, key)| row != id.as_str() || key.is_some_and(|key| action != key) || crate::operation::Operation::split_action_key(&action).is_none_or(|(owner, _)| owner != model)) { continue; }
        if state.apply(crate::upload::UploadEvent::Retried).is_err() { continue; }
        retried.push((seq, position, state));
    }
    retried.sort_by_key(|(seq, position, _)| (*seq, *position));
    Ok(retried)
}
