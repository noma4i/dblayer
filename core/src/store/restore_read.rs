//! The intents of the queue as `Restored` gives them to the owner Outbox.

use crate::operation::OperationStatus;
use crate::owners::write::outbox::{NewOp, OpSource, StoredOp};
use crate::schema::OnFailure;
use crate::transport::RequestOwner;

use super::db_error::DbError;
use super::plan::read_outbox;
use super::Store;
use super::upload_rows::UploadRaw;

pub(crate) struct Restore {
    pub ops: Result<Vec<StoredOp>, DbError>,
    pub transfers: Result<Vec<crate::owners::write::transfer::StoredTransfer>, DbError>,
    /// `(upload_id, key)` of the multipart uploads of removed transfers that wait for the answer of `abort`.
    pub orphans: Vec<(String, String)>,
    /// The rule of every batch that still has an element in the base.
    pub batches: std::collections::BTreeMap<u64, crate::owners::write::transfer::Joint>,
    /// The highest `op_id` a transfer reserved: Outbox gives no `op_id` at or below it.
    pub reserved: Option<u64>,
    /// The saved answers that no operation of the queue owns.
    pub orphan_answers: Vec<u64>,
}

impl Store {
    pub fn failed_restores(&self) -> Result<Vec<super::DroppedRecord>, DbError> {
        Ok(self.restore_failed()?.into_iter().map(|row| super::DroppedRecord { table: row.table, key: row.key, reason: row.reason }).collect())
    }


    /// The `on_failure` rule comes from the operation the model declares for `(model, action)`: the base has no column for it. An action that declares no mutation is an error that names it.

    pub(crate) fn read_restore(&self) -> Result<Restore, DbError> {
        let (entries, pending, uploads, answers, orphans, batches, orphan_answers) = self.read_models(&[], |reader| {
            let connection = reader.connection();
            let pending = crate::transport::encrypted::state::pending(connection).map_err(crate::store::StoreError::from)?;
            let answers = crate::transport::encrypted::state::answers_of_operations(connection).map_err(crate::store::StoreError::from)?;
            let uploads = super::upload_rows::upload_rows(connection).map_err(crate::store::StoreError::from)?;
            let orphans = connection.prepare("SELECT upload_id, key FROM upload_orphan ORDER BY upload_id").and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let batches = connection.prepare("SELECT batch_id, joint FROM sync_upload_batch").and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let orphan_answers = connection.prepare("SELECT request_id FROM encrypted_answer WHERE operation_id IS NULL OR operation_id NOT IN (SELECT seq FROM op) ORDER BY request_id").and_then(|mut statement| statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            Ok((read_outbox(reader)?, pending, uploads, answers, orphans, batches, orphan_answers))
        })?;
        let owned: Vec<i64> = uploads.iter().filter_map(UploadRaw::owned_op).collect();
        let reserved = uploads.iter().filter_map(UploadRaw::reserved_op).max().and_then(|op| u64::try_from(op).ok());
        let entries = entries.into_iter().filter(|entry| !owned.contains(&entry.operation.id.0)).collect();
        let ops = self.stored_intents(entries, pending.clone(), answers);
        let transfers = self.stored_transfers(uploads);
        let batches = batches.into_iter().filter_map(|(id, text)| Some((u64::try_from(id).ok()?, crate::owners::write::transfer::Joint::from_text(&text)?))).collect();
        let orphan_answers = orphan_answers.into_iter().filter_map(|id| u64::try_from(id).ok()).collect();
        Ok(Restore { ops, transfers, orphans, batches, reserved, orphan_answers })
    }

    fn stored_intents(&self, entries: Vec<super::plan::OutboxOperation>, pending: Option<crate::transport::encrypted::state::Pending>, mut answers: std::collections::BTreeMap<i64, (u64, crate::transport::encrypted::state::StoredAnswer)>) -> Result<Vec<StoredOp>, DbError> {
        let mut stored = Vec::with_capacity(entries.len());
        for entry in entries {
            let operation = &entry.operation;
            let Some((model, action)) = operation.action_key.split_once(':') else {
                return Err(DbError::Internal(format!("the action key `{}` is not model:action", operation.action_key)));
            };
            let Some(row_id) = operation.temp_id.as_ref().or_else(|| operation.row_ids.first()) else {
                return Err(DbError::Internal(format!("the operation {} has no row", operation.id.0)));
            };
            let declared = self.spec(model)?.actions.iter().find(|declaration| declaration.name == action);
            let mutation = declared.and_then(|declaration| declaration.mutation.as_deref());
            if mutation.is_none() {
                return Err(DbError::Internal(format!("the operation {} ({model}.{action}) declares no mutation to restore it with", operation.id.0)));
            }
            let op = NewOp {
                model: model.to_string(),
                row_id: row_id.as_str().to_string(),
                action: action.to_string(),
                on_failure: declared.map_or(OnFailure::Rollback, |declaration| declaration.on_failure),
                origin: OpSource::Api,
                path: String::new(),
                body: entry.input.to_string(),
                effects: Default::default(),
                durable: true,
            };
            let seq = operation.id.0;
            let resume = pending.as_ref().filter(|pending| pending.owner == Some(RequestOwner::Operation { seq })).and_then(|pending| Some((pending.req_id, u64::try_from(pending.sequence).ok()?)));
            let answered = answers.remove(&seq);
            stored.push(StoredOp { answered, op_id: u64::try_from(seq).map_err(|_| DbError::UnknownOperation(seq))?, op, error_mark: operation.status == OperationStatus::Failed, pending: resume, attempts: operation.attempts, cycle_id: 0, restore_failed: None, restore_element: None });
        }
        stored.extend(self.dropped_intents()?);
        Ok(stored)
    }

    /// The operations the layout move could not carry: each stays in the queue as `Failed{RestoreFailed}` until its element is discarded.
    fn dropped_intents(&self) -> Result<Vec<StoredOp>, DbError> {
        let mut dropped = Vec::new();
        for row in self.restore_failed()?.into_iter().filter(|row| row.table == "op") {
            let Ok(columns) = serde_json::from_str::<serde_json::Value>(&row.columns) else { continue };
            let Some(op_id) = columns.get("seq").and_then(serde_json::Value::as_u64) else { continue };
            let Some((model, action)) = columns.get("action_key").and_then(serde_json::Value::as_str).and_then(|key| key.split_once(':')) else { continue };
            let row_id = columns.get("temp_id").and_then(serde_json::Value::as_str).unwrap_or(&row.key).to_string();
            let op = NewOp { model: model.to_string(), row_id, action: action.to_string(), on_failure: OnFailure::Keep, origin: OpSource::Api, path: String::new(), body: String::new(), effects: Default::default(), durable: true };
            dropped.push(StoredOp { answered: None, op_id, op, error_mark: false, pending: None, attempts: 1, cycle_id: 0, restore_failed: Some(row.reason.clone()), restore_element: Some((row.table.clone(), row.key.clone())) });
        }
        Ok(dropped)
    }
}
