//! The intents of the queue as `Restored` gives them to the owner Outbox.

use crate::operation::OperationStatus;
use crate::owners::write::outbox::{NewOp, OpSource, StoredOp};
use crate::schema::OnFailure;
use crate::transport::RequestOwner;

use super::error::DbError;
use super::local_write::read_outbox;
use super::open::Db;
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
}

impl Db {
    pub fn restore_failed(&self) -> Result<Vec<super::open::DroppedRecord>, DbError> {
        Ok(self.shared.store.restore_failed()?.into_iter().map(|row| super::open::DroppedRecord { table: row.table, key: row.key, reason: row.reason }).collect())
    }

    pub fn discard_restore_failed(&self, table: &str, key: &str) -> Result<bool, DbError> { Ok(self.shared.store.discard_restore_failed(table, key)?) }

    /// The `on_failure` rule comes from the operation the model declares for `(model, action)`: the base has no column for it. An action that declares no mutation is an error that names it.
    #[cfg(test)]
    pub(crate) fn read_intents(&self) -> Result<Vec<StoredOp>, DbError> { self.read_restore()?.ops }

    pub(crate) fn read_restore(&self) -> Result<Restore, DbError> {
        let (entries, pending, uploads, answers, orphans, batches) = self.shared.read(&[], |reader| {
            let connection = reader.connection();
            let pending = crate::transport::encrypted::state::pending(connection).map_err(crate::store::StoreError::from)?;
            let answers = crate::transport::encrypted::state::answers_of_operations(connection).map_err(crate::store::StoreError::from)?;
            let uploads = super::upload_rows::upload_rows(connection).map_err(crate::store::StoreError::from)?;
            let orphans = connection.prepare("SELECT upload_id, key FROM upload_orphan ORDER BY upload_id").and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            let batches = connection.prepare("SELECT batch_id, joint FROM sync_upload_batch").and_then(|mut statement| statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()).map_err(crate::store::StoreError::from)?;
            Ok((read_outbox(reader)?, pending, uploads, answers, orphans, batches))
        })?;
        let owned: Vec<i64> = uploads.iter().filter_map(UploadRaw::owned_op).collect();
        let reserved = uploads.iter().filter_map(UploadRaw::reserved_op).max().and_then(|op| u64::try_from(op).ok());
        let entries = entries.into_iter().filter(|entry| !owned.contains(&entry.operation.id.0)).collect();
        let ops = self.stored_intents(entries, pending.clone(), answers);
        let transfers = self.stored_transfers(uploads);
        let batches = batches.into_iter().filter_map(|(id, text)| Some((u64::try_from(id).ok()?, crate::owners::write::transfer::Joint::from_text(&text)?))).collect();
        Ok(Restore { ops, transfers, orphans, batches, reserved })
    }

    fn stored_intents(&self, entries: Vec<super::local_write::OutboxOperation>, pending: Option<crate::transport::encrypted::state::Pending>, mut answers: std::collections::BTreeMap<i64, (u64, crate::transport::encrypted::state::StoredAnswer)>) -> Result<Vec<StoredOp>, DbError> {
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
            stored.push(StoredOp { answered, op_id: u64::try_from(seq).map_err(|_| DbError::UnknownOperation(seq))?, op, error_mark: operation.status == OperationStatus::Failed, pending: resume, attempts: operation.attempts, cycle_id: 0, restore_failed: None });
        }
        Ok(stored)
    }
}
