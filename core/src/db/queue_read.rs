//! The intents of the queue as `Restored` gives them to the owner Outbox.

use crate::operation::OperationStatus;
use crate::owners::write::outbox::{NewOp, Origin, StoredOp};
use crate::schema::OnFailure;
use crate::transport::RequestOwner;

use super::error::DbError;
use super::local_write::read_outbox;
use super::open::Db;

pub(crate) struct Restore {
    pub ops: Result<Vec<StoredOp>, DbError>,
    pub transfers: Result<Vec<crate::owners::write::transfer::StoredTransfer>, DbError>,
    pub pending: Option<crate::transport::encrypted::state::Pending>,
    pub pending_lost: bool,
}

impl Db {
    pub fn restore_failed(&self) -> Result<Vec<super::open::DroppedRecord>, DbError> {
        Ok(self.shared.store.restore_failed()?.into_iter().map(|row| super::open::DroppedRecord { table: row.table, key: row.key, reason: row.reason }).collect())
    }

    pub fn discard_restore_failed(&self, table: &str, key: &str) -> Result<bool, DbError> { Ok(self.shared.store.discard_restore_failed(table, key)?) }

    /// Every open operation of the queue. The `path` of a request and the rule `on_failure` come from the operation the model declares for `(model, action)`:
    /// the base has no column for them. An operation whose action declares no mutation is an error that names it: nothing is guessed.
    pub(crate) fn read_intents(&self) -> Result<Vec<StoredOp>, DbError> {
        let (entries, pending, answers) = self.shared.read(&[], |reader| {
            let connection = reader.connection();
            Ok((read_outbox(reader)?, crate::transport::encrypted::state::pending(connection).map_err(crate::store::StoreError::from)?, crate::transport::encrypted::state::answers_of_operations(connection).map_err(crate::store::StoreError::from)?))
        })?;
        self.stored_intents(entries, pending, answers)
    }

    pub(crate) fn read_restore(&self) -> Result<Restore, DbError> {
        let (entries, pending, uploads, answers) = self.shared.read(&[], |reader| {
            let connection = reader.connection();
            let pending = crate::transport::encrypted::state::pending(connection).map_err(crate::store::StoreError::from)?;
            let answers = crate::transport::encrypted::state::answers_of_operations(connection).map_err(crate::store::StoreError::from)?;
            let uploads = super::queue_write::upload_rows(connection).map_err(crate::store::StoreError::from)?;
            Ok((read_outbox(reader)?, pending, uploads, answers))
        })?;
        let ops = self.stored_intents(entries, pending.clone(), answers);
        let transfers = self.stored_transfers(uploads);
        Ok(Restore { ops, transfers, pending, pending_lost: self.open_report().dropped.iter().any(|row| row.table == "encrypted_pending") })
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
            let Some(mutation) = mutation else {
                return Err(DbError::Internal(format!("the operation {} ({model}.{action}) declares no mutation to restore it with", operation.id.0)));
            };
            let op = NewOp {
                model: model.to_string(),
                row_id: row_id.as_str().to_string(),
                action: action.to_string(),
                on_failure: declared.map_or(OnFailure::Rollback, |declaration| declaration.on_failure),
                origin: Origin::Api,
                path: format!("/{mutation}"),
                body: entry.input.to_string(),
                effects: Default::default(),
                durable: true,
            };
            let seq = operation.id.0;
            let resume = pending.as_ref().filter(|pending| pending.owner == Some(RequestOwner::Operation { seq })).and_then(|pending| Some((pending.req_id, u64::try_from(pending.sequence).ok()?)));
            let answered = answers.remove(&seq);
            stored.push(StoredOp { answered, op_id: u64::try_from(seq).map_err(|_| DbError::UnknownOperation(seq))?, op, error_mark: operation.status == OperationStatus::Failed, pending: resume, attempts: 0, cycle_id: 0, restore_failed: None });
        }
        Ok(stored)
    }
}
