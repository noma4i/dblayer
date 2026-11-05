//! The intents of the queue as `Restored` gives them to the owner Outbox (`outbox.md`, section Open).

use crate::operation::OperationStatus;
use crate::owners::life::session_runner::RESTORED_REQUEST;
use crate::owners::write::outbox::{NewOp, Origin, StoredOp};
use crate::schema::OnFailure;
use crate::transport::RequestOwner;

use super::error::DbError;
use super::local_write::read_outbox;
use super::open::Db;

impl Db {
    pub fn restore_failed(&self) -> Result<Vec<super::open::DroppedRecord>, DbError> {
        Ok(self.shared.store.restore_failed()?.into_iter().map(|row| super::open::DroppedRecord { table: row.table, key: row.key, reason: row.reason }).collect())
    }

    pub fn discard_restore_failed(&self, table: &str, key: &str) -> Result<bool, DbError> { Ok(self.shared.store.discard_restore_failed(table, key)?) }

    /// Every open operation of the queue. The `path` of a request and the rule `on_failure` come from the operation the model declares for `(model, action)`:
    /// the base has no column for them. An operation whose action declares no mutation is an error that names it: nothing is guessed.
    pub(crate) fn read_intents(&self) -> Result<Vec<StoredOp>, DbError> {
        let (entries, pending) = self.shared.read(&[], |reader| Ok((read_outbox(reader)?, crate::transport::encrypted::state::pending(reader.connection()).map_err(crate::store::StoreError::from)?)))?;
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
            };
            let seq = operation.id.0;
            let resume = pending.as_ref().filter(|pending| pending.owner == Some(RequestOwner::Operation { seq })).and_then(|pending| Some((RESTORED_REQUEST, u64::try_from(pending.sequence).ok()?)));
            stored.push(StoredOp { op_id: u64::try_from(seq).map_err(|_| DbError::UnknownOperation(seq))?, op, error_mark: operation.status == OperationStatus::Failed, pending: resume });
        }
        Ok(stored)
    }
}
