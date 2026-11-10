use crate::store::CommitResult;
use crate::owners::store::StoreOp;

use super::file_write::PreparedFile;
use super::row_write::RowWrite;
use super::StoreError;

pub(super) fn apply_common(writer: &mut RowWrite<'_>, op: &StoreOp, index: usize, files: &[PreparedFile], results: &mut Vec<CommitResult>, response: bool) -> Result<bool, StoreError> {
    match op {
        StoreOp::Rows { model, rows } | StoreOp::LocalRows { model, rows } => {
            let local = matches!(op, StoreOp::LocalRows { .. });
            for row in rows {
                if let Some(result) = writer.upsert(model, row, local, response && !local)? { results.push(result); }
            }
        }
        StoreOp::RemoveRows { model, ids } => {
            for id in ids { results.push(writer.remove(model, id)?); }
        }
        StoreOp::ExpireRow { model, row_id, write_revision } => {
            let reader = super::Reader::new(writer.connection, writer.models);
            let expected = i64::try_from(*write_revision).ok();
            if reader.stored_row(model, row_id)?.is_some_and(|row| Some(row.revision) == expected) { results.push(writer.expire(model, row_id)?); }
        }
        StoreOp::LocalPending { model, rows } => results.push(writer.local_pending(model, rows)?),
        StoreOp::LocalFields { model, row_id, values } => {
            let stored = super::Reader::new(writer.connection, writer.models).stored_row(model, row_id)?;
            let Some(stored) = stored else { return Err(StoreError::Constraint(format!("local fields of a row that is not stored: {model}:{row_id}"))) };
            let mut row = stored.row;
            let fields: serde_json::Map<String, serde_json::Value> = serde_json::from_str(values)?;
            for (name, value) in fields { join_local(&mut row, name, value); }
            let joined = crate::owners::store::RowValue { row_id: row_id.clone(), values: serde_json::Value::Object(row).to_string() };
            if let Some(result) = writer.upsert(model, &joined, true, false)? { results.push(result); }
        }
        StoreOp::StaleFamilies { families } => results.push(writer.stale_families(families)?),
        StoreOp::MarkStale { models, keys } => super::windows::mark_stale(writer.connection, models, keys)?,
        StoreOp::Numbers { model, row_id, writes } => {
            if let Some(result) = writer.numbers(model, row_id, writes)? { results.push(result); }
        }
        StoreOp::RemoveIntent { op_id } => {
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            results.push(writer.transitions(vec![crate::store::OperationTransition::Close { seq, status: crate::owners::write::operation_status::OperationStatus::Committed }])?);
        }
        StoreOp::MarkError { op_id, error } => {
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            let transition = match error {
                Some(message) => crate::store::OperationTransition::Fail { seq, status: crate::owners::write::operation_status::OperationStatus::Failed, failure: crate::owners::write::failure::parse(message) },
                None => crate::store::OperationTransition::Retry { seq },
            };
            results.push(writer.transitions(vec![transition])?);
        }
        StoreOp::Attempt { op_id, attempts, cycle_id, cause } => {
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            let cycle_id = i64::try_from(*cycle_id).map_err(|_| StoreError::Constraint("cycle id is too large".into()))?;
            results.push(writer.transitions(vec![crate::store::OperationTransition::Attempt { seq, attempts: i64::from(*attempts), cycle_id, cause: *cause }])?);
        }
        StoreOp::Cause { op_id, cause } => {
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            let (kind, limit) = cause.and_then(|cause| cause.cause_columns()).map_or((None, None), |(kind, limit)| (Some(kind), Some(limit)));
            writer.connection.execute(include_str!("sql/batch_common/op_cause.sql"), rusqlite::params![seq, kind, limit])?;
        }
        StoreOp::Requeue { op_id } => {
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            results.push(writer.transitions(vec![crate::store::OperationTransition::Requeue { seq }])?);
        }
        StoreOp::Pending { pending, .. } => {
            if let Some(row) = super::reset::row_op(op) { super::reset::apply_row_ops(writer.connection, vec![row])?; }
            if let Some(crate::transport::RequestOwner::Operation { seq }) = pending.owner {
                results.push(writer.transitions(vec![crate::store::OperationTransition::Sending { seq }])?);
            }
        }
        StoreOp::Planned { effects } => results.push(super::intent_write::planned(writer, effects)?),
        StoreOp::IntentOrigin { op_id, origin } => super::upload_ops::put_intent_origin(writer.connection, *op_id, origin)?,
        StoreOp::Show { .. } => {}
        StoreOp::Intent { op_id, model, row_id, action, payload, effects, origin, attempts, cycle_id } => {
            results.push(super::intent_write::intent(writer, *op_id, (model, row_id, action), payload, effects, false)?);
            super::upload_ops::put_intent_origin(writer.connection, *op_id, origin)?;
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            let cycle_id = i64::try_from(*cycle_id).map_err(|_| StoreError::Constraint("cycle id is too large".into()))?;
            if writer.connection.execute(include_str!("sql/batch_common/intent_attempts.sql"), rusqlite::params![seq, i64::from(*attempts), cycle_id])? != 1 { return Err(StoreError::Constraint("the intent was not written".into())); }
        }
        StoreOp::Effects { op_id, model, row_id, action, payload, effects } => {
            use rusqlite::OptionalExtension;
            let seq = i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
            let state: Option<String> = writer.connection.query_row("SELECT intent_state FROM op WHERE seq = ?1", [seq], |row| row.get(0)).optional()?;
            if state.as_deref() == Some("ready") { return Err(StoreError::Constraint("operation already has an intent".into())); }
            results.push(super::intent_write::intent(writer, *op_id, (model, row_id, action), payload, effects, true)?);
            writer.connection.execute(include_str!("sql/batch_common/apply_common.sql"), [i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?])?;
        }
        StoreOp::TransferOutcome { transfer_id, cycle_id, outcome } => super::upload_ops::put_outcome(writer.connection, *transfer_id, *cycle_id, outcome, (writer.revision, writer.now))?,
        StoreOp::RetireTransferOutcome { transfer_id, cycle_id } => {
            let id = i64::try_from(*transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?;
            let cycle = i64::try_from(*cycle_id).map_err(|_| StoreError::Constraint("cycle id is too large".into()))?;
            writer.connection.execute(include_str!("sql/upload_ops/retire_transfer_outcome.sql"), rusqlite::params![id, cycle])?;
        }
        StoreOp::RollBack { op_id } => results.push(super::rollback::apply(writer, *op_id)?),
        StoreOp::Window { key, model, page, replace_all, cursor, roots, related, total, base_revision } => {
            let key = crate::owners::read::windows::WindowKey { model: model.clone(), name: key.clone() };
            let page = super::window_values::WindowPage { key: &key, page: *page, replace_all: *replace_all, cursor: cursor.as_deref(), roots, related, total: *total, base_revision: *base_revision };
            results.push(super::window_write::land(writer, &page)?);
        }
        StoreOp::WindowMarker { key, model, marker } => {
            let key = crate::owners::read::windows::WindowKey { model: model.clone(), name: key.clone() };
            results.push(super::window_write::mark(writer, &key, marker)?);
        }
        StoreOp::AttachFile { .. } | StoreOp::AttachCopied { .. } => {
            let record = files.iter().find(|file| file.index == index).ok_or_else(|| StoreError::Constraint("prepared file is missing".into()))?;
            results.push(writer.attach(&record.record)?);
        }
        StoreOp::DetachFile { model, row_id, field } => results.push(writer.detach(model, row_id, field)?),
        StoreOp::Upload { row, .. } => {
            super::upload_ops::apply_upload(writer.connection, op)?;
            results.push(writer.touch_upload(&row.key)?);
        }
        StoreOp::RemoveUploadByTransferId { transfer_id } => {
            let id = i64::try_from(*transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?;
            let key: String = writer.connection.query_row(include_str!("sql/batch_common/upload_key_by_transfer_id.sql"), [id], |row| row.get(0))?;
            super::upload_ops::remove_upload(writer.connection, &key)?;
            results.push(writer.touch_upload(&key)?);
        }
        StoreOp::RemoveUpload { key } | StoreOp::UploadRoundError { key, .. } | StoreOp::UploadShown { key, .. } | StoreOp::UploadStaged { key } => {
            super::upload_ops::apply_upload(writer.connection, op)?;
            results.push(writer.touch_upload(key)?);
        }
        other => {
            if let Some(op) = super::reset::row_op(other) {
                super::reset::apply_row_ops(writer.connection, vec![op])?;
            } else if !super::upload_ops::apply_upload(writer.connection, other)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn join_local(row: &mut serde_json::Map<String, serde_json::Value>, name: String, value: serde_json::Value) {
    match (row.get_mut(&name), value) {
        (Some(serde_json::Value::Object(stored)), serde_json::Value::Object(fields)) => for (key, field) in fields { join_local(stored, key, field); },
        (_, value) => { row.insert(name, value); }
    }
}
