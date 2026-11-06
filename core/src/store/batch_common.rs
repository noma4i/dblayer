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
        StoreOp::StaleFamilies { families } => results.push(writer.stale_families(families)?),
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
        StoreOp::Planned { effects } => results.push(super::intent_write::planned(writer, effects)?),
        StoreOp::Intent { op_id, model, row_id, action, payload, effects, origin } => {
            results.push(super::intent_write::intent(writer, *op_id, model, row_id, action, payload, effects)?);
            super::upload_ops::put_intent_origin(writer.connection, *op_id, origin)?;
        }
        StoreOp::Effects { op_id, model, row_id, action, payload, effects } => {
            results.push(super::intent_write::intent(writer, *op_id, model, row_id, action, payload, effects)?);
            writer.connection.execute(include_str!("sql/batch_common/apply_common.sql"), [i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?])?;
        }
        StoreOp::TransferOutcome { transfer_id, cycle_id, outcome } => super::upload_ops::put_outcome(writer.connection, *transfer_id, *cycle_id, outcome, writer.revision)?,
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
        StoreOp::AttachFile { .. } => {
            let record = files.iter().find(|file| file.index == index).ok_or_else(|| StoreError::Constraint("prepared file is missing".into()))?;
            results.push(writer.attach(&record.record)?);
        }
        StoreOp::DetachFile { model, row_id, field } => results.push(writer.detach(model, row_id, field)?),
        StoreOp::Upload { row, .. } => {
            super::upload_ops::apply_upload(writer.connection, op)?;
            results.push(writer.touch_upload(&row.key)?);
        }
        StoreOp::RemoveUpload { key } | StoreOp::UploadRoundError { key, .. } => {
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
