use rusqlite::{params, Connection};

use crate::owners::store::{IntentOrigin, StoreOp, UploadKind, UploadRow};
use crate::schema::OnFailure;

use super::StoreError;

fn key_parts(key: &str) -> Result<[&str; 3], StoreError> {
    let mut parts = key.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(model), Some(id), Some(action)) => Ok([model, id, action]),
        _ => Err(StoreError::Constraint(format!("invalid transfer key: {key}"))),
    }
}

pub(super) fn put_intent_origin(connection: &Connection, op_id: u64, origin: &IntentOrigin) -> Result<(), StoreError> {
    let id = i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
    let (kind, transfer, group, members) = match origin {
        IntentOrigin::Api => ("api", None, None, &[][..]),
        IntentOrigin::Transfer { transfer_id } => ("transfer", Some(i64::try_from(*transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?), None, &[][..]),
        IntentOrigin::TransferGroup { batch_id, transfer_ids } => ("transfer_group", None, Some(i64::try_from(*batch_id).map_err(|_| StoreError::Constraint("batch id is too large".into()))?), transfer_ids.as_slice()),
    };
    connection.execute(include_str!("sql/upload_ops/put_intent_origin_update_op.sql"), params![id, kind, transfer, group])?;
    connection.execute(include_str!("sql/upload_ops/put_intent_origin_delete_op_transfer.sql"), [id])?;
    for (position, transfer_id) in members.iter().enumerate() {
        let transfer_id = i64::try_from(*transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?;
        connection.execute(include_str!("sql/upload_ops/put_intent_origin_insert_op_transfer.sql"), params![id, position as i64, transfer_id])?;
    }
    Ok(())
}

fn error_columns(error: Option<&str>) -> (Option<&'static str>, Option<&str>) {
    let kind = error.map(|error| if error == "missing_file" { "missing_file" } else if error.starts_with("source_failed:") { "source_failed" } else { "unrecognized" });
    (kind, error.map(|error| error.strip_prefix("source_failed:").unwrap_or(error)))
}

pub(super) fn put_upload(connection: &Connection, row: &UploadRow, kind: UploadKind) -> Result<(), StoreError> {
    let [model, id, action] = key_parts(&row.key)?;
    let (status, stage) = match kind {
        UploadKind::AwaitingSource => ("waiting", "prepare"),
        UploadKind::Failed => ("failed", "prepare"),
        UploadKind::Completed => ("sending", "mutation"),
        UploadKind::ConfirmedParts => ("uploading", "part"),
        UploadKind::SyncUpload | UploadKind::Retrying => ("queued", "prepare"),
    };
    let on_failure = match row.on_failure { OnFailure::Rollback => "rollback", OnFailure::Keep => "keep" };
    let wait_reason = if kind == UploadKind::AwaitingSource { crate::owners::write::transfer::Shown::WaitingSource.wait_reason().and_then(|reason| super::operation::enum_text(&reason)) } else { None };
    let (error_kind, error_message) = error_columns(row.failed.as_deref());
    connection.execute(
        include_str!("sql/upload_ops/put_upload_insert_sync_upload.sql"),
        params![model, id, action, status, stage, row.confirmed_parts, row.parts, error_kind, error_message,
            i64::try_from(row.seq).unwrap_or(i64::MAX), row.source, row.batch.and_then(|group| i64::try_from(group).ok()), row.upload_id, row.completed,
            i64::try_from(row.created_at).unwrap_or(i64::MAX), row.inline, i64::try_from(row.size).unwrap_or(i64::MAX), row.sha256, row.path, row.body,
            on_failure, row.steps_path, row.steps_body, i64::try_from(row.transfer_id).unwrap_or(i64::MAX), i64::try_from(row.cycle_id).unwrap_or(i64::MAX),
            row.signed_id, row.aborted, row.op_id.and_then(|op| i64::try_from(op).ok()), row.fields, row.attempts, wait_reason, row.staged, row.failed_at.and_then(|at| i64::try_from(at).ok())],
    )?;
    connection.execute(include_str!("sql/upload_ops/put_upload_delete_sync_upload_part.sql"), params![model, id, action])?;
    for (index, etag) in row.etags.iter().enumerate() {
        connection.execute(include_str!("sql/upload_ops/put_upload_insert_sync_upload_part.sql"), params![model, id, action, index as i64 + 1, etag])?;
    }
    Ok(())
}

pub(super) fn remove_upload(connection: &Connection, key: &str) -> Result<(), StoreError> {
    let [model, id, action] = key_parts(key)?;
    connection.execute(include_str!("sql/upload_ops/remove_upload_delete_sync_upload.sql"), params![model, id, action])?;
    connection.execute(include_str!("sql/upload_ops/put_upload_delete_sync_upload_part.sql"), params![model, id, action])?;
    connection.execute(include_str!("sql/upload_ops/remove_upload_delete_sync_upload_batch.sql"), [])?;
    Ok(())
}

pub(super) fn put_outcome(connection: &Connection, transfer_id: u64, cycle_id: u64, outcome: &crate::owners::write::outbox::OutboxOutcome, (revision, now): (i64, i64)) -> Result<(), StoreError> {
    use crate::owners::write::outbox::OutboxOutcome;
    let id = i64::try_from(transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?;
    let cycle = i64::try_from(cycle_id).map_err(|_| StoreError::Constraint("cycle id is too large".into()))?;
    let (kind, rev, value) = match outcome {
        OutboxOutcome::Confirmed { result, .. } => ("confirmed", Some(revision), result.as_deref()),
        OutboxOutcome::Rejected { error } => ("rejected", None, Some(error.as_str())),
        OutboxOutcome::Failed { error } => ("failed", None, Some(error.as_str())),
        OutboxOutcome::Discarded => ("discarded", None, None),
        OutboxOutcome::Interrupted => return Err(StoreError::Constraint("interrupted outcome is not durable".into())),
    };
    connection.execute(include_str!("sql/upload_ops/put_outcome.sql"), params![id, cycle, kind, rev, value, now])?;
    Ok(())
}

fn shown_columns(shown: &crate::owners::write::transfer::Shown) -> (Option<String>, Option<String>, Option<String>) {
    use crate::owners::write::transfer::Shown;
    use crate::owners::write::upload::{UploadStage, UploadStatus};
    let (status, stage) = match shown {
        Shown::Copying => (UploadStatus::Preparing, Some(UploadStage::Prepare)),
        Shown::WaitingSource => (UploadStatus::Waiting, Some(UploadStage::Prepare)),
        Shown::Queued => (UploadStatus::Queued, Some(UploadStage::Prepare)),
        Shown::Uploading { progress: 100.. } => (UploadStatus::Sending, Some(UploadStage::Mutation)),
        Shown::Uploading { .. } => (UploadStatus::Uploading, Some(UploadStage::Part)),
        Shown::Failed { .. } => (UploadStatus::Failed, None),
        Shown::Gone => return (None, None, None),
    };
    (super::operation::enum_text(&status), stage.and_then(|stage| super::operation::enum_text(&stage)), shown.wait_reason().and_then(|reason| super::operation::enum_text(&reason)))
}

pub(super) fn apply_upload(connection: &Connection, op: &StoreOp) -> Result<bool, StoreError> {
    match op {
        StoreOp::Upload { row, kind } => put_upload(connection, row, *kind)?,
        StoreOp::RemoveUpload { key } => remove_upload(connection, key)?,
        StoreOp::UploadRoundError { key, error } => {
            let [model, id, action] = key_parts(key)?;
            connection.execute(include_str!("sql/upload_ops/upload_round_error.sql"), params![model, id, action, error])?;
        }
        StoreOp::UploadShown { key, shown, sent, total, attempts } => {
            let [model, id, action] = key_parts(key)?;
            let (status, stage, wait) = shown_columns(shown);
            let failed = if let crate::owners::write::transfer::Shown::Failed { error } = shown { Some(error.as_str()) } else { None };
            let (error_kind, error_message) = error_columns(failed);
            connection.execute(include_str!("sql/upload_ops/upload_shown.sql"), params![model, id, action, status, stage, wait, sent, total, attempts, error_kind, error_message])?;
        }
        StoreOp::UploadStaged { key } => {
            let [model, id, action] = key_parts(key)?;
            connection.execute(include_str!("sql/upload_ops/upload_staged.sql"), params![model, id, action])?;
        }
        StoreOp::UploadBatch { batch_id, op_id, joint } => {
            connection.execute(include_str!("sql/upload_ops/apply_upload_insert_sync_upload_batch.sql"), params![i64::try_from(*batch_id).unwrap_or(i64::MAX), i64::try_from(*op_id).unwrap_or(i64::MAX), joint])?;
        }
        StoreOp::UploadOrphan { upload_id, key } => {
            connection.execute(include_str!("sql/upload_ops/apply_upload_insert_upload_orphan.sql"), params![upload_id, key])?;
        }
        StoreOp::RemoveOrphan { upload_id } => { connection.execute(include_str!("sql/upload_ops/apply_upload_delete_upload_orphan.sql"), [upload_id])?; }
        StoreOp::BlobCachePut { content_sha256, blob_id, created_at } => {
            connection.execute(include_str!("sql/upload_ops/apply_upload_insert_blob_cache.sql"), params![content_sha256, blob_id, i64::try_from(*created_at).unwrap_or(i64::MAX)])?;
        }
        StoreOp::BlobCacheRemove { content_sha256 } => { connection.execute(include_str!("sql/upload_ops/apply_upload_delete_blob_cache.sql"), [content_sha256])?; }
        _ => return Ok(false),
    }
    Ok(true)
}
