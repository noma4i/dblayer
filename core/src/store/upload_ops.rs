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
    connection.execute("UPDATE op SET intent_state = 'ready', origin_kind = ?2, transfer_id = ?3, batch_id = ?4 WHERE seq = ?1", params![id, kind, transfer, group])?;
    connection.execute("DELETE FROM op_transfer WHERE op_id = ?1", [id])?;
    for (position, transfer_id) in members.iter().enumerate() {
        let transfer_id = i64::try_from(*transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))?;
        connection.execute("INSERT INTO op_transfer (op_id, position, transfer_id) VALUES (?1, ?2, ?3)", params![id, position as i64, transfer_id])?;
    }
    Ok(())
}

pub(super) fn put_upload(connection: &Connection, row: &UploadRow, kind: UploadKind) -> Result<(), StoreError> {
    let [model, id, action] = key_parts(&row.key)?;
    let (status, stage) = match kind {
        UploadKind::Failed => ("failed", "prepare"),
        UploadKind::Completed => ("sending", "mutation"),
        UploadKind::ConfirmedParts => ("uploading", "part"),
        UploadKind::SyncUpload | UploadKind::Retrying => ("queued", "prepare"),
    };
    let on_failure = match row.on_failure { OnFailure::Rollback => "rollback", OnFailure::Keep => "keep" };
    connection.execute(
        "INSERT OR REPLACE INTO sync_upload (model, row_id, action, status, stage, sent, total, wait, attempts, error_kind, error_message, seq, source, batch, upload_id, completed, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure, steps_path, steps_body, transfer_id, cycle_id, signed_id, aborted, op_id, fields) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?30, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29)",
        params![model, id, action, status, stage, row.confirmed_parts, row.parts, row.failed.as_ref().map(|_| "unrecognized"), row.failed,
            i64::try_from(row.seq).unwrap_or(i64::MAX), row.source, row.batch.and_then(|group| i64::try_from(group).ok()), row.upload_id, row.completed,
            i64::try_from(row.created_at).unwrap_or(i64::MAX), row.inline, i64::try_from(row.size).unwrap_or(i64::MAX), row.sha256, row.path, row.body,
            on_failure, row.steps_path, row.steps_body, i64::try_from(row.transfer_id).unwrap_or(i64::MAX), i64::try_from(row.cycle_id).unwrap_or(i64::MAX),
            row.signed_id, row.aborted, row.op_id.and_then(|op| i64::try_from(op).ok()), row.fields, row.attempts],
    )?;
    connection.execute("DELETE FROM sync_upload_part WHERE model = ?1 AND row_id = ?2 AND action = ?3", params![model, id, action])?;
    for (index, etag) in row.etags.iter().enumerate() {
        connection.execute("INSERT INTO sync_upload_part (model, row_id, action, number, etag) VALUES (?1, ?2, ?3, ?4, ?5)", params![model, id, action, index as i64 + 1, etag])?;
    }
    Ok(())
}

pub(super) fn remove_upload(connection: &Connection, key: &str) -> Result<(), StoreError> {
    let [model, id, action] = key_parts(key)?;
    connection.execute("DELETE FROM transfer_outcome WHERE transfer_id IN (SELECT transfer_id FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND action = ?3)", params![model, id, action])?;
    connection.execute("DELETE FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND action = ?3", params![model, id, action])?;
    connection.execute("DELETE FROM sync_upload_part WHERE model = ?1 AND row_id = ?2 AND action = ?3", params![model, id, action])?;
    connection.execute("DELETE FROM sync_upload_batch WHERE batch_id NOT IN (SELECT batch FROM sync_upload WHERE batch IS NOT NULL)", [])?;
    Ok(())
}

pub(super) fn put_outcome(connection: &Connection, transfer_id: u64, cycle_id: u64, outcome: &crate::owners::write::outbox::OutboxOutcome, revision: i64) -> Result<(), StoreError> {
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
    connection.execute("INSERT INTO transfer_outcome (transfer_id, cycle_id, kind, revision, value) VALUES (?1, ?2, ?3, ?4, ?5)", params![id, cycle, kind, rev, value])?;
    Ok(())
}

pub(super) fn apply_upload(connection: &Connection, op: &StoreOp) -> Result<bool, StoreError> {
    match op {
        StoreOp::Upload { row, kind } => put_upload(connection, row, *kind)?,
        StoreOp::RemoveUpload { key } => remove_upload(connection, key)?,
        StoreOp::UploadBatch { batch_id, op_id, joint } => {
            connection.execute("INSERT OR REPLACE INTO sync_upload_batch (batch_id, op_id, joint) VALUES (?1, ?2, ?3)", params![i64::try_from(*batch_id).unwrap_or(i64::MAX), i64::try_from(*op_id).unwrap_or(i64::MAX), joint])?;
        }
        StoreOp::UploadOrphan { upload_id, key } => {
            connection.execute("INSERT OR REPLACE INTO upload_orphan (upload_id, key) VALUES (?1, ?2)", params![upload_id, key])?;
        }
        StoreOp::RemoveOrphan { upload_id } => { connection.execute("DELETE FROM upload_orphan WHERE upload_id = ?1", [upload_id])?; }
        StoreOp::BlobCachePut { content_sha256, blob_id, created_at } => {
            connection.execute("INSERT OR REPLACE INTO blob_cache (content_sha256, blob_id, created_at) VALUES (?1, ?2, ?3)", params![content_sha256, blob_id, i64::try_from(*created_at).unwrap_or(i64::MAX)])?;
        }
        StoreOp::BlobCacheRemove { content_sha256 } => { connection.execute("DELETE FROM blob_cache WHERE content_sha256 = ?1", [content_sha256])?; }
        _ => return Ok(false),
    }
    Ok(true)
}
