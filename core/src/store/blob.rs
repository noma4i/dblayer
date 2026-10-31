use std::collections::BTreeMap;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use crate::commit::OperationRecord;
use crate::operation::Operation;
use crate::upload::{BlobSnapshot, BlobState, UploadState, BLOB_LIFETIME_MS};
use super::{ModelInfo, StoreError};

type BlobUploadRow = (Option<i64>, Option<i64>, Option<i64>, bool);

pub(crate) fn snapshot(connection: &Connection, id: &str) -> Result<Option<BlobSnapshot>, StoreError> {
    let row: Option<(Option<i64>, Option<i64>, bool, bool)> = connection.query_row("SELECT seq, consumer_seq, expired, ready FROM blob_upload WHERE id = ?1", [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
    let Some((seq, consumer, expired, ready)) = row else { return Ok(None) };
    let mut upload = match seq {
        Some(seq) => crate::operation::read_operation_uploads(connection, seq)?.remove(&(seq, 0)).ok_or_else(|| StoreError::Constraint("Blob transfer state is missing".into()))?,
        None => UploadState::queued(),
    };
    if seq.is_none() { (upload.signed_id, upload.total_bytes, upload.sent_bytes) = connection.query_row("SELECT signed_id, total_bytes, sent_bytes FROM blob_upload WHERE id = ?1", [id], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64)))?; }
    let operation = seq.map(|seq| crate::operation::read_operation(connection, seq)).transpose()?.flatten();
    let failed = upload.background_refused.is_some() || upload.phase == crate::upload::UploadStatus::Failed || operation.as_ref().is_some_and(|operation| operation.status == crate::operation::OperationStatus::Failed);
    if upload.error.is_none() { upload.error = operation.and_then(|operation| operation.error); }
    let state = if expired { BlobState::Expired } else if seq.is_none() && ready { BlobState::Consumed } else if seq.is_none() { BlobState::Discarded } else if failed { BlobState::Failed } else if upload.is_transferred() { BlobState::Uploaded } else { BlobState::Uploading };
    let signed_id = matches!(state, BlobState::Uploaded | BlobState::Consumed).then(|| upload.signed_id.clone()).flatten();
    Ok(Some(BlobSnapshot { id: id.into(), state, signed_id, upload, consumer }))
}

pub(crate) fn pin(connection: &Connection, seq: i64, record: &OperationRecord, models: &BTreeMap<String, ModelInfo>, now: i64) -> Result<(), StoreError> {
    if record.seq.is_some() { return Ok(()); }
    let Some((model, action)) = Operation::split_action_key(&record.action_key) else { return Ok(()) };
    let Some(declaration) = models.get(&model).and_then(|model| model.spec.actions.iter().find(|declared| declared.name == action)) else { return Ok(()) };
    for (parameter, _) in declaration.args.iter().filter(|(_, kind)| kind.as_str() == "blob") {
        let id = record.input.get(parameter).and_then(Value::as_str).ok_or_else(|| StoreError::Constraint(format!("{parameter} requires a Blob")))?;
        let row: Option<BlobUploadRow> = connection.query_row("SELECT seq, consumer_seq, created_at, expired FROM blob_upload WHERE id = ?1", [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
        let Some((upload, owner, created, expired)) = row else { return Err(StoreError::Constraint("unknown Blob in this database".into())) };
        if expired || created.is_some_and(|created| now.saturating_sub(created) >= BLOB_LIFETIME_MS) && owner.is_none() && snapshot(connection, id)?.is_some_and(|blob| blob.state == BlobState::Uploaded) { return Err(StoreError::Constraint("Blob has expired".into())); }
        if upload.is_none() { return Err(StoreError::Constraint("Blob was discarded".into())); }
        if owner.is_some() { return Err(StoreError::Constraint("Blob is already pinned to an operation".into())); }
        connection.execute("UPDATE blob_upload SET consumer_seq = ?1 WHERE id = ?2", params![seq, id])?;
        connection.execute("INSERT INTO blob_use (consumer_seq, parameter, blob_id) VALUES (?1, ?2, ?3)", params![seq, parameter, id])?;
    }
    Ok(())
}

pub(crate) fn retry(connection: &Connection, seq: i64, models: &BTreeMap<String, ModelInfo>, now: i64) -> Result<(), StoreError> {
    if connection.query_row("SELECT EXISTS (SELECT 1 FROM blob_use WHERE consumer_seq = ?1)", [seq], |row| row.get::<_, bool>(0))? { return Ok(()); }
    let Some(operation) = crate::operation::read_operation(connection, seq)? else { return Ok(()) };
    let input = crate::operation::read_value(connection, seq, "input")?.unwrap_or(Value::Null);
    let record = OperationRecord { seq: None, action_key: operation.action_key, status: operation.status, row_ids: operation.row_ids, temp_id: operation.temp_id, input, transport_started: operation.transport_started, base_revision: Some(operation.base_revision), created_at: operation.created_at, dedupe_key: operation.dedupe_key, rollback: None, upload: None };
    pin(connection, seq, &record, models, now)
}

pub(crate) fn variables(connection: &Connection, seq: i64, input: &Value) -> Result<Option<Value>, StoreError> {
    let uses: Vec<(String, String)> = connection.prepare("SELECT parameter, blob_id FROM blob_use WHERE consumer_seq = ?1")?.query_map([seq], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    let mut input = input.clone();
    for (parameter, id) in uses {
        let blob = snapshot(connection, &id)?.ok_or_else(|| StoreError::Constraint("unknown Blob".into()))?;
        let Some(signed) = blob.signed_id else { return Ok(None) };
        input[parameter] = Value::String(signed);
    }
    Ok(Some(input))
}

pub(crate) fn waiting(connection: &Connection, seq: i64) -> Result<bool, StoreError> {
    Ok(connection.query_row("SELECT EXISTS (SELECT 1 FROM blob_use u JOIN blob_upload b ON b.id = u.blob_id LEFT JOIN op_item i ON i.seq = b.seq AND i.position = 0 WHERE u.consumer_seq = ?1 AND (i.transferred IS NOT 1 OR b.expired = 1))", [seq], |row| row.get(0))?)
}

pub(crate) fn due(connection: &Connection, ready: i64, failed: i64) -> Result<Vec<(String, Option<String>)>, StoreError> {
    Ok(connection.prepare("SELECT b.id, i.file_id FROM (SELECT id, seq FROM blob_upload WHERE consumer_seq IS NULL AND expired = 0 AND ready = 1 AND created_at <= ?1 UNION SELECT id, seq FROM blob_upload WHERE consumer_seq IS NULL AND expired = 0 AND failed_at <= ?2) b JOIN op_item i ON i.seq = b.seq AND i.position = 0")?.query_map(params![ready, failed], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?)
}

pub(crate) fn files(connection: &Connection, consumer: i64) -> Result<Vec<String>, StoreError> {
    Ok(connection.prepare("SELECT i.file_id FROM blob_upload b JOIN op_item i ON i.seq = b.seq WHERE b.consumer_seq = ?1 AND i.file_id IS NOT NULL")?.query_map([consumer], |row| row.get(0))?.collect::<Result<_, _>>()?)
}

pub(crate) fn release(connection: &Connection, seq: i64, consumed: bool) -> Result<Vec<i64>, StoreError> {
    let uploads: Vec<(String, Option<i64>)> = connection.prepare("SELECT id, seq FROM blob_upload WHERE consumer_seq = ?1")?.query_map([seq], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    connection.execute("DELETE FROM blob_use WHERE consumer_seq = ?1", [seq])?;
    connection.execute("UPDATE blob_upload SET consumer_seq = NULL WHERE consumer_seq = ?1", [seq])?;
    let mut closed = Vec::new();
    if consumed {
        for (id, upload) in uploads {
            if let Some(upload) = upload {
                connection.execute("UPDATE blob_upload SET seq = NULL WHERE id = ?1", [id])?;
                crate::operation::delete_operation(connection, upload)?;
                closed.push(upload);
            }
        }
    }
    Ok(closed)
}
