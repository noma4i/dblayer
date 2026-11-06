use std::collections::BTreeMap;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use crate::store::OperationRecord;
use crate::owners::write::operation_status::Operation;
use crate::owners::write::upload::{BlobSnapshot, BlobState, UploadState, BLOB_LIFETIME_MS};
use super::{ModelInfo, StoreError};

type BlobUploadRow = (Option<i64>, Option<i64>, Option<i64>, bool);

pub(crate) fn snapshot(connection: &Connection, id: &str) -> Result<Option<BlobSnapshot>, StoreError> {
    let row: Option<(Option<i64>, Option<i64>, bool, bool)> = connection.query_row(include_str!("sql/blob/snapshot_select_blob_upload.sql"), [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
    let Some((seq, consumer, expired, ready)) = row else { return Ok(None) };
    let mut upload = match seq {
        Some(seq) => crate::store::operation::read_operation_uploads(connection, seq)?.remove(&(seq, 0)).ok_or_else(|| StoreError::Constraint("Blob transfer state is missing".into()))?,
        None => UploadState::queued(),
    };
    if seq.is_none() { (upload.signed_id, upload.total_bytes, upload.sent_bytes) = connection.query_row(include_str!("sql/blob/snapshot_select_blob_upload_2.sql"), [id], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64)))?; }
    let operation = seq.map(|seq| crate::store::operation::read_operation(connection, seq)).transpose()?.flatten();
    let failed = upload.background_refused.is_some() || upload.phase == crate::owners::write::upload::UploadStatus::Failed || operation.as_ref().is_some_and(|operation| operation.status == crate::owners::write::operation_status::OperationStatus::Failed);
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
        let row: Option<BlobUploadRow> = connection.query_row(include_str!("sql/blob/pin_select_blob_upload.sql"), [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional()?;
        let Some((upload, owner, created, expired)) = row else { return Err(StoreError::Constraint("unknown Blob in this database".into())) };
        if expired || created.is_some_and(|created| now.saturating_sub(created) >= BLOB_LIFETIME_MS) && owner.is_none() && snapshot(connection, id)?.is_some_and(|blob| blob.state == BlobState::Uploaded) { return Err(StoreError::Constraint("Blob has expired".into())); }
        if upload.is_none() { return Err(StoreError::Constraint("Blob was discarded".into())); }
        if owner.is_some() { return Err(StoreError::Constraint("Blob is already pinned to an operation".into())); }
        connection.execute(include_str!("sql/blob/pin_update_blob_upload.sql"), params![seq, id])?;
        connection.execute(include_str!("sql/blob/pin_insert_blob_use.sql"), params![seq, parameter, id])?;
    }
    Ok(())
}

pub(crate) fn retry(connection: &Connection, seq: i64, models: &BTreeMap<String, ModelInfo>, now: i64) -> Result<(), StoreError> {
    if connection.query_row(include_str!("sql/blob/retry.sql"), [seq], |row| row.get::<_, bool>(0))? { return Ok(()); }
    let Some(operation) = crate::store::operation::read_operation(connection, seq)? else { return Ok(()) };
    let input = crate::store::operation::read_value(connection, seq, "input")?.unwrap_or(Value::Null);
    let record = OperationRecord { seq: None, action_key: operation.action_key, status: operation.status, row_ids: operation.row_ids, temp_id: operation.temp_id, input, transport_started: operation.transport_started, base_revision: Some(operation.base_revision), created_at: operation.created_at, dedupe_key: operation.dedupe_key, rollback: None, upload: None };
    pin(connection, seq, &record, models, now)
}



pub(crate) fn due(connection: &Connection, ready: i64, failed: i64) -> Result<Vec<(String, Option<String>)>, StoreError> {
    Ok(connection.prepare(include_str!("sql/blob/due.sql"))?.query_map(params![ready, failed], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?)
}


pub(crate) fn release(connection: &Connection, seq: i64, consumed: bool) -> Result<Vec<i64>, StoreError> {
    let uploads: Vec<(String, Option<i64>)> = connection.prepare(include_str!("sql/blob/release_select_blob_upload.sql"))?.query_map([seq], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    connection.execute(include_str!("sql/blob/release_delete_blob_use.sql"), [seq])?;
    connection.execute(include_str!("sql/blob/release_update_blob_upload.sql"), [seq])?;
    let mut closed = Vec::new();
    if consumed {
        for (id, upload) in uploads {
            if let Some(upload) = upload {
                connection.execute(include_str!("sql/blob/release_update_blob_upload_2.sql"), [id])?;
                crate::store::operation::delete_operation(connection, upload)?;
                closed.push(upload);
            }
        }
    }
    Ok(closed)
}
