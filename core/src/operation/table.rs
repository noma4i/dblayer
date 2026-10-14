use std::collections::HashSet;

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};

use super::fsck::FsckAction;
use super::quarantine::{quarantine_add, QuarantineReason};
use super::status::{Operation, OperationId, OperationStatus};
use crate::identity::RowId;

const OPEN_STATUSES: &str = "('pending', 'failed', 'delivery_unknown')";

#[cfg(test)]
pub fn append_operation(connection: &Connection, operation: &Operation, input: &[u8]) -> rusqlite::Result<i64> {
    let row_ids = serde_json::to_vec(&operation.row_ids).expect("row ids serialize");
    connection.execute(
        "INSERT INTO op (action_key, status, row_ids, temp_id, input, transport_started, base_revision, dedupe_key, rollback, created_at, since) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?9)",
        params![
            operation.action_key,
            operation.status.as_sql(),
            row_ids,
            operation.temp_id.as_ref().map(RowId::as_str),
            input,
            operation.transport_started as i64,
            operation.base_revision,
            operation.dedupe_key,
            operation.created_at,
        ],
    )?;
    Ok(connection.last_insert_rowid())
}

pub fn set_status(connection: &Connection, sequence: i64, status: OperationStatus) -> rusqlite::Result<()> {
    connection.execute("UPDATE op SET status = ? WHERE seq = ?", params![status.as_sql(), sequence])?;
    Ok(())
}

pub fn operation_input(connection: &Connection, sequence: i64) -> rusqlite::Result<Option<Vec<u8>>> {
    connection.query_row("SELECT input FROM op WHERE seq = ?", [sequence], |row| row.get(0)).optional()
}

pub fn open_operations(connection: &Connection) -> rusqlite::Result<Vec<Operation>> {
    let sql = format!(
        "SELECT seq, action_key, status, row_ids, temp_id, transport_started, base_revision, created_at, dedupe_key, attempts, error, since, upload \
         FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        let status: String = row.get(2)?;
        let row_ids: Vec<u8> = row.get(3)?;
        let temp_id: Option<String> = row.get(4)?;
        let error: Option<Vec<u8>> = row.get(10)?;
        let upload: Option<Vec<u8>> = row.get(12)?;
        Ok((
            Operation {
                id: OperationId(row.get(0)?),
                action_key: row.get(1)?,
                status: OperationStatus::from_sql(&status).unwrap_or(OperationStatus::Failed),
                row_ids: Vec::new(),
                temp_id: temp_id.as_deref().and_then(|text| RowId::new(text).ok()),
                transport_started: row.get::<_, i64>(5)? != 0,
                base_revision: row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                created_at: row.get(7)?,
                dedupe_key: row.get(8)?,
                attempts: row.get(9)?,
                error: error.and_then(|bytes| serde_json::from_slice(&bytes).ok()),
                since: row.get(11)?,
                upload: upload.and_then(|bytes| serde_json::from_slice(&bytes).ok()),
            },
            OperationStatus::from_sql(&status).is_some(),
            row_ids,
        ))
    })?;
    let mut operations = Vec::new();
    for item in rows {
        let (mut operation, valid, row_ids) = item?;
        let Ok(ids) = serde_json::from_slice::<Vec<RowId>>(&row_ids) else { continue };
        if valid {
            operation.row_ids = ids;
            operations.push(operation);
        }
    }
    Ok(operations)
}

pub fn apply_fsck(connection: &Connection, actions: &[FsckAction], orphans: &Orphans) -> rusqlite::Result<usize> {
    let transaction = connection.unchecked_transaction()?;
    let mut applied = 0;
    for action in actions {
        match action {
            FsckAction::SetStatus(id, status) => {
                set_status(&transaction, id.0, *status)?;
                applied += 1;
            }
            FsckAction::QuarantineOrphan(id) => {
                if quarantine_row(&transaction, id, orphans)? {
                    applied += 1;
                }
            }
            FsckAction::RebuildTempRow(_) | FsckAction::Keep(_) => {}
        }
    }
    transaction.commit()?;
    Ok(applied)
}

pub struct Orphan {
    pub model: String,
    pub table: String,
    pub payload: Vec<u8>,
}

pub type Orphans = std::collections::HashMap<RowId, Orphan>;

fn quarantine_row(connection: &Connection, id: &RowId, orphans: &Orphans) -> rusqlite::Result<bool> {
    let Some(orphan) = orphans.get(id) else { return Ok(false) };
    quarantine_add(connection, &orphan.model, &orphan.payload, QuarantineReason::OrphanTempRow)?;
    connection.execute(&format!("DELETE FROM {} WHERE id = ?", orphan.table), [id.as_str()])?;
    connection.execute("DELETE FROM scope_member WHERE model = ? AND row_id = ?", params![orphan.model, id.as_str()])?;
    Ok(true)
}

#[derive(Debug, Clone, PartialEq)]
pub struct RebuiltRow {
    pub model: String,
    pub id: RowId,
    pub row: Map<String, Value>,
}

pub fn rebuilt_rows(
    connection: &Connection,
    actions: &[FsckAction],
    operations: &[Operation],
    known_models: &HashSet<String>,
) -> rusqlite::Result<Vec<RebuiltRow>> {
    let mut rows = Vec::new();
    for action in actions {
        let FsckAction::RebuildTempRow(sequence) = action else { continue };
        let Some(operation) = operations.iter().find(|operation| operation.id == *sequence) else { continue };
        let Some((model, _)) = Operation::split_action_key(&operation.action_key).filter(|(model, _)| known_models.contains(model)) else {
            continue;
        };
        let Some(temp) = &operation.temp_id else { continue };
        let Some(input) = operation_input(connection, sequence.0)? else { continue };
        let Ok(Value::Object(row)) = serde_json::from_slice::<Value>(&input) else { continue };
        if row.get("id").and_then(Value::as_str) == Some(temp.as_str()) {
            rows.push(RebuiltRow { model, id: temp.clone(), row });
        }
    }
    Ok(rows)
}
