use std::collections::HashSet;

use rusqlite::{params, Connection};
use serde_json::{Map, Value};

use super::fsck::FsckAction;
use super::stored::*;
use super::quarantine::{quarantine_add, QuarantineReason};
use super::status::{Operation, OperationId, OperationStatus};
use crate::identity::RowId;

const OPEN_STATUSES: &str = "('pending', 'failed', 'delivery_unknown')";

#[cfg(test)]
pub fn append_operation(connection: &Connection, operation: &mut Operation, input: &Value) -> rusqlite::Result<i64> {
    connection.execute(
        "INSERT INTO op (action_key, status, temp_id, transport_started, base_revision, dedupe_key, created_at, since) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            operation.action_key,
            operation.status.as_sql(),
            operation.temp_id.as_ref().map(RowId::as_str),
            operation.transport_started as i64,
            operation.base_revision,
            operation.dedupe_key,
            operation.created_at,
        ],
    )?;
    let seq = connection.last_insert_rowid();
    operation.client_mutation_id = connection.query_row("SELECT client_mutation_id FROM op WHERE seq = ?", [seq], |row| row.get(0))?;
    write_rows(connection, seq, &operation.row_ids)?;
    write_value(connection, seq, "input", Some(input))?;
    Ok(seq)
}

pub fn set_status(connection: &Connection, sequence: i64, status: OperationStatus) -> rusqlite::Result<()> {
    connection.execute("UPDATE op SET status = ? WHERE seq = ?", params![status.as_sql(), sequence])?;
    Ok(())
}

pub fn operation_input(connection: &Connection, sequence: i64) -> rusqlite::Result<Option<Value>> {
    read_value(connection, sequence, "input")
}

pub fn open_operations(connection: &Connection) -> rusqlite::Result<Vec<Operation>> {
    let sql = format!(
        "SELECT seq, action_key, status, temp_id, transport_started, base_revision, created_at, dedupe_key, attempts, error_kind, error_message, error_extensions, since, client_mutation_id \
         FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq"
    );
    let mut rows_of = read_all_rows(connection)?;
    let mut uploads = read_uploads(connection)?;
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        let status: String = row.get(2)?;
        let temp_id: Option<String> = row.get(3)?;
        let seq: i64 = row.get(0)?;
        Ok((
            Operation {
                id: OperationId(seq),
                action_key: row.get(1)?,
                status: OperationStatus::from_sql(&status).unwrap_or(OperationStatus::Failed),
                row_ids: Vec::new(),
                temp_id: temp_id.as_deref().and_then(|text| RowId::new(text).ok()),
                transport_started: row.get::<_, i64>(4)? != 0,
                base_revision: row.get::<_, Option<i64>>(5)?.unwrap_or(0),
                created_at: row.get(6)?,
                dedupe_key: row.get(7)?,
                attempts: row.get(8)?,
                error: failure_at(row, 9)?,
                since: row.get(12)?,
                upload: None,
                client_mutation_id: row.get(13)?,
            },
            OperationStatus::from_sql(&status).is_some(),
        ))
    })?;
    let mut operations = Vec::new();
    for item in rows {
        let (mut operation, valid) = item?;
        if valid {
            operation.row_ids = rows_of.remove(&operation.id.0).unwrap_or_default();
            operation.upload = uploads.remove(&operation.id.0);
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
    pub tables: Vec<String>,
    pub search: Option<String>,
    pub payload: Value,
}

pub type Orphans = std::collections::HashMap<RowId, Orphan>;

fn quarantine_row(connection: &Connection, id: &RowId, orphans: &Orphans) -> rusqlite::Result<bool> {
    let Some(orphan) = orphans.get(id) else { return Ok(false) };
    quarantine_add(connection, Some(&orphan.model), &orphan.payload, QuarantineReason::OrphanTempRow)?;
    if let (Some(search), Some(main)) = (&orphan.search, orphan.tables.last()) {
        connection.execute(&format!("DELETE FROM {search} WHERE rowid = (SELECT rowid FROM {main} WHERE id = ?1)"), [id.as_str()])?;
    }
    for (index, table) in orphan.tables.iter().enumerate() {
        let column = if index + 1 == orphan.tables.len() { "id" } else { "row_id" };
        connection.execute(&format!("DELETE FROM {table} WHERE {column} = ?"), [id.as_str()])?;
    }
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
        let Some(Value::Object(row)) = operation_input(connection, sequence.0)? else { continue };
        if row.get("id").and_then(Value::as_str) == Some(temp.as_str()) {
            rows.push(RebuiltRow { model, id: temp.clone(), row });
        }
    }
    Ok(rows)
}
