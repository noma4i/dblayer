use std::collections::{BTreeSet, HashSet};

use rusqlite::{params, Connection, OptionalExtension, Row};
use serde_json::{Map, Value};

use super::fsck::FsckAction;
use super::stored::*;
use super::quarantine::{quarantine_add, QuarantineReason};
use super::status::{Operation, OperationId, OperationStatus};
use crate::identity::{RowId, RowRef};

pub(crate) const OPEN_STATUSES: &str = "('pending', 'failed', 'delivery_unknown')";
const OPERATION_COLUMNS: &str = "seq, action_key, status, temp_id, transport_started, base_revision, created_at, dedupe_key, attempts, error_kind, error_message, since, client_mutation_id";

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
    let sql = format!("SELECT {OPERATION_COLUMNS} FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq");
    let mut rows_of = read_all_rows(connection)?;
    let mut uploads = read_uploads(connection)?;
    let mut extensions = operation_extensions(connection, None)?;
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], operation_at)?;
    let mut operations = Vec::new();
    for item in rows {
        if let Some(mut operation) = item? {
            operation.row_ids = rows_of.remove(&operation.id.0).unwrap_or_default();
            operation.upload = uploads.remove(&operation.id.0);
            attach_extensions(&mut operation.error, extensions.remove(&operation.id.0));
            operations.push(operation);
        }
    }
    Ok(operations)
}

fn operation_at(row: &Row<'_>) -> rusqlite::Result<Option<Operation>> {
    let Some(status) = OperationStatus::from_sql(&row.get::<_, String>(2)?) else { return Ok(None) };
    Ok(Some(Operation {
        id: OperationId(row.get(0)?), action_key: row.get(1)?, status, row_ids: Vec::new(),
        temp_id: row.get::<_, Option<String>>(3)?.as_deref().map(stored_row_id).transpose()?,
        transport_started: row.get::<_, i64>(4)? != 0,
        base_revision: row.get::<_, Option<i64>>(5)?.unwrap_or(0), created_at: row.get(6)?, dedupe_key: row.get(7)?,
        attempts: row.get(8)?, error: failure_at(row, 9)?, since: row.get(11)?, upload: None, client_mutation_id: row.get(12)?,
    }))
}

pub(crate) fn read_operation(connection: &Connection, seq: i64) -> rusqlite::Result<Option<Operation>> {
    let sql = format!("SELECT {OPERATION_COLUMNS} FROM op WHERE seq = ?1 AND status IN {OPEN_STATUSES}");
    let Some(mut operation) = connection.query_row(&sql, [seq], operation_at).optional()?.flatten() else { return Ok(None) };
    operation.row_ids = read_rows(connection, seq)?;
    attach_extensions(&mut operation.error, operation_extensions(connection, Some(seq))?.remove(&seq));
    Ok(Some(operation))
}

pub(crate) fn read_queue_operation(connection: &Connection, seq: i64) -> rusqlite::Result<Option<super::QueueOperation>> {
    let Some(operation) = read_operation(connection, seq)? else { return Ok(None) };
    let uploading = connection.query_row("SELECT EXISTS (SELECT 1 FROM op_item WHERE seq = ?1)", [seq], |row| row.get(0))?;
    Ok(Some(super::QueueOperation { operation, uploading }))
}

pub(crate) fn read_queue(connection: &Connection) -> rusqlite::Result<Vec<super::QueueOperation>> {
    let sql = format!("SELECT {OPERATION_COLUMNS}, EXISTS (SELECT 1 FROM op_item WHERE op_item.seq = op.seq) FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq");
    let mut rows_of = read_all_rows(connection)?;
    let mut extensions = operation_extensions(connection, None)?;
    let mut statement = connection.prepare(&sql)?;
    let mut found = Vec::new();
    for row in statement.query_map([], |row| Ok((operation_at(row)?, row.get::<_, bool>(13)?)))? {
        if let (Some(mut operation), uploading) = row? {
            operation.row_ids = rows_of.remove(&operation.id.0).unwrap_or_default();
            attach_extensions(&mut operation.error, extensions.remove(&operation.id.0));
            found.push(super::QueueOperation { operation, uploading });
        }
    }
    Ok(found)
}

pub(crate) fn row_operation_sequences(connection: &Connection, keys: &BTreeSet<(String, String)>) -> rusqlite::Result<BTreeSet<i64>> {
    let sql = format!("SELECT seq FROM op WHERE seq IN (SELECT seq FROM op_row WHERE row_id = ?2 UNION SELECT seq FROM op WHERE temp_id = ?2) AND substr(action_key, 1, instr(action_key, ':') - 1) = ?1 AND status IN {OPEN_STATUSES}");
    let mut statement = connection.prepare_cached(&sql)?;
    let mut sequences = BTreeSet::new();
    for (model, id) in keys {
        sequences.extend(statement.query_map(params![model, id], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(sequences)
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
    pub tables: Vec<String>,
    pub search: Option<String>,
    pub payload: Value,
}

pub type Orphans = std::collections::HashMap<RowRef, Orphan>;

fn quarantine_row(connection: &Connection, row: &RowRef, orphans: &Orphans) -> rusqlite::Result<bool> {
    let Some(orphan) = orphans.get(row) else { return Ok(false) };
    let id = &row.id;
    quarantine_add(connection, Some(row.model.as_str()), &orphan.payload, QuarantineReason::OrphanTempRow)?;
    if let (Some(search), Some(main)) = (&orphan.search, orphan.tables.last()) {
        connection.execute(&format!("DELETE FROM {search} WHERE rowid = (SELECT rowid FROM {main} WHERE id = ?1)"), [id.as_str()])?;
    }
    for (index, table) in orphan.tables.iter().enumerate() {
        let column = if index + 1 == orphan.tables.len() { "id" } else { "row_id" };
        connection.execute(&format!("DELETE FROM {table} WHERE {column} = ?"), [id.as_str()])?;
    }
    connection.execute("DELETE FROM scope_member WHERE model = ? AND row_id = ?", params![row.model.as_str(), id.as_str()])?;
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
