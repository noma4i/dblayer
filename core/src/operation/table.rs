use std::collections::HashSet;

use rusqlite::{params, Connection, OptionalExtension};

use super::fsck::FsckAction;
use super::once::OnceRequest;
use super::status::{mode_text, parse_mode, Operation, OperationId, OperationStatus};
use crate::identity::RowId;

pub const OP_COLUMNS: [&str; 12] = [
    "seq",
    "action_key",
    "action_mode",
    "status",
    "row_ids",
    "temp_id",
    "input",
    "transport_started",
    "base_revision",
    "created_at",
    "dedupe_key",
    "rollback",
];

const OPEN_STATUSES: &str = "('pending', 'failed', 'delivery_unknown')";

pub fn append_operation(connection: &Connection, operation: &Operation, input: &[u8]) -> rusqlite::Result<i64> {
    let row_ids = serde_json::to_vec(&operation.row_ids).expect("row ids serialize");
    connection.execute(
        "INSERT INTO op (action_key, action_mode, status, row_ids, temp_id, input, transport_started, base_revision, dedupe_key, rollback, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)",
        params![
            operation.action_key,
            mode_text(operation.mode),
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

pub fn mark_transport(connection: &Connection, sequence: i64) -> rusqlite::Result<()> {
    connection.execute("UPDATE op SET transport_started = 1 WHERE seq = ?", [sequence])?;
    Ok(())
}

pub fn delete_operation(connection: &Connection, sequence: i64) -> rusqlite::Result<()> {
    connection.execute("DELETE FROM op WHERE seq = ?", [sequence])?;
    Ok(())
}

pub fn operation_input(connection: &Connection, sequence: i64) -> rusqlite::Result<Option<Vec<u8>>> {
    connection.query_row("SELECT input FROM op WHERE seq = ?", [sequence], |row| row.get(0)).optional()
}

pub fn open_sequences(connection: &Connection, action_key: &str) -> rusqlite::Result<Vec<i64>> {
    let sql = format!("SELECT seq FROM op WHERE action_key = ? AND status IN {OPEN_STATUSES} ORDER BY seq");
    connection.prepare(&sql)?.query_map([action_key], |row| row.get(0))?.collect()
}

pub fn open_operations(connection: &Connection) -> rusqlite::Result<Vec<Operation>> {
    let sql = format!(
        "SELECT seq, action_key, action_mode, status, row_ids, temp_id, transport_started, base_revision, created_at, dedupe_key \
         FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        let mode: String = row.get(2)?;
        let status: String = row.get(3)?;
        let row_ids: Vec<u8> = row.get(4)?;
        let temp_id: Option<String> = row.get(5)?;
        Ok((
            Operation {
                id: OperationId(row.get(0)?),
                action_key: row.get(1)?,
                mode: parse_mode(&mode).unwrap_or(crate::schema::ActionMode::Request),
                status: OperationStatus::from_sql(&status).unwrap_or(OperationStatus::Failed),
                row_ids: Vec::new(),
                temp_id: temp_id.as_deref().and_then(|text| RowId::new(text).ok()),
                transport_started: row.get::<_, i64>(6)? != 0,
                base_revision: row.get::<_, Option<i64>>(7)?.unwrap_or(0),
                created_at: row.get(8)?,
                dedupe_key: row.get(9)?,
            },
            parse_mode(&mode).is_some() && OperationStatus::from_sql(&status).is_some(),
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

pub fn apply_fsck(connection: &Connection, actions: &[FsckAction]) -> rusqlite::Result<usize> {
    let mut applied = 0;
    for action in actions {
        if let FsckAction::SetStatus(id, status) = action {
            set_status(connection, id.0, *status)?;
            applied += 1;
        }
    }
    Ok(applied)
}

pub fn record_once(connection: &Connection, request: &OnceRequest) -> rusqlite::Result<()> {
    let Some(key) = request.dedupe_key.as_deref().filter(|key| !key.is_empty()) else {
        return Ok(());
    };
    connection.execute("INSERT OR IGNORE INTO op_once (namespace, dedupe_key) VALUES (?, ?)", params![request.namespace, key])?;
    Ok(())
}

pub fn committed_once_keys(connection: &Connection) -> rusqlite::Result<HashSet<OnceRequest>> {
    connection
        .prepare("SELECT namespace, dedupe_key FROM op_once")?
        .query_map([], |row| Ok(OnceRequest { namespace: row.get(0)?, dedupe_key: Some(row.get(1)?) }))?
        .collect()
}

pub fn open_once_keys(connection: &Connection) -> rusqlite::Result<HashSet<OnceRequest>> {
    connection
        .prepare("SELECT action_key, dedupe_key FROM op WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'delivery_unknown')")?
        .query_map([], |row| Ok(OnceRequest { namespace: row.get(0)?, dedupe_key: Some(row.get(1)?) }))?
        .collect()
}
