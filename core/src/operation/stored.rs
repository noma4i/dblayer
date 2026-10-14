use std::collections::HashMap;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection, Result, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::identity::RowId;
use crate::nodes::{delete_trees, read_trees, write_tree};
use crate::syncstate::OperationFailure;
use crate::upload::{UploadInfo, UploadPart, UploadState};

pub const UPLOAD_COLUMNS: &str = "up_phase, up_stage, up_scenario, up_total, up_sent, up_retries, up_key, up_upload_id, up_signed_id, up_blob_id";
pub const INFO_COLUMNS: &str = "up_status, up_stage, up_sent, up_total, up_wait, up_attempts, up_error_kind, up_error_message";

pub fn enum_text<T: Serialize>(value: &T) -> Option<String> {
    match serde_json::to_value(value) {
        Ok(Value::String(text)) => Some(text),
        _ => None,
    }
}

pub fn enum_from<T: DeserializeOwned>(text: &str) -> Option<T> {
    serde_json::from_value(Value::String(text.to_string())).ok()
}

fn text_cell(text: Option<String>) -> SqlValue {
    text.map_or(SqlValue::Null, SqlValue::Text)
}

pub fn failure_cells(failure: Option<&OperationFailure>) -> [SqlValue; 2] {
    [text_cell(failure.and_then(|failure| enum_text(&failure.kind))), text_cell(failure.map(|failure| failure.message.clone()))]
}

pub fn failure_at(row: &Row<'_>, first: usize) -> Result<Option<OperationFailure>> {
    let kind: Option<String> = row.get(first)?;
    let message: Option<String> = row.get(first + 1)?;
    Ok(kind.and_then(|kind| enum_from(&kind)).zip(message).map(|(kind, message)| OperationFailure { kind, message }))
}

pub fn upload_cells(state: Option<&UploadState>) -> Vec<SqlValue> {
    let Some(state) = state else { return vec![SqlValue::Null; 10] };
    vec![
        text_cell(enum_text(&state.phase)),
        text_cell(enum_text(&state.stage)),
        text_cell(state.scenario.and_then(|scenario| enum_text(&scenario))),
        SqlValue::Integer(state.total_bytes as i64),
        SqlValue::Integer(state.sent_bytes as i64),
        SqlValue::Integer(i64::from(state.retries)),
        text_cell(state.key.clone()),
        text_cell(state.upload_id.clone()),
        text_cell(state.signed_id.clone()),
        text_cell(state.blob_id.clone()),
    ]
}

pub fn upload_at(row: &Row<'_>, first: usize) -> Result<Option<UploadState>> {
    let Some(phase) = row.get::<_, Option<String>>(first)?.and_then(|text| enum_from(&text)) else { return Ok(None) };
    let stage = row.get::<_, Option<String>>(first + 1)?.and_then(|text| enum_from(&text));
    let Some(stage) = stage else { return Ok(None) };
    Ok(Some(UploadState {
        phase,
        stage,
        scenario: row.get::<_, Option<String>>(first + 2)?.and_then(|text| enum_from(&text)),
        total_bytes: row.get::<_, i64>(first + 3)? as u64,
        sent_bytes: row.get::<_, i64>(first + 4)? as u64,
        retries: row.get::<_, i64>(first + 5)? as u32,
        key: row.get(first + 6)?,
        upload_id: row.get(first + 7)?,
        parts: Vec::new(),
        signed_id: row.get(first + 8)?,
        blob_id: row.get(first + 9)?,
    }))
}

pub fn info_cells(info: Option<&UploadInfo>) -> Vec<SqlValue> {
    let Some(info) = info else { return vec![SqlValue::Null; 8] };
    let [kind, message] = failure_cells(info.error.as_ref());
    vec![
        text_cell(enum_text(&info.status)),
        text_cell(enum_text(&info.stage)),
        SqlValue::Integer(info.sent_bytes as i64),
        SqlValue::Integer(info.total_bytes as i64),
        text_cell(info.wait_reason.and_then(|reason| enum_text(&reason))),
        SqlValue::Integer(i64::from(info.attempts)),
        kind,
        message,
    ]
}

pub fn info_at(row: &Row<'_>, first: usize) -> Result<Option<UploadInfo>> {
    let Some(status) = row.get::<_, Option<String>>(first)?.and_then(|text| enum_from(&text)) else { return Ok(None) };
    let Some(stage) = row.get::<_, Option<String>>(first + 1)?.and_then(|text| enum_from(&text)) else { return Ok(None) };
    Ok(Some(UploadInfo {
        status,
        stage,
        sent_bytes: row.get::<_, i64>(first + 2)? as u64,
        total_bytes: row.get::<_, i64>(first + 3)? as u64,
        wait_reason: row.get::<_, Option<String>>(first + 4)?.and_then(|text| enum_from(&text)),
        attempts: row.get::<_, i64>(first + 5)? as u32,
        error: failure_at(row, first + 6)?,
    }))
}

pub fn write_rows(connection: &Connection, seq: i64, row_ids: &[RowId]) -> Result<()> {
    connection.prepare_cached("DELETE FROM op_row WHERE seq = ?1")?.execute([seq])?;
    let mut insert = connection.prepare_cached("INSERT INTO op_row (seq, pos, row_id) VALUES (?1, ?2, ?3)")?;
    for (pos, id) in row_ids.iter().enumerate() {
        insert.execute(params![seq, pos as i64, id.as_str()])?;
    }
    Ok(())
}

pub fn write_value(connection: &Connection, seq: i64, part: &str, value: Option<&Value>) -> Result<()> {
    let owner = [SqlValue::Integer(seq), SqlValue::Text(part.to_string())];
    delete_trees(connection, "op_value", "seq = ?1 AND part = ?2", &owner)?;
    value.map_or(Ok(()), |value| write_tree(connection, "op_value", &["seq", "part"], &owner, value))
}

pub fn write_parts(connection: &Connection, seq: i64, parts: &[UploadPart]) -> Result<()> {
    connection.prepare_cached("DELETE FROM op_part WHERE seq = ?1")?.execute([seq])?;
    let mut insert = connection.prepare_cached("INSERT INTO op_part (seq, pos, number, etag) VALUES (?1, ?2, ?3, ?4)")?;
    for (pos, part) in parts.iter().enumerate() {
        insert.execute(params![seq, pos as i64, i64::from(part.number), part.etag])?;
    }
    Ok(())
}

pub fn read_value(connection: &Connection, seq: i64, part: &str) -> Result<Option<Value>> {
    let owner = [SqlValue::Integer(seq), SqlValue::Text(part.to_string())];
    Ok(read_trees(connection, "op_value", &["seq", "part"], "seq = ?1 AND part = ?2", &owner)?.into_iter().next().map(|(_, value)| value))
}

pub fn read_rows(connection: &Connection, seq: i64) -> Result<Vec<RowId>> {
    let mut statement = connection.prepare_cached("SELECT row_id FROM op_row WHERE seq = ?1 ORDER BY pos")?;
    let ids = statement.query_map([seq], |row| row.get::<_, String>(0))?;
    Ok(ids.filter_map(|id| id.ok().and_then(|id| RowId::new(&id).ok())).collect())
}

pub fn read_all_rows(connection: &Connection) -> Result<HashMap<i64, Vec<RowId>>> {
    let mut statement = connection.prepare_cached("SELECT seq, row_id FROM op_row ORDER BY seq, pos")?;
    let mut found: HashMap<i64, Vec<RowId>> = HashMap::new();
    for item in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))? {
        let (seq, id) = item?;
        found.entry(seq).or_default().extend(RowId::new(&id).ok());
    }
    Ok(found)
}

pub fn read_all_parts(connection: &Connection) -> Result<HashMap<i64, Vec<UploadPart>>> {
    let mut statement = connection.prepare_cached("SELECT seq, number, etag FROM op_part ORDER BY seq, pos")?;
    let mut found: HashMap<i64, Vec<UploadPart>> = HashMap::new();
    for item in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, UploadPart { number: row.get::<_, i64>(1)? as u32, etag: row.get(2)? })))? {
        let (seq, part) = item?;
        found.entry(seq).or_default().push(part);
    }
    Ok(found)
}

pub fn read_all_fields(connection: &Connection) -> Result<HashMap<i64, Vec<String>>> {
    let mut statement = connection.prepare_cached("SELECT seq, name FROM op_value WHERE part = 'input' AND parent = 0 AND name IS NOT NULL AND name != 'id' ORDER BY seq, pos")?;
    let mut found: HashMap<i64, Vec<String>> = HashMap::new();
    for item in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))? {
        let (seq, name) = item?;
        found.entry(seq).or_default().push(name);
    }
    Ok(found)
}

pub fn delete_operation(connection: &Connection, seq: i64) -> Result<usize> {
    for table in ["op_row", "op_value", "op_part"] {
        connection.prepare_cached(&format!("DELETE FROM {table} WHERE seq = ?1"))?.execute([seq])?;
    }
    connection.prepare_cached("DELETE FROM op WHERE seq = ?1")?.execute([seq])
}
