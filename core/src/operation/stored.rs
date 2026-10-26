use std::collections::HashMap;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection, Result, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::identity::RowId;
use crate::nodes::{delete_trees, read_trees, write_tree};
use crate::syncstate::OperationFailure;
use crate::transport::GraphqlExtensions;
use crate::upload::{UploadInfo, UploadPart, UploadRequest, UploadRetry, UploadState};

pub const UPLOAD_COLUMNS: &str = "phase, stage, scenario, total_bytes, sent_bytes, storage_key, upload_id, signed_id, blob_id, document, operation, transferred, confirmed_bytes, checksum, put_url, url_issued_at, failed_at, error_kind, error_message, offline";
pub const INFO_COLUMNS: &str = "status, stage, sent, total, wait, attempts, error_kind, error_message";

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
    Ok(kind.and_then(|kind| enum_from(&kind)).zip(message).map(|(kind, message)| OperationFailure { kind, message, extensions: None }))
}

fn filter_of(keys: &[&str]) -> String {
    keys.iter().enumerate().map(|(at, key)| format!("{key} = ?{}", at + 1)).collect::<Vec<_>>().join(" AND ")
}

fn to_sql_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn from_sql_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Null, Box::new(error))
}

fn extensions_tree(extensions: &GraphqlExtensions) -> Result<Value> {
    let mut tree = serde_json::to_value(extensions).map_err(to_sql_error)?;
    if let Some(Value::Object(fields)) = tree.get_mut("fields") {
        for field in fields.values_mut() {
            let text = field.as_str().ok_or_else(|| to_sql_error(serde::de::Error::custom("extension field is not a string")))?;
            *field = serde_json::from_str(text).map_err(to_sql_error)?;
        }
    }
    Ok(tree)
}

fn extensions_from_tree(mut tree: Value) -> Result<GraphqlExtensions> {
    if let Some(Value::Object(fields)) = tree.get_mut("fields") {
        for field in fields.values_mut() {
            *field = Value::String(field.to_string());
        }
    }
    serde_json::from_value(tree).map_err(from_sql_error)
}

pub fn write_extensions(connection: &Connection, table: &str, keys: &[&str], owner: &[SqlValue], failure: Option<&OperationFailure>) -> Result<()> {
    delete_trees(connection, table, &filter_of(keys), owner)?;
    let Some(extensions) = failure.and_then(|failure| failure.extensions.as_ref()) else { return Ok(()) };
    write_tree(connection, table, keys, owner, &extensions_tree(extensions)?)
}

pub fn read_extensions(connection: &Connection, table: &str, keys: &[&str], filter: &str, params: &[SqlValue]) -> Result<Vec<(Vec<SqlValue>, GraphqlExtensions)>> {
    read_trees(connection, table, keys, filter, params)?.into_iter()
        .map(|(owner, tree)| extensions_from_tree(tree).map(|extensions| (owner, extensions)))
        .collect()
}

pub fn attach_extensions(error: &mut Option<OperationFailure>, extensions: Option<GraphqlExtensions>) {
    if let Some(error) = error {
        error.extensions = extensions;
    }
}

pub fn write_operation_error(connection: &Connection, seq: i64, failure: Option<&OperationFailure>) -> Result<()> {
    write_extensions(connection, "op_error", &["seq", "item_position"], &[SqlValue::Integer(seq), SqlValue::Integer(-1)], failure)
}

pub fn operation_extensions(connection: &Connection, seq: Option<i64>) -> Result<HashMap<i64, GraphqlExtensions>> {
    let (filter, params) = match seq {
        Some(seq) => ("seq = ?1 AND item_position = -1", vec![SqlValue::Integer(seq)]),
        None => ("item_position = -1", Vec::new()),
    };
    Ok(read_extensions(connection, "op_error", &["seq", "item_position"], filter, &params)?.into_iter()
        .filter_map(|(owner, extensions)| match owner.first() {
            Some(SqlValue::Integer(seq)) => Some((*seq, extensions)),
            _ => None,
        })
        .collect())
}

fn item_extensions(connection: &Connection, seq: Option<i64>) -> Result<HashMap<(i64, i64), GraphqlExtensions>> {
    let (filter, params) = match seq {
        Some(seq) => ("seq = ?1 AND item_position >= 0", vec![SqlValue::Integer(seq)]),
        None => ("item_position >= 0", Vec::new()),
    };
    read_extensions(connection, "op_error", &["seq", "item_position"], filter, &params)?.into_iter()
        .map(|(owner, extensions)| match owner.as_slice() {
            [SqlValue::Integer(seq), SqlValue::Integer(position)] => Ok(((*seq, *position), extensions)),
            _ => Err(rusqlite::Error::InvalidQuery),
        })
        .collect()
}

pub fn upload_cells(state: Option<&UploadState>) -> Result<Vec<SqlValue>> {
    let Some(state) = state else { return Ok(vec![SqlValue::Null; 20]) };
    let [kind, message] = failure_cells(state.error.as_ref());
    Ok(vec![
        text_cell(enum_text(&state.phase)),
        text_cell(enum_text(&state.stage)),
        text_cell(state.scenario.and_then(|scenario| enum_text(&scenario))),
        SqlValue::Integer(state.total_bytes as i64),
        SqlValue::Integer(state.sent_bytes as i64),
        text_cell(state.key.clone()),
        text_cell(state.upload_id.clone()),
        text_cell(state.signed_id.clone()),
        text_cell(state.blob_id.clone()),
        text_cell(state.request.as_ref().map(|request| request.document.clone())),
        text_cell(state.request.as_ref().map(|request| request.name.clone())),
        SqlValue::Integer(i64::from(state.transferred)),
        SqlValue::Integer(state.confirmed_bytes as i64),
        text_cell(state.checksum.clone()),
        text_cell(state.put_url.clone()),
        state.url_issued_at.map_or(SqlValue::Null, |time| SqlValue::Integer(time as i64)),
        state.failed_at.map_or(SqlValue::Null, |time| SqlValue::Integer(time as i64)),
        kind,
        message,
        SqlValue::Integer(i64::from(state.offline)),
    ])
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
        retries: Vec::new(),
        key: row.get(first + 5)?,
        upload_id: row.get(first + 6)?,
        parts: Vec::new(),
        signed_id: row.get(first + 7)?,
        blob_id: row.get(first + 8)?,
        request: row.get::<_, Option<String>>(first + 9)?.zip(row.get::<_, Option<String>>(first + 10)?).map(|(document, name)| UploadRequest { document, name, variables: Value::Null }),
        transferred: row.get::<_, i64>(first + 11)? != 0,
        confirmed_bytes: row.get::<_, i64>(first + 12)? as u64,
        checksum: row.get(first + 13)?,
        put_url: row.get(first + 14)?,
        url_issued_at: row.get::<_, Option<i64>>(first + 15)?.map(|time| time as u64),
        failed_at: row.get::<_, Option<i64>>(first + 16)?.map(|time| time as u64),
        headers: Vec::new(),
        error: failure_at(row, first + 17)?,
        background_refused: None,
        offline: row.get::<_, Option<i64>>(first + 19)?.is_some_and(|flag| flag != 0),
    }))
}

pub fn read_upload_items(connection: &Connection) -> Result<HashMap<(i64, i64), UploadState>> {
    upload_items(connection, None)
}

pub(crate) fn read_operation_uploads(connection: &Connection, seq: i64) -> Result<HashMap<(i64, i64), UploadState>> {
    upload_items(connection, Some(seq))
}

fn upload_items(connection: &Connection, seq: Option<i64>) -> Result<HashMap<(i64, i64), UploadState>> {
    let mut parts = read_parts(connection, seq)?;
    let filter = if seq.is_some() { "WHERE seq = ?1" } else { "" };
    let mut statement = connection.prepare(&format!("SELECT seq, position, {UPLOAD_COLUMNS}, background_refused FROM op_item {filter} ORDER BY seq, position"))?;
    let mut found = HashMap::new();
    let mut extensions = item_extensions(connection, seq)?;
    for item in statement.query_map(rusqlite::params_from_iter(seq), |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, upload_at(row, 2)?, row.get::<_, Option<String>>(2 + UPLOAD_COLUMNS.split(", ").count())?)))? {
        let (seq, position, state, refused) = item?;
        if let Some(mut state) = state {
            state.background_refused = refused;
            attach_extensions(&mut state.error, extensions.remove(&(seq, position)));
            let mut headers = connection.prepare_cached("SELECT name, value FROM op_item_header WHERE seq = ?1 AND position = ?2 ORDER BY name")?;
            state.headers = headers.query_map(params![seq, position], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<Vec<_>>>()?;
            state.parts = parts.remove(&(seq, position)).unwrap_or_default();
            let mut retries = connection.prepare_cached("SELECT stage, part_number, attempts, retry_at FROM op_item_retry WHERE seq = ?1 AND position = ?2 ORDER BY stage, part_number")?;
            state.retries = retries.query_map(params![seq, position], |row| {
                let stage: String = row.get(0)?;
                Ok(UploadRetry {
                    stage: enum_from(&stage).ok_or_else(|| rusqlite::Error::InvalidParameterName(stage))?,
                    part_number: row.get(1)?, attempts: row.get(2)?, retry_at: row.get::<_, Option<i64>>(3)?.map(|time| time as u64),
                })
            })?.collect::<Result<Vec<_>>>()?;
            if let Some(request) = state.request.as_mut() {
                request.variables = read_item_value(connection, seq, position, "request")?.unwrap_or(Value::Null);
            }
            found.insert((seq, position), state);
        }
    }
    Ok(found)
}

pub fn read_uploads(connection: &Connection) -> Result<HashMap<i64, UploadState>> {
    let mut items: Vec<_> = read_upload_items(connection)?.into_iter().collect();
    items.sort_by_key(|(key, _)| *key);
    let mut uploads = HashMap::new();
    for ((seq, _), state) in items { uploads.entry(seq).or_insert(state); }
    Ok(uploads)
}

pub fn write_upload(connection: &Connection, seq: i64, position: i64, state: &UploadState) -> Result<usize> {
    let columns: Vec<&str> = UPLOAD_COLUMNS.split(", ").collect();
    let marks = (3..columns.len() + 3).map(|at| format!("?{at}")).collect::<Vec<_>>().join(", ");
    let assignments = columns.iter().map(|name| format!("{name} = excluded.{name}")).collect::<Vec<_>>().join(", ");
    let mut values = vec![SqlValue::Integer(seq), SqlValue::Integer(position)];
    values.extend(upload_cells(Some(state))?);
    let saved = connection.execute(&format!("INSERT INTO op_item (seq, position, {UPLOAD_COLUMNS}) SELECT ?1, ?2, {marks} WHERE EXISTS (SELECT 1 FROM op WHERE seq = ?1) ON CONFLICT (seq, position) DO UPDATE SET {assignments}"), rusqlite::params_from_iter(values))?;
    if saved > 0 {
        connection.execute("DELETE FROM op_item_retry WHERE seq = ?1 AND position = ?2", params![seq, position])?;
        for retry in &state.retries {
            connection.execute("INSERT INTO op_item_retry (seq, position, stage, part_number, attempts, retry_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![seq, position, enum_text(&retry.stage), retry.part_number, retry.attempts, retry.retry_at.map(|time| time as i64)])?;
        }
        connection.execute("DELETE FROM op_item_header WHERE seq = ?1 AND position = ?2", params![seq, position])?;
        for (name, value) in &state.headers {
            connection.execute("INSERT INTO op_item_header (seq, position, name, value) VALUES (?1, ?2, ?3, ?4)", params![seq, position, name, value])?;
        }
        write_extensions(connection, "op_error", &["seq", "item_position"], &[SqlValue::Integer(seq), SqlValue::Integer(position)], state.error.as_ref())?;
        write_parts(connection, seq, position, &state.parts)?;
        write_item_value(connection, seq, position, "request", state.request.as_ref().map(|request| &request.variables))?;
    }
    Ok(saved)
}

pub fn info_cells(info: Option<&UploadInfo>) -> Result<Vec<SqlValue>> {
    let Some(info) = info else { return Ok(vec![SqlValue::Null; 8]) };
    let [kind, message] = failure_cells(info.error.as_ref());
    Ok(vec![
        text_cell(enum_text(&info.status)),
        text_cell(enum_text(&info.stage)),
        SqlValue::Integer(info.sent_bytes as i64),
        SqlValue::Integer(info.total_bytes as i64),
        text_cell(info.wait_reason.and_then(|reason| enum_text(&reason))),
        SqlValue::Integer(i64::from(info.attempts)),
        kind,
        message,
    ])
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
    write_item_value(connection, seq, -1, part, value)
}

pub fn write_item_value(connection: &Connection, seq: i64, position: i64, part: &str, value: Option<&Value>) -> Result<()> {
    let owner = [SqlValue::Integer(seq), SqlValue::Integer(position), SqlValue::Text(part.to_string())];
    delete_trees(connection, "op_value", "seq = ?1 AND item_position = ?2 AND part = ?3", &owner)?;
    value.map_or(Ok(()), |value| write_tree(connection, "op_value", &["seq", "item_position", "part"], &owner, value))
}

pub fn write_parts(connection: &Connection, seq: i64, position: i64, parts: &[UploadPart]) -> Result<()> {
    connection.prepare_cached("DELETE FROM op_part WHERE seq = ?1 AND item_position = ?2")?.execute(params![seq, position])?;
    let mut insert = connection.prepare_cached("INSERT INTO op_part (seq, item_position, number, etag, confirmed_bytes) VALUES (?1, ?2, ?3, ?4, ?5)")?;
    for part in parts {
        insert.execute(params![seq, position, i64::from(part.number), part.etag, part.confirmed_bytes as i64])?;
    }
    Ok(())
}

pub fn read_value(connection: &Connection, seq: i64, part: &str) -> Result<Option<Value>> {
    read_item_value(connection, seq, -1, part)
}

pub fn read_item_value(connection: &Connection, seq: i64, position: i64, part: &str) -> Result<Option<Value>> {
    let owner = [SqlValue::Integer(seq), SqlValue::Integer(position), SqlValue::Text(part.to_string())];
    Ok(read_trees(connection, "op_value", &["seq", "item_position", "part"], "seq = ?1 AND item_position = ?2 AND part = ?3", &owner)?.into_iter().next().map(|(_, value)| value))
}

pub fn read_rows(connection: &Connection, seq: i64) -> Result<Vec<RowId>> {
    let mut statement = connection.prepare_cached("SELECT row_id FROM op_row WHERE seq = ?1 ORDER BY pos")?;
    let ids = statement.query_map([seq], |row| row.get::<_, String>(0))?;
    ids.map(|id| stored_row_id(&id?)).collect()
}

pub fn stored_row_id(text: &str) -> Result<RowId> {
    RowId::new(text).map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error)))
}

pub fn read_all_rows(connection: &Connection) -> Result<HashMap<i64, Vec<RowId>>> {
    let mut statement = connection.prepare_cached("SELECT seq, row_id FROM op_row ORDER BY seq, pos")?;
    let mut found: HashMap<i64, Vec<RowId>> = HashMap::new();
    for item in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))? {
        let (seq, id) = item?;
        found.entry(seq).or_default().push(stored_row_id(&id)?);
    }
    Ok(found)
}

fn read_parts(connection: &Connection, seq: Option<i64>) -> Result<HashMap<(i64, i64), Vec<UploadPart>>> {
    let filter = if seq.is_some() { "WHERE seq = ?1" } else { "" };
    let mut statement = connection.prepare_cached(&format!("SELECT seq, item_position, number, etag, confirmed_bytes FROM op_part {filter} ORDER BY seq, item_position, number"))?;
    let mut found: HashMap<(i64, i64), Vec<UploadPart>> = HashMap::new();
    for item in statement.query_map(rusqlite::params_from_iter(seq), |row| Ok(((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?), UploadPart { number: row.get::<_, i64>(2)? as u32, etag: row.get(3)?, confirmed_bytes: row.get::<_, i64>(4)? as u64 })))? {
        let (owner, part) = item?;
        found.entry(owner).or_default().push(part);
    }
    Ok(found)
}

pub(crate) fn read_fields(connection: &Connection, seq: i64) -> Result<Vec<String>> {
    let mut statement = connection.prepare_cached("SELECT name FROM op_value WHERE seq = ?1 AND item_position = -1 AND part = 'input' AND parent = 0 AND name IS NOT NULL AND name != 'id' ORDER BY pos")?;
    let fields = statement.query_map([seq], |row| row.get(0))?.collect();
    fields
}

pub(crate) fn record_orphan_uploads(connection: &Connection, seq: i64, position: Option<i64>) -> Result<()> {
    let mut statement = connection.prepare_cached("SELECT position, storage_key, upload_id FROM op_item WHERE seq = ?1 AND upload_id IS NOT NULL AND storage_key IS NOT NULL AND (?2 IS NULL OR position = ?2)")?;
    let pending = statement.query_map(params![seq, position], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))?.collect::<Result<Vec<_>>>()?;
    for (item, key, upload_id) in pending {
        let input = read_item_value(connection, seq, item, "input")?.and_then(|value| serde_json::from_value::<crate::upload::UploadInput>(value).ok());
        let Some(steps) = input.and_then(|input| input.steps.multipart) else { continue };
        connection.prepare_cached("INSERT INTO upload_orphan (storage_key, upload_id, abort_operation) VALUES (?1, ?2, ?3)")?.execute(params![key, upload_id, steps.abort])?;
    }
    Ok(())
}

pub fn delete_operation(connection: &Connection, seq: i64) -> Result<usize> {
    connection.execute("DELETE FROM file WHERE id IN (SELECT file_id FROM op_item WHERE seq = ?1) AND field LIKE 'op_item.%' AND NOT EXISTS (SELECT 1 FROM op_item WHERE op_item.file_id = file.id AND seq != ?1)", [seq])?;
    for table in ["op_row", "op_value", "op_error", "op_part", "op_item_header", "op_item_retry", "op_item"] {
        connection.prepare_cached(&format!("DELETE FROM {table} WHERE seq = ?1"))?.execute([seq])?;
    }
    connection.prepare_cached("DELETE FROM op WHERE seq = ?1")?.execute([seq])
}
