use std::collections::HashMap;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection, Result, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::identity::RowId;
use crate::store::nodes::{delete_trees, read_trees, write_tree};
use crate::value::sync::OperationFailure;
use crate::transport::GraphqlExtensions;
use crate::owners::write::upload::UploadInfo;

pub fn enum_text<T: Serialize>(value: &T) -> Option<String> {
    match serde_json::to_value(value) {
        Ok(Value::String(text)) => Some(text),
        _ => None,
    }
}

pub fn enum_from<T: DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_value(Value::String(text.to_string())).map_err(from_sql_error)
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
    Ok(kind.map(|kind| enum_from(&kind)).transpose()?.zip(message).map(|(kind, message)| OperationFailure { kind, message, extensions: None }))
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

pub fn info_at(row: &Row<'_>, first: usize) -> Result<Option<UploadInfo>> {
    let Some(status) = row.get::<_, Option<String>>(first)?.map(|text| enum_from(&text)).transpose()? else { return Ok(None) };
    let stage = enum_from(&row.get::<_, String>(first + 1)?)?;
    Ok(Some(UploadInfo {
        status,
        stage,
        sent_bytes: row.get::<_, i64>(first + 2)? as u64,
        total_bytes: row.get::<_, i64>(first + 3)? as u64,
        wait_reason: row.get::<_, Option<String>>(first + 4)?.map(|text| enum_from(&text)).transpose()?,
        attempts: row.get::<_, i64>(first + 5)? as u32,
        error: failure_at(row, first + 6)?,
        local_url: None,
        processing: None,
        copy_file: None,
    }))
}

pub fn write_rows(connection: &Connection, seq: i64, row_ids: &[RowId]) -> Result<()> {
    connection.prepare_cached(include_str!("../sql/operation/write_rows_delete_op_row.sql"))?.execute([seq])?;
    let mut insert = connection.prepare_cached(include_str!("../sql/operation/write_rows_insert_op_row.sql"))?;
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

pub fn read_value(connection: &Connection, seq: i64, part: &str) -> Result<Option<Value>> {
    read_item_value(connection, seq, -1, part)
}

pub fn read_item_value(connection: &Connection, seq: i64, position: i64, part: &str) -> Result<Option<Value>> {
    let owner = [SqlValue::Integer(seq), SqlValue::Integer(position), SqlValue::Text(part.to_string())];
    Ok(read_trees(connection, "op_value", &["seq", "item_position", "part"], "seq = ?1 AND item_position = ?2 AND part = ?3", &owner)?.into_iter().next().map(|(_, value)| value))
}

pub fn read_rows(connection: &Connection, seq: i64) -> Result<Vec<RowId>> {
    let mut statement = connection.prepare_cached(include_str!("../sql/operation/read_rows.sql"))?;
    let ids = statement.query_map([seq], |row| row.get::<_, String>(0))?;
    ids.map(|id| stored_row_id(&id?)).collect()
}

pub fn stored_row_id(text: &str) -> Result<RowId> {
    RowId::new(text).map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error)))
}

pub fn read_all_rows(connection: &Connection) -> Result<HashMap<i64, Vec<RowId>>> {
    let mut statement = connection.prepare_cached(include_str!("../sql/operation/read_all_rows.sql"))?;
    let mut found: HashMap<i64, Vec<RowId>> = HashMap::new();
    for item in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))? {
        let (seq, id) = item?;
        found.entry(seq).or_default().push(stored_row_id(&id)?);
    }
    Ok(found)
}

pub(crate) fn read_fields(connection: &Connection, seq: i64) -> Result<Vec<String>> {
    let mut statement = connection.prepare_cached(include_str!("../sql/operation/read_fields.sql"))?;
    let fields = statement.query_map([seq], |row| row.get(0))?.collect();
    fields
}

pub fn delete_operation(connection: &Connection, seq: i64) -> Result<usize> {
    connection.execute(include_str!("../sql/operation/delete_operation_delete_op_transfer.sql"), [seq])?;
    for table in ["op_row", "op_value", "op_error", "op_effect_value", "op_effect"] {
        connection.prepare_cached(&format!(include_str!("../sql/operation/delete_operation_delete_table.sql"), table = table))?.execute([seq])?;
    }
    connection.prepare_cached(include_str!("../sql/operation/delete_operation_delete_op.sql"))?.execute([seq])
}
