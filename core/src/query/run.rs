use rusqlite::{params_from_iter, Connection};
use serde_json::{Map, Value};

use crate::ddl::RECORD_VERSION;

use super::statement::{CompiledStatement, QueryError};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowsRead {
    pub rows: Vec<Map<String, Value>>,
    pub stale_version_drops: usize,
    pub rejected_records: usize,
}

pub fn fetch_rows(connection: &Connection, statement: &CompiledStatement) -> Result<RowsRead, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    let records = prepared.query_map(params_from_iter(statement.bindings.iter()), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?))
    })?;
    let mut read = RowsRead::default();
    for record in records {
        let (id, record_version, payload) = record?;
        if record_version != RECORD_VERSION {
            read.stale_version_drops += 1;
            continue;
        }
        match serde_json::from_slice::<Value>(&payload) {
            Ok(Value::Object(row)) if row.get("id").and_then(Value::as_str) == Some(id.as_str()) => read.rows.push(row),
            _ => read.rejected_records += 1,
        }
    }
    Ok(read)
}

pub fn fetch_count(connection: &Connection, statement: &CompiledStatement) -> Result<i64, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    Ok(prepared.query_row(params_from_iter(statement.bindings.iter()), |row| row.get(0))?)
}

pub fn fetch_keys(connection: &Connection, statement: &CompiledStatement) -> Result<Vec<(String, i64)>, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    let keys = prepared.query_map(params_from_iter(statement.bindings.iter()), |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(keys.collect::<Result<_, _>>()?)
}
