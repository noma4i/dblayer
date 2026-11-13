use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

use super::statement::{CompiledStatement, QueryError};

pub fn fetch_count(connection: &Connection, statement: &CompiledStatement) -> Result<i64, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    Ok(prepared.query_row(params_from_iter(statement.bindings.iter()), |row| row.get(0))?)
}

pub fn fetch_keys(connection: &Connection, statement: &CompiledStatement) -> Result<Vec<(String, i64)>, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    let keys = prepared.query_map(params_from_iter(statement.bindings.iter()), |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(keys.collect::<Result<_, _>>()?)
}

pub struct RankedRow {
    pub id: String,
    pub revision: i64,
    pub cells: Vec<Value>,
    pub id_key: String,
}

pub fn fetch_ranked(connection: &Connection, statement: &CompiledStatement) -> Result<Vec<RankedRow>, QueryError> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    let width = prepared.column_count();
    let rows = prepared.query_map(params_from_iter(statement.bindings.iter()), |row| {
        let cells = (2..width - 1).map(|index| row.get::<_, Value>(index)).collect::<Result<_, _>>()?;
        Ok(RankedRow { id: row.get(0)?, revision: row.get(1)?, cells, id_key: row.get(width - 1)? })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}
