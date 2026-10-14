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
