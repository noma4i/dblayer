use rusqlite::{params, params_from_iter, Connection};
use serde_json::{Map, Value};

use crate::ddl::fts_name;
use crate::schema::search_text;

use super::rows::placeholders;
use super::{ModelInfo, StoreError};

pub(crate) fn touches_search(info: &ModelInfo, changed: &std::collections::BTreeSet<String>) -> bool {
    info.search.iter().any(|path| path.first().is_some_and(|field| changed.contains(field)))
}

pub(crate) fn reindex(connection: &Connection, info: &ModelInfo, id: &str, row: &Map<String, Value>) -> Result<(), StoreError> {
    let table = fts_name(info.spec.key.as_str());
    let rowid: i64 = connection.query_row(&format!(include_str!("sql/search/reindex_select.sql"), info.table), params![id], |found| found.get(0))?;
    connection.prepare_cached(&format!(include_str!("sql/search/reindex_delete_table.sql"), table = table))?.execute(params![rowid])?;
    let texts: Vec<Option<String>> = info.search.iter().map(|path| search_text(row, path)).collect();
    if texts.iter().all(Option::is_none) {
        return Ok(());
    }
    let columns: Vec<String> = (0..texts.len()).map(|index| format!("c{index}")).collect();
    let sql = format!(include_str!("sql/search/reindex_insert_table.sql"), columns.join(", "), placeholders(texts.len()), table = table);
    let values = std::iter::once(rusqlite::types::Value::Integer(rowid)).chain(texts.into_iter().map(|text| text.map_or(rusqlite::types::Value::Null, rusqlite::types::Value::Text)));
    connection.prepare_cached(&sql)?.execute(params_from_iter(values))?;
    Ok(())
}

pub(crate) fn unindex(connection: &Connection, info: &ModelInfo, id: &str) -> Result<(), StoreError> {
    if info.search.is_empty() {
        return Ok(());
    }
    connection
        .prepare_cached(&format!(include_str!("sql/search/unindex.sql"), fts_name(info.spec.key.as_str()), info.table))?
        .execute(params![id])?;
    Ok(())
}
