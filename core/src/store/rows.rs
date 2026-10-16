#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::ddl::{manifest_read, ColumnType, REVISION_SEQ_KEY};
use crate::identity::touches::touch;
use crate::identity::RowId;
use crate::value::fractional_between;

#[cfg(test)]
use super::Store;
use crate::nodes::read_trees;
use super::shape::{assemble, read_cells, read_tables, take_leaf};
use super::{ModelInfo, StoreError};
use crate::ddl::FieldPlan;

pub(crate) const CHUNK: usize = 500;

pub(crate) fn column_value(kind: ColumnType, value: Option<&Value>) -> SqlValue {
    let Some(value) = value else { return SqlValue::Null };
    match (kind, value) {
        (ColumnType::Text, Value::String(text)) => SqlValue::Text(text.clone()),
        (ColumnType::Integer, Value::Bool(flag)) => SqlValue::Integer(i64::from(*flag)),
        (ColumnType::Integer, Value::Number(number)) => number
            .as_i64()
            .or_else(|| number.as_f64().filter(|float| float.fract() == 0.0).map(|float| float as i64))
            .map_or(SqlValue::Null, SqlValue::Integer),
        (ColumnType::Real, Value::Number(number)) => number.as_f64().map_or(SqlValue::Null, SqlValue::Real),
        _ => SqlValue::Null,
    }
}

/// `with_revisions` also reads the revision of every field; without it `field_rev` is empty.
pub(crate) fn fetch_stored(connection: &Connection, info: &ModelInfo, ids: &[String], with_revisions: bool) -> Result<BTreeMap<String, StoredRow>, StoreError> {
    let layout = &info.layout;
    let count = layout.row_columns().len();
    let revs: &[(String, String)] = if with_revisions { layout.rev_columns() } else { &[] };
    let select = if with_revisions { &layout.select } else { &layout.select_lean };
    let mut found = BTreeMap::new();
    for chunk in ids.chunks(CHUNK) {
        let sql = format!("SELECT {} FROM {} WHERE id IN ({})", select, info.table, placeholders(chunk.len()));
        let mut statement = connection.prepare_cached(&sql)?;
        let mut partial = Vec::new();
        let rows = statement.query_map(params_from_iter(chunk.iter()), |row| {
            let cells = read_cells(row, count, 3)?;
            let mut next = 3 + count;
            let mut field_rev = BTreeMap::new();
            for (field, _) in revs {
                if let Some(revision) = row.get::<_, Option<i64>>(next)? {
                    field_rev.insert(field.clone(), revision);
                }
                next += 1;
            }
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, cells, field_rev))
        })?;
        for item in rows {
            partial.push(item?);
        }
        let present: Vec<String> = partial.iter().map(|(id, ..)| id.clone()).collect();
        let tables = if layout.tables.is_empty() || present.is_empty() { HashMap::new() } else { read_tables(connection, layout, &present)? };
        let mut free: HashMap<String, Map<String, Value>> = HashMap::new();
        if let (Some(table), false) = (&layout.value_table, present.is_empty()) {
            let filter = format!("row_id IN ({})", placeholders(present.len()));
            let owners: Vec<SqlValue> = present.iter().cloned().map(SqlValue::Text).collect();
            for (owner, value) in read_trees(connection, table, &["row_id", "field"], &filter, &owners)? {
                if let [SqlValue::Text(row_id), SqlValue::Text(field)] = owner.as_slice() {
                    free.entry(row_id.clone()).or_default().insert(field.clone(), value);
                }
            }
        }
        for (id, revision, born_rev, mut cells, field_rev) in partial {
            let mut data = Map::new();
            data.insert("id".to_string(), Value::String(id.clone()));
            for plan in &layout.fields {
                match plan {
                    FieldPlan::Leaf { field, leaf } => {
                        if let Some(value) = take_leaf(leaf, &mut cells) {
                            data.insert(field.clone(), value);
                        }
                    }
                    FieldPlan::Typed { field, member, .. } => {
                        if let Some(value) = assemble(member, layout, &cells, tables.get(&id).map(Vec::as_slice).unwrap_or_default(), 0) {
                            data.insert(field.clone(), value);
                        }
                    }
                    FieldPlan::Rest { .. } => {}
                }
            }
            data.extend(free.remove(&id).unwrap_or_default());
            let row_id = RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
            found.insert(id, StoredRow { id: row_id, revision, born_rev, field_rev, row: data });
        }
    }
    Ok(found)
}

pub(crate) fn placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub id: RowId,
    pub revision: i64,
    pub born_rev: i64,
    pub field_rev: BTreeMap<String, i64>,
    pub row: Map<String, Value>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ScopeRowsResult {
    pub rows: Vec<Map<String, Value>>,
    pub dangling_members: usize,
}

pub struct Reader<'a> {
    connection: &'a Connection,
    models: &'a BTreeMap<String, ModelInfo>,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(connection: &'a Connection, models: &'a BTreeMap<String, ModelInfo>) -> Self {
        Self { connection, models }
    }

    pub(crate) fn connection(&self) -> &'a Connection {
        self.connection
    }

    fn model(&self, key: &str) -> Result<&'a ModelInfo, StoreError> {
        self.models.get(key).ok_or_else(|| StoreError::UnknownModel(key.to_string()))
    }

    pub fn revision_seq(&self) -> Result<i64, StoreError> {
        let value = manifest_read(self.connection, REVISION_SEQ_KEY)?;
        Ok(value.and_then(|text| text.parse().ok()).unwrap_or(0))
    }

    pub fn stored_row(&self, model: &str, id: &str) -> Result<Option<StoredRow>, StoreError> {
        Ok(fetch_stored(self.connection, self.model(model)?, &[id.to_string()], true)?.remove(id))
    }

    pub fn stored_rows(&self, model: &str, ids: &[String]) -> Result<BTreeMap<String, StoredRow>, StoreError> {
        fetch_stored(self.connection, self.model(model)?, ids, false)
    }

    pub fn point(&self, model: &str, id: &str) -> Result<Option<Map<String, Value>>, StoreError> {
        Ok(self.stored_rows(model, &[id.to_string()])?.remove(id).map(|stored| stored.row))
    }

    #[cfg(test)]
    pub fn row_count(&self, model: &str) -> Result<i64, StoreError> {
        let info = self.model(model)?;
        Ok(self.connection.query_row(&format!("SELECT COUNT(*) FROM {}", info.table), [], |row| row.get(0))?)
    }

    #[cfg(test)]
    pub fn scope_rows(&self, model: &str, scope_key: &str, limit: Option<usize>) -> Result<ScopeRowsResult, StoreError> {
        let members = self.scope_members(model, scope_key)?;
        let ids: Vec<String> = members.into_iter().map(|(id, _)| id).collect();
        let mut stored = self.stored_rows(model, &ids)?;
        let mut result = ScopeRowsResult::default();
        for id in ids {
            match stored.remove(&id) {
                Some(row) => result.rows.push(row.row),
                None => result.dangling_members += 1,
            }
        }
        if let Some(limit) = limit {
            result.rows.truncate(limit);
        }
        Ok(result)
    }

    pub fn scope_members(&self, model: &str, scope_key: &str) -> Result<Vec<(String, String)>, StoreError> {
        let mut statement = self.connection.prepare_cached(
            "SELECT row_id, order_key FROM scope_member WHERE model = ?1 AND scope_key = ?2 ORDER BY order_key, row_id",
        )?;
        let rows = statement.query_map(params![model, scope_key], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let members: Vec<(String, String)> = rows.collect::<Result<_, _>>()?;
        touch(members.len());
        Ok(members)
    }

    pub fn scope_has_member(&self, model: &str, scope_key: &str, row_id: &str) -> Result<bool, StoreError> {
        let found: Option<i64> = self
            .connection
            .query_row("SELECT 1 FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND row_id = ?3", params![model, scope_key, row_id], |row| row.get(0))
            .optional()?;
        Ok(found.is_some())
    }

    pub fn scope_first_key(&self, model: &str, scope_key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row("SELECT MIN(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2", params![model, scope_key], |row| row.get(0))?)
    }

    pub fn scope_last_key(&self, model: &str, scope_key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row("SELECT MAX(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2", params![model, scope_key], |row| row.get(0))?)
    }

    pub fn free_order_key(&self, model: &str, scope_key: &str, row_id: &str, wanted: &str, taken: &HashSet<String>) -> Result<String, StoreError> {
        let mut key = wanted.to_string();
        loop {
            let holder: Option<String> = self
                .connection
                .query_row("SELECT row_id FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND order_key = ?3", params![model, scope_key, key], |row| row.get(0))
                .optional()?;
            if holder.as_deref().is_none_or(|holder| holder == row_id) && !taken.contains(&key) {
                return Ok(key);
            }
            let upper: Option<String> = self
                .connection
                .query_row("SELECT MIN(order_key) FROM scope_member WHERE model = ?1 AND scope_key = ?2 AND order_key > ?3", params![model, scope_key, key], |row| row.get(0))?;
            key = fractional_between(Some(&key), upper.as_deref()).map_err(|error| StoreError::Constraint(error.to_string()))?;
        }
    }

    pub fn scopes_of(&self, model: &str, row_id: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare_cached("SELECT scope_key FROM scope_member WHERE model = ?1 AND row_id = ?2")?;
        let keys = statement.query_map(params![model, row_id], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }

    #[cfg(test)]
    pub fn count_members(&self, model: &str, scope_key: &str) -> Result<i64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM scope_member WHERE model = ?1 AND scope_key = ?2",
            params![model, scope_key],
            |row| row.get(0),
        )?)
    }

    pub fn tombstone_seq(&self, model: &str, id: &str) -> Result<Option<i64>, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT seq FROM tombstone WHERE model = ?1 AND id = ?2", params![model, id], |row| row.get(0))
            .optional()?)
    }

    #[cfg(test)]
    pub fn tombstones(&self, model: &str, ids: &[String]) -> Result<BTreeMap<String, i64>, StoreError> {
        let mut found = BTreeMap::new();
        for chunk in ids.chunks(CHUNK) {
            let sql = format!("SELECT id, seq FROM tombstone WHERE model = ? AND id IN ({})", placeholders(chunk.len()));
            let mut statement = self.connection.prepare_cached(&sql)?;
            let arguments = std::iter::once(model).chain(chunk.iter().map(String::as_str));
            let rows = statement.query_map(params_from_iter(arguments), |row| Ok((row.get(0)?, row.get(1)?)))?;
            for item in rows {
                let (id, seq): (String, i64) = item?;
                found.insert(id, seq);
            }
        }
        Ok(found)
    }

    #[cfg(test)]
    pub fn tombstoned_ids(&self, model: &str, ids: &[String]) -> Result<BTreeSet<String>, StoreError> {
        Ok(self.tombstones(model, ids)?.into_keys().collect())
    }

    #[cfg(test)]
    pub fn is_tombstoned(&self, model: &str, id: &str) -> Result<bool, StoreError> {
        Ok(self.tombstone_seq(model, id)?.is_some())
    }
}

#[cfg(test)]
impl Store {
    pub fn point(&self, model: &str, id: &str) -> Result<Option<Map<String, Value>>, StoreError> {
        self.read(&[model], |reader| reader.point(model, id))
    }

    pub fn rows_by_ids(&self, model: &str, ids: &[RowId]) -> Result<Vec<Map<String, Value>>, StoreError> {
        let names: Vec<String> = ids.iter().map(|id| id.as_str().to_string()).collect();
        let mut stored = self.read(&[model], |reader| reader.stored_rows(model, &names))?;
        Ok(names.iter().filter_map(|name| stored.remove(name)).map(|row| row.row).collect())
    }

    pub fn scope_rows(&self, model: &str, scope_key: &str, limit: Option<usize>) -> Result<ScopeRowsResult, StoreError> {
        let result = self.read(&[model], |reader| reader.scope_rows(model, scope_key, limit))?;
        self.lock().counters.membership_missing_entity += result.dangling_members;
        Ok(result)
    }

    pub fn row_count(&self, model: &str) -> Result<i64, StoreError> {
        self.read(&[model], |reader| reader.row_count(model))
    }
}
