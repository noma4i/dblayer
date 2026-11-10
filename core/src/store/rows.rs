#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, StatementStatus};
use serde_json::{Map, Value};

use crate::store::EdgeValue;
use crate::store::ddl::{manifest_read, ColumnType, REVISION_SEQ_KEY};
use crate::identity::touches::read_members;
use crate::identity::RowId;
use crate::value::fractional_between;

#[cfg(test)]
use super::Store;
use crate::store::nodes::read_trees;
use super::shape::{assemble, read_cells, read_tables, take_leaf};
use super::{ModelInfo, StoreError};
use crate::store::ddl::FieldPlan;

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
        let sql = format!(include_str!("sql/rows/fetch_stored.sql"), select, info.table, placeholders(chunk.len()));
        let mut statement = connection.prepare_cached(&sql)?;
        let mut partial = Vec::new();
        let rows = statement.query_map(params_from_iter(chunk.iter()), |row| {
            let cells = read_cells(row, count, 3)?;
            let mut field_rev = BTreeMap::new();
            for (next, (field, _)) in (3 + count..).zip(revs) {
                if let Some(revision) = row.get::<_, Option<i64>>(next)? {
                    field_rev.insert(field.clone(), revision);
                }
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

pub(crate) const MEMBER_EXISTS_SQL: &str = include_str!("sql/rows/placeholders.sql");

pub(crate) fn read_revision(connection: &Connection) -> Result<i64, StoreError> {
    manifest_read(connection, REVISION_SEQ_KEY)?.map(|text| {
        text.parse::<i64>().map_err(|error| StoreError::Constraint(format!("invalid stored revision: {error}")))
    }).transpose().map(|revision| revision.unwrap_or(0))
}

pub struct Reader<'a> {
    connection: &'a Connection,
    models: &'a BTreeMap<String, ModelInfo>,
    overlay: Option<&'a super::overlay::OverlaySnapshot>,
}

#[derive(Debug, Default)]
pub struct EdgeRead {
    pub members: std::collections::BTreeMap<String, Vec<(String, EdgeValue)>>,
    pub steps: usize,
    pub scans: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(connection: &'a Connection, models: &'a BTreeMap<String, ModelInfo>) -> Self {
        Self { connection, models, overlay: None }
    }

    pub(super) fn with_overlay(connection: &'a Connection, models: &'a BTreeMap<String, ModelInfo>, overlay: &'a super::overlay::OverlaySnapshot) -> Self { Self { connection, models, overlay: Some(overlay) } }

    pub(crate) fn connection(&self) -> &'a Connection {
        self.connection
    }

    pub(super) fn models(&self) -> &'a BTreeMap<String, ModelInfo> { self.models }

    fn model(&self, key: &str) -> Result<&'a ModelInfo, StoreError> {
        self.models.get(key).ok_or_else(|| StoreError::UnknownModel(key.to_string()))
    }

    pub fn revision_seq(&self) -> Result<i64, StoreError> {
        read_revision(self.connection)
    }

    pub fn stored_row(&self, model: &str, id: &str) -> Result<Option<StoredRow>, StoreError> {
        Ok(fetch_stored(self.connection, self.model(model)?, &[id.to_string()], true)?.remove(id))
    }

    pub fn stored_rows(&self, model: &str, ids: &[String]) -> Result<BTreeMap<String, StoredRow>, StoreError> {
        let info = self.model(model)?;
        let mut rows = fetch_stored(self.connection, info, ids, false)?;
        if let Some(overlay) = self.overlay {
            for (id, row) in &mut rows {
                let key = super::overlay::RowKey { model: model.to_string(), row_id: id.clone() };
                for field in &info.spec.ephemeral {
                    let value = overlay.values.get(&key).and_then(|values| values.get(&field.field.name)).map(|value| value.value.clone()).unwrap_or_else(|| field.initial());
                    row.row.insert(field.field.name.clone(), value);
                }
            }
        }
        Ok(rows)
    }

    pub fn point(&self, model: &str, id: &str) -> Result<Option<Map<String, Value>>, StoreError> {
        Ok(self.stored_rows(model, &[id.to_string()])?.remove(id).map(|stored| stored.row))
    }

    #[cfg(test)]
    pub fn row_count(&self, model: &str) -> Result<i64, StoreError> {
        let info = self.model(model)?;
        Ok(self.connection.query_row(&format!(include_str!("sql/rows/row_count.sql"), info.table), [], |row| row.get(0))?)
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
            include_str!("sql/rows/scope_members.sql"),
        )?;
        let rows = statement.query_map(params![model, scope_key], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let members: Vec<(String, String)> = rows.collect::<Result<_, _>>()?;
        read_members(members.len());
        Ok(members)
    }

    pub(crate) fn scope_changes_since(&self, model: &str, scope_key: &str, revision: i64) -> Result<Vec<(String, Option<String>)>, StoreError> {
        let mut statement = self.connection.prepare_cached(
            include_str!("sql/rows/scope_changes_since.sql"),
        )?;
        let rows = statement.query_map(params![model, scope_key, revision], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn scope_first_ids(&self, model: &str, scope_key: &str, limit: usize) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare_cached(
            include_str!("sql/rows/scope_first_ids.sql"),
        )?;
        let rows = statement.query_map(params![model, scope_key, limit as i64], |row| row.get(0))?;
        let ids: Vec<String> = rows.collect::<Result<_, _>>()?;
        read_members(ids.len());
        Ok(ids)
    }

    pub fn scope_edges(&self, model: &str, scope_key: &str, row_ids: &[&str]) -> Result<EdgeRead, StoreError> {
        let mut statement = self.connection.prepare_cached(
            include_str!("sql/rows/scope_edges.sql"),
        )?;
        let mut read = EdgeRead::default();
        let ids = serde_json::to_string(row_ids).map_err(|error| StoreError::Constraint(error.to_string()))?;
        statement.reset_status(StatementStatus::VmStep);
        statement.reset_status(StatementStatus::FullscanStep);
        let mut rows = statement.query(params![model, scope_key, ids])?;
        while let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                let values = read.members.entry(id).or_default();
                let Some(name) = row.get::<_, Option<String>>(1)? else { continue };
                let value = match (row.get::<_, Option<i64>>(2)?, row.get::<_, Option<f64>>(3)?, row.get::<_, Option<String>>(4)?) {
                    (Some(whole), _, _) => EdgeValue::Whole(whole),
                    (_, Some(fraction), _) => EdgeValue::Fraction(fraction),
                    (_, _, Some(text)) => EdgeValue::Text(text),
                    _ => continue,
                };
                values.push((name, value));
        }
        drop(rows);
        read.steps += usize::try_from(statement.get_status(StatementStatus::VmStep)).unwrap_or_default();
        read.scans += usize::try_from(statement.get_status(StatementStatus::FullscanStep)).unwrap_or_default();
        Ok(read)
    }

    pub fn scope_members_among(&self, model: &str, scope_key: &str, row_ids: &[&str]) -> Result<Vec<String>, StoreError> {
        let mut found = Vec::new();
        for row_id in row_ids {
            if self.scope_has_member(model, scope_key, row_id)? {
                found.push(row_id.to_string());
            }
        }
        read_members(found.len());
        Ok(found)
    }

    pub fn scope_has_member(&self, model: &str, scope_key: &str, row_id: &str) -> Result<bool, StoreError> {
        Ok(self.connection.prepare_cached(MEMBER_EXISTS_SQL)?.exists(params![model, scope_key, row_id])?)
    }

    pub fn scope_member_key(&self, model: &str, scope_key: &str, row_id: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row(include_str!("sql/rows/scope_member_key.sql"), params![model, scope_key, row_id], |row| row.get(0)).optional()?)
    }

    pub fn scope_first_key(&self, model: &str, scope_key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row(include_str!("sql/rows/scope_first_key.sql"), params![model, scope_key], |row| row.get(0))?)
    }

    pub fn scope_last_key(&self, model: &str, scope_key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row(include_str!("sql/rows/scope_last_key.sql"), params![model, scope_key], |row| row.get(0))?)
    }

    pub fn free_order_key(&self, model: &str, scope_key: &str, row_id: &str, wanted: &str, taken: &HashSet<String>) -> Result<String, StoreError> {
        let mut key = wanted.to_string();
        loop {
            let holder: Option<String> = self
                .connection
                .query_row(include_str!("sql/rows/free_order_key_select_scope_member.sql"), params![model, scope_key, key], |row| row.get(0))
                .optional()?;
            if holder.as_deref().is_none_or(|holder| holder == row_id) && !taken.contains(&key) {
                return Ok(key);
            }
            let upper: Option<String> = self
                .connection
                .query_row(include_str!("sql/rows/free_order_key_select_scope_member_2.sql"), params![model, scope_key, key], |row| row.get(0))?;
            key = fractional_between(Some(&key), upper.as_deref()).map_err(|error| StoreError::Constraint(error.to_string()))?;
        }
    }

    pub fn scopes_of(&self, model: &str, row_id: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare_cached(include_str!("sql/rows/scopes_of.sql"))?;
        let keys = statement.query_map(params![model, row_id], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }

    pub fn scope_last_id(&self, model: &str, scope_key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.connection.query_row(
            include_str!("sql/rows/scope_last_id.sql"),
            params![model, scope_key], |row| row.get(0),
        ).optional()?)
    }

    pub fn scope_member_count(&self, model: &str, scope_key: &str) -> Result<i64, StoreError> {
        Ok(self.connection.query_row(include_str!("sql/rows/scope_member_count.sql"), params![model, scope_key], |row| row.get(0))?)
    }

    pub fn tombstone_seq(&self, model: &str, id: &str) -> Result<Option<i64>, StoreError> {
        Ok(self
            .connection
            .query_row(include_str!("sql/rows/tombstone_seq.sql"), params![model, id], |row| row.get(0))
            .optional()?)
    }

    #[cfg(test)]
    pub fn tombstones(&self, model: &str, ids: &[String]) -> Result<BTreeMap<String, i64>, StoreError> {
        let mut found = BTreeMap::new();
        for chunk in ids.chunks(CHUNK) {
            let sql = format!(include_str!("sql/rows/tombstones.sql"), placeholders(chunk.len()));
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

    #[cfg(test)]
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
