#[cfg(test)]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashSet};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::ddl::{manifest_read, ColumnType, RECORD_VERSION, REVISION_SEQ_KEY};
use crate::identity::RowId;
use crate::value::fractional_between;

#[cfg(test)]
use super::Store;
use super::{ModelInfo, StoreError};

pub(crate) const CHUNK: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationVerdict {
    Accepted,
    StaleVersion,
    IdentityMismatch,
    Unreadable,
}

impl HydrationVerdict {
    #[cfg(test)]
    pub fn is_silent_drop(self) -> bool {
        self == HydrationVerdict::StaleVersion
    }
}

pub(crate) fn hydrate(record_version: i64, payload: &[u8], expected_id: &str) -> (HydrationVerdict, Option<Map<String, Value>>) {
    if record_version != RECORD_VERSION {
        return (HydrationVerdict::StaleVersion, None);
    }
    match serde_json::from_slice::<Value>(payload) {
        Ok(Value::Object(row)) if row.get("id").and_then(Value::as_str) == Some(expected_id) => {
            (HydrationVerdict::Accepted, Some(row))
        }
        Ok(Value::Object(_)) => (HydrationVerdict::IdentityMismatch, None),
        _ => (HydrationVerdict::Unreadable, None),
    }
}

pub(crate) fn encode_row(row: &Map<String, Value>) -> Result<Option<(String, Vec<u8>)>, serde_json::Error> {
    let Some(id) = row.get("id").and_then(Value::as_str).filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    Ok(Some((id.to_string(), serde_json::to_vec(row)?)))
}

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

pub(crate) fn placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub id: RowId,
    pub revision: i64,
    pub born_rev: i64,
    pub field_rev: BTreeMap<String, i64>,
    pub verdict: HydrationVerdict,
    pub row: Option<Map<String, Value>>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ScopeRowsResult {
    pub rows: Vec<Map<String, Value>>,
    pub stale_version_drops: usize,
    pub rejected_records: usize,
    pub dangling_members: usize,
    pub bad_ids: Vec<String>,
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
        Ok(self.stored_rows(model, &[id.to_string()])?.remove(id))
    }

    pub fn stored_rows(&self, model: &str, ids: &[String]) -> Result<BTreeMap<String, StoredRow>, StoreError> {
        let info = self.model(model)?;
        let mut found = BTreeMap::new();
        for chunk in ids.chunks(CHUNK) {
            let sql = format!(
                "SELECT id, revision, born_rev, field_rev, record_version, payload FROM {} WHERE id IN ({})",
                info.table,
                placeholders(chunk.len())
            );
            let mut statement = self.connection.prepare_cached(&sql)?;
            let rows = statement.query_map(params_from_iter(chunk.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            })?;
            for item in rows {
                let (id, revision, born_rev, field_rev, record_version, payload) = item?;
                let (verdict, row) = hydrate(record_version, &payload, &id);
                let field_rev: BTreeMap<String, i64> = serde_json::from_slice(&field_rev).unwrap_or_default();
                let Ok(row_id) = RowId::new(&id) else { continue };
                found.insert(id, StoredRow { id: row_id, revision, born_rev, field_rev, verdict, row });
            }
        }
        Ok(found)
    }

    pub fn point(&self, model: &str, id: &str) -> Result<Option<Map<String, Value>>, StoreError> {
        Ok(self.stored_row(model, id)?.and_then(|stored| stored.row))
    }

    #[cfg(test)]
    pub fn row_count(&self, model: &str) -> Result<i64, StoreError> {
        let info = self.model(model)?;
        Ok(self.connection.query_row(&format!("SELECT COUNT(*) FROM {}", info.table), [], |row| row.get(0))?)
    }

    #[cfg(test)]
    pub fn scope_rows(&self, model: &str, scope_key: &str, limit: Option<usize>) -> Result<ScopeRowsResult, StoreError> {
        let info = self.model(model)?;
        let sql = format!(
            "SELECT m.row_id, r.id, r.record_version, r.payload FROM scope_member m \
             LEFT JOIN {} r ON r.id = m.row_id \
             WHERE m.model = ?1 AND m.scope_key = ?2 ORDER BY m.order_key, m.row_id",
            info.table
        );
        let mut statement = self.connection.prepare_cached(&sql)?;
        let rows = statement.query_map(params![model, scope_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
            ))
        })?;
        let mut result = ScopeRowsResult::default();
        for item in rows {
            let (member, present, record_version, payload) = item?;
            let (Some(_), Some(record_version), Some(payload)) = (present, record_version, payload) else {
                result.dangling_members += 1;
                continue;
            };
            match hydrate(record_version, &payload, &member) {
                (HydrationVerdict::Accepted, Some(row)) => result.rows.push(row),
                (HydrationVerdict::StaleVersion, _) => {
                    result.stale_version_drops += 1;
                    result.bad_ids.push(member);
                }
                _ => {
                    result.rejected_records += 1;
                    result.bad_ids.push(member);
                }
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
        Ok(rows.collect::<Result<_, _>>()?)
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
        let stored = self.read(|reader| reader.stored_row(model, id))?;
        let Some(stored) = stored else { return Ok(None) };
        if stored.verdict != HydrationVerdict::Accepted {
            self.drop_bad_rows(model, &[id.to_string()], &[stored.verdict])?;
        }
        Ok(stored.row)
    }

    pub fn rows_by_ids(&self, model: &str, ids: &[RowId]) -> Result<Vec<Map<String, Value>>, StoreError> {
        let names: Vec<String> = ids.iter().map(|id| id.as_str().to_string()).collect();
        let mut stored = self.read(|reader| reader.stored_rows(model, &names))?;
        let bad: Vec<(String, HydrationVerdict)> = stored
            .iter()
            .filter(|(_, row)| row.verdict != HydrationVerdict::Accepted)
            .map(|(id, row)| (id.clone(), row.verdict))
            .collect();
        let bad_ids: Vec<String> = bad.iter().map(|(id, _)| id.clone()).collect();
        let verdicts: Vec<HydrationVerdict> = bad.iter().map(|(_, verdict)| *verdict).collect();
        self.drop_bad_rows(model, &bad_ids, &verdicts)?;
        Ok(names.iter().filter_map(|name| stored.remove(name)).filter_map(|row| row.row).collect())
    }

    pub fn scope_rows(&self, model: &str, scope_key: &str, limit: Option<usize>) -> Result<ScopeRowsResult, StoreError> {
        let result = self.read(|reader| reader.scope_rows(model, scope_key, limit))?;
        {
            let mut inner = self.lock();
            inner.counters.membership_missing_entity += result.dangling_members;
        }
        if !result.bad_ids.is_empty() {
            let verdicts: Vec<HydrationVerdict> = result.bad_ids.iter().map(|_| HydrationVerdict::Unreadable).collect();
            self.drop_bad_rows_counted(model, &result.bad_ids, &verdicts, result.stale_version_drops, result.rejected_records)?;
        }
        Ok(result)
    }

    pub fn row_count(&self, model: &str) -> Result<i64, StoreError> {
        self.read(|reader| reader.row_count(model))
    }

    fn drop_bad_rows(&self, model: &str, ids: &[String], verdicts: &[HydrationVerdict]) -> Result<(), StoreError> {
        let stale = verdicts.iter().filter(|verdict| verdict.is_silent_drop()).count();
        let rejected = verdicts.len() - stale;
        self.drop_bad_rows_counted(model, ids, verdicts, stale, rejected)
    }

    fn drop_bad_rows_counted(
        &self,
        model: &str,
        ids: &[String],
        _verdicts: &[HydrationVerdict],
        stale: usize,
        rejected: usize,
    ) -> Result<(), StoreError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut inner = self.lock();
        let table = inner.model(model)?.table.clone();
        let transaction = inner.connection.unchecked_transaction()?;
        for id in ids {
            transaction.execute(&format!("DELETE FROM {table} WHERE id = ?1"), params![id])?;
            transaction.execute("DELETE FROM scope_member WHERE model = ?1 AND row_id = ?2", params![model, id])?;
        }
        transaction.commit()?;
        inner.counters.stale_version_drops += stale;
        inner.counters.rejected_records += rejected;
        Ok(())
    }
}
