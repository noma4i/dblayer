use rusqlite::{params, Connection, OptionalExtension};

use crate::identity::ListKey;
use crate::schema::PageDirection;

use super::{Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowRecord {
    pub key: String,
    pub cursor: Option<String>,
    pub has_next: bool,
    pub total: Option<i64>,
    pub pages: i64,
    pub issued: i64,
    pub applied: i64,
    pub generation: i64,
}

/// What the Store keeps of the window of a key between runs: when it landed, until when it is fresh, and where its pages stand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedWindow {
    pub landed_at: i64,
    pub fresh_until: i64,
    pub stale: bool,
    pub cursor: Option<String>,
    pub pages: u32,
    pub has_rows: bool,
    pub generation: u64,
    /// The marker of a media window and the number of its members: what `MarkerCheck` compares the answer with.
    pub marker: Option<crate::store::WindowMarker>,
    pub members: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowFreshness {
    pub landed_at: i64,
    pub stale_since: Option<i64>,
}

pub(super) fn read_window(connection: &Connection, key: &str) -> Result<Option<WindowRecord>, StoreError> {
    let found = connection.query_row(
        "SELECT key, cursor, has_next, issued, applied, generation, total, pages FROM query_window WHERE key = ?1",
        params![key],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?, row.get::<_, i64>(5)?, row.get::<_, Option<i64>>(6)?, row.get::<_, i64>(7)?)),
    ).optional()?;
    let Some((key, cursor, has_next, issued, applied, generation, total, pages)) = found else { return Ok(None) };
    Ok(Some(WindowRecord { key, cursor, has_next: has_next != 0, total, pages, issued, applied, generation }))
}

pub(super) fn read_marker(connection: &Connection, key: &str) -> Result<Option<crate::store::WindowMarker>, StoreError> {
    let mut statement = connection.prepare_cached("SELECT sequence, field, value FROM window_marker WHERE window_key = ?1")?;
    let rows = statement.query_map(params![key], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?)))?;
    let mut marker: Option<crate::store::WindowMarker> = None;
    for row in rows {
        let (sequence, field, value) = row?;
        marker.get_or_insert_with(|| crate::store::WindowMarker { sequence, values: Default::default() }).values.insert(field, value);
    }
    Ok(marker)
}

pub(super) fn max_sequence(connection: &Connection) -> Result<i64, StoreError> {
    Ok(connection.query_row("SELECT COALESCE(MAX(MAX(issued, applied, COALESCE(stale_since, 0))), 0) FROM query_window", [], |row| row.get(0))?)
}

pub(crate) fn window_identity(spec: &crate::schema::ModelSpec, model: &str, relation: &str, remote: &crate::schema::RemoteSpec, params: &serde_json::Map<String, serde_json::Value>) -> Result<(crate::store::ScopeRef, String), crate::store::DbError> {
    let scope_key = spec.selection_key(relation, params).map_err(|error| crate::store::DbError::Internal(error.to_string()))?;
    let direction = match remote.shape {
        crate::schema::RemoteShape::Connection { direction, .. } => direction,
        _ => PageDirection::Forward,
    };
    let key = window_key(scope_key.as_str(), direction);
    Ok((crate::store::ScopeRef::new(crate::identity::ModelKey::new(model), scope_key), key))
}

impl WindowFreshness {
    pub fn is_fresh(&self, now: i64, window: i64) -> bool {
        self.stale_since.is_none() && now - self.landed_at < window
    }
}

pub fn family_prefix(model: &str, relation: &str) -> String {
    ListKey::compose(&[model, relation]).as_str().to_string()
}

pub(super) fn like_prefix(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for character in prefix.chars() {
        if matches!(character, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('%');
    pattern
}

pub fn window_key(scope_key: &str, direction: PageDirection) -> String {
    let suffix = match direction {
        PageDirection::Forward => "forward",
        PageDirection::Backward => "backward",
    };
    format!("{scope_key}:{suffix}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
    pub total: Option<i64>,
}

pub(super) fn upsert_window(connection: &Connection, record: &WindowRecord, landed_at: i64) -> Result<(), StoreError> {
    connection.execute(
        "INSERT INTO query_window (key, cursor, has_next, total, pages, issued, applied, generation, landed_at) \
         VALUES (?1, ?2, ?3, ?8, ?9, ?4, ?5, ?6, ?7) \
         ON CONFLICT (key) DO UPDATE SET cursor = excluded.cursor, has_next = excluded.has_next, total = excluded.total, pages = excluded.pages, \
         issued = excluded.issued, applied = excluded.applied, \
         generation = excluded.generation",
        params![
            record.key,
            record.cursor,
            i64::from(record.has_next),
            record.issued,
            record.applied,
            record.generation,
            landed_at,
            record.total,
            record.pages,
        ],
    )?;
    Ok(())
}

pub(super) fn mark_families(connection: &Connection, families: &[(String, String)], since: i64) -> Result<usize, StoreError> {
    let mut update = connection.prepare_cached("UPDATE query_window SET stale_since = MAX(COALESCE(stale_since, 0), ?2) WHERE key LIKE ?1 ESCAPE '\\' AND applied < ?2")?;
    let mut marked = 0;
    for (model, relation) in families {
        marked += update.execute(params![like_prefix(&family_prefix(model, relation)), since])?;
    }
    Ok(marked)
}

impl Store {

    #[cfg(test)]
    pub fn save_window(&self, record: &WindowRecord) -> Result<(), StoreError> {
        self.write(|inner| upsert_window(&inner.connection, record, inner.now_ms()))
    }

    pub fn window(&self, key: &str) -> Result<Option<WindowRecord>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        read_window(&inner.connection, key)
    }

    pub fn window_freshness(&self, key: &str) -> Result<Option<WindowFreshness>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        Ok(inner
            .connection
            .query_row("SELECT landed_at, stale_since FROM query_window WHERE key = ?1", params![key], |row| {
                Ok(WindowFreshness { landed_at: row.get(0)?, stale_since: row.get(1)? })
            })
            .optional()?)
    }



    #[cfg(test)]
    pub(crate) fn invalidate_family(&self, model: &str, relation: &str, since: i64) -> Result<usize, StoreError> {
        self.write(|inner| {
            mark_families(&inner.connection, &[(model.to_string(), relation.to_string())], since)
        })
    }

    #[cfg(test)]
    fn max_window_sequence(&self) -> Result<i64, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        max_sequence(&inner.connection)
    }

    #[cfg(test)]
    pub(crate) fn invalidate_family_now(&self, model: &str, relation: &str) -> Result<usize, StoreError> {
        let since = self.max_window_sequence()? + 1;
        self.invalidate_family(model, relation, since)
    }

    #[cfg(test)]
    pub fn stale_window_keys(&self) -> Result<Vec<String>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let mut statement = inner.connection.prepare("SELECT key FROM query_window WHERE stale_since IS NOT NULL ORDER BY key")?;
        let keys = statement.query_map([], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }
}
