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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowFreshness {
    pub landed_at: i64,
    pub invalidated: bool,
}

impl WindowFreshness {
    pub fn is_fresh(&self, now: i64, window: i64) -> bool {
        !self.invalidated && now - self.landed_at < window
    }
}

pub fn family_prefix(model: &str, relation: &str) -> String {
    ListKey::compose(&[model, relation]).as_str().to_string()
}

fn like_prefix(prefix: &str) -> String {
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

impl Store {
    #[cfg(test)]
    pub fn save_window(&self, record: &WindowRecord) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        upsert_window(&inner.connection, record, inner.now_ms())
    }

    pub fn window(&self, key: &str) -> Result<Option<WindowRecord>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let found = inner
            .connection
            .query_row(
                "SELECT key, cursor, has_next, issued, applied, generation, total, pages FROM query_window WHERE key = ?1",
                params![key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((key, cursor, has_next, issued, applied, generation, total, pages)) = found else { return Ok(None) };
        Ok(Some(WindowRecord { key, cursor, has_next: has_next != 0, total, pages, issued, applied, generation }))
    }

    pub fn window_freshness(&self, key: &str) -> Result<Option<WindowFreshness>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        Ok(inner
            .connection
            .query_row("SELECT landed_at, invalidated FROM query_window WHERE key = ?1", params![key], |row| {
                Ok(WindowFreshness { landed_at: row.get(0)?, invalidated: row.get::<_, i64>(1)? != 0 })
            })
            .optional()?)
    }

    pub fn invalidate_family(&self, model: &str, relation: &str) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let pattern = like_prefix(&family_prefix(model, relation));
        Ok(inner
            .connection
            .execute("UPDATE query_window SET invalidated = 1 WHERE key LIKE ?1 ESCAPE '\\'", params![pattern])?)
    }

    #[cfg(test)]
    pub fn stale_window_keys(&self) -> Result<Vec<String>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let mut statement = inner.connection.prepare("SELECT key FROM query_window WHERE invalidated = 1 ORDER BY key")?;
        let keys = statement.query_map([], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }
}
