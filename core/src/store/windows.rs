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
    pub stale_since: Option<i64>,
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
    pub(crate) fn marker(&self, key: &str, generation: i64) -> Result<Option<crate::commit::WindowMarker>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let mut statement = inner.connection.prepare_cached("SELECT m.sequence, m.field, m.value FROM window_marker m JOIN query_window w ON w.key = m.window_key AND w.generation = m.generation WHERE m.window_key = ?1 AND m.generation = ?2 AND w.has_next = 0")?;
        let mut rows = statement.query(params![key, generation])?;
        let mut marker = None;
        while let Some(row) = rows.next()? {
            let found = marker.get_or_insert_with(|| crate::commit::WindowMarker { sequence: 0, values: Default::default() });
            found.sequence = row.get(0)?;
            found.values.insert(row.get(1)?, row.get(2)?);
        }
        Ok(marker)
    }

    #[cfg(test)]
    pub fn save_window(&self, record: &WindowRecord) -> Result<(), StoreError> {
        self.write_store(&crate::ffi::legacy::Authority::Internal(crate::ffi::legacy::Internal::for_tests()), |inner| upsert_window(&inner.connection, record, inner.now_ms()))
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
            .query_row("SELECT landed_at, stale_since FROM query_window WHERE key = ?1", params![key], |row| {
                Ok(WindowFreshness { landed_at: row.get(0)?, stale_since: row.get(1)? })
            })
            .optional()?)
    }

    pub(crate) fn invalidate_family(&self, authority: &crate::ffi::legacy::Authority, model: &str, relation: &str, since: i64) -> Result<usize, StoreError> {
        self.write_store(authority, |inner| {
            if let crate::ffi::legacy::Authority::Admitted(admitted) = authority {
                if !admitted.is_current() { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration)); }
            }
            mark_families(&inner.connection, &[(model.to_string(), relation.to_string())], since)
        })
    }

    pub(crate) fn invalidate_windows(&self, authority: &crate::ffi::legacy::Authority, keys: &[String], since: i64) -> Result<usize, StoreError> {
        self.write_store(authority, |inner| {
            let mut marked = 0;
            for key in keys {
                marked += inner.connection.execute("UPDATE query_window SET stale_since = MAX(COALESCE(stale_since, 0), ?2) WHERE key = ?1 AND applied < ?2", params![key, since])?;
            }
            Ok(marked)
        })
    }

    #[cfg(test)]
    pub(crate) fn invalidate_family_now(&self, model: &str, relation: &str) -> Result<usize, StoreError> {
        let admitted = self.lifecycle.admit(crate::ffi::legacy::Origin::Explicit, "test invalidate").map_err(|_| StoreError::Refused)?;
        let since = self.max_window_sequence()? + 1;
        self.invalidate_family(&admitted.authority(), model, relation, since)
    }

    #[cfg(test)]
    pub fn stale_window_keys(&self) -> Result<Vec<String>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let mut statement = inner.connection.prepare("SELECT key FROM query_window WHERE stale_since IS NOT NULL ORDER BY key")?;
        let keys = statement.query_map([], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }

    pub(crate) fn max_window_sequence(&self) -> Result<i64, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        Ok(inner.connection.query_row("SELECT COALESCE(MAX(MAX(issued, applied, COALESCE(stale_since, 0))), 0) FROM query_window", [], |row| row.get(0))?)
    }
}
