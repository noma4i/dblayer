use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, OptionalExtension};

use crate::identity::{RowId, ScopeKey};
use crate::schema::PageDirection;

use super::{Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowRecord {
    pub key: String,
    pub cursor: Option<String>,
    pub has_next: bool,
    pub ids: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidationScope {
    Exact { key: String },
    Family { model: String, relation: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationRequest {
    pub scope: InvalidationScope,
}

impl InvalidationRequest {
    pub fn exact(key: &str) -> Self {
        Self { scope: InvalidationScope::Exact { key: key.to_string() } }
    }

    pub fn family(model: &str, relation: &str) -> Self {
        Self { scope: InvalidationScope::Family { model: model.to_string(), relation: relation.to_string() } }
    }

    pub fn deduplicate(requests: &[InvalidationRequest]) -> Vec<InvalidationRequest> {
        let families: Vec<String> = requests
            .iter()
            .filter_map(|request| match &request.scope {
                InvalidationScope::Family { model, relation } => Some(family_prefix(model, relation)),
                InvalidationScope::Exact { .. } => None,
            })
            .collect();
        let mut seen = BTreeSet::new();
        let mut kept = Vec::new();
        for request in requests {
            let covered = match &request.scope {
                InvalidationScope::Exact { key } => families.iter().any(|prefix| key.starts_with(prefix.as_str())),
                InvalidationScope::Family { .. } => false,
            };
            let identity = format!("{:?}", request.scope);
            if !covered && seen.insert(identity) {
                kept.push(request.clone());
            }
        }
        kept
    }
}

pub fn family_prefix(model: &str, relation: &str) -> String {
    ScopeKey::compose(&[model, relation]).as_str().to_string()
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWindow {
    pub key: String,
    pub direction: PageDirection,
    pub cursor: Option<String>,
    pub has_next: bool,
    pub count: usize,
    pub row_ids: Vec<RowId>,
}

impl QueryWindow {
    pub fn new(key: &str, direction: PageDirection) -> Self {
        Self { key: key.to_string(), direction, cursor: None, has_next: false, count: 0, row_ids: Vec::new() }
    }
}

pub struct Pagination;

impl Pagination {
    pub fn can_load_more(window: Option<&QueryWindow>, max_pages: Option<usize>) -> bool {
        let Some(window) = window else { return false };
        if !window.has_next || window.cursor.as_deref().is_none_or(str::is_empty) {
            return false;
        }
        max_pages.is_none_or(|limit| window.count < limit)
    }

    pub fn advance(window: &QueryWindow, page_info: &PageInfo, row_ids: &[RowId]) -> Result<QueryWindow, StoreError> {
        if page_info.has_next_page && page_info.end_cursor.as_deref().is_none_or(str::is_empty) {
            return Err(StoreError::Constraint(format!("malformed page info for {}", window.key)));
        }
        let mut known: BTreeSet<&RowId> = window.row_ids.iter().collect();
        let fresh: Vec<RowId> = row_ids.iter().filter(|id| known.insert(id)).cloned().collect();
        Ok(QueryWindow {
            key: window.key.clone(),
            direction: window.direction,
            cursor: page_info.end_cursor.clone(),
            has_next: page_info.has_next_page,
            count: window.count + 1,
            row_ids: window.row_ids.iter().cloned().chain(fresh).collect(),
        })
    }

    pub fn replace_rows(window: &QueryWindow, row_ids: Vec<RowId>) -> QueryWindow {
        QueryWindow { row_ids, ..window.clone() }
    }

    pub fn visible_rows(row_ids: &[RowId], limit: Option<i64>) -> Vec<RowId> {
        match limit {
            None => row_ids.to_vec(),
            Some(limit) if limit < 0 => Vec::new(),
            Some(limit) => row_ids.iter().take(limit as usize).cloned().collect(),
        }
    }

    pub fn survivors(ids: &[String], removed: &BTreeSet<String>) -> Vec<String> {
        ids.iter().filter(|id| !removed.contains(*id)).cloned().collect()
    }

    pub fn affected_keys(windows: &BTreeMap<String, Vec<String>>, touched: &BTreeSet<String>) -> Vec<String> {
        if touched.is_empty() {
            return Vec::new();
        }
        windows.iter().filter(|(_, ids)| ids.iter().any(|id| touched.contains(id))).map(|(key, _)| key.clone()).collect()
    }
}

impl Store {
    pub fn save_window(&self, record: &WindowRecord) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let ids = serde_json::to_vec(&record.ids)?;
        inner.connection.execute(
            "INSERT INTO query_window (key, cursor, has_next, ids, issued, applied, generation, count, landed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
             ON CONFLICT (key) DO UPDATE SET cursor = excluded.cursor, has_next = excluded.has_next, \
             ids = excluded.ids, issued = excluded.issued, applied = excluded.applied, \
             generation = excluded.generation, count = excluded.count",
            params![
                record.key,
                record.cursor,
                i64::from(record.has_next),
                ids,
                record.issued,
                record.applied,
                record.generation,
                record.ids.len() as i64,
                inner.now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn window(&self, key: &str) -> Result<Option<WindowRecord>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let found = inner
            .connection
            .query_row(
                "SELECT key, cursor, has_next, ids, issued, applied, generation FROM query_window WHERE key = ?1",
                params![key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((key, cursor, has_next, ids, issued, applied, generation)) = found else { return Ok(None) };
        let ids: Vec<serde_json::Value> = serde_json::from_slice(&ids)?;
        let ids = ids.into_iter().filter_map(|value| value.as_str().map(str::to_string)).collect();
        Ok(Some(WindowRecord { key, cursor, has_next: has_next != 0, ids, issued, applied, generation }))
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

    pub fn drop_window(&self, key: &str) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.connection.execute("DELETE FROM query_window WHERE key = ?1", params![key])?;
        Ok(())
    }

    pub fn window_keys(&self, model: &str) -> Result<Vec<String>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let pattern = like_prefix(ScopeKey::compose(&[model]).as_str());
        let mut statement =
            inner.connection.prepare("SELECT key FROM query_window WHERE key LIKE ?1 ESCAPE '\\' ORDER BY key")?;
        let keys = statement.query_map(params![pattern], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }

    pub fn drop_model_windows(&self, model: &str) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let pattern = like_prefix(ScopeKey::compose(&[model]).as_str());
        Ok(inner.connection.execute("DELETE FROM query_window WHERE key LIKE ?1 ESCAPE '\\'", params![pattern])?)
    }

    pub fn invalidate_window(&self, key: &str) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.connection.execute("UPDATE query_window SET invalidated = 1 WHERE key = ?1", params![key])?;
        Ok(())
    }

    pub fn invalidate_family(&self, model: &str, relation: &str) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let pattern = like_prefix(&family_prefix(model, relation));
        Ok(inner
            .connection
            .execute("UPDATE query_window SET invalidated = 1 WHERE key LIKE ?1 ESCAPE '\\'", params![pattern])?)
    }

    pub fn apply_invalidation(&self, requests: &[InvalidationRequest]) -> Result<usize, StoreError> {
        let deduplicated = InvalidationRequest::deduplicate(requests);
        for request in &deduplicated {
            match &request.scope {
                InvalidationScope::Exact { key } => self.invalidate_window(key)?,
                InvalidationScope::Family { model, relation } => {
                    self.invalidate_family(model, relation)?;
                }
            }
        }
        Ok(deduplicated.len())
    }

    pub fn clear_invalidation(&self, key: &str, landed_at: i64) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.connection.execute(
            "UPDATE query_window SET invalidated = 0, landed_at = ?1 WHERE key = ?2",
            params![landed_at, key],
        )?;
        Ok(())
    }

    pub fn stale_window_keys(&self) -> Result<Vec<String>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        let mut statement = inner.connection.prepare("SELECT key FROM query_window WHERE invalidated = 1 ORDER BY key")?;
        let keys = statement.query_map([], |row| row.get(0))?;
        Ok(keys.collect::<Result<_, _>>()?)
    }
}
