use std::collections::BTreeSet;

use rusqlite::{params, Connection};

use crate::identity::RowRef;

use super::{Store, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneRetention {
    pub ttl_ms: i64,
    pub min_age_ms: i64,
    pub cap: i64,
    pub overflow_cap: i64,
}

impl TombstoneRetention {
    pub const DEFAULT: TombstoneRetention =
        TombstoneRetention { ttl_ms: 86_400_000, min_age_ms: 600_000, cap: 10_000, overflow_cap: 20_000 };

    pub fn is_expired(&self, deleted_at: i64, now: i64) -> bool {
        now - deleted_at > self.ttl_ms
    }

    pub fn eviction_count(&self, total: i64) -> i64 {
        if total > self.cap {
            total - self.cap
        } else {
            0
        }
    }

    pub fn ignores_minimum_age(&self, total: i64) -> bool {
        total > self.overflow_cap
    }

    pub fn evictable(&self, entries: &[(RowRef, i64)], now: i64, total: i64) -> Vec<RowRef> {
        let mut sorted: Vec<&(RowRef, i64)> = entries.iter().collect();
        sorted.sort_by_key(|entry| entry.1);
        let mut seen = BTreeSet::new();
        let mut evicted: Vec<RowRef> = Vec::new();
        let key = |row: &RowRef| (row.model.clone(), row.id.clone());
        for (row, deleted_at) in sorted.iter().map(|entry| (&entry.0, entry.1)) {
            if self.is_expired(deleted_at, now) && seen.insert(key(row)) {
                evicted.push(row.clone());
            }
        }
        let limit = self.eviction_count(total) as usize;
        if evicted.len() >= limit {
            return evicted;
        }
        let ignores_age = self.ignores_minimum_age(total);
        for (row, deleted_at) in sorted.iter().map(|entry| (&entry.0, entry.1)) {
            if evicted.len() >= limit {
                break;
            }
            if seen.contains(&key(row)) || !(ignores_age || now - deleted_at > self.min_age_ms) {
                continue;
            }
            seen.insert(key(row));
            evicted.push(row.clone());
        }
        evicted
    }

    pub fn temp_row_is_stale(
        created_at: i64,
        now: i64,
        drop_after: Option<i64>,
        open_row_ids: &BTreeSet<crate::identity::RowId>,
        row: &crate::identity::RowId,
    ) -> bool {
        match drop_after {
            Some(drop_after) if !open_row_ids.contains(row) => now - created_at > drop_after,
            _ => false,
        }
    }
}

pub(crate) fn write_tombstone(
    connection: &Connection,
    model: &str,
    id: &str,
    seq: i64,
    deleted_at: i64,
) -> Result<(), StoreError> {
    connection.execute(
        "INSERT OR REPLACE INTO tombstone (model, id, seq, deleted_at) VALUES (?1, ?2, ?3, ?4)",
        params![model, id, seq, deleted_at],
    )?;
    Ok(())
}

pub(crate) fn prune_tombstones(
    connection: &Connection,
    now: i64,
    retention: &TombstoneRetention,
) -> Result<usize, StoreError> {
    let mut removed =
        connection.execute("DELETE FROM tombstone WHERE deleted_at < ?1", params![now - retention.ttl_ms])?;
    let total: i64 = connection.query_row("SELECT COUNT(*) FROM tombstone", [], |row| row.get(0))?;
    let excess = retention.eviction_count(total);
    if excess > 0 {
        let age_clause = if retention.ignores_minimum_age(total) { 1 } else { 0 };
        removed += connection.execute(
            "DELETE FROM tombstone WHERE (model, id) IN \
             (SELECT model, id FROM tombstone WHERE ?1 = 1 OR deleted_at < ?2 ORDER BY deleted_at LIMIT ?3)",
            params![age_clause, now - retention.min_age_ms, excess],
        )?;
    }
    Ok(removed)
}

impl Store {
    pub fn mark_tombstone(&self, model: &str, id: &str, seq: i64, deleted_at: i64) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        write_tombstone(&inner.connection, model, id, seq, deleted_at)
    }

    pub fn is_tombstoned(&self, model: &str, id: &str) -> Result<bool, StoreError> {
        self.read(|reader| reader.is_tombstoned(model, id))
    }

    pub fn tombstoned_ids(&self, model: &str, ids: &[String]) -> Result<BTreeSet<String>, StoreError> {
        self.read(|reader| reader.tombstoned_ids(model, ids))
    }

    pub fn tombstone_count(&self) -> Result<i64, StoreError> {
        let inner = self.lock();
        Ok(inner.connection.query_row("SELECT COUNT(*) FROM tombstone", [], |row| row.get(0))?)
    }

    pub fn evict_tombstones(&self, retention: &TombstoneRetention) -> Result<usize, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        prune_tombstones(&inner.connection, inner.now_ms(), retention)
    }
}
