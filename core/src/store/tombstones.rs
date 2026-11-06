#[cfg(test)]
use std::collections::BTreeSet;

use rusqlite::{params, Connection};

use super::{Store, StoreError};

#[derive(Clone, Copy)]
pub(crate) enum TombstonePrune {
    Skip,
    WithPins(Option<u64>),
}

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
    min_pin_revision: Option<u64>,
) -> Result<usize, StoreError> {
    let mut removed =
        connection.execute("DELETE FROM tombstone WHERE deleted_at < ?1 AND (?2 IS NULL OR seq <= ?2)", params![now - retention.ttl_ms, min_pin_revision.map(|value| i64::try_from(value).unwrap_or(i64::MAX))])?;
    let total: i64 = connection.query_row("SELECT COUNT(*) FROM tombstone", [], |row| row.get(0))?;
    let excess = retention.eviction_count(total);
    if excess > 0 {
        let age_clause = if retention.ignores_minimum_age(total) { 1 } else { 0 };
        removed += connection.execute(
            "DELETE FROM tombstone WHERE (model, id) IN \
             (SELECT model, id FROM tombstone WHERE (?1 = 1 OR deleted_at < ?2) AND (?4 IS NULL OR seq <= ?4) ORDER BY deleted_at LIMIT ?3)",
            params![age_clause, now - retention.min_age_ms, excess, min_pin_revision.map(|value| i64::try_from(value).unwrap_or(i64::MAX))],
        )?;
    }
    Ok(removed)
}

impl Store {
    #[cfg(test)]
    pub fn mark_tombstone(&self, model: &str, id: &str, seq: i64, deleted_at: i64) -> Result<(), StoreError> {
        self.write_store(&crate::lifecycle::authority::Authority::Internal(crate::lifecycle::authority::Internal::for_tests()), |inner| write_tombstone(&inner.connection, model, id, seq, deleted_at))
    }

    #[cfg(test)]
    pub fn is_tombstoned(&self, model: &str, id: &str) -> Result<bool, StoreError> {
        self.read(&[model], |reader| reader.is_tombstoned(model, id))
    }

    #[cfg(test)]
    pub fn tombstoned_ids(&self, model: &str, ids: &[String]) -> Result<BTreeSet<String>, StoreError> {
        self.read(&[model], |reader| reader.tombstoned_ids(model, ids))
    }

    #[cfg(test)]
    pub fn tombstone_count(&self) -> Result<i64, StoreError> {
        let inner = self.lock();
        Ok(inner.connection.query_row("SELECT COUNT(*) FROM tombstone", [], |row| row.get(0))?)
    }

    #[cfg(test)]
    pub fn evict_tombstones(&self, retention: &TombstoneRetention) -> Result<usize, StoreError> {
        self.write_store(&crate::lifecycle::authority::Authority::Internal(crate::lifecycle::authority::Internal::for_tests()), |inner| prune_tombstones(&inner.connection, inner.now_ms(), retention, None))
    }
}
