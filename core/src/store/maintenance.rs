use std::collections::BTreeSet;

use rusqlite::params_from_iter;
use time::macros::format_description;
use time::OffsetDateTime;

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};

use super::tombstones::{prune_tombstones, TombstoneRetention};
use super::{Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempRowPolicy {
    pub model: ModelKey,
    pub drop_after_ms: i64,
    pub protected: BTreeSet<RowId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceReport {
    pub tombstone_drops: usize,
    pub temp_rows_dropped: usize,
    pub commit: Option<CommitResult>,
}

fn iso_millis(milliseconds: i64) -> Option<String> {
    let nanoseconds = i128::from(milliseconds).checked_mul(1_000_000)?;
    OffsetDateTime::from_unix_timestamp_nanos(nanoseconds)
        .ok()?
        .format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"))
        .ok()
}

impl Store {
    pub fn maintain(&self, temp_rows: &[TempRowPolicy]) -> Result<MaintenanceReport, StoreError> {
        let (tombstone_drops, ids, generation) = {
            let inner = self.lock();
            inner.ensure_healthy()?;
            let now = inner.now_ms();
            let tombstone_drops = prune_tombstones(&inner.connection, now, &TombstoneRetention::DEFAULT)?;
            let mut ids: Vec<(ModelKey, RowId)> = Vec::new();
            for policy in temp_rows {
                let info = inner.model(policy.model.as_str())?;
                if !info.promoted.iter().any(|column| column.column == "created_at") {
                    return Err(StoreError::Constraint(format!("model {} has no created_at column", policy.model)));
                }
                let cutoff = iso_millis(now - policy.drop_after_ms).unwrap_or_default();
                let sql = format!(
                    "SELECT id FROM {} WHERE id LIKE 'temp-%' AND (created_at IS NULL OR created_at < ?)",
                    info.table
                );
                let mut statement = inner.connection.prepare(&sql)?;
                let found = statement.query_map(params_from_iter([cutoff]), |row| row.get::<_, String>(0))?;
                for id in found {
                    let id = RowId::new(&id?).map_err(|error| StoreError::Constraint(error.to_string()))?;
                    if !policy.protected.contains(&id) {
                        ids.push((policy.model.clone(), id));
                    }
                }
            }
            (tombstone_drops, ids, inner.generation)
        };
        if ids.is_empty() {
            return Ok(MaintenanceReport { tombstone_drops, temp_rows_dropped: 0, commit: None });
        }
        let mut envelope = CommitEnvelope::new(WriteOrigin::Snapshot, generation);
        envelope.entity_ops = ids
            .iter()
            .map(|(model, id)| EntityOp::Destroy { model: model.clone(), id: id.clone(), reason: DestroyReason::Expired })
            .collect();
        let result = self.commit(envelope)?;
        Ok(MaintenanceReport { tombstone_drops, temp_rows_dropped: ids.len(), commit: Some(result) })
    }
}
