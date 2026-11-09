use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::store::{CommitResult, RowChange, RowChangeKind};
use crate::identity::RowRef;

use super::{ModelInfo, Reader, StoreError};

pub(super) type MemoryExpiries = (Vec<crate::owners::store::MemoryExpiry>, Vec<(String, String)>);

pub(super) fn expiry(connection: &Connection, models: &BTreeMap<String, ModelInfo>, results: &[CommitResult], now: i64) -> Result<MemoryExpiries, StoreError> {
    let reader = Reader::new(connection, models);
    let mut written = BTreeSet::new();
    let mut removed = BTreeSet::new();
    for result in results {
        written.extend(result.renewed.iter().cloned());
        for change in &result.rows {
            match change.kind {
                RowChangeKind::Inserted | RowChangeKind::Updated => { written.insert(change.row.clone()); }
                RowChangeKind::Destroyed => { removed.insert(change.row.clone()); }
                _ => {}
            }
        }
    }
    let mut expiring = Vec::new();
    for row in written {
        let Some(info) = models.get(row.model.as_str()).filter(|info| info.spec.is_memory()) else { continue };
        let Some(duration) = info.spec.expire_window() else { continue };
        if reader.point(row.model.as_str(), row.id.as_str())?.is_none() { continue; }
        removed.remove(&row);
        expiring.push(crate::owners::store::MemoryExpiry { model: row.model.to_string(), row_id: row.id.to_string(), written_at: u64::try_from(now).unwrap_or(0), expires_after: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX) });
    }
    let removed = removed.into_iter().filter(|row| models.get(row.model.as_str()).is_some_and(|info| info.spec.is_memory())).map(|row| (row.model.to_string(), row.id.to_string())).collect();
    Ok((expiring, removed))
}

pub(super) fn consolidate(connection: &Connection, models: &BTreeMap<String, ModelInfo>, originals: BTreeMap<RowRef, Option<Map<String, Value>>>, results: Vec<CommitResult>, revision: i64) -> Result<CommitResult, StoreError> {
    let mut combined = CommitResult::noop(uuid::Uuid::new_v4());
    combined.revision = Some(revision);
    let mut rows: BTreeMap<RowRef, Vec<RowChange>> = BTreeMap::new();
    for result in results {
        for change in result.rows { rows.entry(change.row.clone()).or_default().push(change); }
        combined.renewed.extend(result.renewed);
        combined.scopes.extend(result.scopes);
        combined.members.extend(result.members);
        combined.server_removed.extend(result.server_removed);
        combined.operations.extend(result.operations);
        combined.queue_operations.extend(result.queue_operations);
        combined.replaced.extend(result.replaced);
        combined.unknown_operation_acks += result.unknown_operation_acks;
        combined.snapshot = result.snapshot.or(combined.snapshot);
        combined.item_snapshots.extend(result.item_snapshots);
    }
    let reader = Reader::new(connection, models);
    for (row, changes) in rows {
        let Some(before) = originals.get(&row) else {
            combined.rows.extend(changes);
            continue;
        };
        let after = reader.point(row.model.as_str(), row.id.as_str())?;
        let kind = match (before, &after) {
            (None, None) => continue,
            (None, Some(_)) => RowChangeKind::Inserted,
            (Some(_), None) => RowChangeKind::Destroyed,
            (Some(before), Some(after)) if before != after => RowChangeKind::Updated,
            (Some(_), Some(_)) => match changes.iter().rev().find(|change| matches!(change.kind, RowChangeKind::Touched | RowChangeKind::Synced)) {
                Some(change) => change.kind,
                None => continue,
            },
        };
        let fields: BTreeSet<String> = match (before, &after) {
            (Some(before), Some(after)) => before.keys().chain(after.keys()).filter(|key| before.get(*key) != after.get(*key)).cloned().collect(),
            (None, Some(after)) => after.keys().cloned().collect(),
            _ => BTreeSet::new(),
        };
        combined.rows.push(RowChange { row, kind, fields });
    }
    Ok(combined)
}

/// The parent models whose `sets:` fields a batch could not write: a child landed and its parent row is not in the base (P-DATA-62).
pub(super) fn stale_parents(connection: &Connection, models: &BTreeMap<String, ModelInfo>, results: &[CommitResult]) -> Result<Vec<String>, StoreError> {
    let reader = Reader::new(connection, models);
    let mut stale = BTreeSet::new();
    let landed: BTreeSet<RowRef> = results.iter().flat_map(|result| result.rows.iter()).filter(|change| matches!(change.kind, RowChangeKind::Inserted | RowChangeKind::Updated)).map(|change| change.row.clone()).collect();
    for row in landed {
        let Some(info) = models.get(row.model.as_str()) else { continue };
        let Some(stored) = reader.point(row.model.as_str(), row.id.as_str())? else { continue };
        for association in info.spec.associations.iter().filter(|association| association.kind == crate::schema::AssociationKind::BelongsTo && !association.sets.is_empty()) {
            let Some(parent) = stored.get(&association.foreign_key).and_then(crate::value::read_id) else { continue };
            if reader.point(association.target.as_str(), parent.as_str())?.is_none() { stale.insert(association.target.to_string()); }
        }
    }
    Ok(stale.into_iter().collect())
}
