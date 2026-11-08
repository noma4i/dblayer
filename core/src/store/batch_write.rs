use std::collections::BTreeMap;

use crate::store::files::{FileDirectories, Location};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreBatch};
use crate::schema::AssociationSpec;

use super::row_write::RowWrite;
use super::{Reader, Store, StoreError, TombstoneRetention};

const OVERLAY_ROW_LIMIT: usize = 1024;

/// What the transaction of a batch leaves for the steps after it.
struct WrittenBatch<'a> {
    committed: super::Committed<'a>,
    observed: Vec<String>,
    live_files: std::collections::HashSet<String>,
    discarded: Vec<String>,
    expiry: ExpiryParts,
    resets: ResetParts,
    candidates: usize,
    shown: Option<super::Row>,
}

type ExpiryParts = (Vec<crate::owners::store::MemoryExpiry>, Vec<(String, String)>, Vec<String>);
type ResetParts = (Vec<u64>, Vec<crate::owners::store::EphemeralReset>);

pub(crate) struct BatchWritten {
    pub revision: u64,
    pub observed: Vec<String>,
    pub faults: Vec<String>,
    pub expiring: Vec<crate::owners::store::MemoryExpiry>,
    pub removed_memory: Vec<(String, String)>,
    pub stale_parents: Vec<String>,
    pub removed_operations: Vec<u64>,
    pub resets: Vec<crate::owners::store::EphemeralReset>,
    pub notice: crate::owners::contract::CommittedBroadcast,
    pub shown: Option<super::Row>,
}

impl Store {
    pub(crate) fn write_batch(&self, batch: &StoreBatch, files: &FileDirectories, guards: &BTreeMap<String, Vec<AssociationSpec>>, min_pin_revision: Option<u64>) -> Result<BatchWritten, StoreError> {
        let now = self.lock().now_ms();
        let prepared = super::file_write::prepare_files(files, &batch.ops, now)?;
        let written = self.write_batch_prepared(batch, guards, min_pin_revision, now, &prepared);
        match written {
            Err(error) => {
                for file in prepared.iter().filter(|file| file.record.location == Location::Directory) {
                    files.discard_partial(&file.record.id)?;
                }
                Err(error)
            }
            Ok(WrittenBatch { committed: result, observed, live_files, discarded, expiry: (expiring, removed_memory, stale_parents), resets: (removed_operations, resets), candidates, shown }) => {
                self.lock().counters.outbox_candidates += candidates;
                let mut faults = Vec::new();
                for file in prepared.iter().filter(|file| file.record.location == Location::Directory) {
                    let removed = if live_files.contains(&file.record.id) { files.finish(&file.record.id) } else { files.discard_partial(&file.record.id) };
                    if let Err(error) = removed { faults.push(format!("file {}: {error}", file.record.id)); }
                }
                for id in discarded {
                    if let Err(error) = files.discard(std::slice::from_ref(&id)) { faults.push(format!("remove file {id}: {error}")); }
                }
                let revision = result.result().revision.ok_or_else(|| StoreError::Invalid("a committed batch has no revision".into()))?;
                let notice = crate::owners::contract::CommittedBroadcast::of_commit(result.result());
                result.publish();
                Ok(BatchWritten { revision: u64::try_from(revision).map_err(|_| StoreError::Constraint("negative revision".into()))?, observed, faults, expiring, removed_memory, stale_parents, removed_operations, resets, notice, shown })
            }
        }
    }

    fn write_batch_prepared<'a>(&'a self, batch: &StoreBatch, guards: &BTreeMap<String, Vec<AssociationSpec>>, min_pin_revision: Option<u64>, now: i64, files: &[super::file_write::PreparedFile]) -> Result<WrittenBatch<'a>, StoreError> {
        let inner = self.lock();
        if !inner.ready { return Err(StoreError::Refused); }
        let transaction = inner.connection.unchecked_transaction()?;
        transaction.execute_batch(include_str!("sql/batch_write/write_batch_prepared_create_table_batch_file_deletions.sql"))?;
        let base_revision = Reader::new(&transaction, &inner.models).revision_seq()?;
        let revision = base_revision.checked_add(1).ok_or_else(|| StoreError::Constraint("revision overflow".into()))?;
        let own = batch.ops.iter().find_map(|op| match op { crate::owners::store::StoreOp::RemoveIntent { op_id } => i64::try_from(*op_id).ok(), _ => None }).unwrap_or(-1);
        let mut originals = BTreeMap::new();
        let mut results = Vec::new();
        let mut writer = RowWrite { connection: &transaction, models: &inner.models, guards, generation: inner.generation, revision, now, originals: &mut originals, closing: (own >= 0).then_some(own), candidates: 0 };
        let mut overlay = Vec::new();
        let observed = if batch.author == BatchAuthor::Feed {
            super::feed_write::land(&mut writer, &batch.rows, &mut overlay, &mut results)?
        } else {
            for (model, text) in &batch.rows {
                let value: serde_json::Value = serde_json::from_str(text)?;
                let row_id = value.get("id").and_then(serde_json::Value::as_str).ok_or_else(|| StoreError::Constraint("row has no id".into()))?;
                let row = RowValue { row_id: row_id.into(), values: text.clone() };
                if let Some(result) = writer.upsert(model, &row, false, batch.author == BatchAuthor::Outbox)? { results.push(result); }
            }
            Vec::new()
        };
        let mut writes_database = !results.is_empty() || (batch.author != BatchAuthor::Feed && !batch.rows.is_empty());
        let mut resets = Vec::new();
        let mut rolled_back = std::collections::BTreeSet::new();
        for (index, op) in batch.ops.iter().enumerate() {
            if let crate::owners::store::StoreOp::EphemeralRows { model, rows } = op {
                for row in rows {
                    let values = serde_json::from_str::<serde_json::Value>(&row.values)?.as_object().cloned().ok_or_else(|| StoreError::Constraint("ephemeral values must be an object".into()))?;
                    overlay.push((model.clone(), row.row_id.clone(), values));
                }
                continue;
            }
            if let crate::owners::store::StoreOp::ResetEphemeral { model, row_id, field, position } = op {
                resets.push((model.clone(), row_id.clone(), field.clone(), *position));
                continue;
            }
            writes_database = true;
            if let crate::owners::store::StoreOp::Swap { model, temp, real } = op {
                results.push(super::swap::apply(&mut writer, model, temp, real, own)?);
                continue;
            }
            if let crate::owners::store::StoreOp::RemoveIntent { op_id } = op {
                if rolled_back.contains(op_id) { continue; }
            }
            if !super::batch_common::apply_common(&mut writer, op, index, files, &mut results, batch.author == BatchAuthor::Outbox)? {
                return Err(StoreError::Constraint(format!("unsupported store operation: {op:?}")));
            }
            if let crate::owners::store::StoreOp::RollBack { op_id } = op { rolled_back.insert(*op_id); }
        }
        let candidates = writer.candidates;
        let mut shown = None;
        for op in &batch.ops {
            let (crate::owners::store::StoreOp::Intent { effects: crate::owners::store::IntentEffects { shown: Some((model, row_id)), .. }, .. } | crate::owners::store::StoreOp::Planned { effects: crate::owners::store::IntentEffects { shown: Some((model, row_id)), .. } } | crate::owners::store::StoreOp::Show { model, row_id }) = op else { continue };
            shown = self.load(&Reader::new(&transaction, &inner.models), model, std::slice::from_ref(row_id)).map_err(|error| StoreError::Constraint(error.to_string()))?.remove(row_id.as_str());
        }
        let mut live_files = std::collections::HashSet::new();
        for file in files {
            let present: bool = transaction.query_row(include_str!("sql/batch_write/write_batch_prepared_select_file.sql"), [&file.record.id], |row| row.get(0))?;
            if present { live_files.insert(file.record.id.clone()); }
        }
        let discarded = transaction.prepare(include_str!("sql/batch_write/write_batch_prepared_select_batch_file_deletions.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?;
        transaction.execute_batch(include_str!("sql/batch_write/write_batch_prepared_drop_trigger_batch_file_delete.sql"))?;
        let (expiring, removed_memory) = super::batch_result::expiry(&transaction, &inner.models, &results, inner.now_ms())?;
        let stale_parents = super::batch_result::stale_parents(&transaction, &inner.models, &results)?;
        let removed_operations: Vec<u64> = if batch.author == BatchAuthor::Feed { results.iter().flat_map(|result| result.operations.iter()).filter_map(|seq| u64::try_from(*seq).ok()).collect() } else { Vec::new() };
        let target_revision = if writes_database { revision } else { base_revision };
        let mut combined = super::batch_result::consolidate(&transaction, &inner.models, originals, results, target_revision)?;
        let current = self.overlays.get(base_revision).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))?;
        let mut snapshot = if writes_database || !overlay.is_empty() || !resets.is_empty() { self.overlays.next(base_revision, writes_database).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))? } else { current.as_ref().clone() };
        let (changes, arms) = super::overlay_write::apply(&mut snapshot, &inner.models, overlay, u64::try_from(inner.now_ms()).unwrap_or(0))?;
        combined.rows.extend(changes);
        for change in combined.rows.iter().filter(|change| change.kind == crate::store::RowChangeKind::Destroyed) {
            snapshot.remove_row(&super::overlay::RowKey { model: change.row.model.to_string(), row_id: change.row.id.to_string() });
        }
        combined.rows.extend(super::overlay_write::reset(&mut snapshot, &inner.models, &resets)?);
        snapshot.prune_missing(OVERLAY_ROW_LIMIT, |row| Reader::new(&transaction, &inner.models).stored_row(&row.model, &row.row_id).ok().flatten().is_some());
        let position = snapshot.position;
        combined.position = position;
        if writes_database {
            super::tombstones::prune_tombstones(&transaction, now, &TombstoneRetention::DEFAULT, min_pin_revision)?;
            crate::store::ddl::manifest_write(&transaction, crate::store::ddl::REVISION_SEQ_KEY, &revision.to_string())?;
            self.overlays.commit(snapshot, transaction)?;
        } else {
            transaction.rollback()?;
            if position != current.position { self.overlays.publish(snapshot); }
        }
        Ok(WrittenBatch { committed: self.commit_announcement(inner, combined), observed, live_files, discarded, expiry: (expiring, removed_memory, stale_parents), resets: (removed_operations, arms), candidates, shown })
    }
}
