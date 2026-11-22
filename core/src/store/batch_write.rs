use std::collections::BTreeMap;

use crate::store::files::{FileDirectories, Location};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreBatch};

use super::row_write::RowWrite;
use super::{Reader, Store, StoreError, TombstoneRetention};

const OVERLAY_ROW_LIMIT: usize = 1024;

/// What the transaction of a batch leaves for the steps after it.
struct WrittenBatch<'a> {
    committed: super::Committed<'a>,
    base_revision: i64,
    observed: Vec<String>,
    refused: Vec<usize>,
    live_files: std::collections::HashSet<String>,
    discarded: Vec<String>,
    expiry: ExpiryParts,
    resets: ResetParts,
    candidates: usize,
    pointer_rows: usize,
    shown: Vec<Option<super::Row>>,
    ended_temps: Vec<(String, String)>,
    swaps: Vec<(String, String, String)>,
}

type ExpiryParts = (Vec<crate::owners::store::MemoryExpiry>, Vec<(String, String)>, Vec<String>);
type ResetParts = (Vec<u64>, Vec<(String, String)>, Vec<crate::owners::store::EphemeralReset>);

pub(crate) struct BatchWritten {
    pub revision: u64,
    pub observed: Vec<String>,
    /// The events of a Feed batch refused alone (P-DATA-99), by their place in the batch.
    pub refused: Vec<usize>,
    pub faults: Vec<String>,
    pub expiring: Vec<crate::owners::store::MemoryExpiry>,
    pub removed_memory: Vec<(String, String)>,
    pub stale_parents: Vec<String>,
    pub removed_operations: Vec<u64>,
    /// The rows a server delete removed: the Outbox discards every operation of them.
    pub removed_rows: Vec<(String, String)>,
    /// The temporary ids the batch ended: the Outbox ends the operations that carry them.
    pub ended_temps: Vec<(String, String)>,
    /// The `(model, temp, real)` replacements of the batch: the Outbox addresses the open operations of the row to `real`.
    pub swaps: Vec<(String, String, String)>,
    pub resets: Vec<crate::owners::store::EphemeralReset>,
    /// The notice of the batch: `None` when the batch has no revision and nobody is told.
    pub notice: Option<crate::owners::contract::CommittedBroadcast>,
    /// The row of each `Show` of the batch, in the order of the ops.
    pub shown: Vec<Option<super::Row>>,
    /// The files the batch deleted: a detach of an attached file deletes one.
    pub files_removed: usize,
}

impl Store {
    pub(crate) fn write_batch(&self, batch: &StoreBatch, files: &FileDirectories, min_pin_revision: Option<u64>) -> Result<BatchWritten, StoreError> {
        let now = self.lock().now_ms();
        let prepared = super::file_write::prepare_files(files, &batch.ops, now)?;
        let written = self.write_batch_prepared(batch, min_pin_revision, now, &prepared);
        match written {
            Err(error) => {
                for file in prepared.iter().filter(|file| file.record.location == Location::Directory) {
                    files.discard_partial(&file.record.id)?;
                }
                Err(error)
            }
            Ok(WrittenBatch { committed: result, base_revision, observed, refused, live_files, discarded, expiry: (expiring, removed_memory, stale_parents), resets: (removed_operations, removed_rows, resets), candidates, pointer_rows, shown, ended_temps, swaps }) => {
                let mut store = self.lock();
                store.counters.outbox_candidates += candidates;
                store.counters.pointer_rows_read += pointer_rows;
                drop(store);
                let mut faults = Vec::new();
                for file in prepared.iter().filter(|file| file.record.location == Location::Directory) {
                    let removed = if live_files.contains(&file.record.id) { files.finish(&file.record.id) } else { files.discard_partial(&file.record.id) };
                    if let Err(error) = removed { faults.push(format!("file {}: {error}", file.record.id)); }
                }
                let files_removed = discarded.len();
                for id in discarded {
                    if let Err(error) = files.discard(std::slice::from_ref(&id)) { faults.push(format!("remove file {id}: {error}")); }
                }
                let revision = result.result().revision.unwrap_or(base_revision);
                let notice = result.result().revision.map(|_| crate::owners::contract::CommittedBroadcast::of_commit(result.result()));
                result.publish();
                Ok(BatchWritten { revision: u64::try_from(revision).map_err(|_| StoreError::Constraint("negative revision".into()))?, observed, refused, faults, expiring, removed_memory, stale_parents, removed_operations, removed_rows, ended_temps, swaps, resets, notice, shown, files_removed })
            }
        }
    }

    fn write_batch_prepared<'a>(&'a self, batch: &StoreBatch, min_pin_revision: Option<u64>, now: i64, files: &[super::file_write::PreparedFile]) -> Result<WrittenBatch<'a>, StoreError> {
        let inner = self.lock();
        if !inner.ready { return Err(StoreError::Refused); }
        let transaction = inner.connection.unchecked_transaction()?;
        transaction.execute_batch(include_str!("sql/batch_write/write_batch_prepared_create_table_batch_file_deletions.sql"))?;
        transaction.execute_batch(&format!(include_str!("sql/batch_write/write_batch_prepared_create_table_batch_released_temps.sql"), flag = crate::identity::permanent_row_flag("OLD.row_id")))?;
        let base_revision = Reader::new(&transaction, &inner.models).revision_seq()?;
        let revision = base_revision.checked_add(1).ok_or_else(|| StoreError::Constraint("revision overflow".into()))?;
        let closing: Vec<i64> = batch.ops.iter().filter_map(|op| match op { crate::owners::store::StoreOp::RemoveIntent { op_id } => Some(i64::try_from(*op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))), _ => None }).collect::<Result<_, _>>()?;
        let changes_before = transaction.total_changes();
        let mut originals = BTreeMap::new();
        let mut results = Vec::new();
        let mut writer = RowWrite { connection: &transaction, models: &inner.models, generation: inner.generation, revision, now, originals: &mut originals, closing: closing.clone(), candidates: 0, current: Default::default(), feed_fields: Default::default(), feed_tombstones: Default::default(), feed_hidden: None, carried: Default::default(), event: None };
        let mut overlay = Vec::new();
        let rows_source = if batch.author == BatchAuthor::Outbox { crate::schema::RowSource::Response } else { crate::schema::RowSource::Page };
        let (observed, refused) = if batch.author == BatchAuthor::Feed {
            super::feed_write::land(&mut writer, &batch.rows, &mut overlay, &mut results)?
        } else {
            for (model, text) in &batch.rows {
                let value: serde_json::Value = serde_json::from_str(text)?;
                let row_id = value.get("id").and_then(serde_json::Value::as_str).ok_or_else(|| StoreError::Constraint("row has no id".into()))?;
                let row = RowValue { row_id: row_id.into(), values: text.clone() };
                if let Some(result) = writer.upsert(model, &row, rows_source)? { results.push(result); }
            }
            (Vec::new(), Vec::new())
        };
        let mut resets = Vec::new();
        let mut rolled_back = std::collections::BTreeSet::new();
        let mut virtual_transfers = Vec::new();
        for (index, op) in batch.ops.iter().enumerate() {
            if let crate::owners::store::StoreOp::UploadShown { key, shown, virtual_transfer: Some((id, op_id)), .. } = op {
                virtual_transfers.push((key.clone(), shown.clone(), *id, *op_id));
            }
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
            if matches!(op, crate::owners::store::StoreOp::Show { .. }) { continue; }
            if let crate::owners::store::StoreOp::OweReread { model, id, relation } = op {
                match super::reread_owed::owe(&mut writer, model, id, relation)? {
                    Some(super::reread_owed::Owed::New(result)) => results.push(result),
                    Some(super::reread_owed::Owed::Raised) | None => {}
                }
                continue;
            }
            if let crate::owners::store::StoreOp::Swap { model, temp, real } = op {
                results.push(super::swap::apply(&mut writer, model, temp, real, &closing)?);
                continue;
            }
            if let crate::owners::store::StoreOp::RemoveIntent { op_id } = op {
                if rolled_back.contains(op_id) { continue; }
            }
            if !super::batch_common::apply_common(&mut writer, op, index, files, &mut results, rows_source)? {
                return Err(StoreError::Constraint(format!("unsupported store operation: {op:?}")));
            }
            if let crate::owners::store::StoreOp::RollBack { op_id } = op { rolled_back.insert(*op_id); }
        }
        let (derived, pointer_rows, named_gone) = super::follow_pointer::settle_parents(&Reader::new(&transaction, &inner.models), &inner.models, &results, |plan| writer.apply(&plan))?;
        results.extend(derived);
        if let Some(touched) = super::touch_ancestors::touch_ancestors(&mut writer)? { results.push(touched); }
        let current = self.overlays.get(base_revision).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))?;
        let mut shown_transfers = current.virtual_transfers.clone();
        let mut virtual_changed = false;
        for (key, shown, transfer_id, op_id) in &virtual_transfers {
            match shown {
                crate::owners::write::transfer::Shown::Failed { error } => {
                    let next = super::overlay::VirtualTransfer { transfer_id: *transfer_id, op_id: *op_id, error: error.clone() };
                    virtual_changed |= shown_transfers.insert(key.clone(), next.clone()).is_none_or(|previous| previous != next);
                }
                crate::owners::write::transfer::Shown::Gone => virtual_changed |= shown_transfers.remove(key).is_some(),
                _ => {}
            }
        }
        let writes_database = transaction.total_changes() > changes_before || results.iter().any(|result| !result.is_noop()) || virtual_changed;
        let candidates = writer.candidates;
        let mut live_files = std::collections::HashSet::new();
        for file in files {
            let present: bool = transaction.query_row(include_str!("sql/batch_write/write_batch_prepared_select_file.sql"), [&file.record.id], |row| row.get(0))?;
            if present { live_files.insert(file.record.id.clone()); }
        }
        let discarded = transaction.prepare(include_str!("sql/batch_write/write_batch_prepared_select_batch_file_deletions.sql"))?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?;
        let released = transaction.prepare(include_str!("sql/batch_write/write_batch_prepared_select_batch_released_temps.sql"))?.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?.collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?;
        transaction.execute_batch(include_str!("sql/batch_write/write_batch_prepared_drop_trigger_batch_file_delete.sql"))?;
        let (expiring, removed_memory) = super::batch_result::expiry(&transaction, &inner.models, &results, inner.now_ms())?;
        let mut stale_parents = super::batch_result::stale_parents(&transaction, &inner.models, &results)?;
        if batch.author == BatchAuthor::Feed && !stale_parents.is_empty() {
            super::windows::mark_stale(&transaction, &stale_parents, &[], &[])?;
        }
        stale_parents.extend(named_gone.into_iter().filter(|model| !stale_parents.contains(model)).collect::<Vec<_>>());
        let removed_operations: Vec<u64> = if batch.author == BatchAuthor::Feed { results.iter().flat_map(|result| result.operations.iter()).filter_map(|seq| u64::try_from(*seq).ok()).collect() } else { Vec::new() };
        let removed_rows: Vec<(String, String)> = results.iter().flat_map(|result| result.server_removed.iter()).map(|row| (row.model.to_string(), row.id.to_string())).collect();
        let mut ended_temps: Vec<(String, String)> = Vec::new();
        let reader = Reader::new(&transaction, &inner.models);
        let ended_candidates: std::collections::BTreeSet<(String, String)> = results.iter().flat_map(|result| result.ended_temps.iter().cloned()).chain(released).collect();
        for (model, temp) in ended_candidates {
            let kept = reader.stored_row(&model, &temp)?.is_some() || transaction.query_row(include_str!("sql/ddl/read_temp_kept.sql"), rusqlite::params![model, temp], |row| row.get::<_, bool>(0))?;
            if !kept && transaction.execute(include_str!("sql/ddl/end_temp_row_alias.sql"), rusqlite::params![model, temp])? > 0 { ended_temps.push((model, temp)); }
        }
        let swaps: Vec<(String, String, String)> = results.iter().flat_map(|result| result.replaced.iter()).map(|(from, real)| (from.model.to_string(), from.id.to_string(), real.to_string())).collect();
        let target_revision = if writes_database { revision } else { base_revision };
        for result in &results {
            super::cloud_waiting::mark_changed(&transaction, inner.cloud_scope.as_ref().and_then(|attachment| attachment.capture_domain()), result)?;
        }
        let mut combined = super::batch_result::consolidate(&transaction, &inner.models, &originals, results, target_revision)?;
        super::cloud::capture_local(&transaction, inner.cloud_scope.as_ref().and_then(|attachment| attachment.capture_domain()), &inner.models, &originals, &combined, now)?;
        let mut shown = Vec::new();
        for op in &batch.ops {
            let (crate::owners::store::StoreOp::Intent { effects: crate::owners::store::IntentEffects { shown: Some((model, row_id)), .. }, .. } | crate::owners::store::StoreOp::Planned { effects: crate::owners::store::IntentEffects { shown: Some((model, row_id)), .. } } | crate::owners::store::StoreOp::Show { model, row_id }) = op else { continue };
            shown.push(self.load(&Reader::new(&transaction, &inner.models), model, std::slice::from_ref(row_id)).map_err(|error| StoreError::Constraint(error.to_string()))?.remove(row_id.as_str()));
        }
        let changes_state = writes_database || !overlay.is_empty() || !resets.is_empty();
        let mut snapshot = if changes_state { self.overlays.next(base_revision, writes_database).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))? } else { current.as_ref().clone() };
        snapshot.virtual_transfers = shown_transfers;
        let (changes, arms) = super::overlay_write::apply(&mut snapshot, &inner.models, overlay, u64::try_from(inner.now_ms()).unwrap_or(0))?;
        combined.rows.extend(changes);
        for change in combined.rows.iter().filter(|change| change.kind == crate::store::RowChangeKind::Destroyed) {
            snapshot.remove_row(&super::overlay::RowKey { model: change.row.model.to_string(), row_id: change.row.id.to_string() });
        }
        combined.rows.extend(super::overlay_write::reset(&mut snapshot, &inner.models, &resets)?);
        snapshot.prune_missing(OVERLAY_ROW_LIMIT, |row| Ok::<_, StoreError>(Reader::new(&transaction, &inner.models).stored_row(&row.model, &row.row_id)?.is_some()))?;
        let position = snapshot.position;
        combined.position = position;
        if !changes_state { combined.revision = None; }
        let journal = self.context.changed.prepare_commit(&combined);
        if writes_database {
            super::tombstones::prune_tombstones(&transaction, now, &TombstoneRetention::DEFAULT, min_pin_revision)?;
            crate::store::ddl::manifest_write(&transaction, crate::store::ddl::REVISION_SEQ_KEY, &revision.to_string())?;
            self.overlays.commit(snapshot, transaction)?;
        } else {
            transaction.rollback()?;
            if position != current.position { self.overlays.publish(snapshot); }
        }
        if let Some(journal) = journal { journal.finish(); }
        Ok(WrittenBatch { committed: self.commit_announcement(inner, combined), base_revision, observed, refused, live_files, discarded, expiry: (expiring, removed_memory, stale_parents), resets: (removed_operations, removed_rows, arms), candidates, pointer_rows, shown, ended_temps, swaps })
    }
}
