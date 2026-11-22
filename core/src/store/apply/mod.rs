use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::store::{
    BatchPlan, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, EdgeValue, EdgeWrite, RowChange, RowChangeKind, ScopeOp, ScopeRef, WindowSave,
};
use crate::owners::write::operation_status::OperationStatus;
use crate::store::ddl::{manifest_write, REVISION_SEQ_KEY};
use crate::identity::{Generation, ModelKey, RowRef};
use crate::store::operation::{delete_operation, failure_cells, write_operation_error, read_rows, stored_row_id, write_rows, write_value};
use crate::owners::write::operation_machine::OperationEvent;
use crate::store::sync_state::{operation_keys, recompute_sync, SyncKey};
use crate::value::sync::OperationFailure;
use super::row_write::EventUndo;

use super::file_ops::{apply_file_ops, release_files};
use super::rows::{column_value, fetch_stored, placeholders, Reader, StoredRow, CHUNK};
use super::search::{reindex, touches_search, unindex};
use super::stable;
use crate::store::nodes::{delete_trees, read_trees, texts, write_tree};
use super::shape::{delete_tables, insert_children, member_values, normalize, normalize_leaf, null_marker};
use crate::store::ddl::FieldPlan;
use super::windows::{upsert_window, WindowRecord};
use super::tombstones::write_tombstone;

use super::{Committed, Inner, ModelInfo, Store, StoreError};

impl Store {
    pub(crate) fn erase_cloud_domain(&self, domain: &str) -> Result<BTreeSet<(String, String)>, StoreError> {
        let mut inner = self.lock();
        inner.ensure_healthy()?;
        if crate::store::ddl::manifest_read(&inner.connection, "cloud_domain")?.as_deref() != Some(domain) { return Ok(BTreeSet::new()); }
        if inner.cloud_scope.as_ref().is_some_and(|attachment| attachment.mode == super::CloudAttachmentMode::Live) {
            return Err(StoreError::Invalid("cloud scope is active".into()));
        }
        let now_ms = inner.now_ms();
        let transaction = inner.connection.unchecked_transaction()?;
        let revision = Reader::new(&transaction, &inner.models).revision_seq()? + 1;
        let mut plan = BatchPlan::from_source(crate::schema::RowSource::Local);
        let mut rows = BTreeSet::new();
        {
            let mut statement = transaction.prepare(include_str!("../sql/cloud_identities.sql"))?;
            let identities = statement.query_map([domain], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
            for identity in identities {
                let (model, row_id) = identity?;
                if !inner.models.contains_key(&model) { continue; }
                let id = crate::identity::RowId::new(&row_id).map_err(|error| StoreError::Invalid(error.to_string()))?;
                rows.insert((model.clone(), row_id));
                plan.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&model), id, reason: DestroyReason::Expired });
            }
        }
        super::cloud::reset(&transaction, domain)?;
        transaction.execute(include_str!("../sql/boot/boot_delete_manifest.sql"), ["cloud_domain"])?;
        let result = apply_transaction(self, &inner, transaction, &plan, now_ms, revision, false)?;
        if let Some(attachment) = &mut inner.cloud_scope { attachment.mode = super::CloudAttachmentMode::Erased; }
        self.finish(inner, result).publish();
        Ok(rows)
    }

    pub(crate) fn land_local_cloud(&self, scope: &crate::owners::cloud::CloudScope, records: Vec<(crate::cloud::CloudRecord, Option<Vec<u8>>)>, progress: Option<&[u8]>) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.require_cloud_scope(scope)?;
        let domain = scope.domain.as_str();
        let now_ms = inner.now_ms();
        let transaction = inner.connection.unchecked_transaction()?;
        let revision = Reader::new(&transaction, &inner.models).revision_seq()? + 1;
        let (plan, _) = cloud_plan(&transaction, &inner.models, domain, records, revision)?;
        if let Some(progress) = progress { super::cloud::write_progress(&transaction, domain, progress)?; }
        let result = apply_transaction(self, &inner, transaction, &plan, now_ms, revision, false)?;
        self.finish(inner, result).publish();
        Ok(())
    }

    pub(crate) fn replay_cloud_waiting(&self, scope: &crate::owners::cloud::CloudScope, limit: usize) -> Result<usize, StoreError> {
        let mut count = 0;
        while count < limit {
            let inner = self.lock();
            inner.ensure_healthy()?;
            inner.require_cloud_scope(scope)?;
            let transaction = inner.connection.unchecked_transaction()?;
            let Some(entry) = super::cloud_waiting::next_ready(&transaction, &scope.domain)? else { break };
            transaction.execute("DELETE FROM cloud_waiting_ready WHERE domain = ?1 AND seq = ?2", params![scope.domain, entry.seq])?;
            let revision = Reader::new(&transaction, &inner.models).revision_seq()? + 1;
            let (plan, deferred) = cloud_plan(&transaction, &inner.models, &scope.domain, vec![(entry.record, entry.system_fields)], revision)?;
            if !deferred { super::cloud_waiting::remove(&transaction, &scope.domain, entry.seq)?; }
            let result = apply_transaction(self, &inner, transaction, &plan, inner.now_ms(), revision, false)?;
            self.finish(inner, result).publish();
            count += 1;
        }
        Ok(count)
    }

    #[cfg(test)]
    pub(crate) fn commit_deferred(&self, envelope: BatchPlan) -> Result<Committed<'_>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        self.write_envelope(inner, envelope)
    }

    pub(crate) fn commit(&self, envelope: BatchPlan) -> Result<CommitResult, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        self.write_envelope(inner, envelope).map(Committed::publish)
    }

    fn write_envelope<'a>(&'a self, inner: std::sync::MutexGuard<'_, Inner>, envelope: BatchPlan) -> Result<Committed<'a>, StoreError> {
        let now_ms = inner.now_ms();
        if envelope.is_empty() {
            return Ok(Committed::unannounced(CommitResult::noop(envelope.tx_id)));
        }
        match self.attempt_in_epoch(&inner, &envelope, now_ms) {
            Ok(result) => Ok(self.finish(inner, result)),
            Err(first) if first.is_apply_failure() => match self.attempt_in_epoch(&inner, &envelope, now_ms) {
                Ok(result) => Ok(self.finish(inner, result)),
                Err(second) => {
                    if second.is_apply_failure() {
                        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = Some(second.to_string());
                    }
                    Err(second)
                }
            },
            Err(other) => Err(other),
        }
    }

    /// The SQL commit of a write that belongs to a session epoch: the epoch check and the commit happen while the lifecycle holds its commit line, so a transition either comes before the check and refuses the write, or waits for the commit.
    fn attempt_in_epoch(&self, inner: &Inner, envelope: &BatchPlan, now_ms: i64) -> Result<(CommitResult, u32), StoreError> {
        let _network = envelope.network.as_ref().map(|network| network.enter());
        if envelope.network.as_ref().is_some_and(|network| !network.holds()) { return Err(StoreError::Plan(crate::store::PlanFault::NetworkMoved)); }
        #[cfg(test)]
        if envelope.network.is_some() {
            let hook = self.after_network_check.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(hook) = hook { hook(); }
        }
        let committed = attempt(self, inner, envelope, now_ms)?;
        if let Some(newest) = committed.0.queue_operations.iter().filter(|(_, entry)| entry.is_some()).map(|(seq, _)| *seq).max() {
            self.newest_operation.fetch_max(newest, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(committed)
    }

    fn finish<'a>(&'a self, mut inner: std::sync::MutexGuard<'_, Inner>, result: (CommitResult, u32)) -> Committed<'a> {
        inner.counters.unknown_operation_acks += result.1 as usize;
        self.commit_announcement(inner, result.0)
    }
}

fn attempt(store: &Store, inner: &Inner, envelope: &BatchPlan, now_ms: i64) -> Result<(CommitResult, u32), StoreError> {
    let transaction = inner.connection.unchecked_transaction()?;
    let revision = Reader::new(&transaction, &inner.models).revision_seq()? + 1;
    apply_transaction(store, inner, transaction, envelope, now_ms, revision, true)
}

fn apply_transaction(store: &Store, inner: &Inner, transaction: rusqlite::Transaction<'_>, envelope: &BatchPlan, now_ms: i64, revision: i64, capture: bool) -> Result<(CommitResult, u32), StoreError> {
    let mut originals = BTreeMap::new();
    let capture_rows = capture && inner.cloud_scope.as_ref().and_then(|attachment| attachment.capture_domain()).is_some();
    let first = apply_envelope_at(&transaction, &inner.models, envelope, (now_ms, revision, inner.generation), capture_rows.then_some(&mut originals), None)?;
    let apply = |plan: BatchPlan| apply_envelope_at(&transaction, &inner.models, &plan, (now_ms, revision, inner.generation), capture_rows.then_some(&mut originals), None);
    let (derived, _, _) = super::follow_pointer::settle_parents(&Reader::new(&transaction, &inner.models), &inner.models, std::slice::from_ref(&first), apply)?;
    let outcome = match derived { Some(derived) => super::batch_result::consolidate(&transaction, &inner.models, &BTreeMap::new(), vec![first, derived], revision)?, None => first };
    super::cloud_waiting::mark_changed(&transaction, inner.cloud_scope.as_ref().and_then(|attachment| attachment.capture_domain()), &outcome)?;
    if capture { super::cloud::capture_local(&transaction, inner.cloud_scope.as_ref().and_then(|attachment| attachment.capture_domain()), &inner.models, &originals, &outcome, now_ms)?; }
    let journal = store.context.changed.prepare_commit(&outcome);
    if capture && outcome.is_noop() && envelope.window.is_none() && envelope.window_debts.is_empty() && envelope.pending_locals.is_empty() {
        transaction.rollback()?;
    } else if let Some(revision) = outcome.revision {
        let snapshot = store.overlays.next(revision - 1, true).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))?;
        store.overlays.commit(snapshot, transaction)?;
    } else {
        transaction.commit()?;
    }
    if let Some(journal) = journal { journal.finish(); }
    let acks = outcome.unknown_operation_acks;
    Ok((outcome, acks))
}

fn cloud_plan(tx: &rusqlite::Transaction<'_>, models: &BTreeMap<String, ModelInfo>, domain: &str, records: Vec<(crate::cloud::CloudRecord, Option<Vec<u8>>)>, revision: i64) -> Result<(BatchPlan, bool), StoreError> {
    use super::cloud::{self, project};
    use crate::cloud::{Choice, StampedValue};
    let mut plan = BatchPlan::from_source(crate::schema::RowSource::Local);
    let reader = Reader::new(tx, models);
    let mut seen = BTreeSet::new();
    let mut deferred = false;
    for (record, system_fields) in records {
        if !seen.insert((record.model.clone(), record.row_id.clone())) { return Err(StoreError::Invalid("duplicate cloud record".into())); }
        let info = models.get(&record.model).filter(|info| !info.spec.is_memory()).ok_or_else(|| StoreError::Invalid("invalid cloud model".into()))?;
        let id = crate::identity::RowId::new(&record.row_id).map_err(|error| StoreError::Invalid(error.to_string()))?;
        if id.is_temp() || id.as_str() != record.row_id { return Err(StoreError::Invalid("invalid cloud identity".into())); }
        if record.deleted && !record.fields.is_empty() { return Err(StoreError::Invalid("cloud tombstone has fields".into())); }
        if let (Some(name), Some(incoming)) = (&info.spec.server_revision, record.server_revision) {
            if record.fields.iter().any(|field| field.path.as_slice() == [name.as_str()] && field.data.value.as_i64() != Some(incoming)) { return Err(StoreError::Invalid("conflicting cloud revision".into())); }
        }
        let mut fields = Vec::with_capacity(record.fields.len());
        for field in &record.fields {
            let (_, rule, local) = project::field(&info.spec, &field.path).ok_or_else(|| StoreError::Invalid("invalid cloud field".into()))?;
            fields.push((field, rule, local));
        }
        fields.sort_unstable_by(|left, right| left.0.path.cmp(&right.0.path));
        if fields.windows(2).any(|pair| pair[1].0.path.starts_with(&pair[0].0.path)) {
            return Err(StoreError::Invalid("overlapping cloud fields".into()));
        }
        if !info.spec.is_local() && super::operation::has_open_row_operation(tx, &record.model, &record.row_id)? {
            super::cloud_waiting::enqueue(tx, domain, &record, system_fields.as_deref(), &[(record.model.as_str(), record.row_id.as_str())])?;
            deferred = true;
            continue;
        }
        let deleted: bool = tx.query_row(include_str!("../sql/cloud_deleted.sql"), params![domain, record.model, record.row_id], |row| row.get(0)).optional()?.unwrap_or(false);
        if record.deleted {
            let pending = cloud::PendingRecord { model: record.model.clone(), row_id: record.row_id.clone(), version: revision, deleted: true };
            cloud::stage(tx, domain, &pending)?;
            cloud::acknowledge(tx, domain, &pending, None)?;
            cloud::write_system_fields(tx, domain, &record.model, &record.row_id, system_fields.as_deref())?;
            plan.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(&record.model), id, reason: DestroyReason::Local });
            continue;
        }
        if deleted { continue; }
        let stored = reader.stored_row(&record.model, &record.row_id)?;
        let current_revision = info.spec.server_revision.as_ref().and_then(|field| stored.as_ref()?.row.get(field)).and_then(Value::as_i64);
        let revision_order = record.server_revision.zip(current_revision).map(|(incoming, current)| incoming.cmp(&current));
        let mut row = stored.as_ref().map(|row| row.row.clone()).unwrap_or_else(|| Map::from_iter([("id".into(), Value::String(record.row_id.clone()))]));
        let before = project::project(&info.spec, &row);
        let stamps: BTreeMap<_, _> = cloud::field_stamps(tx, domain, &record.model, &record.row_id)?.into_iter().collect();
        let mut changed = stored.is_none();
        let mut marks = Vec::new();
        for (field, rule, local) in fields {
            if !local && revision_order.is_some_and(|order| order.is_lt()) { continue; }
            if (local || !revision_order.is_some_and(|order| order.is_gt())) &&
                ((1..field.path.len()).any(|length| stamps.get(&field.path[..length]).is_some_and(|stamp| *stamp >= field.data.stamp)) ||
                stamps.range(field.path.clone()..).take_while(|(path, _)| path.starts_with(&field.path))
                    .any(|(path, stamp)| path.len() > field.path.len() && *stamp >= field.data.stamp)) { continue; }
            let old = before.get(&field.path);
            let value = if field.data.value.is_null() { Value::Null } else {
                let mut candidate = row.clone();
                project::set(&mut candidate, &field.path, field.data.value.clone())?;
                apply_rows::canonicalize(info, &mut candidate)?;
                project::project(&info.spec, &candidate).get(&field.path).cloned().ok_or_else(|| StoreError::Invalid("invalid cloud value".into()))?
            };
            let incoming = StampedValue { value, stamp: field.data.stamp };
            let stamp = stamps.get(&field.path).copied();
            let wins = match stamp {
                Some(stamp) => {
                    let stored = StampedValue { value: old.cloned().unwrap_or(Value::Null), stamp };
                    let choice = crate::cloud::choose_field(&stored, &incoming, rule).map_err(|_| StoreError::Invalid("conflicting cloud stamp".into()))?;
                    if !local && revision_order.is_some_and(|order| order.is_gt()) && stamp != incoming.stamp {
                        rule.map_or(std::cmp::Ordering::Equal, |rule| rule.rank(&stored.value, &incoming.value)).is_le()
                    } else { choice == Choice::Incoming }
                }
                _ => old.is_none_or(|old| rule.is_none_or(|rule| !rule.rank(old, &incoming.value).is_gt())),
            };
            if wins {
                project::set(&mut row, &field.path, incoming.value)?;
                marks.push((&field.path, incoming.stamp));
                changed = true;
            }
        }
        if let (Some(name), Some(incoming)) = (&info.spec.server_revision, record.server_revision) {
            if revision_order.is_none_or(|order| order.is_gt()) && row.get(name).and_then(Value::as_i64) != Some(incoming) {
                row.insert(name.clone(), Value::from(incoming));
                changed = true;
            }
        }
        let mut normalized = apply_rows::without_nulls(info, &row);
        apply_rows::canonicalize(info, &mut normalized)?;
        let mut missing = Vec::new();
        for association in info.spec.associations.iter().filter(|association| association.kind == crate::schema::AssociationKind::BelongsTo) {
            if let Some(parent) = normalized.get(&association.foreign_key).and_then(crate::value::read_id) {
                if reader.point(association.target.as_str(), parent.as_str())?.is_none() {
                    missing.push((association.target.as_str(), parent));
                }
            }
        }
        if !missing.is_empty() {
            let dependencies: Vec<_> = missing.iter().map(|(model, id)| (*model, id.as_str())).collect();
            super::cloud_waiting::enqueue(tx, domain, &record, system_fields.as_deref(), &dependencies)?;
            deferred = true;
            continue;
        }
        for (path, stamp) in &marks {
            cloud::write_field_stamp(tx, domain, &record.model, &record.row_id, path, *stamp)?;
        }
        if let Some(latest) = marks.into_iter().map(|(_, stamp)| stamp).max() {
            cloud::observe_stamp(tx, domain, latest)?;
        }
        if changed {
            plan.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(&record.model), id, row });
        }
        cloud::write_system_fields(tx, domain, &record.model, &record.row_id, system_fields.as_deref())?;
    }
    Ok((plan, deferred))
}

mod apply_rows;
pub(crate) use apply_rows::*;
mod apply_envelope;
pub(in crate::store) use apply_envelope::*;
mod apply_transitions;
pub(crate) use apply_transitions::*;
