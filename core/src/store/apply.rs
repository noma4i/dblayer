use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::store::{
    BatchPlan, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, EdgeValue, EdgeWrite, RowChange, RowChangeKind, ScopeOp, ScopeRef, WindowSave,
};
use crate::operation::OperationStatus;
use crate::store::ddl::{manifest_write, REVISION_SEQ_KEY};
use crate::identity::{Generation, ModelKey, RowRef};
use crate::operation::{delete_operation, OperationEvent, failure_cells, write_operation_error, read_rows, stored_row_id, write_rows, write_value};
use crate::syncstate::{operation_keys, recompute_sync_with, OperationFailure, SyncKey};

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
    #[cfg(test)]
    pub(crate) fn commit_deferred(&self, envelope: BatchPlan) -> Result<Committed<'_>, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(envelope.generation)?;
        self.write_envelope(inner, envelope)
    }

    pub(crate) fn commit(&self, envelope: BatchPlan) -> Result<CommitResult, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(envelope.generation)?;
        self.write_envelope(inner, envelope).map(Committed::publish)
    }

    fn write_envelope<'a>(&'a self, inner: std::sync::MutexGuard<'_, Inner>, envelope: BatchPlan) -> Result<Committed<'a>, StoreError> {
        let now_ms = inner.now_ms();
        envelope.validate(inner.generation).map_err(StoreError::Plan)?;
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
    let outcome = apply_envelope(&transaction, &inner.models, envelope, now_ms)?;
    if outcome.is_noop() && envelope.window.is_none() && envelope.window_debts.is_empty() && envelope.pending_locals.is_empty() {
        transaction.rollback()?;
    } else if let Some(revision) = outcome.revision {
        let snapshot = store.overlays.next(revision - 1, true).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))?;
        store.overlays.commit(snapshot, transaction)?;
    } else {
        transaction.commit()?;
    }
    let acks = outcome.unknown_operation_acks;
    Ok((outcome, acks))
}

include!("apply_rows.rs");
include!("apply_envelope.rs");
include!("apply_transitions.rs");
