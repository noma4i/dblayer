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
    let first = apply_envelope_at(&transaction, &inner.models, envelope, (now_ms, revision, inner.generation), None, None)?;
    let apply = |plan: BatchPlan| apply_envelope_at(&transaction, &inner.models, &plan, (now_ms, revision, inner.generation), None, None);
    let (derived, _, _) = super::follow_pointer::settle_parents(&Reader::new(&transaction, &inner.models), &inner.models, std::slice::from_ref(&first), apply)?;
    let outcome = match derived { Some(derived) => super::batch_result::consolidate(&transaction, &inner.models, BTreeMap::new(), vec![first, derived], revision)?, None => first };
    let journal = store.context.changed.prepare_commit(&outcome);
    if outcome.is_noop() && envelope.window.is_none() && envelope.window_debts.is_empty() && envelope.pending_locals.is_empty() {
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

mod apply_rows;
pub(crate) use apply_rows::*;
mod apply_envelope;
pub(in crate::store) use apply_envelope::*;
mod apply_transitions;
pub(crate) use apply_transitions::*;
