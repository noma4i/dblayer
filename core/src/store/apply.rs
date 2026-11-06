use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::commit::{
    CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationRecord, OperationTransition, EdgeValue, EdgeWrite, RowChange, RowChangeKind, ScopeOp, ScopeRef, WindowSave,
};
use crate::operation::OperationStatus;
use crate::commit::WriteOrigin;
use crate::ddl::{manifest_write, REVISION_SEQ_KEY};
use crate::identity::{Generation, ModelKey, RowRef};
use crate::operation::{delete_operation, OperationEvent, failure_cells, write_operation_error, read_rows, stored_row_id, write_rows, write_value};
use crate::syncstate::{operation_keys, recompute_sync_with, OperationFailure, SyncKey};

use super::file_ops::{apply_file_ops, release_files};
use super::rows::{column_value, fetch_stored, placeholders, Reader, StoredRow, CHUNK};
use super::search::{reindex, touches_search, unindex};
use super::stable;
use crate::nodes::{delete_trees, read_trees, texts, write_tree};
use super::shape::{delete_tables, insert_children, member_values, normalize, normalize_leaf, null_marker};
use crate::ddl::FieldPlan;
use super::windows::{upsert_window, WindowRecord};
use super::tombstones::{prune_tombstones, write_tombstone, TombstoneRetention};
use crate::lifecycle::authority::Authority;
#[cfg(test)]
use crate::lifecycle::authority::Internal;

use super::{Committed, Inner, ModelInfo, Store, StoreError, TombstonePrune};

#[derive(Debug, thiserror::Error)]
pub enum PlanCommitError<E> {
    #[error("planning failed")]
    Plan(E),
    #[error(transparent)]
    Store(StoreError),
}

impl<E> From<StoreError> for PlanCommitError<E> {
    fn from(error: StoreError) -> Self {
        PlanCommitError::Store(error)
    }
}

impl Store {
    #[cfg(test)]
    pub(crate) fn commit(&self, envelope: CommitEnvelope) -> Result<CommitResult, StoreError> {
        self.commit_as(&Authority::Internal(Internal::for_tests()), envelope)
    }

    pub(crate) fn commit_as(&self, authority: &Authority, envelope: CommitEnvelope) -> Result<CommitResult, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(authority, envelope.generation)?;
        match self.write_planned(authority, inner, Ok::<_, std::convert::Infallible>(Some(envelope)), TombstonePrune::Skip) {
            Ok(committed) => Ok(committed.publish()),
            Err(PlanCommitError::Store(error)) => Err(error),
            Err(PlanCommitError::Plan(never)) => match never {},
        }
    }

    fn plan_under_writer<E>(
        &self,
        authority: &Authority,
        generation: Generation,
        prune: TombstonePrune,
        plan: impl FnOnce(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<Committed<'_>, PlanCommitError<E>> {
        let mut inner = self.lock();
        inner.ensure_healthy()?;
        inner.ensure_generation(authority, generation)?;
        inner.counters.plans_under_writer += 1;
        let planned = plan(&Reader::new(&inner.connection, &inner.models));
        self.write_planned(authority, inner, planned, prune)
    }

    /// Plans on a read connection while the writer stays free. The writer only compares the revision the plan was read at and applies it. When another commit landed first, the writer is released and the plan runs again off the writer. Every repeat means another commit succeeded, so the loop has no attempt limit. Databases with `memory` models and in-memory databases plan under the writer. `plan` must only read and build from data it can build again.
    #[cfg(test)]
    pub(crate) fn commit_prepared<E>(
        &self,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<CommitResult, PlanCommitError<E>> {
        self.commit_prepared_deferred(&Authority::Internal(Internal::for_tests()), generation, plan).map(Committed::publish)
    }

    #[cfg(test)]
    pub(crate) fn commit_prepared_deferred<E>(
        &self,
        authority: &Authority,
        generation: Generation,
        plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<Committed<'_>, PlanCommitError<E>> {
        self.commit_prepared_with_pin(authority, generation, TombstonePrune::Skip, plan)
    }

    pub(crate) fn commit_prepared_with_pin<E>(
        &self,
        authority: &Authority,
        generation: Generation,
        prune: TombstonePrune,
        mut plan: impl FnMut(&Reader<'_>) -> Result<Option<CommitEnvelope>, E>,
    ) -> Result<Committed<'_>, PlanCommitError<E>> {
        if !self.reads_off_writer() {
            return self.plan_under_writer(authority, generation, prune, plan);
        }
        loop {
            let (revision, planned) = self.read(&[], |reader| Ok((reader.revision_seq()?, plan(reader))))?;
            #[cfg(test)]
            {
                let hook = self.before_write.lock().unwrap_or_else(|error| error.into_inner()).take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            let mut inner = self.lock();
            inner.ensure_healthy()?;
            inner.ensure_generation(authority, generation)?;
            inner.counters.plans_off_writer += 1;
            if Reader::new(&inner.connection, &inner.models).revision_seq()? != revision {
                continue;
            }
            return self.write_planned(authority, inner, planned, prune);
        }
    }

    fn write_planned<'a, E>(&'a self, authority: &Authority, inner: std::sync::MutexGuard<'_, Inner>, planned: Result<Option<CommitEnvelope>, E>, prune: TombstonePrune) -> Result<Committed<'a>, PlanCommitError<E>> {
        let now_ms = inner.now_ms();
        let envelope = planned.map_err(PlanCommitError::Plan)?;
        let Some(envelope) = envelope else {
            return Ok(Committed::unannounced(CommitResult::noop(uuid::Uuid::new_v4())));
        };
        envelope.validate(inner.generation).map_err(StoreError::Envelope)?;
        if envelope.is_empty() {
            return Ok(Committed::unannounced(CommitResult::noop(envelope.tx_id)));
        }
        match self.attempt_in_epoch(authority, &inner, &envelope, now_ms, prune) {
            Ok(result) => Ok(self.finish(inner, result)),
            Err(first) if first.is_apply_failure() => match self.attempt_in_epoch(authority, &inner, &envelope, now_ms, prune) {
                Ok(result) => Ok(self.finish(inner, result)),
                Err(second) => {
                    if second.is_apply_failure() {
                        *inner.poison.lock().unwrap_or_else(|error| error.into_inner()) = Some(second.to_string());
                    }
                    Err(second.into())
                }
            },
            Err(other) => Err(other.into()),
        }
    }

    /// The SQL commit of a write that belongs to a session epoch: the epoch check and the commit happen while the lifecycle holds its commit line, so a transition either comes before the check and refuses the write, or waits for the commit.
    fn attempt_in_epoch(&self, authority: &Authority, inner: &Inner, envelope: &CommitEnvelope, now_ms: i64, prune: TombstonePrune) -> Result<(CommitResult, u32), StoreError> {
        let _line = match authority {
            Authority::Admitted(admitted) => {
                if !std::sync::Arc::ptr_eq(&self.lifecycle, admitted.life()) { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration)); }
                let line = admitted.life().commit_line();
                if !admitted.is_current() { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration)); }
                Some(line)
            }
            Authority::Internal(_) | Authority::Owner => None,
        };
        #[cfg(test)]
        if matches!(authority, Authority::Admitted(_)) {
            let hook = self.after_epoch_check.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(hook) = hook { hook(); }
        }
        let _network = envelope.network.as_ref().map(|network| network.enter());
        if envelope.network.as_ref().is_some_and(|network| !network.holds()) { return Err(StoreError::Envelope(crate::commit::EnvelopeFault::NetworkMoved)); }
        #[cfg(test)]
        if envelope.network.is_some() {
            let hook = self.after_network_check.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(hook) = hook { hook(); }
        }
        let committed = attempt(self, inner, envelope, now_ms, prune)?;
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

fn attempt(store: &Store, inner: &Inner, envelope: &CommitEnvelope, now_ms: i64, prune: TombstonePrune) -> Result<(CommitResult, u32), StoreError> {
    let transaction = inner.connection.unchecked_transaction()?;
    let outcome = apply_envelope(&transaction, &inner.models, envelope, now_ms, prune)?;
    if outcome.is_noop() && envelope.window.is_none() && envelope.window_debts.is_empty() && envelope.pending_locals.is_empty() {
        transaction.rollback()?;
    } else {
        if let Some(revision) = outcome.revision {
            let snapshot = store.overlays.next(revision - 1, true).ok_or_else(|| StoreError::Constraint("overlay revision is unavailable".into()))?;
            store.overlays.publish(snapshot);
        }
        transaction.commit()?;
    }
    let acks = outcome.unknown_operation_acks;
    Ok((outcome, acks))
}

include!("apply_rows.rs");
include!("apply_envelope.rs");
include!("apply_transitions.rs");
