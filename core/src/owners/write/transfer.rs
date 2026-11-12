//! Owner `Transfer`: file upload steps. One instance per epoch with an identity.
//! The types are in `transfer_types`, the rows of the table in `transfer_{open,accept,steps,end}`; this file holds the helpers of the rows and the dispatch of the inputs.

use std::collections::BTreeMap;

pub use super::transfer_types::*;
use super::outbox::{Fault, NewOp, OpSource, OutboxInput, OutboxOutcome, Receipt, ReserveTarget};
use super::outbox_types::Phase;
use super::rounds::RoundsInput;
use crate::owners::contract::{BatchAuthor, Drained, EpochEnded, Online, RejectReason, ReplyId};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, UploadKind};
use crate::runtime::RetryPolicy;
use crate::runtime::{Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

impl TransferState {
    fn remove_upload(transfer: &Transfer) -> StoreOp {
        match &transfer.restore_element {
            Some((key, _)) => StoreOp::DiscardRestoreFailed { table: "sync_upload".into(), key: key.clone() },
            None => StoreOp::RemoveUpload { key: transfer.key.clone() },
        }
    }
    pub(super) fn shown_now(&self, id: TransferId) -> Shown {
        let Some(transfer) = self.transfers.get(&id) else { return Shown::Gone };
        if matches!(transfer.stage, TransferStage::Copying | TransferStage::PersistingCopy) {
            match transfer.supply_back {
                Some(TransferStage::AwaitingSource) => return Shown::WaitingSource,
                Some(TransferStage::Failed) => return Shown::Failed { error: transfer.error.clone().unwrap_or_default() },
                _ => {}
            }
        }
        match transfer.stage {
            TransferStage::Accepting | TransferStage::Copying | TransferStage::CopyReady => Shown::Copying,
            TransferStage::AwaitingSource => Shown::WaitingSource,
            TransferStage::Failed => Shown::Failed { error: transfer.error.clone().unwrap_or_default() },
            TransferStage::Done => Shown::Gone,
            TransferStage::Completing | TransferStage::PersistingComplete | TransferStage::BatchReady | TransferStage::PersistingBatchIntent | TransferStage::Mutating => Shown::Uploading { progress: 100 },
            TransferStage::Uploading | TransferStage::PersistingPart => match transfer.source.as_ref().filter(|source| source.parts > 0) {
                Some(source) => Shown::Uploading { progress: transfer.confirmed_parts * 100 / source.parts },
                None => Shown::Failed { error: "Internal(InvalidSource)".into() },
            },
            _ => Shown::Queued,
        }
    }

    /// What a joining reader gets: the value the readers got last with its revision, `Gone` while no value of the key was published.
    pub(super) fn published_by_key(&self, key: &str) -> (Shown, i64) { self.published.get(key).cloned().unwrap_or((Shown::Gone, 0)) }

    /// The Store op that writes the shown value with the counters it shows.
    pub(super) fn shown_op(transfer: &super::transfer_types::Transfer, shown: Shown) -> StoreOp {
        let refused = transfer.round_error.is_some() && matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading);
        let error = if let (Shown::Failed { error }, false) = (&shown, refused) { Some(error.clone()) } else { None };
        StoreOp::UploadShown { key: transfer.key.clone(), shown, sent: transfer.confirmed_parts, total: transfer.source.as_ref().map_or(0, |source| source.parts), attempts: transfer.attempts, error, virtual_transfer: transfer.restore_element.as_ref().map(|_| (transfer.transfer_id, transfer.op_id)) }
    }

    /// `uploads[action]` and every `Blob` stream of the transfer get `shown`.
    pub(super) fn show(&mut self, id: TransferId, shown: Shown, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let key = transfer.key.clone();
        let columns = shown.columns();
        transfer.last_shown = Some(shown.clone());
        let write = shown != Shown::Gone && transfer.stored_shown.is_some_and(|stored| stored != columns);
        if write {
            let waiting = self.pending.values().any(|pending| pending.kind == StoreKind::Shown && pending.written.get(&id) == Some(&columns));
            if !waiting {
                let op = Self::shown_op(transfer, shown);
                let batch = self.send_batch(vec![op], fx);
                self.pending.insert(batch, Pending { ids: Vec::new(), kind: StoreKind::Shown, orphan: None, written: BTreeMap::from([(id, columns)]) });
            }
            return;
        }
        let revision = self.transfers.get(&id).map_or(0, |transfer| transfer.revision);
        self.publish_shown(&key, shown, revision, fx);
    }

    fn publish_shown(&mut self, key: &str, shown: Shown, revision: i64, fx: &mut Fx) {
        if shown == Shown::Gone { self.published.remove(key); } else { self.published.insert(key.to_string(), (shown.clone(), revision)); }
        for stream_id in self.subscribers.get(key).into_iter().flatten() {
            run(fx, TransferEffect::Status { stream_id: *stream_id, key: key.to_string(), shown: shown.clone(), revision });
        }
    }

    /// The Store holds `columns` of the row: a later value of the transfer is written, the value of this batch is published when `publish`.
    fn shown_committed(&mut self, id: TransferId, columns: (u8, bool), (publish, revision): (bool, i64), reconcile: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stored_shown = Some(columns);
        transfer.revision = transfer.revision.max(revision);
        if !reconcile { return; }
        let key = transfer.key.clone();
        match transfer.last_shown.clone() {
            Some(shown) if shown != Shown::Gone && shown.columns() != columns => self.show(id, shown, fx),
            Some(shown) if publish => self.publish_shown(&key, shown, revision, fx),
            _ => {}
        }
    }

    pub(super) fn release_step(&mut self, id: TransferId, fx: &mut Fx) {
        if let Some(request_id) = self.transfers.get_mut(&id).and_then(|transfer| transfer.pending_step.take()) {
            run(fx, TransferEffect::ReleaseStep { request_id });
        }
    }

    /// The pair of a restored step was answered by Session: the transfer may start its attempt.
    pub(super) fn held_step_answered(&mut self, now: Time, request_id: u64, outcome: crate::owners::session::AuthorOutcome, fx: &mut Fx) {
        if let Some(upload_id) = self.orphans.keys().find(|upload_id| super::wire::abort_request_id(upload_id) == request_id).cloned() {
            if !self.orphan_flight.contains(&upload_id) || self.pending.values().any(|pending| pending.kind == StoreKind::OrphanRemoved && pending.orphan.as_ref().is_some_and(|(removing, _)| *removing == upload_id)) {
                return;
            }
            use crate::owners::session::{AuthorOutcome, SessionError};
            let result = match outcome {
                AuthorOutcome::Response { body, .. } => {
                    let answer: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    let refused = answer.get("errors").and_then(serde_json::Value::as_array).is_some_and(|errors| !errors.is_empty()) || answer.get("data").and_then(|data| crate::owners::read::codec::root_value(data, &self.abort_root)).is_none_or(serde_json::Value::is_null);
                    if refused { Err(AbortFault { temporary: false, status: None }) } else { Ok(()) }
                }
                AuthorOutcome::Error { class, error, .. } => Err(AbortFault { temporary: matches!(class, crate::owners::contract::ErrorClass::Network | crate::owners::contract::ErrorClass::Server), status: if let SessionError::Http { status } = error { Some(status) } else { None } }),
                AuthorOutcome::Dropped => Err(AbortFault { temporary: true, status: None }),
            };
            return self.abort_done(upload_id, result, fx);
        }
        let seeded = match outcome { crate::owners::session::AuthorOutcome::Response { body, kind } => Some((request_id, Ok((body, kind)))), _ => None };
        for transfer in self.transfers.values_mut().filter(|transfer| transfer.pending_step == Some(request_id)) {
            transfer.pending_step = None;
            transfer.seeded.clone_from(&seeded);
        }
        self.start_in_order(now, fx);
    }

    pub(super) fn step_answered(&mut self, id: TransferId) {
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.pending_step = None; }
    }

    pub(super) fn set(&mut self, id: TransferId, stage: TransferStage) {
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = stage;
        }
    }

    /// One Store batch for `ids`: a row of `sync_upload` per transfer, or its removal.
    pub(super) fn store(&mut self, ids: &[TransferId], kind: StoreKind, fx: &mut Fx) { self.store_basis(ids, kind, BTreeMap::new(), fx); }

    /// The same batch again with the generations the Store named (`Rejected(StaleGeneration)`).
    fn store_basis(&mut self, ids: &[TransferId], kind: StoreKind, basis: BTreeMap<String, u64>, fx: &mut Fx) {
        if kind == StoreKind::BatchIntent {
            return self.store_batch_intent(ids, basis, fx);
        }
        let leaving: Vec<TransferId> = if kind == StoreKind::Removed { ids.iter().copied().filter(|id| matches!(self.transfers.get(id).and_then(|transfer| transfer.removal.as_ref()), Some(Removal::Cancel))).collect() } else { Vec::new() };
        let numbers: Vec<StoreOp> = leaving.into_iter().flat_map(|id| self.renumber_after(id)).collect();
        let mut prefix = Vec::new();
        if kind == StoreKind::SyncUpload {
            let replacements: Vec<_> = ids.iter().filter_map(|id| self.replacements.get(id).cloned()).collect();
            for (old, operation) in replacements {
                let Some(transfer) = self.transfers.get(&old) else { continue };
                if transfer.stage != TransferStage::Removing || transfer.prior != Some(TransferStage::Failed) { continue; }
                prefix.push(Self::remove_upload(transfer));
                if transfer.restore_element.is_some() { prefix.push(Self::shown_op(transfer, Shown::Gone)); }
                if let Some(op_id) = transfer.op_id.filter(|_| transfer.has_operation) { prefix.push(StoreOp::RollBack { op_id }); }
                if let Some(op_id) = operation.as_ref().map(|op| op.op_id).or_else(|| transfer.op_id.filter(|_| transfer.has_operation)) { prefix.push(StoreOp::RemoveIntent { op_id }); }
                if let Some(operation) = operation {
                    prefix.push(StoreOp::RemoveAnswer { request_id: operation.request_id });
                    if operation.release_pending { prefix.push(StoreOp::ReleasePendingOwner { request_id: operation.request_id }); }
                }
                if let Some(upload_id) = Self::removal_orphan(transfer, &Removal::Discarded) { prefix.push(StoreOp::UploadOrphan { upload_id, key: transfer.key.clone() }); }
                prefix.push(StoreOp::RetireTransferOutcome { transfer_id: old, cycle_id: transfer.cycle_id });
                prefix.push(StoreOp::TransferOutcome { transfer_id: old, cycle_id: transfer.cycle_id, outcome: OutboxOutcome::Discarded });
            }
        }
        let ops: Vec<StoreOp> = prefix.into_iter().chain(ids
            .iter()
            .filter_map(|id| self.transfers.get(id).map(|transfer| (*id, transfer)))
            .flat_map(|(id, transfer)| {
                let kind = if kind == StoreKind::MixedOutcomes {
                    if transfer.stage == TransferStage::Removing { StoreKind::Removed } else { StoreKind::Failed }
                } else { kind };
                let own = transfer.op_id.filter(|_| !transfer.submitted || transfer.prior == Some(TransferStage::Failed));
                let mut ops = Vec::new();
                if kind == StoreKind::Removed {
                    ops.push(Self::remove_upload(transfer));
                    if transfer.restore_element.is_some() { ops.push(Self::shown_op(transfer, Shown::Gone)); }
                    ops.push(StoreOp::TransferOutcome { transfer_id: id, cycle_id: transfer.cycle_id, outcome: transfer.outcomes.get(&transfer.cycle_id).filter(|outcome| !matches!(outcome, OutboxOutcome::Ended(_))).cloned().unwrap_or(OutboxOutcome::Discarded) });
                    if let (TransferStage::Removing, Some(Removal::Confirmed { row_id: Some(row_id), .. })) = (transfer.stage, &transfer.removal) {
                        ops.push(StoreOp::Show { model: transfer.mutation.model.clone(), row_id: row_id.clone() });
                    }
                    if let Some(upload_id) = transfer.removal.as_ref().and_then(|removal| Self::removal_orphan(transfer, removal)) {
                        ops.push(StoreOp::UploadOrphan { upload_id, key: transfer.key.clone() });
                    }
                    if let Some(op_id) = own {
                        match transfer.removal {
                            Some(Removal::Confirmed { .. }) if transfer.batch_id.is_some() => ops.push(StoreOp::RemoveIntent { op_id }),
                            Some(Removal::Confirmed { .. }) => {}
                            _ => {
                                if transfer.has_operation { ops.push(StoreOp::RollBack { op_id }); }
                                ops.push(StoreOp::RemoveIntent { op_id });
                            }
                        }
                    }
                } else {
                    let cached_copy = kind == StoreKind::SyncUpload && transfer.stage == TransferStage::PersistingCopy && transfer.from_cache;
                    let kind = if cached_copy { StoreKind::Completed } else { kind };
                    if kind == StoreKind::Retrying || (transfer.stage == TransferStage::PersistingCopy && transfer.supply_back == Some(TransferStage::Failed)) {
                        if let Some(outcome) = transfer.outcomes.get(&transfer.cycle_id).filter(|outcome| !matches!(outcome, OutboxOutcome::Ended(_))) {
                            ops.push(StoreOp::TransferOutcome { transfer_id: id, cycle_id: transfer.cycle_id, outcome: outcome.clone() });
                        }
                    }
                    if transfer.stage == TransferStage::PersistingCopy && transfer.supply_back == Some(TransferStage::Failed) {
                        if let Some(previous) = transfer.previous.as_ref() {
                            if let Some(upload_id) = previous.upload_id.clone() {
                                ops.push(StoreOp::UploadOrphan { upload_id, key: previous.key.clone() });
                            }
                        }
                        ops.push(StoreOp::Upload { row: upload_row(transfer, kind), kind: UploadKind::Retrying });
                    }
                    let upload_kind = if kind == StoreKind::SyncUpload && matches!(transfer.stage, TransferStage::Accepting | TransferStage::AwaitingSource) && transfer.source.is_none() { UploadKind::AwaitingSource } else { upload_kind(kind) };
                    ops.push(StoreOp::Upload { row: upload_row(transfer, kind), kind: upload_kind });
                    if let (StoreKind::ConfirmedParts | StoreKind::Completed | StoreKind::Failed, Some(request_id)) = (kind, transfer.answer_saved) {
                        ops.push(StoreOp::RemoveAnswer { request_id });
                    }
                    let shown = transfer.last_shown.clone().unwrap_or_else(|| upload_kind.shown());
                    if !matches!(kind, StoreKind::Retrying | StoreKind::Failed) && transfer.stage != TransferStage::PersistingCopy && shown != Shown::Gone && shown.columns() != upload_kind.shown().columns() {
                        ops.push(Self::shown_op(transfer, shown));
                    }
                    if kind == StoreKind::Failed && transfer.failure_abort {
                        if let Some(upload_id) = transfer.upload_id.clone() {
                            ops.push(StoreOp::UploadOrphan { upload_id, key: transfer.key.clone() });
                        }
                    }
                    if transfer.stage == TransferStage::PersistingCopy && transfer.cache_stale {
                        ops.push(StoreOp::BlobCacheRemove { content_sha256: transfer.sha256.clone() });
                    }
                    if let (StoreKind::Completed, TransferStage::PersistingComplete, false, Some(blob_id), Some(created_at)) = (kind, transfer.stage, transfer.from_cache, transfer.signed_id.clone(), transfer.blob_created_at) {
                        ops.push(StoreOp::BlobCachePut { content_sha256: transfer.sha256.clone(), blob_id, created_at: created_at.0 });
                    }
                    if let (StoreKind::SyncUpload, TransferStage::Accepting, Some(group)) = (kind, transfer.stage, transfer.batch_id) {
                        if let Some(entry) = self.groups.get(&group).filter(|entry| entry.ids.first() == Some(&transfer.transfer_id)) {
                            if let Some(op_id) = entry.op_id {
                                ops.push(StoreOp::UploadBatch { batch_id: group, op_id, joint: entry.joint.to_text() });
                            }
                        }
                    }
                    if let (Some(op_id), StoreKind::SyncUpload, TransferStage::Accepting, None) = (own, kind, transfer.stage, transfer.prior) {
                        if !transfer.mutation.effects.is_empty() {
                            ops.push(StoreOp::Effects { op_id, created_at: transfer.created_at.0, model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), action: transfer.mutation.action.clone(), payload: transfer.mutation.body.clone(), effects: transfer.mutation.effects.clone() });
                        }
                    }
                    if let (Some(op_id), true, TransferStage::PersistingCopy, Some(_)) = (own, matches!(kind, StoreKind::SyncUpload | StoreKind::Completed), transfer.stage, transfer.supply_back) {
                        let payload = transfer.new_source.as_ref().map_or_else(|| transfer.mutation.body.clone(), |source| source.body.clone());
                        ops.push(StoreOp::Effects { op_id, created_at: transfer.created_at.0, model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), action: transfer.mutation.action.clone(), payload, effects: transfer.supply_values.clone() });
                        if transfer.supply_local != "{}" && !transfer.supply_local.is_empty() {
                            ops.push(StoreOp::LocalFields { model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), values: transfer.supply_local.clone() });
                        }
                        ops.push(StoreOp::Show { model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone() });
                    }
                    if kind == StoreKind::SyncUpload && transfer.stage == TransferStage::Accepting {
                        ops.push(StoreOp::Show { model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone() });
                    }
                    if let (Some(op_id), StoreKind::Completed, None, Some(signed)) = (own, kind, transfer.batch_id, transfer.signed_id.as_ref()) {
                        let body = transfer.new_source.as_ref().filter(|_| transfer.supply_back.is_some()).map_or_else(|| transfer.mutation.body.clone(), |source| source.body.clone());
                        ops.push(StoreOp::Intent { op_id, created_at: transfer.created_at.0, model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), action: transfer.mutation.action.clone(), payload: body.replace(SIGNED_ID_SENTINEL, signed), effects: Default::default(), origin: crate::owners::store::IntentOrigin::Transfer { transfer_id: transfer.transfer_id }, attempts: 1, cycle_id: cycle_of(transfer, kind) });
                    }
                }
                ops
            }))
            .chain(numbers)
            .collect();
        let columns: BTreeMap<String, (u8, bool)> = ops.iter().filter_map(|op| match op {
            StoreOp::Upload { row, kind } => Some((row.key.clone(), kind.shown().columns())),
            StoreOp::UploadShown { key, shown, .. } => Some((key.clone(), shown.columns())),
            _ => None,
        }).collect();
        let written: BTreeMap<TransferId, (u8, bool)> = ids.iter().filter_map(|id| self.transfers.get(id).and_then(|transfer| columns.get(&transfer.key)).map(|columns| (*id, *columns))).collect();
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: ids.to_vec(), kind, orphan: None, written });
        let intents = if kind == StoreKind::SyncUpload { ids.len() as u64 } else { 0 };
        let mut batch = StoreBatch { author: BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Epoch(self.epoch), basis, rows: Vec::new(), intents, credential: None, ops };
        let entries = batch.models().into_iter().map(|model| {
            let generation = batch.basis.get(&model).copied().or_else(|| self.generations.get(&model).copied()).unwrap_or(self.base_generation);
            (model, generation)
        }).chain(batch.basis.clone());
        batch.basis = crate::owners::store::basis_for(entries);
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    /// The multipart upload a removed transfer leaves on the server: it gets `abort` until the server answers.
    pub(super) fn orphan_of(transfer: &Transfer) -> Option<String> { transfer.upload_id.clone().filter(|_| !transfer.aborted && transfer.signed_id.is_none()) }

    pub(super) fn debt_live(&self, upload_id: &str, transfer: Option<TransferId>) -> bool {
        transfer.is_none_or(|id| self.transfers.get(&id).and_then(Self::orphan_of).as_deref() == Some(upload_id))
    }

    pub(super) fn debt_of(&self, pending: &Pending) -> Option<(String, Key, Option<TransferId>)> {
        let (upload_id, key, transfer) = match (&pending.orphan, pending.ids.first().and_then(|id| self.transfers.get(id))) {
            (Some((upload_id, key)), _) => (upload_id.clone(), key.clone(), None),
            (None, Some(transfer)) => (transfer.upload_id.clone()?, transfer.key.clone(), Some(transfer.transfer_id)),
            (None, None) => return None,
        };
        self.debt_live(&upload_id, transfer).then_some((upload_id, key, transfer))
    }

    pub(super) fn removal_orphan(transfer: &Transfer, removal: &Removal) -> Option<String> {
        match removal {
            Removal::Cancel => Self::orphan_of(transfer),
            Removal::Discarded => transfer.upload_id.clone().filter(|_| !transfer.aborted),
            _ => None,
        }
    }

    /// `abort` of every orphan whose abort is not in flight, when the network is up. `network`: the call comes from `Online(true)` or `RestoreReady`, so an abort already in flight is owed again.
    pub(super) fn send_orphans(&mut self, network: bool, fx: &mut Fx) {
        if self.online != Some(true) || self.ended || self.declarations.is_empty() {
            return;
        }
        for (upload_id, key) in &self.orphans {
            if self.orphan_flight.insert(upload_id.clone()) {
                run(fx, TransferEffect::AbortOrphan { key: key.clone(), upload_id: upload_id.clone() });
            } else if network {
                self.orphan_owed.insert(upload_id.clone());
            }
        }
    }

    /// The debt of a minted `upload_id` is data of the owner: it is written under `Control`, whatever epoch refused the write. `Closed` leaves the debt in memory, the next open writes it again.
    pub(super) fn write_control_orphan(&mut self, upload_id: String, key: String, fx: &mut Fx) {
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: Vec::new(), kind: StoreKind::ControlOrphan, orphan: Some((upload_id.clone(), key.clone())), written: BTreeMap::new() });
        let batch = StoreBatch { author: BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Control, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::UploadOrphan { upload_id, key }] };
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn commit_ops(&mut self, ops: Vec<StoreOp>, fx: &mut Fx) -> Option<crate::owners::contract::BatchId> {
        if ops.is_empty() {
            return None;
        }
        let id = self.send_batch(ops, fx);
        self.pending.insert(id, Pending { ids: Vec::new(), kind: StoreKind::Removed, orphan: None, written: BTreeMap::new() });
        Some(id)
    }

    /// A batch whose answer the owner does not wait for: the shown value of a row.
    pub(super) fn send_batch(&mut self, ops: Vec<StoreOp>, fx: &mut Fx) -> crate::owners::contract::BatchId {
        let id = self.next_batch;
        self.next_batch += 1;
        let mut batch = StoreBatch { author: BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Epoch(self.epoch), basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops };
        batch.basis = crate::owners::store::basis_for(batch.models().into_iter().map(|model| (self.generations.get(&model).copied().unwrap_or(self.base_generation), model)).map(|(generation, model)| (model, generation)));
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
        id
    }

    pub(super) fn abort_done(&mut self, upload_id: String, result: Result<(), AbortFault>, fx: &mut Fx) {
        let owed = self.orphan_owed.remove(&upload_id);
        if let Some(fault) = result.err().filter(|fault| !matches!(fault.status, Some(404 | 410))) {
            let key = self.orphans.get(&upload_id).filter(|_| fault.temporary && owed && self.online == Some(true) && !self.ended).cloned();
            match key {
                Some(key) => run(fx, TransferEffect::AbortOrphan { key, upload_id }),
                None => { self.orphan_flight.remove(&upload_id); }
            }
            return;
        }
        let Some(key) = self.orphans.get(&upload_id).cloned() else {
            self.orphan_flight.remove(&upload_id);
            return;
        };
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: Vec::new(), kind: StoreKind::OrphanRemoved, orphan: Some((upload_id.clone(), key)), written: BTreeMap::new() });
        let batch = StoreBatch { author: BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Epoch(self.epoch), basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::RemoveOrphan { upload_id }] };
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn rounds_needed(&self, ids: Vec<TransferId>, fx: &mut Fx) {
        if let (Some(rounds), false) = (self.peers.rounds, ids.is_empty()) {
            fx.push(Effect::Send(Outbound::new(rounds, RoundsInput::Needed { epoch: self.epoch, ids })));
        }
    }

    pub(super) fn rounds_idle(&self, id: TransferId, fx: &mut Fx) {
        if let Some(rounds) = self.peers.rounds {
            fx.push(Effect::Send(Outbound::new(rounds, RoundsInput::Idle { epoch: self.epoch, transfer_id: id })));
        }
    }

    pub(super) fn receipt(&self, id: TransferId) -> Receipt { Receipt { epoch: self.epoch, op_id: id, cycle_id: self.transfers.get(&id).map_or(0, |transfer| transfer.cycle_id), revision: self.transfers.get(&id).map_or(0, |transfer| transfer.revision), optimistic: self.transfers.get(&id).and_then(|transfer| transfer.optimistic.clone()) } }

    /// The immutable outcome of the current cycle: every waiter of the cycle gets it once.
    pub(super) fn settle(&mut self, id: TransferId, outcome: OutboxOutcome, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let cycle = transfer.cycle_id;
        transfer.outcomes.entry(cycle).or_insert_with(|| outcome.clone());
        if matches!(outcome, OutboxOutcome::Ended(_)) && self.confirmed_discards.contains_key(&id) {
            self.answer_discarded_outcome(id, TransferAnswer::Outcome(outcome, None), fx);
            return;
        }
        if matches!(outcome, OutboxOutcome::Discarded) {
            if let Some((_, error)) = self.confirmed_discards.get(&id).cloned() {
                self.answer_discarded_outcome(id, TransferAnswer::Outcome(OutboxOutcome::Rejected { error }, None), fx);
                return;
            }
        }
        if let OutboxOutcome::Failed { error } = &outcome {
            if transfer.outcome_waiters.iter().any(|(waits, _, discard)| *waits == cycle && *discard) {
                self.confirmed_discards.insert(id, (cycle, error.clone()));
                self.discard_failed(id, None, fx);
                return;
            }
        }
        self.confirmed_discards.remove(&id);
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut transfer.outcome_waiters).into_iter().partition(|(waits, _, _)| *waits == cycle);
        transfer.outcome_waiters = rest;
        let outcome = transfer.outcomes[&cycle].clone();
        let row = self.landed_rows.get(&(id, cycle)).cloned();
        for (_, waiter, _) in mine {
            reply(fx, waiter, TransferAnswer::Outcome(outcome.clone(), row.clone()));
        }
    }

    pub(super) fn answer_discarded_outcome(&mut self, id: TransferId, answer: TransferAnswer, fx: &mut Fx) {
        let Some((cycle, _)) = self.confirmed_discards.remove(&id) else { return };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut transfer.outcome_waiters).into_iter().partition(|(waits, _, _)| *waits == cycle);
        transfer.outcome_waiters = rest;
        for (_, waiter, _) in mine { reply(fx, waiter, answer.clone()); }
    }

    /// The transfer ends: `Done`, `uploads[action]` gone, the copy file goes by `DeleteFile`.
    pub(super) fn finish(&mut self, id: TransferId, fx: &mut Fx) {
        self.show(id, Shown::Gone, fx);
        let had_source = self.transfers.get(&id).is_some_and(|transfer| transfer.source.is_some());
        self.set(id, TransferStage::Done);
        if had_source { self.rounds_idle(id, fx); }
    }

    pub(super) fn delete_copy(&self, id: TransferId, fx: &mut Fx) {
        if let Some(transfer) = self.transfers.get(&id) {
            let holder = match &transfer.restore_element {
                Some((_, holder)) => *holder,
                None => Some(transfer.transfer_id),
            };
            if let Some(holder) = holder { run(fx, TransferEffect::DeleteFile { path: file_id(holder) }); }
        }
    }

    pub(super) fn timer(&mut self) -> ScheduleId {
        self.next_timer += 1;
        ScheduleId(self.next_timer)
    }

    pub(super) fn schedule_expire(&mut self, fx: &mut Fx) {
        let id = self.timer();
        fx.push(Effect::Schedule { id, after: std::time::Duration::ZERO, input: TransferInput::Tick { kind: TransferTick::Expire } });
    }

    #[cfg(test)]
    pub fn minted_attempt(&self, upload_id: &str) -> Option<AttemptId> { self.minted.get(upload_id).map(|debt| debt.attempt_id) }

    /// The Store refused the write of a minted `upload_id`: the debt stays, and with `retry` the write goes again after the backoff.
    pub(super) fn owe_minted(&mut self, pending: &Pending, retry: bool, fx: &mut Fx) {
        let Some((upload_id, key, transfer)) = self.debt_of(pending) else { return };
        let attempts = self.minted.get(&upload_id).map_or(1, |debt| debt.attempts + 1);
        let attempt_id = self.next_attempt;
        self.next_attempt += 1;
        if retry {
            fx.push(Effect::Backoff { id: ScheduleId(RETRY_TIMER + attempt_id), policy: RetryPolicy::Upload, attempt: attempts.max(1), input: TransferInput::Tick { kind: TransferTick::Minted { upload_id: upload_id.clone(), attempt_id } } });
        }
        self.minted.insert(upload_id, MintedDebt { transfer, key, attempts, attempt_id });
    }

    pub(super) fn schedule_retry(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.waiting_retry = true;
        transfer.in_flight = false;
        let attempt_id = self.next_attempt;
        self.next_attempt += 1;
        let attempt = transfer.attempts;
        transfer.attempt_id = attempt_id;
        let timer = ScheduleId(RETRY_TIMER + attempt_id);
        fx.push(Effect::Backoff { id: timer, policy: RetryPolicy::Upload, attempt: attempt.max(1), input: TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id } } });
    }

    /// Everything that waits for the call answers `answer`; `Interrupted` for the end of the epoch and for the close.
    pub(super) fn interrupt(&mut self, id: TransferId, cause: crate::ffi::EndCause, fx: &mut Fx) {
        self.release_step(id, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let fault = Fault::Ended(cause.clone());
        let mut replies: Vec<ReplyId> = Vec::new();
        replies.extend(transfer.accept_reply.take());
        replies.extend(transfer.cancel_reply.take().map(|(reply, _)| reply));
        replies.extend(transfer.retry_reply.take());
        replies.extend(transfer.supply_reply.take());
        replies.extend(transfer.staged_failed.take().and_then(|(rep, _)| rep));
        replies.append(&mut transfer.discard_waiters);
        let in_flight = std::mem::replace(&mut transfer.in_flight, false) && transfer.stage != TransferStage::PersistingAttempt;
        let grouped = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
        if transfer.supply_back.is_some() && (transfer.stage == TransferStage::PersistingCopy || (!self.closing && transfer.stage == TransferStage::Copying)) {
            if !self.closing && transfer.stage == TransferStage::Copying { run(fx, TransferEffect::CopyCancel { transfer_id: id }); }
            run(fx, TransferEffect::DeleteFile { path: file_id(id) });
        }
        for reply_id in replies.into_iter().chain(grouped) {
            reply(fx, reply_id, TransferAnswer::Err(fault.clone()));
        }
        if in_flight {
            run(fx, TransferEffect::CancelStep { transfer_id: id });
        }
        self.settle(id, OutboxOutcome::Ended(cause), fx);
        self.set(id, TransferStage::Done);
        self.show(id, Shown::Gone, fx);
    }

    pub(super) fn maybe_drained(&mut self, fx: &mut Fx) {
        let accepting = self.scan().any(|transfer| transfer.stage == TransferStage::Accepting);
        if self.closing && !accepting && !std::mem::replace(&mut self.drained, true) {
            run(fx, TransferEffect::Drained(Drained { owner: "transfer" }));
        }
    }

    fn committed(&mut self, now: Time, (batch, revision, shown): (crate::owners::contract::BatchId, i64, Vec<Option<crate::store::Row>>), fx: &mut Fx) {
        let Some(pending) = self.pending.remove(&batch) else { return };
        if pending.kind == StoreKind::SyncUpload {
            for id in &pending.ids {
                if let Some((old, _)) = self.replacements.remove(id) {
                    if let Some(transfer) = self.transfers.get_mut(&old) { transfer.outcomes.insert(transfer.cycle_id, OutboxOutcome::Discarded); }
                    self.removing_committed(now, &[old], BTreeMap::new(), fx);
                    fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::FinishReplacement { target: ReserveTarget::Transfer(*id), committed: true })));
                }
            }
        }
        for id in &pending.ids {
            let Some(transfer) = self.transfers.get_mut(id) else { continue };
            transfer.revision = revision;
            if matches!(pending.kind, StoreKind::ConfirmedParts | StoreKind::Completed | StoreKind::Failed) { transfer.answer_saved = None; }
        }
        for (id, columns) in &pending.written {
            let transition = pending.kind == StoreKind::Retrying
                || (matches!(pending.kind, StoreKind::Failed | StoreKind::MixedOutcomes) && self.stage_of(*id) == Some(TransferStage::PersistingFailure))
                || (matches!(pending.kind, StoreKind::SyncUpload | StoreKind::Completed) && self.stage_of(*id) == Some(TransferStage::PersistingCopy));
            self.shown_committed(*id, *columns, (pending.kind == StoreKind::Shown, revision), !transition, fx);
        }
        if pending.kind == StoreKind::StagedRestored { return self.staged_restored(pending.ids[0], fx); }
        if matches!(pending.kind, StoreKind::Shown | StoreKind::StagedCleared) { return; }
        if pending.kind == StoreKind::OrphanRemoved {
            if let Some((upload_id, _)) = pending.orphan {
                self.orphans.remove(&upload_id);
                self.orphan_flight.remove(&upload_id);
                self.orphan_owed.remove(&upload_id);
            }
            return self.maybe_drained(fx);
        }
        let writes_id = matches!(pending.kind, StoreKind::SyncUpload | StoreKind::ConfirmedParts | StoreKind::Completed | StoreKind::Failed | StoreKind::Retrying | StoreKind::BatchIntent | StoreKind::MixedOutcomes);
        let transfers = &self.transfers;
        self.minted.retain(|upload_id, debt| {
            let written = debt.transfer.is_some_and(|id| pending.ids.contains(&id) && (matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan | StoreKind::ControlOrphan) || (writes_id && transfers.get(&id).is_some_and(|transfer| transfer.upload_id.as_deref() == Some(upload_id.as_str())))));
            !written && pending.orphan.as_ref().is_none_or(|(minted, _)| minted != upload_id)
        });
        if matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan | StoreKind::ControlOrphan) {
            if let Some((upload_id, key)) = pending.orphan {
                self.orphans.insert(upload_id, key);
                self.send_orphans(false, fx);
            }
            return self.maybe_drained(fx);
        }
        if pending.kind == StoreKind::MixedOutcomes {
            let mut rows = self.shown_rows(&pending.ids, &shown);
            for &id in &pending.ids {
                match self.stage_of(id) {
                    Some(TransferStage::Removing) => self.removing_committed(now, &[id], rows.remove(&id).into_iter().map(|row| (id, row)).collect(), fx),
                    Some(TransferStage::PersistingFailure) => self.failure_committed(id, fx),
                    _ => {}
                }
            }
            self.maybe_drained(fx);
            return;
        }
        let stage = pending.ids.first().and_then(|id| self.stage_of(*id));
        if stage == Some(TransferStage::Accepting) {
            for (id, row) in pending.ids.iter().zip(&shown) {
                if let Some(transfer) = self.transfers.get_mut(id) { transfer.optimistic = row.clone(); }
            }
        }
        if stage == Some(TransferStage::PersistingCopy) {
            for (id, row) in pending.ids.iter().zip(&shown) {
                if let Some(transfer) = self.transfers.get_mut(id).filter(|transfer| transfer.supply_back.is_some()) { transfer.optimistic = row.clone(); }
            }
        }
        match stage {
            Some(TransferStage::Accepting) => self.accepting_committed(&pending.ids, fx),
            Some(TransferStage::PersistingCopy) => self.copy_committed(now, &pending.ids, fx),
            Some(TransferStage::PersistingAttempt) => self.attempt_committed(now, pending.ids[0], fx),
            Some(TransferStage::PersistingBatchIntent) => self.batch_intent_committed(now, &pending.ids, fx),
            Some(TransferStage::PersistingPart) => self.part_committed(now, pending.ids[0], fx),
            Some(TransferStage::PersistingComplete) => self.complete_committed(now, pending.ids[0], fx),
            Some(TransferStage::PersistingRetry) if self.transfers.get(&pending.ids[0]).is_some_and(|transfer| transfer.next_step == NextStep::Mutation && !transfer.intent_live) => self.retry_recreate_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingRetry) if self.transfers.get(&pending.ids[0]).is_some_and(|transfer| transfer.next_step == NextStep::Mutation) => self.retry_mutation_committed(&pending.ids, fx),
            Some(TransferStage::PersistingRetry) => self.retry_committed(now, pending.ids[0], fx),
            Some(TransferStage::PersistingFailure) => {
                for &id in &pending.ids {
                    self.failure_committed(id, fx);
                }
            }
            Some(TransferStage::Removing) => self.removing_committed(now, &pending.ids, self.shown_rows(&pending.ids, &shown), fx),
            _ => {}
        }
        self.maybe_drained(fx);
    }

    fn rejected(&mut self, now: Time, rejected: crate::owners::contract::Rejected, fx: &mut Fx) {
        let Some(pending) = self.pending.remove(&rejected.batch) else { return };
        let (ends, storage) = match &rejected.reason {
            RejectReason::Closed | RejectReason::NoSession => (true, None),
            RejectReason::Storage(detail) => (false, Some(detail.clone())),
            RejectReason::StaleGeneration { .. } | RejectReason::Invalid(_) => (false, None),
        };
        if pending.kind == StoreKind::SyncUpload {
            for id in &pending.ids {
                if let Some((old, _)) = self.replacements.remove(id) {
                    if let Some(transfer) = self.transfers.get_mut(&old) { transfer.stage = TransferStage::Failed; transfer.removal = None; transfer.prior = None; }
                    fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::FinishReplacement { target: ReserveTarget::Transfer(*id), committed: false })));
                    run(fx, TransferEffect::DeleteFile { path: file_id(*id) });
                    if ends { self.interrupt(old, super::outbox_types::end_cause(&rejected.reason), fx); }
                }
            }
        }
        if let (StoreKind::OrphanRemoved, Some((upload_id, _))) = (pending.kind, &pending.orphan) {
            self.orphan_flight.remove(upload_id);
            return;
        }
        if pending.kind == StoreKind::StagedCleared { return; }
        if pending.kind == StoreKind::StagedRestored { return self.staged_restore_rejected(pending.ids[0], rejected.reason, fx); }
        if matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan | StoreKind::ControlOrphan) {
            let minted = self.debt_of(&pending).map(|(upload_id, key, _)| (upload_id, key));
            match &rejected.reason {
                RejectReason::Storage(_) | RejectReason::StaleGeneration { .. } => self.owe_minted(&pending, true, fx),
                RejectReason::Invalid(_) => self.owe_minted(&pending, false, fx),
                RejectReason::Closed | RejectReason::NoSession => {
                    if let (StoreKind::ControlOrphan, Some((upload_id, key))) = (pending.kind, minted.clone()) {
                        let attempt_id = self.next_attempt;
                        self.next_attempt += 1;
                        self.minted.insert(upload_id, MintedDebt { transfer: None, key, attempts: 1, attempt_id });
                    } else if let (false, Some((upload_id, key))) = (pending.kind == StoreKind::ControlOrphan, minted) {
                        self.write_control_orphan(upload_id, key, fx);
                    }
                }
            }
            return;
        }
        if pending.kind == StoreKind::MixedOutcomes && storage.is_some() {
            self.schedule_retry(pending.ids[0], fx);
            return;
        }
        let stage = pending.ids.first().and_then(|id| self.stage_of(*id));
        let persisting = matches!(stage, Some(TransferStage::PersistingCopy | TransferStage::PersistingAttempt | TransferStage::PersistingBatchIntent | TransferStage::PersistingRetry | TransferStage::PersistingPart | TransferStage::PersistingComplete | TransferStage::PersistingFailure | TransferStage::Removing));
        match (&rejected.reason, persisting) {
            (RejectReason::StaleGeneration { generations, .. }, true) => {
                self.generations.extend(generations.clone());
                return self.store_basis(&pending.ids, pending.kind, generations.clone(), fx);
            }
            (RejectReason::Invalid(_), true) => {
                if stage == Some(TransferStage::PersistingCopy) && self.transfers.get(&pending.ids[0]).is_some_and(|transfer| transfer.supply_back.is_some()) {
                    self.source_rejected(pending.ids[0], Fault::Invalid, fx);
                    return;
                }
                if stage == Some(TransferStage::PersistingFailure) {
                    if let Some(rep) = self.transfers.get_mut(&pending.ids[0]).and_then(|transfer| transfer.supply_reply.take()) { reply(fx, rep, TransferAnswer::Err(Fault::Invalid)); }
                }
                if stage == Some(TransferStage::PersistingBatchIntent) {
                    for &id in &pending.ids { self.mark_failure(now, id, "Internal(Invalid)".into(), fx); }
                    self.store(&pending.ids, StoreKind::Failed, fx);
                    return;
                }
                self.invalid_write(now, &pending.ids, stage == Some(TransferStage::PersistingFailure), fx);
                self.start_in_order(now, fx);
                return;
            }
            (RejectReason::StaleGeneration { .. } | RejectReason::Invalid(_), false) | (RejectReason::NoSession | RejectReason::Closed | RejectReason::Storage(_), _) => {}
        }
        match (stage, storage) {
            (Some(TransferStage::Accepting), _) => self.accepting_rejected(&pending.ids, rejected.reason, fx),
            (Some(TransferStage::Removing), _) => self.removing_rejected(now, &pending.ids, rejected.reason, fx),
            (Some(TransferStage::PersistingRetry), Some(_)) => self.retry_rejected(pending.ids[0], rejected.reason, fx),
            (Some(TransferStage::PersistingBatchIntent), Some(_)) => self.schedule_retry(pending.ids[0], fx),
            (Some(TransferStage::PersistingFailure), Some(_)) => self.failure_rejected(pending.ids[0], fx),
            (Some(TransferStage::PersistingCopy), Some(_)) => self.copy_rejected(&pending.ids, rejected.reason, fx),
            (Some(TransferStage::PersistingAttempt | TransferStage::PersistingPart | TransferStage::PersistingComplete), Some(error)) => {
                self.persist_failed(now, pending.ids[0], error, fx);
                self.start_in_order(now, fx);
            }
            (Some(_), None) => {
                for id in pending.ids {
                    self.interrupt(id, super::outbox_types::end_cause(&rejected.reason), fx);
                }
            }
            (None, _) | (Some(_), Some(_)) => {}
        }
        self.maybe_drained(fx);
    }

    /// The prologue of `Online` and `PowerSaving`: the fields first, then the row.
    fn network_changed(&mut self, now: Time, online: Option<bool>, saving: Option<bool>, fx: &mut Fx) {
        let was_online = self.online;
        if online.is_some() {
            self.online = online;
        }
        if let Some(saving) = saving {
            self.saving = saving;
        }
        self.leave_network(fx);
        if online == Some(true) && was_online != Some(true) {
            self.expire_failed(fx);
            self.send_orphans(true, fx);
        }
        self.start_in_order(now, fx);
    }

    fn handle(&mut self, now: Time, input: TransferInput, fx: &mut Fx) {
        if let (Some(error), TransferInput::Accept { reply: id, .. } | TransferInput::AcceptBatch { reply: id, .. } | TransferInput::Cancel { reply: id, .. } | TransferInput::SupplySource { reply: id, .. } | TransferInput::SourceFailed { reply: id, .. } | TransferInput::Discard { reply: id, .. } | TransferInput::Retry { reply: id, .. } | TransferInput::SelectRow { reply: id, .. }) = (&self.queue_error, &input) {
            return reply(fx, *id, TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        match input {
            TransferInput::Accept { epoch, item, reply } => self.accept(now, epoch, vec![*item], None, reply, fx),
            TransferInput::AcceptBatch { epoch, items, joint, reply } => self.accept(now, epoch, items, Some(*joint), reply, fx),
            TransferInput::Restored { .. } | TransferInput::RestoreFailed { .. } | TransferInput::ReservedFloor { .. } => {}
            TransferInput::AbortDone { upload_id, result } => self.abort_done(upload_id, result, fx),
            TransferInput::RegisterOperations { declarations } => self.register(now, declarations, fx),
            TransferInput::RestoreReady { epoch } => self.restore_ready(now, epoch, fx),
            TransferInput::RoundStatus { epoch, transfer_ids, refused } => self.round_status(epoch, &transfer_ids, refused, fx),
            TransferInput::CopyDone { transfer_id, attempt_id, size, sha256, place, cached } => self.copy_done(transfer_id, attempt_id, CopiedBlob { size, sha256, place, cached }, fx),
            TransferInput::CopyFailed { transfer_id, attempt_id, error } => self.copy_failed(transfer_id, attempt_id, error, fx),
            TransferInput::StagedPromoted { transfer_id } => self.staged_promoted(now, transfer_id, fx),
            TransferInput::StepAnswer { request_id, outcome } => self.held_step_answered(now, request_id, outcome, fx),
            TransferInput::StagedPromoteFailed { transfer_id, detail } => self.staged_promote_failed(transfer_id, detail, fx),
            TransferInput::OpReserved { target, result } => self.op_reserved(target, result, fx),
            TransferInput::ReplacementReserved { target, result } => {
                let (result, operation) = match result { Ok((id, operation)) => (Ok(id), operation), Err(fault) => (Err(fault), None) };
                if let ReserveTarget::Transfer(id) = target { if let Some(entry) = self.replacements.get_mut(&id) { entry.1 = operation; } }
                self.op_reserved(target, result, fx);
            }
            TransferInput::StepOk { transfer_id, attempt_id, part, upload_id, etag } => self.step_ok(now, transfer_id, attempt_id, part, upload_id, etag, fx),
            TransferInput::UploadMinted { transfer_id, upload_id, key, .. } => self.upload_minted(transfer_id, upload_id, key, fx),
            TransferInput::StepError { transfer_id, attempt_id, temporary, error } => self.step_error(now, transfer_id, attempt_id, (temporary, error), fx),
            TransferInput::Completed { transfer_id, attempt_id, signed_id } => self.completed(now, transfer_id, attempt_id, signed_id, fx),
            TransferInput::CompleteError { transfer_id, attempt_id, temporary, error } => self.complete_error(now, transfer_id, attempt_id, (temporary, error), fx),
            TransferInput::MutationOutcome { transfer_id, cycle_id, outcome, intent_removed: _, row_never_created } => {
                if let Some((row, outcomes)) = self.waiting.get_mut(&transfer_id) {
                    if row.cycle_id.max(1) == cycle_id { outcomes.entry(transfer_id).or_default().insert(cycle_id, outcome); }
                } else if self.transfers.get(&transfer_id).is_some_and(|transfer| transfer.cycle_id == cycle_id) {
                    self.mutation_outcome(now, transfer_id, (outcome, row_never_created), fx);
                }
            }
            TransferInput::MutationOutcomes { batch_id, members, intent_removed } => {
                let outcomes = members.into_iter().filter_map(|(id, cycle, outcome)| {
                    if let Some((row, saved)) = self.waiting.get_mut(&id) {
                        if row.cycle_id.max(1) == cycle { saved.entry(id).or_default().insert(cycle, outcome); }
                        None
                    } else {
                        self.transfers.get(&id).is_some_and(|transfer| transfer.cycle_id == cycle).then_some((id, outcome))
                    }
                }).collect();
                self.finish_batch(now, batch_id, outcomes, intent_removed, fx);
            }
            TransferInput::RetryOutcome { cycles, result } => self.retry_mutation_outcome(&cycles, result, fx),
            TransferInput::DiscardOutcome { key, result } => self.discard_outcome(&key, result, fx),
            TransferInput::Outcome { receipt, reply, discard_on_failure } => self.outcome(receipt, reply, discard_on_failure, fx),
            TransferInput::StatusInterestPlus { key, stream_id } => self.interest_plus(key, stream_id, fx),
            TransferInput::StatusInterestMinus { key, stream_id } => self.interest_minus(&key, stream_id),
            TransferInput::Committed(committed) => self.committed(now, (committed.batch, committed.revision, committed.shown), fx),
            TransferInput::Rejected(rejected) => self.rejected(now, rejected, fx),
            TransferInput::Cancel { key, reply } => self.cancel(now, &key, reply, true, fx),
            TransferInput::SupplySource { epoch, key, source, reply } => self.supply_source(epoch, &key, source, reply, fx),
            TransferInput::SourceFailed { epoch, key, message, reply } => self.source_failed(now, epoch, &key, message, reply, fx),
            TransferInput::Discard { key, reply } => self.cancel(now, &key, reply, false, fx),
            TransferInput::SelectRow { model, rows, outbox_min, kind, reply, version } => self.select_row((model, rows), outbox_min, kind, reply, version, fx),
            TransferInput::SelectConfirmed { reply: reply_id } => {
                if let Some((id, kind)) = self.selecting.remove(&reply_id) {
                    if let Some(key) = self.transfers.get(&id).map(|transfer| transfer.key.clone()) {
                        match kind { super::outbox_types::SelectKind::Retry => self.retry(&key, None, reply_id, fx), super::outbox_types::SelectKind::Discard => self.cancel(now, &key, reply_id, false, fx) }
                    } else {
                        reply(fx, reply_id, TransferAnswer::Err(Fault::NotFound));
                    }
                }
            }
            TransferInput::Retry { key, source, reply } => self.retry(&key, source, reply, fx),
            TransferInput::SlotFreed => self.start_in_order(now, fx),
            TransferInput::Online(Online(on)) => self.network_changed(now, Some(on), None, fx),
            TransferInput::PowerSaving(saving) => self.network_changed(now, None, Some(saving), fx),
            TransferInput::EpochStarted(started) => { self.epoch = started.epoch; self.base_generation = started.generation; },
            TransferInput::Tick { kind, .. } => self.tick(now, kind, fx),
            TransferInput::EpochEnded(ended) => self.epoch_ended(ended.cause, fx),
            TransferInput::Closing => self.close(fx),
        }
    }

    /// `Restoring`: the calls on the keys of the base wait for `Restored`, the rest of the table works as in the other phases.
    fn restoring(&mut self, now: Time, input: TransferInput, fx: &mut Fx) {
        match input {
            TransferInput::ReservedFloor { transfer_id } => self.next_id = self.next_id.max(transfer_id.saturating_add(1)),
            TransferInput::Restored { uploads, outcomes, orphans, batches, held_orphan } if !self.ended => {
                let owned: std::collections::BTreeSet<&String> = uploads.iter().filter_map(|row| row.upload_id.as_ref()).collect();
                let (live, orphans): (Vec<_>, Vec<_>) = orphans.into_iter().partition(|(upload_id, _)| owned.contains(upload_id));
                self.commit_ops(live.into_iter().map(|(upload_id, _)| StoreOp::RemoveOrphan { upload_id }).collect(), fx);
                self.orphans.extend(orphans);
                self.orphan_flight.extend(held_orphan.filter(|upload_id| self.orphans.contains_key(upload_id)));
                self.restore_groups(batches);
                self.restored(now, uploads, outcomes, fx)
            }
            TransferInput::RestoreFailed { error } if !self.ended => {
                self.queue_error = Some(error);
                self.phase = Phase::Classified;
                run(fx, TransferEffect::RestoreClassified { epoch: self.epoch });
                self.replay_held(now, fx);
            }
            held @ (TransferInput::Accept { .. } | TransferInput::AcceptBatch { .. }) if Self::in_epoch(&held, self.epoch) => self.held.push(held),
            held @ (TransferInput::Cancel { .. } | TransferInput::SupplySource { .. } | TransferInput::SourceFailed { .. } | TransferInput::Retry { .. } | TransferInput::Discard { .. } | TransferInput::SelectRow { .. } | TransferInput::Outcome { .. } | TransferInput::MutationOutcome { .. } | TransferInput::MutationOutcomes { .. }) => self.held.push(held),
            TransferInput::EpochStarted(started) => {
                self.epoch = started.epoch;
                self.base_generation = started.generation;
                run(fx, TransferEffect::Started { epoch: started.epoch });
                run(fx, TransferEffect::SnapshotRequest { epoch: started.epoch });
            }
            TransferInput::Closing => {
                self.answer_held(Fault::Closed, fx);
                self.close(fx);
            }
            TransferInput::EpochEnded(ended) => {
                self.ended = true;
                self.epoch_ended(ended.cause, fx);
            }
            other => self.handle(now, other, fx),
        }
    }

    /// A new epoch starts a new instance of the table: the declarations of the operations and the counters of ids stay, the rest of the state of the older epoch goes.
    fn begin_epoch(&mut self, epoch: Epoch) {
        let fresh = Self::new(epoch, self.slots, self.peers);
        let kept = Self { abort_root: std::mem::take(&mut self.abort_root), orphans: std::mem::take(&mut self.orphans), minted: std::mem::take(&mut self.minted), pending: std::mem::take(&mut self.pending), declarations: std::mem::take(&mut self.declarations), next_id: self.next_id, next_seq: self.next_seq, next_group: self.next_group, next_batch: self.next_batch, next_attempt: self.next_attempt, next_timer: self.next_timer, put_timeout: self.put_timeout, ..fresh };
        *self = kept;
    }

    fn in_epoch(input: &TransferInput, epoch: Epoch) -> bool { matches!(input, TransferInput::Accept { epoch: found, .. } | TransferInput::AcceptBatch { epoch: found, .. } if *found == epoch) }

    /// `Restored`: the accepts replay, the other calls on the keys of the base wait for `RestoreReady`.
    pub(super) fn replay_held_accepts(&mut self, now: Time, fx: &mut Fx) {
        let (accepts, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.held).into_iter().partition(|input| matches!(input, TransferInput::Accept { .. } | TransferInput::AcceptBatch { .. }));
        self.held = rest;
        for input in accepts { self.handle(now, input, fx); }
    }

    pub(super) fn replay_held(&mut self, now: Time, fx: &mut Fx) {
        let held = std::mem::take(&mut self.held);
        let any = !held.is_empty();
        self.starts_held = true;
        for input in held {
            self.handle(now, input, fx);
        }
        self.starts_held = false;
        if any { self.start_in_order(now, fx); }
    }

    /// Every held call gets exactly one answer.
    pub(super) fn answer_held(&mut self, fault: Fault, fx: &mut Fx) {
        for input in std::mem::take(&mut self.held) {
            match input {
                TransferInput::Accept { reply: id, .. } | TransferInput::AcceptBatch { reply: id, .. } | TransferInput::Cancel { reply: id, .. } | TransferInput::SupplySource { reply: id, .. } | TransferInput::SourceFailed { reply: id, .. } | TransferInput::Retry { reply: id, .. } | TransferInput::Discard { reply: id, .. } | TransferInput::SelectRow { reply: id, .. } => reply(fx, id, TransferAnswer::Err(fault.clone())),
                TransferInput::Outcome { reply: id, .. } => {
                    let cause = if fault == Fault::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
                    reply(fx, id, TransferAnswer::Outcome(OutboxOutcome::Ended(cause), None))
                }
                _ => {}
            }
        }
    }

    /// The final mutation goes to Outbox as an operation of the origin `Transfer{key}`.
    pub(super) fn submit_mutation(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.submitted = true;
        let signed = transfer.signed_id.clone().unwrap_or_default();
        let mut op = NewOp { origin: OpSource::Transfer { transfer_id: transfer.transfer_id, cycle_id: transfer.cycle_id }, body: transfer.mutation.body.replace(SIGNED_ID_SENTINEL, &signed), ..transfer.mutation.clone() };
        let input = match transfer.op_id {
            Some(op_id) => {
                op.effects = Default::default();
                OutboxInput::SubmitReserved { epoch: self.epoch, op_id, op, created_at: transfer.created_at.0 }
            }
            None => OutboxInput::Submit { epoch: self.epoch, op, reply: NO_REPLY },
        };
        fx.push(Effect::Send(Outbound::new(self.peers.outbox, input)));
    }
}

pub struct Transfers;

impl StateOwner for Transfers {
    const EPOCHED: bool = true;

    type State = TransferState;
    type Input = TransferInput;
    type Effect = TransferEffect;

    fn step(state: &mut TransferState, input: TransferInput, now: Time) -> Effects<Self> {
        let mut fx: Fx = Vec::new();
        if let TransferInput::EpochStarted(started) = &input {
            state.begin_epoch(started.epoch);
        }
        if state.phase == Phase::Restoring {
            state.restoring(now, input, &mut fx);
        } else {
            state.handle(now, input, &mut fx);
        }
        fx
    }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> TransferInput { TransferInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> TransferInput { TransferInput::Closing }
}

pub(super) fn upload_kind(kind: StoreKind) -> UploadKind {
    match kind {
        StoreKind::SyncUpload | StoreKind::UploadId | StoreKind::LateOrphan | StoreKind::ControlOrphan | StoreKind::OrphanRemoved | StoreKind::Removed | StoreKind::BatchIntent | StoreKind::MixedOutcomes | StoreKind::Shown | StoreKind::StagedCleared => UploadKind::SyncUpload,
        StoreKind::StagedRestored => UploadKind::Failed,
        StoreKind::ConfirmedParts => UploadKind::ConfirmedParts,
        StoreKind::Completed => UploadKind::Completed,
        StoreKind::Failed => UploadKind::Failed,
        StoreKind::Retrying => UploadKind::Retrying,
    }
}
