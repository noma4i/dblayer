//! Owner `Transfer`: the opening: `Restored`, `RegisterOperations`, `RestoreReady` and the class of every restored row.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::OutboxOutcome;
use super::outbox_types::Phase;
use super::transfer::{run, Fx, TransferState};
use super::transfer_types::*;

impl TransferState {
    fn insert_restored(&mut self, row: &StoredTransfer, stage: TransferStage) -> TransferId {
        let item = BatchItem { key: row.key.clone(), fields: row.fields.clone(), urgent: false, source: row.source.clone(), mutation: row.mutation.clone(), steps: row.steps.clone() };
        let id = row.transfer_id;
        self.next_id = self.next_id.max(id + 1);
        let created = self.create(item, row.batch.filter(|group| !row.completed && self.groups.contains_key(group)), row.created_at, stage);
        if let Some(mut transfer) = self.transfers.remove(&created) {
            transfer.transfer_id = id;
            transfer.seq = row.seq;
            transfer.restored = true;
            transfer.upload_id = row.upload_id.clone();
            transfer.confirmed_parts = row.confirmed_parts;
            transfer.attempts = row.attempts;
            transfer.etags = row.etags.clone();
            transfer.signed_id = row.signed_id.clone();
            transfer.aborted = row.aborted;
            transfer.op_id = row.op_id;
            transfer.submitted = row.completed;
            transfer.place = Some(row.place);
            transfer.size = row.size;
            transfer.sha256 = row.sha256.clone();
            transfer.cycle_id = row.cycle_id.max(1);
            transfer.next_step = if row.confirmed_parts >= row.source.parts { NextStep::Complete } else { NextStep::Part(row.confirmed_parts + 1) };
            let group = transfer.batch_id;
            self.transfers.insert(id, transfer);
            if let Some(group) = group {
                self.rejoin(id, group);
            }
        }
        self.next_seq = self.next_seq.max(row.seq + 1);
        id
    }

    /// The element is in the group once, in the order of `seq`.
    fn rejoin(&mut self, id: TransferId, group: u64) {
        let seqs: BTreeMap<TransferId, u64> = self.transfers.values().map(|transfer| (transfer.transfer_id, transfer.seq)).collect();
        if let Some(entry) = self.groups.get_mut(&group) {
            entry.ids.retain(|found| *found != id);
            entry.ids.push(id);
            entry.ids.sort_by_key(|found| seqs.get(found).copied().unwrap_or(u64::MAX));
        }
    }

    pub(super) fn restore_groups(&mut self, batches: BTreeMap<u64, Joint>) {
        for (group, joint) in batches {
            self.next_group = self.next_group.max(group + 1);
            self.groups.insert(group, Group { ids: Vec::new(), reply: None, ready: BTreeSet::new(), joint, phase: BatchPhase::Collecting, op_row: None });
        }
    }

    fn failed_restored(&mut self, id: TransferId, error: String, fx: &mut Fx) {
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = TransferStage::Failed;
            transfer.error = Some(error.clone());
        }
        self.settle(id, OutboxOutcome::Failed { error: error.clone() }, fx);
        self.show(id, Shown::Failed { error }, fx);
        self.schedule_expire(fx);
    }

    /// Every restored row has exactly one class.
    pub(super) fn classify(&mut self, row: StoredTransfer, outcomes: &BTreeMap<TransferId, OutboxOutcome>, fx: &mut Fx) {
        if let Some(error) = row.restore_failed.clone() {
            let id = self.insert_restored(&row, TransferStage::Failed);
            let _ = error;
            return self.failed_restored(id, "RestoreFailed".into(), fx);
        }
        if !self.declarations.contains(&(row.mutation.model.clone(), row.mutation.action.clone())) {
            let id = self.insert_restored(&row, TransferStage::WaitingDeclaration);
            self.waiting.insert(id, (row, outcomes.clone()));
            return;
        }
        self.classify_declared(row, outcomes, fx);
    }

    fn classify_declared(&mut self, row: StoredTransfer, outcomes: &BTreeMap<TransferId, OutboxOutcome>, fx: &mut Fx) {
        let id = self.insert_restored(&row, TransferStage::Ready);
        if row.copying {
            return self.restore_copy(id, &row, fx);
        }
        if row.completed {
            return self.restore_completed(id, &row, outcomes.get(&id), fx);
        }
        if let Some(error) = row.failed.clone() {
            return self.failed_restored(id, error, fx);
        }
        if !row.file_present {
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.stage = TransferStage::PersistingFailure;
            }
            return self.begin_failure(id, "missing_file".into(), false, fx);
        }
        let shown = if row.confirmed_parts >= row.source.parts { Shown::Uploading { progress: 100 } } else { Shown::Queued };
        self.show(id, shown, fx);
        self.rounds_needed(vec![id], fx);
        self.schedule_expire(fx);
    }

    fn restore_copy(&mut self, id: TransferId, row: &StoredTransfer, fx: &mut Fx) {
        if !row.file_present {
            return self.begin_failure(id, "missing_file".into(), false, fx);
        }
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Copying;
        transfer.attempt_id = attempt_id;
        let effect = TransferEffect::Copy { transfer_id: id, attempt_id, key: transfer.key.clone(), source: transfer.source.clone() };
        self.show(id, Shown::Copying, fx);
        run(fx, effect);
        self.rounds_needed(vec![id], fx);
    }

    fn restore_completed(&mut self, id: TransferId, row: &StoredTransfer, saved: Option<&OutboxOutcome>, fx: &mut Fx) {
        match saved {
            Some(OutboxOutcome::Confirmed { revision, result }) => {
                self.remove_with(id, Removal::Confirmed { revision: *revision, result: result.clone() });
                self.store(&[id], StoreKind::Removed, fx);
            }
            Some(OutboxOutcome::Discarded) => {
                self.remove_with(id, Removal::Discarded);
                self.store(&[id], StoreKind::Removed, fx);
            }
            Some(OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error }) => {
                self.set(id, TransferStage::PersistingFailure);
                self.begin_failure(id, error.clone(), false, fx);
            }
            Some(OutboxOutcome::Interrupted) | None => {
                let group = row.batch.filter(|group| self.open_groups.contains(group) && self.groups.contains_key(group));
                if let Some(group) = group {
                    if let Some(transfer) = self.transfers.get_mut(&id) {
                        transfer.batch_id = Some(group);
                        transfer.stage = TransferStage::BatchReady;
                    }
                    self.rejoin(id, group);
                    return self.show(id, Shown::Uploading { progress: 100 }, fx);
                }
                self.set(id, TransferStage::Mutating);
                self.show(id, Shown::Uploading { progress: 100 }, fx);
                if !row.has_intent {
                    self.submit_mutation(id, fx);
                }
                self.rounds_needed(vec![id], fx);
            }
        }
    }

    pub(super) fn restored(&mut self, mut uploads: Vec<StoredTransfer>, outcomes: BTreeMap<TransferId, OutboxOutcome>, fx: &mut Fx) {
        uploads.sort_by_key(|row| row.seq);
        self.open_groups = uploads.iter().filter(|row| !row.completed).filter_map(|row| row.batch).collect();
        for row in uploads {
            self.classify(row, &outcomes, fx);
        }
        self.phase = Phase::Classified;
        run(fx, TransferEffect::RestoreClassified { epoch: self.epoch });
        self.replay_held(fx);
    }

    /// A declaration arrives: a transfer that waits for it is classified again.
    pub(super) fn register(&mut self, declarations: Vec<(String, String)>, fx: &mut Fx) {
        self.declarations.extend(declarations);
        if self.phase == Phase::Restoring {
            return;
        }
        let ready: Vec<TransferId> = self.waiting.iter().filter(|(_, (row, _))| self.declarations.contains(&(row.mutation.model.clone(), row.mutation.action.clone()))).map(|(id, _)| *id).collect();
        for id in ready {
            if let Some((row, outcomes)) = self.waiting.remove(&id) {
                self.transfers.remove(&id);
                for entry in self.groups.values_mut() {
                    entry.ids.retain(|found| *found != id);
                }
                self.classify_declared(row, &outcomes, fx);
            }
        }
    }

    /// `RestoreReady`: the network start of the restored transfers goes in the order of `seq`.
    pub(super) fn restore_ready(&mut self, epoch: crate::runtime::Epoch, fx: &mut Fx) {
        if epoch != self.epoch || self.phase != Phase::Classified {
            return;
        }
        self.phase = Phase::Ready;
        self.send_orphans(fx);
        self.start_in_order(fx);
    }
}
