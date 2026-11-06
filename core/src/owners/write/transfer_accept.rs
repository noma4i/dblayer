//! Owner `Transfer`: the acceptance of a file (`Accept`, `AcceptBatch`), the copy of the source and the common write of the acceptance.

use std::collections::BTreeSet;

use super::outbox::{Fault, OutboxInput};
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::files::Location;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::runtime::{Effect, Epoch, Outbound, Time};

impl TransferState {
    pub(super) fn create(&mut self, item: BatchItem, batch_id: Option<u64>, now: Time, stage: TransferStage) -> TransferId {
        let (transfer_id, seq) = (self.next_id, self.next_seq);
        self.next_id += 1;
        self.next_seq += 1;
        let BatchItem { key, urgent, source, mutation, steps } = item;
        let transfer = Transfer {
            transfer_id,
            key,
            seq,
            source,
            batch_id,
            epoch: self.epoch,
            stage,
            attempt_id: 0,
            attempts: 0,
            in_flight: false,
            waiting_retry: false,
            retry_due: false,
            next_step: NextStep::Part(1),
            upload_id: None,
            confirmed_parts: 0,
            etags: Vec::new(),
            last_part: false,
            aborted: false,
            signed_id: None,
            created_at: now,
            error: None,
            cycle_id: 1,
            place: None,
            size: 0,
            sha256: String::new(),
            mutation,
            steps,
            restored: false,
            urgent,
            op_id: None,
            submitted: false,
            round_error: None,
            removal: None,
            prior: None,
            failure_abort: false,
            outcomes: Default::default(),
            outcome_waiters: Vec::new(),
            accept_reply: None,
            cancel_reply: None,
            retry_reply: None,
            discard_waiters: Vec::new(),
            discard_sent: false,
            new_source: None,
            closing: false,
        };
        self.transfers.insert(transfer_id, transfer);
        transfer_id
    }

    fn valid(item: &BatchItem) -> bool { !item.source.path.is_empty() && item.source.parts > 0 }

    /// `Accept` and `AcceptBatch`: every element is checked before the transaction, the rows of all N elements go in one batch.
    pub(super) fn accept(&mut self, epoch: Epoch, mut items: Vec<BatchItem>, batch: bool, rep: ReplyId, now: Time, fx: &mut Fx) {
        if epoch != self.epoch || self.closing || self.ended {
            return reply(fx, rep, TransferAnswer::Err(if self.closing { Fault::Interrupted } else { Fault::NoSession }));
        }
        if items.is_empty() || !items.iter().all(Self::valid) {
            return reply(fx, rep, TransferAnswer::Err(Fault::Invalid));
        }
        self.now = self.now.max(now);
        let group = batch.then(|| {
            self.next_group += 1;
            self.next_group - 1
        });
        if items.iter().any(|item| self.open_id(&item.key).is_some()) {
            return reply(fx, rep, TransferAnswer::Err(Fault::InFlight));
        }
        if !batch && !items[0].mutation.effects.is_empty() {
            let item = items.remove(0);
            let id = self.create(item, None, now, TransferStage::Reserving);
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.accept_reply = Some(rep);
            }
            fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::ReserveOp { epoch: self.epoch, transfer_id: id })));
            return;
        }
        let ids: Vec<TransferId> = items.into_iter().map(|item| self.create(item, group, now, TransferStage::Accepting)).collect();
        match group {
            Some(group) => {
                self.groups.insert(group, Group { ids: ids.clone(), reply: Some(rep), ready: BTreeSet::new() });
            }
            None => {
                if let Some(transfer) = self.transfers.get_mut(&ids[0]) {
                    transfer.accept_reply = Some(rep);
                }
            }
        }
        self.store(&ids, StoreKind::SyncUpload, fx);
    }

    /// `Reserving x OpReserved`: the batch of the acceptance writes the optimistic effects under the reserved `op_id`. An answer of a transfer that is not `Reserving` is foreign.
    pub(super) fn op_reserved(&mut self, id: TransferId, result: Result<u64, Fault>, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id).filter(|transfer| transfer.stage == TransferStage::Reserving) else { return };
        match result {
            Ok(op_id) => {
                transfer.op_id = Some(op_id);
                transfer.stage = TransferStage::Accepting;
                self.store(&[id], StoreKind::SyncUpload, fx);
            }
            Err(fault) => {
                transfer.stage = TransferStage::Done;
                if let Some(rep) = transfer.accept_reply.take() {
                    reply(fx, rep, TransferAnswer::Err(fault));
                }
            }
        }
    }

    /// `Accepting x Committed`: the copy starts, the round is asked for.
    pub(super) fn accepting_committed(&mut self, ids: &[TransferId], fx: &mut Fx) {
        for &id in ids {
            if self.transfers.get(&id).is_some_and(|transfer| transfer.closing) {
                self.interrupt(id, fx);
                continue;
            }
            let attempt_id = self.attempt();
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            transfer.stage = TransferStage::Copying;
            transfer.attempt_id = attempt_id;
            let effect = TransferEffect::Copy { transfer_id: id, attempt_id, key: transfer.key.clone(), source: transfer.source.clone() };
            self.show(id, Shown::Copying, fx);
            run(fx, effect);
        }
        let live: Vec<TransferId> = ids.iter().copied().filter(|id| self.stage_of(*id) == Some(TransferStage::Copying)).collect();
        self.rounds_needed(live, fx);
    }

    /// `Accepting x Rejected`: no receipt, nothing is accepted. A retry with a new source goes back to `Failed`.
    pub(super) fn accepting_rejected(&mut self, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        let fault = if matches!(reason, RejectReason::Storage(_)) { Fault::Rejected(reason) } else if self.closing { Fault::Interrupted } else { Fault::Rejected(reason) };
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            let group_reply = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
            if let Some(rep) = transfer.accept_reply.take().or(group_reply) {
                reply(fx, rep, TransferAnswer::Err(fault.clone()));
            }
            if transfer.prior.take() == Some(TransferStage::Failed) {
                transfer.stage = TransferStage::Failed;
                continue;
            }
            transfer.stage = TransferStage::Done;
        }
    }

    fn group_of(&self, id: TransferId) -> Option<u64> { self.transfers.get(&id).and_then(|transfer| transfer.batch_id) }

    pub(super) fn copy_done(&mut self, id: TransferId, attempt_id: AttemptId, size: u64, sha256: String, place: Location, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.stage != TransferStage::Copying || transfer.attempt_id != attempt_id {
            return;
        }
        transfer.size = size;
        transfer.sha256 = sha256;
        transfer.place = Some(place);
        let Some(group) = self.group_of(id) else {
            self.set(id, TransferStage::PersistingCopy);
            return self.store(&[id], StoreKind::SyncUpload, fx);
        };
        let Some(entry) = self.groups.get_mut(&group) else { return };
        entry.ready.insert(id);
        if entry.ready.len() < entry.ids.len() {
            return self.set(id, TransferStage::CopyReady);
        }
        let ids = entry.ids.clone();
        for &member in &ids {
            self.set(member, TransferStage::PersistingCopy);
        }
        self.store(&ids, StoreKind::SyncUpload, fx);
    }

    /// `CopyFailed` of the current attempt: a call that waits gets the error and the metadata goes, a restored transfer is `Failed`.
    pub(super) fn copy_failed(&mut self, id: TransferId, attempt_id: AttemptId, error: String, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        if transfer.stage != TransferStage::Copying || transfer.attempt_id != attempt_id {
            return;
        }
        let ids = match self.group_of(id) {
            Some(group) => self.groups.get(&group).map(|group| group.ids.clone()).unwrap_or_default(),
            None => vec![id],
        };
        let waiting = ids.len() > 1 || self.transfers[&id].accept_reply.is_some();
        if !waiting {
            self.set(id, TransferStage::PersistingFailure);
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.error = Some(error);
                transfer.failure_abort = false;
            }
            return self.store(&[id], StoreKind::Failed, fx);
        }
        for &member in &ids {
            if member != id && self.stage_of(member) == Some(TransferStage::Copying) {
                run(fx, TransferEffect::CopyCancel { transfer_id: member });
            }
            self.remove_with(member, Removal::CopyFailed { error: error.clone() });
        }
        self.store(&ids, StoreKind::Removed, fx);
    }

    /// The transfer waits in `Removing` for the batch of its removal.
    pub(super) fn remove_with(&mut self, id: TransferId, removal: Removal) {
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = TransferStage::Removing;
            transfer.removal = Some(removal);
        }
    }

    /// `PersistingCopy x Committed`: the receipt, the transfer is `Ready`.
    pub(super) fn copy_committed(&mut self, ids: &[TransferId], fx: &mut Fx) {
        let mut receipts = Vec::new();
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            if transfer.prior.take() == Some(TransferStage::Failed) {
                transfer.cycle_id += 1;
            }
            transfer.stage = TransferStage::Ready;
            transfer.next_step = NextStep::Part(1);
            transfer.confirmed_parts = 0;
            transfer.upload_id = None;
            transfer.error = None;
            transfer.etags.clear();
            receipts.push(self.receipt(id));
            self.show(id, Shown::Queued, fx);
        }
        for (position, &id) in ids.iter().enumerate() {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            if let Some(rep) = transfer.accept_reply.take() {
                reply(fx, rep, TransferAnswer::Receipt(receipts[position].clone()));
            }
        }
        if let Some(group) = ids.first().and_then(|id| self.group_of(*id)) {
            if let Some(rep) = self.groups.get_mut(&group).and_then(|group| group.reply.take()) {
                reply(fx, rep, TransferAnswer::Receipts(receipts));
            }
        }
        self.rounds_needed(ids.to_vec(), fx);
        self.start_in_order(fx);
    }

    /// `PersistingCopy x Rejected(Storage)`: the copy is not durable, the whole batch is removed.
    pub(super) fn copy_rejected(&mut self, ids: &[TransferId], fx: &mut Fx) {
        for &id in ids {
            self.remove_with(id, Removal::Storage);
        }
        self.store(ids, StoreKind::Removed, fx);
    }
}
