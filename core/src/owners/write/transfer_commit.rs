//! Owner `Transfer`: a file accepted before its row (D-372): `Commit` gives the transfer its row and its final mutation, the opening discards what no `Commit` reached.

use super::outbox::Fault;
use super::transfer::{reply, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::runtime::{Epoch, Time};

impl TransferState {
    /// `Commit`: one Store batch writes the row, its effects and the body of the final mutation, whatever stage the file is in.
    pub(super) fn commit(&mut self, now: Time, epoch: Epoch, key: &str, request: CommitRequest, rep: ReplyId, fx: &mut Fx) {
        if self.closing || self.ended { return reply(fx, rep, TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))); }
        if epoch != self.epoch { return reply(fx, rep, TransferAnswer::Err(Fault::NoSession)); }
        let Some(id) = self.open_id(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.stage == TransferStage::Removing { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)); }
        if !transfer.held || transfer.commit_reply.is_some() || transfer.commands.iter().any(|command| matches!(command, Waiting::Commit(..))) { return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)); }
        let open = matches!(transfer.stage, TransferStage::Preparing | TransferStage::Measuring | TransferStage::PersistingPrepared | TransferStage::Ready | TransferStage::PersistingAttempt | TransferStage::Uploading | TransferStage::PersistingPart | TransferStage::Held | TransferStage::Failed);
        let restart = now.0 >= transfer.created_at.0.saturating_add(EXPIRY_MS);
        if Self::defers_removal(transfer.stage, transfer.batch_id.is_some(), transfer.supply_back.is_some(), false) || (restart && transfer.stage == TransferStage::Uploading) {
            transfer.commands.push(Waiting::Commit(epoch, request, rep));
            self.removal_pending.insert(id);
            return;
        }
        if !open { return reply(fx, rep, TransferAnswer::Err(Fault::InFlight)); }
        if request.refused { return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)); }
        let Some(body) = merged_body(&transfer.mutation.body, &request.variables, request.wrap.as_deref()) else { return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)) };
        transfer.commit = Some(CommitPlan { effects: request.effects, body, restart, stage: transfer.stage });
        transfer.commit_reply = Some(rep);
        if transfer.stage == TransferStage::Held || (restart && transfer.stage == TransferStage::Ready) { transfer.stage = TransferStage::PersistingCommit; }
        self.store(&[id], StoreKind::Commit, fx);
    }

    /// `Commit x Committed`: the row is shown, `complete` goes by the usual rows when the parts are all on the server.
    pub(super) fn commit_committed(&mut self, now: Time, id: TransferId, shown: &[Option<crate::store::Row>], fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(plan) = transfer.commit.take() else { return };
        let orphan = if plan.restart { transfer.upload_id.take().map(|upload_id| (upload_id, transfer.key.clone())) } else { None };
        if plan.restart {
            transfer.cycle_id += 1;
            transfer.confirmed_parts = 0;
            transfer.etags.clear();
            transfer.signed_id = None;
            transfer.aborted = false;
            transfer.blob_created_at = None;
            transfer.next_step = NextStep::Part(1);
            transfer.attempts = 0;
        }
        transfer.held = false;
        transfer.has_operation = !plan.effects.is_empty();
        transfer.mutation.body = plan.body;
        transfer.optimistic = shown.first().cloned().flatten();
        let rep = transfer.commit_reply.take();
        let resumed = matches!(transfer.stage, TransferStage::Held | TransferStage::PersistingCommit);
        if resumed { transfer.stage = TransferStage::Ready; if !plan.restart { transfer.next_step = NextStep::Complete; } }
        if let Some((upload_id, key)) = orphan { self.orphans.insert(upload_id, key); self.send_orphans(false, fx); }
        let receipt = self.receipt(id);
        if let Some(rep) = rep { reply(fx, rep, TransferAnswer::Receipt(receipt)); }
        if resumed {
            self.show_queued(id, fx);
            self.rounds_needed(vec![id], fx);
        }
        self.start_in_order(now, fx);
    }

    /// `Commit x Rejected`: the transfer stays as it was, held, and the call learns the refusal.
    pub(super) fn commit_rejected(&mut self, now: Time, id: TransferId, reason: RejectReason, fx: &mut Fx) {
        if let RejectReason::Closed | RejectReason::NoSession = reason {
            return self.interrupt(id, super::outbox_types::end_cause(&reason), fx);
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let prior = transfer.commit.take().map(|plan| plan.stage);
        if transfer.stage == TransferStage::PersistingCommit { transfer.stage = prior.unwrap_or(TransferStage::Held); }
        if let Some(rep) = transfer.commit_reply.take() { reply(fx, rep, TransferAnswer::Err(Fault::of_reject(reason))); }
        self.start_in_order(now, fx);
    }

    /// The opening finds a held row: the editor that began it died with the process, the transfer goes like a `discard`.
    pub(super) fn discard_held(&mut self, id: TransferId, fx: &mut Fx) {
        self.remove_with(id, Removal::Discarded, fx);
        self.store(&[id], StoreKind::Removed, fx);
    }
}
