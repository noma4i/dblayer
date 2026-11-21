//! Owner `Transfer`: the prepare step of a source (D-364): the slot, the run, the measure of the result and the swap of the copy for it.

use std::time::Duration;

use super::failure;
use super::outbox_types::Phase;
use super::transfer::{run, Fx, TransferState};
use super::transfer_types::*;
use crate::ffi::PrepareOutcome;
use crate::runtime::{Effect, Time};
use crate::value::sync::PrepareReason;

impl TransferState {
    /// The slot is held from the run to the commit of the result.
    fn prepare_busy(&self) -> bool {
        self.scan().any(|transfer| matches!(transfer.stage, TransferStage::Measuring | TransferStage::PersistingPrepared) || (transfer.stage == TransferStage::Preparing && transfer.prep.as_ref().is_some_and(|prep| prep.phase != PreparePhase::Queued)))
    }

    pub(super) fn free_prepare_slot(&mut self, fx: &mut Fx) {
        let id = self.timer();
        fx.push(Effect::Schedule { id, after: Duration::ZERO, input: TransferInput::PrepareSlotFreed });
    }

    /// The copy is durable: the transfer waits for the slot.
    pub(super) fn queue_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Preparing;
        transfer.prep = Some(PrepareRun { phase: PreparePhase::Queued, percent: 0, name: None, mime: None });
        self.show(id, Shown::Preparing { progress: 0 }, fx);
        self.free_prepare_slot(fx);
    }

    /// A restored transfer whose copy is on disk starts the step again: the result of the older attempt goes.
    pub(super) fn restore_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        self.clear_prepare(id, fx);
        self.queue_prepare(id, fx);
        self.rounds_needed(vec![id], fx);
        self.schedule_expire(fx);
    }

    pub(super) fn clear_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        if self.transfers.get(&id).is_none_or(|transfer| transfer.clearing.is_some()) { return; }
        let clear_id = self.attempt();
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.clearing = Some(clear_id); }
        run(fx, TransferEffect::ClearPrepare { transfer_id: id, clear_id });
    }

    pub(super) fn prepare_cleared(&mut self, id: TransferId, clear_id: AttemptId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.clearing != Some(clear_id) { return; }
        transfer.clearing = None;
        if transfer.stage == TransferStage::Preparing && transfer.prep.as_ref().is_some_and(|prep| prep.phase == PreparePhase::Queued) { self.free_prepare_slot(fx); }
    }

    /// The step stops: the platform and the measure of the attempt are cancelled, the files of the attempt go.
    pub(super) fn stop_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        match (transfer.stage, transfer.prep.as_ref().map(|prep| prep.phase)) {
            (TransferStage::Preparing, Some(PreparePhase::Run)) => run(fx, TransferEffect::PrepareCancel { transfer_id: id, attempt_id: transfer.attempt_id }),
            (TransferStage::Measuring, _) => run(fx, TransferEffect::CopyCancel { transfer_id: id }),
            (TransferStage::Preparing, _) => {}
            _ => return,
        }
        self.clear_prepare(id, fx);
    }

    /// `PrepareSlotFreed`: the waiting transfer with the smallest `(seq, position)` starts when the slot is free.
    pub(super) fn start_prepares(&mut self, fx: &mut Fx) {
        if self.ended || self.prepare_busy() { return; }
        let next = self.scan().filter(|transfer| transfer.stage == TransferStage::Preparing && transfer.prep.as_ref().is_some_and(|prep| prep.phase == PreparePhase::Queued) && transfer.clearing.is_none() && (!transfer.restored || self.phase == Phase::Ready)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.transfer_id);
        let Some(id) = next else { return };
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(descriptor) = transfer.prepare.clone() else { return };
        transfer.attempt_id = attempt_id;
        if let Some(prep) = transfer.prep.as_mut() { prep.phase = PreparePhase::Run; }
        run(fx, TransferEffect::Prepare { transfer_id: id, attempt_id, descriptor });
    }

    /// An input of an attempt the owner does not know: the platform is told to stop it (R2), an old attempt of a live step changes nothing (R4).
    fn current(&self, id: TransferId, attempt_id: AttemptId, stages: &[TransferStage], fx: &mut Fx) -> bool {
        match self.transfers.get(&id) {
            Some(transfer) if stages.contains(&transfer.stage) => transfer.attempt_id == attempt_id,
            _ => {
                run(fx, TransferEffect::PrepareCancel { transfer_id: id, attempt_id });
                false
            }
        }
    }

    pub(super) fn prepare_progress(&mut self, id: TransferId, attempt_id: AttemptId, percent: u32, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Preparing], fx) { return; }
        let Some(prep) = self.transfers.get_mut(&id).and_then(|transfer| transfer.prep.as_mut()).filter(|prep| prep.phase == PreparePhase::Run && percent > prep.percent) else { return };
        prep.percent = percent;
        self.show(id, Shown::Preparing { progress: percent }, fx);
    }

    pub(super) fn prepared(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, outcome: PrepareOutcome, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Preparing], fx) { return; }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(prep) = transfer.prep.as_mut().filter(|prep| prep.phase == PreparePhase::Run) else { return };
        match outcome {
            PrepareOutcome::Done { name, mime } => {
                prep.name = name;
                prep.mime = mime;
                prep.percent = 100;
                transfer.stage = TransferStage::Measuring;
                self.show(id, Shown::Preparing { progress: 100 }, fx);
                run(fx, TransferEffect::Measure { transfer_id: id, attempt_id });
            }
            PrepareOutcome::Unchanged { name, mime } => {
                let Some(place) = transfer.place else { return self.fail_prepare(now, id, PrepareReason::Failed, "the copy of the source has no place".into(), fx) };
                let copy = CopiedBlob { size: transfer.size, sha256: transfer.sha256.clone(), place, cached: CachedBlob::Miss };
                prep.name = name;
                prep.mime = mime;
                prep.percent = 100;
                self.show(id, Shown::Preparing { progress: 100 }, fx);
                self.persist_prepared(now, id, (copy, false), fx);
            }
            PrepareOutcome::Failed { message } => self.fail_prepare(now, id, PrepareReason::Failed, message, fx),
            PrepareOutcome::CannotDecode { message } => self.fail_prepare(now, id, PrepareReason::DeviceCannotDecode, message, fx),
            PrepareOutcome::Cancelled => self.fail_prepare(now, id, PrepareReason::Failed, "the platform cancelled the preparation".into(), fx),
        }
    }

    /// The result is read: the row of the transfer takes its size and hash, the copy of the source stays until `Committed`.
    pub(super) fn measured(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, copy: CopiedBlob, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Measuring], fx) { return; }
        self.persist_prepared(now, id, (copy, true), fx);
    }

    /// The one write of the result, for `Done` (`swap`: the prepared file replaces the copy at `Committed`) and `Unchanged`; the descriptor goes in the same batch.
    fn persist_prepared(&mut self, now: Time, id: TransferId, (copy, swap): (CopiedBlob, bool), fx: &mut Fx) {
        let sizing = self.sizing;
        if self.transfers.get(&id).is_some_and(|transfer| transfer.over_limit(copy.size)) { return self.fail_too_large(now, id, fx); }
        let steps = match self.transfers.get(&id).and_then(|transfer| transfer.steps.as_ref()).map(|steps| serde_json::from_str::<super::upload_codec::StepsBody>(&steps.body)).transpose() {
            Ok(steps) => steps,
            Err(error) => return self.fail_prepare(now, id, PrepareReason::Failed, format!("Internal(Invalid): the steps body is not readable: {error}"), fx),
        };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if swap {
            let mut before = transfer.clone();
            before.previous = None;
            transfer.previous = Some(Box::new(before));
        }
        let CopiedBlob { size, sha256, place, cached } = copy;
        transfer.size = size;
        transfer.sha256 = sha256;
        transfer.place = Some(place);
        let cached = if transfer.held { CachedBlob::Miss } else { cached };
        transfer.cache_stale = cached == CachedBlob::Stale;
        if let CachedBlob::Fresh { blob_id } = cached {
            transfer.from_cache = true;
            transfer.signed_id = Some(blob_id);
        }
        let (parts, multipart) = sizing.of(size, transfer.multipart_forced);
        if let Some(source) = transfer.source.as_mut() { source.parts = parts; }
        let (name, mime) = transfer.prep.as_ref().map_or((None, None), |prep| (prep.name.clone(), prep.mime.clone()));
        if let Some(steps) = steps {
            let steps = super::upload_codec::StepsBody { multipart, name: name.unwrap_or(steps.name.clone()), mime: mime.unwrap_or(steps.mime.clone()), ..steps };
            transfer.steps = Some(steps.steps());
        }
        transfer.stage = TransferStage::PersistingPrepared;
        self.store(&[id], StoreKind::SyncUpload, fx);
    }

    /// The measure failed: the transfer fails with the copy of the source in place.
    pub(super) fn measure_failed(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, error: String, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Measuring], fx) { return; }
        self.fail_prepare(now, id, PrepareReason::Failed, error, fx);
    }

    /// `PersistingPrepared x Committed`: the result takes the place of the copy.
    pub(super) fn prepared_committed(&mut self, now: Time, id: TransferId, fx: &mut Fx) {
        if self.transfers.get(&id).is_some_and(|transfer| transfer.previous.is_some()) {
            return run(fx, TransferEffect::PromoteStaged { transfer_id: id });
        }
        self.prepared_ready(now, id, fx);
    }

    /// The result is the file of the transfer: the upload goes on from the first part.
    pub(super) fn prepared_ready(&mut self, now: Time, id: TransferId, fx: &mut Fx) {
        self.schedule_expire(fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.prepare = None;
        transfer.prep = None;
        self.enter_upload(now, id, fx);
        self.free_prepare_slot(fx);
        self.start_in_order(now, fx);
    }

    /// The write of the result was refused, or the swap of the files failed: the copy of the source and the settings stay for `retry`.
    pub(super) fn fail_prepare(&mut self, now: Time, id: TransferId, reason: PrepareReason, message: String, fx: &mut Fx) {
        self.fail_step(now, id, failure::of_prepare(reason, message), fx);
    }

    /// The file that goes to the network is larger than the limit of its source: nothing is sent.
    pub(super) fn fail_too_large(&mut self, now: Time, id: TransferId, fx: &mut Fx) {
        let error = failure::of_too_large("the file is larger than the size limit of its source".into());
        if self.transfers.get(&id).is_some_and(|transfer| transfer.prep.is_some()) { self.fail_step(now, id, error, fx); } else { self.begin_failure(now, id, error, false, fx); }
    }

    fn fail_step(&mut self, now: Time, id: TransferId, error: String, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if let Some(before) = transfer.previous.take() {
            transfer.size = before.size;
            transfer.sha256 = before.sha256;
            transfer.place = before.place;
            transfer.source = before.source;
            transfer.steps = before.steps;
            transfer.from_cache = before.from_cache;
            transfer.signed_id = before.signed_id;
            transfer.cache_stale = before.cache_stale;
        }
        transfer.prep = None;
        self.clear_prepare(id, fx);
        self.free_prepare_slot(fx);
        self.schedule_expire(fx);
        self.begin_failure(now, id, error, false, fx);
    }
}
