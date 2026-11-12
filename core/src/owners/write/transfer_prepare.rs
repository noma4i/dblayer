//! Owner `Transfer`: the prepare step of a source (D-364): the slot, the probe, the run, the measure of the result and the swap of the copy for it.

use std::time::Duration;

use super::failure;
use super::outbox_types::Phase;
use super::transfer::{run, Fx, TransferState};
use super::transfer_types::*;
use crate::ffi::upload_wire::{CannotDecode, WirePrepare};
use crate::ffi::{PrepareOutcome, PreparePlan, ProbeOutcome};
use crate::owners::store::StoreOp;
use crate::runtime::{Effect, Time};
use crate::value::sync::PrepareReason;

/// The plan of the run for what the platform read, `None` when the source goes as it is.
fn plan_of(settings: &WirePrepare, probed: &ProbeOutcome) -> Result<Option<PreparePlan>, String> {
    match (settings, probed) {
        (WirePrepare::Video { limits, mirror, .. }, ProbeOutcome::Video { source, metadata_keys }) => {
            let limits = limits.clone().unwrap_or_else(crate::prepare::default_video_limits);
            let plan = crate::prepare::plan_video(*source, limits.clone()).map_err(|error| error.to_string())?;
            let strip_metadata = crate::prepare::video_metadata_to_strip(metadata_keys.clone());
            if !plan.reencode && !*mirror && strip_metadata.is_empty() { return Ok(None); }
            Ok(Some(PreparePlan::Video { plan, strip_metadata, audio_bitrate: limits.audio_bitrate, audio_channels: limits.audio_channels, audio_sample_rate: limits.audio_sample_rate, mirror: *mirror }))
        }
        (WirePrepare::Image { edit, quality, format }, ProbeOutcome::Image { width, height, exif_orientation, .. }) => {
            let plan = crate::prepare::photo_plan(*width, *height, *exif_orientation, *edit).ok_or_else(|| "the image cannot be planned".to_string())?;
            Ok(Some(PreparePlan::Image { plan, quality: crate::prepare::jpeg_quality(quality.unwrap_or(f64::NAN)), format: *format }))
        }
        _ => Err("the platform read a file of another kind than the settings ask for".into()),
    }
}

/// The plan that writes the container again without the private keys: no decoder is needed.
fn repack_of(plan: &PreparePlan, metadata_keys: &[String]) -> Option<PreparePlan> {
    let strip_metadata = crate::prepare::video_metadata_to_strip(metadata_keys.to_vec());
    match plan {
        PreparePlan::Video { plan, audio_bitrate, audio_channels, audio_sample_rate, .. } if !strip_metadata.is_empty() => Some(PreparePlan::Video { plan: crate::prepare::VideoPlan { reencode: false, ..*plan }, strip_metadata, audio_bitrate: *audio_bitrate, audio_channels: *audio_channels, audio_sample_rate: *audio_sample_rate, mirror: false }),
        _ => None,
    }
}

impl TransferState {
    /// The slot is held from the probe to the commit of the result.
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
        transfer.prep = Some(PrepareRun { phase: PreparePhase::Queued, plan: None, metadata_keys: Vec::new(), percent: 0, name: None, mime: None });
        self.show(id, Shown::Preparing { progress: 0 }, fx);
        self.free_prepare_slot(fx);
    }

    /// A restored transfer whose copy is on disk starts the step again: the result of the older attempt goes.
    pub(super) fn restore_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        self.delete_prepare_files(id, fx);
        self.queue_prepare(id, fx);
        self.rounds_needed(vec![id], fx);
        self.schedule_expire(fx);
    }

    fn delete_prepare_files(&self, id: TransferId, fx: &mut Fx) {
        run(fx, TransferEffect::DeleteFile { path: prepared_file_id(id) });
        run(fx, TransferEffect::DeleteFile { path: staged_file_id(id) });
    }

    /// The step stops: the platform and the measure of the attempt are cancelled, the files of the attempt go.
    pub(super) fn stop_prepare(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        match (transfer.stage, transfer.prep.as_ref().map(|prep| prep.phase)) {
            (TransferStage::Preparing, Some(PreparePhase::Probe | PreparePhase::Run)) => run(fx, TransferEffect::PrepareCancel { transfer_id: id, attempt_id: transfer.attempt_id }),
            (TransferStage::Measuring, _) => run(fx, TransferEffect::CopyCancel { transfer_id: id }),
            (TransferStage::Preparing, _) => {}
            _ => return,
        }
        self.delete_prepare_files(id, fx);
    }

    /// `PrepareSlotFreed`: the waiting transfer with the smallest `(seq, position)` starts when the slot is free.
    pub(super) fn start_prepares(&mut self, fx: &mut Fx) {
        if self.ended || self.prepare_busy() { return; }
        let next = self.scan().filter(|transfer| transfer.stage == TransferStage::Preparing && transfer.prep.as_ref().is_some_and(|prep| prep.phase == PreparePhase::Queued) && (!transfer.restored || self.phase == Phase::Ready)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.transfer_id);
        let Some(id) = next else { return };
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.attempt_id = attempt_id;
        if let Some(prep) = transfer.prep.as_mut() { prep.phase = PreparePhase::Probe; }
        run(fx, TransferEffect::Probe { transfer_id: id, attempt_id });
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

    pub(super) fn probed(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, outcome: ProbeOutcome, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Preparing], fx) { return; }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(prep) = transfer.prep.as_mut().filter(|prep| prep.phase == PreparePhase::Probe) else { return };
        let Some(settings) = transfer.prepare.clone() else { return };
        if let ProbeOutcome::Failed { message } = &outcome { return self.fail_prepare(now, id, PrepareReason::Failed, message.clone(), fx); }
        let plan = match plan_of(&settings, &outcome) {
            Ok(plan) => plan,
            Err(message) => return self.fail_prepare(now, id, PrepareReason::Failed, message, fx),
        };
        if let ProbeOutcome::Video { metadata_keys, .. } = outcome { prep.metadata_keys = metadata_keys; }
        match plan {
            None => self.pass_through(now, id, fx),
            Some(plan) => {
                prep.phase = PreparePhase::Run;
                prep.plan = Some(plan.clone());
                run(fx, TransferEffect::Prepare { transfer_id: id, attempt_id, plan });
            }
        }
    }

    /// The source goes as it is: its copy is the file of the transfer.
    fn pass_through(&mut self, now: Time, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let key = transfer.key.clone();
        transfer.prepare = None;
        transfer.prep = None;
        self.send_batch(vec![StoreOp::RemovePrepare { key }], fx);
        self.enter_upload(id, fx);
        self.free_prepare_slot(fx);
        self.start_in_order(now, fx);
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
            PrepareOutcome::Failed { message } => self.fail_prepare(now, id, PrepareReason::Failed, message, fx),
            PrepareOutcome::CannotDecode { message } => self.cannot_decode(now, id, message, fx),
        }
    }

    fn cannot_decode(&mut self, now: Time, id: TransferId, message: String, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        let (Some(prep), Some(WirePrepare::Video { on_cannot_decode, .. })) = (transfer.prep.as_ref(), transfer.prepare.as_ref()) else { return self.fail_prepare(now, id, PrepareReason::Failed, message, fx) };
        let repacking = matches!(&prep.plan, Some(PreparePlan::Video { plan, .. }) if !plan.reencode);
        if *on_cannot_decode == CannotDecode::Fail || repacking { return self.fail_prepare(now, id, PrepareReason::DeviceCannotDecode, message, fx); }
        let repack = prep.plan.as_ref().and_then(|plan| repack_of(plan, &prep.metadata_keys));
        let Some(plan) = repack else {
            self.delete_prepare_files(id, fx);
            return self.pass_through(now, id, fx);
        };
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.attempt_id = attempt_id;
        if let Some(prep) = transfer.prep.as_mut() { prep.plan = Some(plan.clone()); }
        run(fx, TransferEffect::Prepare { transfer_id: id, attempt_id, plan });
    }

    /// The result is read: the row of the transfer takes its size and hash, the copy of the source stays until `Committed`.
    pub(super) fn measured(&mut self, id: TransferId, attempt_id: AttemptId, copy: CopiedBlob, fx: &mut Fx) {
        if !self.current(id, attempt_id, &[TransferStage::Measuring], fx) { return; }
        let sizing = self.sizing;
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let mut before = transfer.clone();
        before.previous = None;
        transfer.previous = Some(Box::new(before));
        let CopiedBlob { size, sha256, place, cached } = copy;
        transfer.size = size;
        transfer.sha256 = sha256;
        transfer.place = Some(place);
        transfer.cache_stale = cached == CachedBlob::Stale;
        if let CachedBlob::Fresh { blob_id } = cached {
            transfer.from_cache = true;
            transfer.signed_id = Some(blob_id);
        }
        let (parts, multipart) = sizing.of(size);
        if let Some(source) = transfer.source.as_mut() { source.parts = parts; }
        let (name, mime) = transfer.prep.as_ref().map_or((None, None), |prep| (prep.name.clone(), prep.mime.clone()));
        if let Some(steps) = transfer.steps.as_ref().and_then(|steps| serde_json::from_str::<super::upload_codec::StepsBody>(&steps.body).ok()) {
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
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.prepare = None;
        transfer.prep = None;
        self.enter_upload(id, fx);
        self.free_prepare_slot(fx);
        self.start_in_order(now, fx);
    }

    /// The write of the result was refused, or the swap of the files failed: the copy of the source and the settings stay for `retry`.
    pub(super) fn fail_prepare(&mut self, now: Time, id: TransferId, reason: PrepareReason, message: String, fx: &mut Fx) {
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
        self.delete_prepare_files(id, fx);
        self.free_prepare_slot(fx);
        self.schedule_expire(fx);
        self.begin_failure(now, id, failure::of_prepare(reason, message), false, fx);
    }
}
