//! Harness of the owner-level tests of the owner `Transfer`: the owner on `DetScheduler` with typed peers (Store, Outbox, Rounds) and the book of what it gave out.

use std::marker::PhantomData;
use std::sync::PoisonError;

use crate::store::files::Location;
use crate::owners::contract::{BatchId, Online, RejectReason, Rejected, ReplyId};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter, UploadKind, UploadRow};
use crate::spec::support::{new_log, take, Discard, Emitted, Log, NoEffect, Silent, Spy, SpyState};
use crate::owners::write::outbox::{Fault, NewOp, Outbox, OutboxEffect, OutboxInput, OutboxOutcome, OutboxPeers, OutboxState, OpSource, Receipt, ReserveTarget};
use crate::owners::write::rounds::{Rounds, RoundsInput};
use crate::owners::write::transfer::{BatchItem, Joint, NextStep, Shown, Source, Steps, StoredTransfer, Transfer, TransferAnswer, TransferEffect as Run, TransferId, TransferInput, TransferPeers, TransferStage, TransferState, TransferTick, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, Effects, Epoch, EpochStamp, StateOwner, Time};
use crate::schema::OnFailure;
use crate::spec::support::committed_at;

pub(super) const EPOCH: Epoch = 1;
pub(super) const DAY: u64 = 86_400_000;
const STORE_AT: usize = 0;
const OUTBOX_AT: usize = 1;
pub(super) const TRANSFER_AT: usize = 2;
const ROUNDS_AT: usize = 3;

/// A peer that keeps the typed inputs it receives.
pub(super) struct Tap<O: StateOwner>(PhantomData<fn() -> O>);

impl<O: StateOwner> StateOwner for Tap<O>
where
    O::Input: Clone,
{
    const EPOCHED: bool = false;

    type State = Log<O::Input>;
    type Input = O::Input;
    type Effect = NoEffect;

    fn step(state: &mut Log<O::Input>, input: O::Input, _now: crate::runtime::Time) -> Effects<Self> {
        state.lock().unwrap_or_else(PoisonError::into_inner).push(input);
        Vec::new()
    }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

/// The stream that follows every key of the twin.
const SPY: u64 = u64::MAX;

/// What the owner gave out in one or more steps.
#[derive(Clone, Debug, Default)]
pub(super) struct Out {
    pub runs: Vec<Run>,
    pub batches: Vec<StoreBatch>,
    /// The batches that only write the shown value of a row: the owner does not wait for their answer.
    pub shown: Vec<StoreBatch>,
    pub outbox: Vec<OutboxInput>,
    pub rounds: Vec<RoundsInput>,
    pub timers: usize,
    pub schedules: Vec<(std::time::Duration, String)>,
    /// The ids of the timers set, in the order of `schedules`.
    pub timer_ids: Vec<crate::runtime::ScheduleId>,
    pub cancels: usize,
}

impl Out {
    pub fn replies(&self, reply: ReplyId) -> Vec<TransferAnswer> { self.runs.iter().filter_map(|run| if let Run::Reply { reply: found, answer } = run { (*found == reply).then(|| answer.clone()) } else { None }).collect() }

    pub fn all_replies(&self) -> usize { self.runs.iter().filter(|run| matches!(run, Run::Reply { .. })).count() }

    /// What the stream `SPY` follows: it follows every key of the transfers the twin takes, so these are the real `Status` effects.
    pub fn shows(&self) -> Vec<Shown> { self.runs.iter().filter_map(|run| if let Run::Status { stream_id: SPY, shown, .. } = run { Some(shown.clone()) } else { None }).collect() }

    pub fn steps(&self) -> Vec<(Option<u32>, u64)> { self.runs.iter().filter_map(|run| if let Run::Step { part, attempt_id, .. } = run { Some((*part, *attempt_id)) } else { None }).collect() }

    pub fn count(&self, pick: fn(&Run) -> bool) -> usize { self.runs.iter().filter(|run| pick(run)).count() }

    pub fn completes(&self) -> usize { self.count(|run| matches!(run, Run::Complete { .. })) }

    pub fn aborts(&self) -> usize { self.count(|run| matches!(run, Run::Abort { .. })) }

    /// The upload ids of the `abort` calls of transfers that are gone.
    pub fn orphan_aborts(&self) -> Vec<String> { self.runs.iter().filter_map(|run| if let Run::AbortOrphan { upload_id, .. } = run { Some(upload_id.clone()) } else { None }).collect() }

    pub fn orphans_written(&self) -> Vec<(String, String)> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::UploadOrphan { upload_id, key } = op { Some((upload_id.clone(), key.clone())) } else { None }).collect() }

    pub fn orphans_removed(&self) -> Vec<String> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RemoveOrphan { upload_id } = op { Some(upload_id.clone()) } else { None }).collect() }

    pub fn delete_paths(&self) -> Vec<String> { self.runs.iter().filter_map(|run| if let Run::DeleteFile { path } = run { Some(path.clone()) } else { None }).collect() }

    pub fn releases(&self) -> Vec<u64> { self.runs.iter().filter_map(|run| if let Run::ReleaseStep { request_id } = run { Some(*request_id) } else { None }).collect() }

    pub fn cancel_steps(&self) -> usize { self.count(|run| matches!(run, Run::CancelStep { .. })) }

    pub fn copies(&self) -> usize { self.count(|run| matches!(run, Run::Copy { .. })) }

    pub fn staged_copies(&self) -> usize { self.count(|run| matches!(run, Run::Copy { staged: true, .. })) }

    pub fn promotions(&self) -> usize { self.count(|run| matches!(run, Run::PromoteStaged { .. })) }

    pub fn deleted(&self) -> Vec<String> { self.runs.iter().filter_map(|run| if let Run::DeleteFile { path } = run { Some(path.clone()) } else { None }).collect() }

    pub fn copy_cancels(&self) -> usize { self.count(|run| matches!(run, Run::CopyCancel { .. })) }

    pub fn deletes(&self) -> usize { self.count(|run| matches!(run, Run::DeleteFile { .. })) }

    pub fn drained(&self) -> usize { self.count(|run| matches!(run, Run::Drained(_))) }

    pub fn classified(&self) -> usize { self.count(|run| matches!(run, Run::RestoreClassified { .. })) }


    pub fn statuses(&self) -> Vec<(u64, Shown)> { self.runs.iter().filter_map(|run| if let Run::Status { stream_id, shown, .. } = run { (*stream_id != SPY).then(|| (*stream_id, shown.clone())) } else { None }).collect() }

    /// The final mutations handed to Outbox: of a batch, of a single element, reserved or not.
    pub fn submit_ops(&self) -> Vec<&NewOp> { self.outbox.iter().filter_map(|input| match input { OutboxInput::Submit { op, .. } | OutboxInput::SubmitReserved { op, .. } => Some(op), _ => None }).collect() }

    pub fn outbox_retries(&self) -> usize { self.outbox.iter().filter(|input| matches!(input, OutboxInput::RetryTransfer { .. })).count() }

    pub fn numbers(&self) -> Vec<(String, Vec<(String, i64)>)> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Numbers { row_id, writes, .. } = op { Some((row_id.clone(), writes.clone())) } else { None }).collect() }

    pub fn rolled_back(&self) -> Vec<u64> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RollBack { op_id } = op { Some(*op_id) } else { None }).collect() }

    pub fn submits(&self) -> usize { self.outbox.iter().filter(|input| matches!(input, OutboxInput::Submit { .. } | OutboxInput::SubmitReserved { .. })).count() }

    pub fn outbox_discards(&self) -> usize { self.outbox.iter().filter(|input| matches!(input, OutboxInput::Discard { .. } | OutboxInput::DiscardTransfer { .. })).count() }

    pub fn rows(&self) -> Vec<(&UploadRow, UploadKind)> {
        self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Upload { row, kind } = op { Some((row, *kind)) } else { None }).collect()
    }

    pub fn kinds(&self) -> Vec<UploadKind> { self.rows().into_iter().map(|(_, kind)| kind).collect() }

    pub fn round_errors(&self) -> Vec<(String, Option<String>)> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::UploadRoundError { key, error } = op { Some((key.clone(), error.clone())) } else { None }).collect() }

    /// The shown values that the batches wrote to `sync_upload`.
    pub fn stored_shows(&self) -> Vec<Shown> { self.shown.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::UploadShown { shown, .. } = op { Some(shown.clone()) } else { None }).collect() }

    pub fn shown_batches(&self) -> Vec<BatchId> { self.shown.iter().map(|batch| batch.id).collect() }

    pub fn staged_cleared(&self) -> Vec<String> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::UploadStaged { key } = op { Some(key.clone()) } else { None }).collect() }

    pub fn outcomes_written(&self) -> Vec<(u64, u64, OutboxOutcome)> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::TransferOutcome { transfer_id, cycle_id, outcome } = op { Some((*transfer_id, *cycle_id, outcome.clone())) } else { None }).collect() }

    pub fn removed(&self) -> Vec<String> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RemoveUpload { key } = op { Some(key.clone()) } else { None }).collect() }

    pub fn writes(&self) -> usize { self.batches.len() }

    pub fn round_needed(&self) -> Vec<Vec<TransferId>> { self.rounds.iter().filter_map(|input| if let RoundsInput::Needed { ids, .. } = input { Some(ids.clone()) } else { None }).collect() }

    pub fn round_idle(&self) -> Vec<TransferId> { self.rounds.iter().filter_map(|input| if let RoundsInput::Idle { transfer_id, .. } = input { Some(*transfer_id) } else { None }).collect() }
}

pub(super) fn mutation(key: &str) -> NewOp {
    let mut parts = key.splitn(3, ':');
    let (model, row, action) = (parts.next().unwrap_or("Moment"), parts.next().unwrap_or("r"), parts.next().unwrap_or("upload"));
    let effects = crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: model.into(), row_id: row.into(), values: "{}".into() }], ..Default::default() };
    NewOp { model: model.into(), row_id: row.into(), action: action.into(), on_failure: OnFailure::Keep, origin: OpSource::Api, path: "/moments".into(), body: String::new(), effects, expires_after: None }
}

pub(super) fn item(key: &str, parts: u32) -> BatchItem { BatchItem { key: key.into(), held: false, fields: Default::default(), urgent: false, source: Some(Source { path: format!("/src/{key}"), parts }), prepare: None, mutation: mutation(key), steps: Some(Steps { path: "/upload".into(), body: String::new() }), probed: None, dimensions: None, max_bytes: None } }

/// The batch of the tests: two to ten elements go in one mutation.
pub(super) fn joint() -> Joint { Joint { minimum: 2, maximum: 10, items_key: "items".into(), mutation: mutation("Batch:r:upload"), renumber: None } }

pub(super) fn stored(id: TransferId, key: &str, parts: u32) -> StoredTransfer {
    StoredTransfer {
        transfer_id: id,
        attempts: 0,
        key: key.into(),
        seq: id,
        source: Some(Source { path: format!("/src/{key}"), parts }),
        prepare: None,
        awaiting_source: false,
        batch: None,
        fields: Default::default(),
        upload_id: None,
        confirmed_parts: 0,
        floor: 0,
        weight: None,
        weighed_prepare: false,
        batch_floor: 0,
        etags: Vec::new(),
        completed: false,
        signed_id: None,
        failed: None,
        aborted: false,
        op_id: Some(id),
        copying: false,
        file_present: true,
        created_at: Time(0),
        place: Location::Directory,
        size: u64::from(parts) * 5 * 1024 * 1024,
        sha256: "h".into(),
        cycle_id: 1,
        has_intent: false,
        has_operation: true,
        mutation: mutation(key),
        steps: Some(Steps { path: "/upload".into(), body: String::new() }),
        staged: false,
        failed_at: None,
        blob_created_at: None, held: false,
        restore_failed: None,
        restore_element: None,
        pending_step: None,
        saved_answer: None, max_bytes: None,
    }
}

pub(super) struct Twin {
    pub det: DetScheduler,
    log: Log<Emitted<Run>>,
    store: Log<StoreInput>,
    outbox: Log<OutboxInput>,
    rounds: Log<RoundsInput>,
    reply: ReplyId,
    unanswered: Vec<(BatchId, EpochStamp)>,
    ended: bool,
    seen_ids: std::collections::BTreeMap<String, TransferId>,
    followed: std::collections::BTreeSet<String>,
    /// The batches of shown values wait for the test to answer them.
    pub hold_shown: bool,
    /// What the real Outbox of `with_outbox` gave out; empty when Outbox is a peer that keeps its inputs.
    outbox_fx: Log<Emitted<OutboxEffect>>,
}

impl Twin {
    /// The owner in the phase `Restoring`: nothing restored, no network signal.
    pub fn bare(slots: u32) -> Self { Self::build(slots, false) }

    /// Transfer and the real Outbox on one scheduler: the messages between them run in their real order, the Store keeps the batches of both.
    pub fn bare_with_outbox() -> Self { Self::build(1, true) }

    fn build(slots: u32, real_outbox: bool) -> Self {
        let mut det = DetScheduler::new(7);
        let (store, outbox, rounds) = (new_log(), new_log(), new_log());
        let outbox_fx = new_log();
        assert_eq!(det.add::<Tap<StoreWriter>>("store", store.clone(), Silent).index(), STORE_AT);
        if real_outbox {
            let peers = OutboxPeers { store: Address::new(STORE_AT), session: None, transfer: Some(Address::new(TRANSFER_AT)) };
            assert_eq!(det.add::<Spy<Outbox>>("outbox", SpyState::new(OutboxState::new(EPOCH, peers), &outbox_fx), Discard).index(), OUTBOX_AT);
        } else {
            assert_eq!(det.add::<Tap<Outbox>>("outbox", outbox.clone(), Silent).index(), OUTBOX_AT);
        }
        let log = new_log();
        let peers = TransferPeers { store: Address::new(STORE_AT), outbox: Address::new(OUTBOX_AT), rounds: Some(Address::new(ROUNDS_AT)) };
        assert_eq!(det.add::<Spy<Transfers>>("transfer", SpyState::new(TransferState::new(EPOCH, slots, peers), &log), Discard).index(), TRANSFER_AT);
        assert_eq!(det.add::<Tap<Rounds>>("rounds", rounds.clone(), Silent).index(), ROUNDS_AT);
        det.start_epoch(EPOCH);
        Self { det, log, store, outbox, rounds, reply: 100, unanswered: Vec::new(), ended: false, seen_ids: Default::default(), followed: Default::default(), hold_shown: false, outbox_fx }
    }

    /// Both owners after their restore, the network up, the operations `Message:send` and `Moment:upload` declared.
    pub fn with_outbox() -> Self {
        let mut twin = Self::bare_with_outbox();
        twin.post_outbox(OutboxInput::RegisterOperations { declarations: vec![("Message".into(), "send".into()), ("Moment".into(), "upload".into())], complete: false });
        twin.post_outbox(OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
        twin.post_outbox(OutboxInput::RestoreReady { epoch: EPOCH });
        twin.post_outbox(OutboxInput::Online(Online(true)));
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Message".into(), "send".into()), ("Moment".into(), "upload".into())], complete: false });
        twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        twin.post(TransferInput::Online(Online(true)));
        twin
    }

    pub fn post_outbox(&mut self, input: OutboxInput) -> Out {
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.det.run_until_quiet(10_000).expect("the owners are quiet");
        self.collect()
    }

    /// The answer of the Store for the oldest batch of either owner: both owners get it, the author takes it.
    pub fn answer_both(&mut self, refused: Option<RejectReason>) -> Out {
        let (batch, _) = self.unanswered.remove(0);
        let (to_outbox, to_transfer) = match refused {
            None => (
                OutboxInput::Committed(committed_at(batch, 1, &[])),
                TransferInput::Committed(committed_at(batch, 1, &[])),
            ),
            Some(reason) => (OutboxInput::Rejected(Rejected { batch, reason: reason.clone() }), TransferInput::Rejected(Rejected { batch, reason })),
        };
        let mut out = self.post_outbox(to_outbox);
        let next = self.post(to_transfer);
        out.runs.extend(next.runs);
        out.batches.extend(next.batches);
        out.outbox.extend(next.outbox);
        out
    }

    /// The answers the real Outbox gave to callers since the last call.
    pub fn outbox_replies(&mut self) -> Vec<(ReplyId, crate::owners::write::outbox::OutboxAnswer)> {
        take(&self.outbox_fx).into_iter().filter_map(|emitted| match emitted { Emitted::Run(OutboxEffect::Reply { reply, answer }) => Some((reply, answer)), _ => None }).collect()
    }

    /// The operations of the real Outbox that are not `Done`.
    pub fn open_operations(&self) -> usize {
        self.det.state(Address::<Spy<Outbox>>::new(OUTBOX_AT)).map_or(0, |spy: &SpyState<Outbox>| spy.inner.ops.values().filter(|operation| operation.stage != crate::owners::write::outbox_types::Stage::Done).count())
    }

    /// The owner after `Restored` of `rows`: the operations of the rows are declared, the network has not spoken yet.
    pub fn opened(rows: Vec<StoredTransfer>, outcomes: std::collections::BTreeMap<TransferId, OutboxOutcome>) -> (Self, Out) { Self::opened_with(rows, outcomes, Default::default()) }

    /// The same with the rules of the batches kept in the base.
    pub fn opened_with(rows: Vec<StoredTransfer>, outcomes: std::collections::BTreeMap<TransferId, OutboxOutcome>, batches: std::collections::BTreeMap<u64, Joint>) -> (Self, Out) {
        let mut twin = Self::bare(1);
        let declarations = rows.iter().map(|row| (row.mutation.model.clone(), row.mutation.action.clone())).collect();
        twin.post(TransferInput::RegisterOperations { declarations, complete: false });
        let outcomes = outcomes.into_iter().map(|(id, outcome)| (id, [(1, outcome)].into())).collect();
        let batches = batches.into_iter().map(|(id, joint)| (id, crate::owners::write::transfer::StoredGroup { joint, op_id: Some(1_000_000 + id), intent_members: Vec::new() })).collect();
        let out = twin.post(TransferInput::Restored { uploads: rows, outcomes, orphans: Vec::new(), batches, held_orphan: None });
        (twin, out)
    }

    /// Every input in `inputs` leaves the owner quiet and the state of `id` as it was.
    pub fn ignores(&mut self, id: TransferId, inputs: Vec<TransferInput>) {
        let stage = self.stage(id);
        for input in inputs {
            let label = format!("{input:?}");
            let out = self.post(input);
            assert!(Self::quiet(&out), "{label}: {out:?}");
            assert_eq!(self.stage(id), stage, "{label}");
        }
    }

    /// The five inputs a state of the table ignores for other inputs: results of other states and timers.
    pub fn stray_results(&self, id: TransferId) -> Vec<TransferInput> {
        let attempt_id = self.attempt(id) + 1000;
        vec![
            TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(1), upload_id: None, etag: None },
            TransferInput::StepError { transfer_id: id, attempt_id, temporary: true, error: refusal() },
            TransferInput::Completed { transfer_id: id, attempt_id, signed_id: None },
            TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: true, error: refusal() },
            TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id } },
        ]
    }

    /// The owner after `Restored` and `RestoreReady`, with the network up.
    pub fn new() -> Self { Self::with_slots(1) }

    pub fn with_slots(slots: u32) -> Self {
        let mut twin = Self::bare(slots);
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
        twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        twin.post(TransferInput::Online(Online(true)));
        twin
    }

    pub fn offline() -> Self {
        let mut twin = Self::new();
        twin.post(TransferInput::Online(Online(false)));
        twin
    }

    fn collect(&mut self) -> Out {
        let seen: Vec<_> = self.state().transfers.values().map(|transfer| (transfer.key.clone(), transfer.transfer_id)).collect();
        self.seen_ids.extend(seen);
        let mut out = Out::default();
        for emitted in take(&self.log) {
            match emitted {
                Emitted::Run(run) => out.runs.push(run),
                Emitted::Schedule { id, after, input } => {
                    out.timers += 1;
                    out.schedules.push((after, input));
                    out.timer_ids.push(id);
                }
                Emitted::Cancel(_) => out.cancels += 1,
                Emitted::Send { .. } | Emitted::Broadcast(_) => {}
            }
        }
        for input in take(&self.store) {
            if let StoreInput::Commit(batch) = input {
                if batch.ops.iter().all(|op| matches!(op, StoreOp::UploadShown { .. })) { out.shown.push(batch); continue; }
                self.unanswered.push((batch.id, batch.stamp));
                out.batches.push(batch);
            }
        }
        out.outbox = take(&self.outbox);
        out.rounds = take(&self.rounds);
        out
    }

    fn follow(&mut self, input: &TransferInput) {
        let keys: Vec<String> = match input {
            TransferInput::Accept { item, .. } => vec![item.key.clone()],
            TransferInput::AcceptBatch { items, .. } => items.iter().map(|item| item.key.clone()).collect(),
            TransferInput::Restored { uploads, .. } => uploads.iter().map(|row| row.key.clone()).collect(),
            _ => Vec::new(),
        };
        for key in keys {
            if self.followed.insert(key.clone()) {
                self.det.post(Address::<Transfers>::new(TRANSFER_AT), EpochStamp::Epoch(EPOCH), TransferInput::StatusInterestPlus { key, stream_id: SPY });
                self.det.run_until_quiet(10_000).expect("the owner is quiet");
                self.collect();
            }
        }
    }

    pub fn post(&mut self, input: TransferInput) -> Out {
        let mut out = self.post_held(input);
        let mut waiting = if self.hold_shown { Vec::new() } else { out.shown.clone() };
        while let Some(shown) = waiting.pop() {
            let next = self.commit_shown(shown.id);
            waiting.extend(next.shown.clone());
            out.runs.extend(next.runs);
            out.batches.extend(next.batches);
            out.outbox.extend(next.outbox);
            out.rounds.extend(next.rounds);
            out.timers += next.timers;
            out.schedules.extend(next.schedules);
            out.timer_ids.extend(next.timer_ids);
            out.cancels += next.cancels;
        }
        out
    }

    pub fn at(&mut self, now: Time) -> Out {
        self.det.tick(now);
        self.collect()
    }

    fn post_held(&mut self, input: TransferInput) -> Out {
        self.follow(&input);
        let stamp = if self.ended { EpochStamp::Control } else { EpochStamp::Epoch(EPOCH) };
        self.det.post(Address::<Transfers>::new(TRANSFER_AT), stamp, input);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        self.collect()
    }

    pub fn state(&self) -> &TransferState { self.det.state(Address::<Spy<Transfers>>::new(TRANSFER_AT)).map(|spy: &SpyState<Transfers>| &spy.inner).expect("the owner state") }

    pub fn t(&self, id: TransferId) -> &Transfer { &self.state().transfers[&id] }

    pub fn stage(&self, id: TransferId) -> TransferStage {
        match self.state().transfers.get(&id) {
            Some(transfer) => transfer.stage,
            None if self.seen_ids.values().any(|seen| *seen == id) => TransferStage::Done,
            None => panic!("unknown transfer {id}"),
        }
    }

    pub fn reply(&mut self) -> ReplyId {
        self.reply += 1;
        self.reply
    }

    pub fn attempt(&self, id: TransferId) -> u64 { self.t(id).attempt_id }

    pub fn answer(&mut self, reply: impl FnOnce(BatchId) -> TransferInput) -> Out {
        let (batch, stamp) = self.unanswered.remove(0);
        let kept = self.ended;
        self.ended = stamp == EpochStamp::Control;
        let out = self.post(reply(batch));
        self.ended = kept;
        out
    }

    /// `Committed` for the oldest batch the Store has not answered.
    pub fn commit(&mut self) -> Out { self.answer(|batch| TransferInput::Committed(committed_at(batch, 1, &[]))) }

    /// `Committed` for a batch of shown values: the Store answers them apart from the batches the tests answer in order.
    pub fn commit_shown(&mut self, batch: BatchId) -> Out { self.post_held(TransferInput::Committed(committed_at(batch, 1, &[]))) }

    pub fn reject_shown(&mut self, batch: BatchId) -> Out { self.refuse_shown(batch, RejectReason::Storage("disk".into())) }

    pub fn refuse_shown(&mut self, batch: BatchId, reason: RejectReason) -> Out { self.post_held(TransferInput::Rejected(Rejected { batch, reason })) }

    pub fn has_pending(&self) -> bool { !self.unanswered.is_empty() }

    pub fn reject(&mut self, reason: RejectReason) -> Out { self.answer(|batch| TransferInput::Rejected(Rejected { batch, reason })) }

    pub fn accepting(&mut self, key: &str, parts: u32) -> (TransferId, ReplyId, Out) {
        let reply = self.reply();
        let initial = self.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(key, parts)), reply });
        let id = self.state().open_id(key).expect("an accepting transfer");
        assert!(matches!(initial.outbox.as_slice(), [OutboxInput::ReserveOp { target: ReserveTarget::Transfer(found), .. }] if *found == id));
        let out = self.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
        (id, reply, out)
    }

    pub fn copying(&mut self, key: &str, parts: u32) -> TransferId {
        let (id, ..) = self.accepting(key, parts);
        self.commit();
        assert_eq!(self.stage(id), TransferStage::Copying);
        id
    }

    pub fn copy_done(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        let size = self.state().transfers.get(&id).and_then(|transfer| transfer.source.as_ref()).map_or(10, |source| u64::from(source.parts) * 5 * 1024 * 1024);
        self.post(TransferInput::CopyDone { transfer_id: id, attempt_id, size, sha256: "h".into(), place: Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss })
    }

    pub fn copy_failed(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::CopyFailed { transfer_id: id, attempt_id, error: "unreadable".into() })
    }

    pub fn persisting_copy(&mut self, key: &str, parts: u32) -> TransferId {
        let id = self.copying(key, parts);
        self.copy_done(id);
        assert_eq!(self.stage(id), TransferStage::PersistingCopy);
        id
    }

    /// `Ready` (offline) or `Uploading` (online) after the common write of the acceptance.
    pub fn accepted(&mut self, key: &str, parts: u32) -> TransferId {
        let id = self.persisting_copy(key, parts);
        self.commit();
        id
    }

    pub fn step_ok(&mut self, id: TransferId, part: u32, upload_id: Option<&str>) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(part), upload_id: upload_id.map(str::to_string), etag: Some(format!("e{part}")) })
    }

    pub fn step_error(&mut self, id: TransferId, temporary: bool) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::StepError { transfer_id: id, attempt_id, temporary, error: refusal() })
    }

    pub fn completed(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::Completed { transfer_id: id, attempt_id, signed_id: Some("signed".into()) })
    }

    pub fn complete_error(&mut self, id: TransferId, temporary: bool) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::CompleteError { transfer_id: id, attempt_id, temporary, error: refusal() })
    }

    pub fn tick_retry(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        self.det.tick(Time(1));
        self.post(TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id } })
    }

    pub fn put_timeout(&mut self, id: TransferId, attempt_id: u64) -> Out {
        self.det.tick(Time(1));
        self.post(TransferInput::Tick { kind: TransferTick::PutTimeout { transfer_id: id, attempt_id } })
    }

    pub fn put_started(&mut self, id: TransferId, attempt_id: u64) -> Out { self.post(TransferInput::PutStarted { transfer_id: id, attempt_id }) }

    pub fn online(&mut self, on: bool) -> Out { self.post(TransferInput::Online(Online(on))) }

    pub fn cancel(&mut self, key: &str) -> (ReplyId, Out) {
        let reply = self.reply();
        (reply, self.post(TransferInput::Cancel { key: key.into(), reply }))
    }

    pub fn discard(&mut self, key: &str) -> (ReplyId, Out) {
        let reply = self.reply();
        (reply, self.post(TransferInput::Discard { key: key.into(), reply }))
    }

    pub fn retry(&mut self, key: &str, source: Option<Source>) -> (ReplyId, Out) {
        let reply = self.reply();
        let source = source.map(|source| crate::owners::write::transfer::SuppliedSource { source, prepare: None, steps: Steps { path: "/upload".into(), body: String::new() }, values: Default::default(), local: String::new(), variables: Default::default(), wrap: None, max_bytes: None, probed: None });
        (reply, self.post(TransferInput::Retry { key: key.into(), source, reply }))
    }

    pub fn outcome(&mut self, id: TransferId, cycle_id: u64) -> (ReplyId, Out) {
        let reply = self.reply();
        let receipt = Receipt { epoch: EPOCH, op_id: id, cycle_id, revision: 0, optimistic: None };
        (reply, self.post(TransferInput::Outcome { receipt, reply , discard_on_failure: false}))
    }

    pub fn mutation_outcome(&mut self, key: &str, outcome: OutboxOutcome) -> Out {
        if let Some(group) = key.strip_prefix("batch:").and_then(|value| value.parse::<u64>().ok()) {
            let members = self.state().transfers.values().filter(|transfer| transfer.batch_id == Some(group) && transfer.stage == TransferStage::Mutating).map(|transfer| (transfer.transfer_id, transfer.cycle_id, outcome.clone())).collect();
            let intent_removed = matches!(outcome, OutboxOutcome::Confirmed { .. } | OutboxOutcome::Rejected { .. } | OutboxOutcome::Discarded);
            return self.post(TransferInput::MutationOutcomes { batch_id: group, members, intent_removed });
        }
        let id = *self.seen_ids.get(key).expect("transfer for outcome");
        let cycle_id = self.t(id).cycle_id;
        let intent_removed = matches!(outcome, OutboxOutcome::Confirmed { .. } | OutboxOutcome::Rejected { .. } | OutboxOutcome::Discarded);
        self.post(TransferInput::MutationOutcome { transfer_id: id, cycle_id, outcome, intent_removed, row_never_created: false })
    }

    pub fn end_epoch(&mut self) -> Out { self.end_epoch_by(Cause::SessionReset) }

    pub fn end_epoch_by(&mut self, cause: Cause) -> Out {
        self.ended = true;
        self.det.end_epoch(EPOCH, cause);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        self.collect()
    }

    pub fn close(&mut self) -> Out {
        self.det.close();
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        self.collect()
    }

    /// `Uploading` with a multipart upload: `parts` parts, the first step done.
    pub fn uploading(&mut self, key: &str, parts: u32) -> TransferId {
        let id = self.accepted(key, parts);
        if self.stage(id) == TransferStage::PersistingAttempt {
            self.commit();
        }
        assert_eq!(self.stage(id), TransferStage::Uploading);
        id
    }

    /// `PersistingFailure` after a permanent error of the first step.
    pub fn persisting_failure(&mut self, key: &str) -> TransferId {
        let id = self.uploading(key, 2);
        self.step_error(id, false);
        assert_eq!(self.stage(id), TransferStage::PersistingFailure);
        id
    }

    pub fn failed(&mut self, key: &str) -> TransferId {
        let id = self.persisting_failure(key);
        self.commit();
        assert_eq!(self.stage(id), TransferStage::Failed);
        id
    }

    /// `Completing`: the only part is confirmed and written.
    pub fn completing(&mut self, key: &str) -> TransferId {
        let id = self.uploading(key, 1);
        self.step_ok(id, 1, None);
        self.commit();
        self.commit();
        assert_eq!(self.stage(id), TransferStage::Completing);
        id
    }

    pub fn persisting_complete(&mut self, key: &str) -> TransferId {
        let id = self.completing(key);
        self.completed(id);
        assert_eq!(self.stage(id), TransferStage::PersistingComplete);
        id
    }

    pub fn mutating(&mut self, key: &str) -> TransferId {
        let id = self.persisting_complete(key);
        self.commit();
        assert_eq!(self.stage(id), TransferStage::Mutating);
        id
    }

    /// The elements of a batch of `parts` parts each, copied and written. The first one starts, the others are `Ready`.
    pub fn accepted_batch(&mut self, keys: &[&str], parts: u32) -> Vec<TransferId> { self.accepted_items(keys.iter().map(|key| item(key, parts)).collect(), joint()) }

    /// The same for ready made elements and the rules of the batch.
    pub fn accepted_items(&mut self, items: Vec<BatchItem>, joint: Joint) -> Vec<TransferId> {
        let reply = self.reply();
        let keys: Vec<String> = items.iter().map(|item| item.key.clone()).collect();
        self.post(TransferInput::AcceptBatch { joint: Box::new(joint), epoch: EPOCH, items, reply });
        let ids: Vec<TransferId> = keys.iter().map(|key| self.state().open_id(key).expect("an element")).collect();
        self.reserve_batch(&ids);
        self.commit();
        for id in &ids {
            self.copy_done(*id);
        }
        self.commit();
        ids
    }

    pub fn reserve_batch(&mut self, ids: &[TransferId]) -> Out {
        for id in ids {
            self.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(*id), result: Ok(*id) });
        }
        let group = self.t(ids[0]).batch_id.expect("group id");
        self.post(TransferInput::OpReserved { target: ReserveTarget::Group(group), result: Ok(1_000_000 + group) })
    }

    /// `Mutating` of every element of a batch: all files are on the server and the mutation of the batch is on its way. The owner has a slot for each element.
    pub fn batch_mutating(&mut self, keys: &[&str]) -> Vec<TransferId> {
        let ids = self.accepted_batch(keys, 1);
        while ids.iter().any(|id| self.stage(*id) != TransferStage::Mutating) {
            if !self.unanswered.is_empty() {
                self.commit();
            } else if let Some(id) = ids.iter().find(|id| self.stage(**id) == TransferStage::Uploading) {
                self.step_ok(*id, 1, None);
            } else if let Some(id) = ids.iter().find(|id| self.stage(**id) == TransferStage::Completing) {
                self.completed(*id);
            } else {
                panic!("batch cannot advance");
            }
        }
        assert!(ids.iter().all(|id| self.stage(*id) == TransferStage::Mutating));
        ids
    }

    /// A `Ready` transfer behind a full slot: the network is up, the slot is held by another transfer.
    pub fn ready_behind(&mut self, key: &str) -> TransferId {
        self.uploading("busy:r:upload", 2);
        let id = self.accepted(key, 2);
        assert_eq!(self.stage(id), TransferStage::Ready);
        id
    }

    pub fn next_step(&self, id: TransferId) -> NextStep { self.t(id).next_step }

    pub fn fault(out: &Out, reply: ReplyId) -> Option<Fault> { out.replies(reply).into_iter().find_map(|answer| if let TransferAnswer::Err(fault) = answer { Some(fault) } else { None }) }

    /// The effects of the owner in a trace as a count of its outputs (a quiet step gives nothing).
    pub fn quiet(out: &Out) -> bool { out.runs.is_empty() && out.batches.is_empty() && out.outbox.is_empty() && out.rounds.is_empty() && out.timers == 0 }
}

/// `retry` on a transfer whose write is in flight is refused with `InFlight` and writes nothing (`cancel` and `discard` wait for the stage: `xfer_removal_waits_for_the_stage`).
pub fn refuses_calls(twin: &mut Twin, key: &str) {
    let (reply, out) = twin.retry(key, None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "retry");
    assert_eq!(out.writes(), 0, "retry");
}

pub fn storage() -> RejectReason { RejectReason::Storage("disk".into()) }

pub fn refusal() -> String { crate::owners::write::failure::of_graphql("refused".into(), None) }

/// What the executor of the owner has still to do: the delete of a file is a task of its own, the read of the result and the swap of a file happen when they are picked.
#[derive(Clone, Debug)]
pub(super) enum Pending {
    Input(TransferInput),
    Delete(String),
    Clear { transfer_id: TransferId, clear_id: u64 },
    Measure { transfer_id: TransferId, attempt_id: u64 },
    Promote { transfer_id: TransferId },
    Written { transfer_id: TransferId, attempt_id: u64 },
}

/// The prepare files of the transfers (`.prepared`, `.staged`) as the executor keeps them.
#[derive(Default)]
pub(super) struct PrepareFiles {
    files: std::collections::BTreeSet<String>,
}

impl PrepareFiles {
    /// The answers and tasks an effect leaves behind.
    pub fn effect(&mut self, run: &Run, pending: &mut Vec<Pending>) {
        match run {
            Run::DeleteFile { path } => pending.push(Pending::Delete(path.clone())),
            Run::ClearPrepare { transfer_id, clear_id } => pending.push(Pending::Clear { transfer_id: *transfer_id, clear_id: *clear_id }),
            Run::Probe { transfer_id, attempt_id, .. } => pending.push(Pending::Input(TransferInput::Probed { transfer_id: *transfer_id, attempt_id: *attempt_id, outcome: crate::ffi::ProbeOutcome::Image { width: 1000, height: 1000, exif_orientation: 1, format: crate::prepare::ImageFormat::Jpeg } })),
            Run::Prepare { transfer_id, attempt_id, .. } => pending.push(Pending::Written { transfer_id: *transfer_id, attempt_id: *attempt_id }),
            Run::Measure { transfer_id, attempt_id } => pending.push(Pending::Measure { transfer_id: *transfer_id, attempt_id: *attempt_id }),
            Run::PromoteStaged { transfer_id } => pending.push(Pending::Promote { transfer_id: *transfer_id }),
            Run::Step { transfer_id, attempt_id, part, .. } => pending.push(Pending::Input(TransferInput::StepOk { transfer_id: *transfer_id, attempt_id: *attempt_id, part: *part, upload_id: None, etag: part.map(|part| format!("e{part}")) })),
            Run::Complete { transfer_id, attempt_id, .. } => pending.push(Pending::Input(TransferInput::Completed { transfer_id: *transfer_id, attempt_id: *attempt_id, signed_id: Some("signed".into()) })),
            _ => {}
        }
    }

    /// The pick of a pending task: the input it gives back, if any.
    pub fn deliver(&mut self, pending: Pending) -> Option<TransferInput> {
        use crate::owners::write::transfer::{prepared_file_id, staged_file_id, CachedBlob};
        match pending {
            Pending::Input(input) => Some(input),
            Pending::Delete(path) => { self.files.remove(&path); None }
            Pending::Clear { transfer_id, clear_id } => {
                self.files.remove(&prepared_file_id(transfer_id));
                self.files.remove(&staged_file_id(transfer_id));
                Some(TransferInput::PrepareCleared { transfer_id, clear_id })
            }
            Pending::Written { transfer_id, attempt_id } => {
                self.files.insert(prepared_file_id(transfer_id));
                Some(TransferInput::Prepared { transfer_id, attempt_id, outcome: crate::ffi::PrepareOutcome::Done { name: None, mime: None } })
            }
            Pending::Measure { transfer_id, attempt_id } => {
                if self.files.remove(&prepared_file_id(transfer_id)) {
                    self.files.insert(staged_file_id(transfer_id));
                    Some(TransferInput::CopyDone { transfer_id, attempt_id, size: 10, sha256: "p".into(), place: Location::Directory, cached: CachedBlob::Miss })
                } else {
                    Some(TransferInput::CopyFailed { transfer_id, attempt_id, error: "the prepared file is gone".into() })
                }
            }
            Pending::Promote { transfer_id } => {
                if self.files.remove(&staged_file_id(transfer_id)) { Some(TransferInput::StagedPromoted { transfer_id }) } else { Some(TransferInput::StagedPromoteFailed { transfer_id, detail: "the staged file is gone".into() }) }
            }
        }
    }
}
