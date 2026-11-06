//! Harness of the owner-level tests of the owner `Transfer`: the owner on `DetScheduler` with typed peers (Store, Outbox, Rounds) and the book of what it gave out.

use std::marker::PhantomData;
use std::sync::PoisonError;

use crate::files::Location;
use crate::owners::contract::{BatchId, Committed, Online, RejectReason, Rejected, ReplyId};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter, UploadKind, UploadRow};
use crate::owners::support::{new_log, take, Discard, Emitted, Log, NoEffect, Silent, Spy, SpyState};
use crate::owners::write::outbox::{Fault, NewOp, Outbox, OutboxInput, OutboxOutcome, Origin, Receipt};
use crate::owners::write::rounds::{Rounds, RoundsInput};
use crate::owners::write::transfer::{BatchItem, NextStep, Shown, Source, Steps, StoredTransfer, Transfer, TransferAnswer, TransferEffect as Run, TransferId, TransferInput, TransferPeers, TransferStage, TransferState, TransferTick, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, Effects, Epoch, EpochStamp, StateOwner, Time};
use crate::schema::OnFailure;

pub(super) const EPOCH: Epoch = 1;
pub(super) const DAY: u64 = 86_400_000;
const STORE_AT: usize = 0;
const OUTBOX_AT: usize = 1;
const TRANSFER_AT: usize = 2;
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

    fn step(state: &mut Log<O::Input>, input: O::Input) -> Effects<Self> {
        state.lock().unwrap_or_else(PoisonError::into_inner).push(input);
        Vec::new()
    }

    fn tick(now: Time) -> O::Input { O::tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

/// What the owner gave out in one or more steps.
#[derive(Clone, Debug, Default)]
pub(super) struct Out {
    pub runs: Vec<Run>,
    pub batches: Vec<StoreBatch>,
    pub outbox: Vec<OutboxInput>,
    pub rounds: Vec<RoundsInput>,
    pub timers: usize,
}

impl Out {
    pub fn replies(&self, reply: ReplyId) -> Vec<TransferAnswer> { self.runs.iter().filter_map(|run| if let Run::Reply { reply: found, answer } = run { (*found == reply).then(|| answer.clone()) } else { None }).collect() }

    pub fn all_replies(&self) -> usize { self.runs.iter().filter(|run| matches!(run, Run::Reply { .. })).count() }

    pub fn shows(&self) -> Vec<Shown> { self.runs.iter().filter_map(|run| if let Run::Shows { shown, .. } = run { Some(shown.clone()) } else { None }).collect() }

    pub fn steps(&self) -> Vec<(Option<u32>, u64)> { self.runs.iter().filter_map(|run| if let Run::Step { part, attempt_id, .. } = run { Some((*part, *attempt_id)) } else { None }).collect() }

    pub fn count(&self, pick: fn(&Run) -> bool) -> usize { self.runs.iter().filter(|run| pick(run)).count() }

    pub fn completes(&self) -> usize { self.count(|run| matches!(run, Run::Complete { .. })) }

    pub fn aborts(&self) -> usize { self.count(|run| matches!(run, Run::Abort { .. })) }

    pub fn cancel_steps(&self) -> usize { self.count(|run| matches!(run, Run::CancelStep { .. })) }

    pub fn copies(&self) -> usize { self.count(|run| matches!(run, Run::Copy { .. })) }

    pub fn copy_cancels(&self) -> usize { self.count(|run| matches!(run, Run::CopyCancel { .. })) }

    pub fn deletes(&self) -> usize { self.count(|run| matches!(run, Run::DeleteFile { .. })) }

    pub fn drained(&self) -> usize { self.count(|run| matches!(run, Run::Drained(_))) }

    pub fn classified(&self) -> usize { self.count(|run| matches!(run, Run::RestoreClassified { .. })) }

    pub fn refused(&self) -> Vec<Option<String>> { self.runs.iter().filter_map(|run| if let Run::Refused { reason, .. } = run { Some(reason.clone()) } else { None }).collect() }

    pub fn statuses(&self) -> Vec<(u64, Shown)> { self.runs.iter().filter_map(|run| if let Run::Status { stream_id, shown, .. } = run { Some((*stream_id, shown.clone())) } else { None }).collect() }

    pub fn submits(&self) -> usize { self.outbox.iter().filter(|input| matches!(input, OutboxInput::Submit { .. })).count() }

    pub fn outbox_discards(&self) -> usize { self.outbox.iter().filter(|input| matches!(input, OutboxInput::Discard { .. })).count() }

    pub fn rows(&self) -> Vec<(&UploadRow, UploadKind)> {
        self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Upload { row, kind } = op { Some((row, *kind)) } else { None }).collect()
    }

    pub fn kinds(&self) -> Vec<UploadKind> { self.rows().into_iter().map(|(_, kind)| kind).collect() }

    pub fn removed(&self) -> Vec<String> { self.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RemoveUpload { key } = op { Some(key.clone()) } else { None }).collect() }

    pub fn writes(&self) -> usize { self.batches.len() }

    pub fn round_needed(&self) -> Vec<Vec<TransferId>> { self.rounds.iter().filter_map(|input| if let RoundsInput::Needed { ids, .. } = input { Some(ids.clone()) } else { None }).collect() }

    pub fn round_idle(&self) -> Vec<TransferId> { self.rounds.iter().filter_map(|input| if let RoundsInput::Idle { transfer_id, .. } = input { Some(*transfer_id) } else { None }).collect() }
}

pub(super) fn mutation(key: &str) -> NewOp {
    let mut parts = key.splitn(3, ':');
    let (model, row, action) = (parts.next().unwrap_or("Moment"), parts.next().unwrap_or("r"), parts.next().unwrap_or("upload"));
    NewOp { model: model.into(), row_id: row.into(), action: action.into(), on_failure: OnFailure::Keep, origin: Origin::Api, path: "/moments".into(), body: String::new(), effects: Default::default(), durable: true }
}

pub(super) fn item(key: &str, parts: u32) -> BatchItem { BatchItem { key: key.into(), urgent: false, source: Source { path: format!("/src/{key}"), parts }, mutation: mutation(key), steps: Steps { path: "/upload".into(), body: String::new() } } }

pub(super) fn stored(id: TransferId, key: &str, parts: u32) -> StoredTransfer {
    StoredTransfer {
        transfer_id: id,
        key: key.into(),
        seq: id,
        source: Source { path: format!("/src/{key}"), parts },
        batch: false,
        upload_id: None,
        confirmed_parts: 0,
        etags: Vec::new(),
        completed: false,
        signed_id: None,
        failed: None,
        aborted: false,
        op_id: None,
        copying: false,
        file_present: true,
        created_at: Time(0),
        place: Location::Directory,
        size: 1,
        sha256: "h".into(),
        cycle_id: 1,
        has_intent: false,
        mutation: mutation(key),
        steps: Steps { path: "/upload".into(), body: String::new() },
        restore_failed: None,
    }
}

pub(super) struct Twin {
    pub det: DetScheduler,
    log: Log<Emitted<Run>>,
    store: Log<StoreInput>,
    outbox: Log<OutboxInput>,
    rounds: Log<RoundsInput>,
    reply: ReplyId,
    unanswered: Vec<BatchId>,
}

impl Twin {
    /// The owner in the phase `Restoring`: nothing restored, no network signal.
    pub fn bare(slots: u32) -> Self {
        let mut det = DetScheduler::new(7);
        let (store, outbox, rounds) = (new_log(), new_log(), new_log());
        assert_eq!(det.add::<Tap<StoreWriter>>("store", store.clone(), Silent).index(), STORE_AT);
        assert_eq!(det.add::<Tap<Outbox>>("outbox", outbox.clone(), Silent).index(), OUTBOX_AT);
        let log = new_log();
        let peers = TransferPeers { store: Address::new(STORE_AT), outbox: Address::new(OUTBOX_AT), rounds: Some(Address::new(ROUNDS_AT)) };
        assert_eq!(det.add::<Spy<Transfers>>("transfer", SpyState::new(TransferState::new(EPOCH, slots, peers), &log), Discard).index(), TRANSFER_AT);
        assert_eq!(det.add::<Tap<Rounds>>("rounds", rounds.clone(), Silent).index(), ROUNDS_AT);
        det.start_epoch(EPOCH);
        Self { det, log, store, outbox, rounds, reply: 100, unanswered: Vec::new() }
    }

    /// The owner after `Restored` of `rows`: the operations of the rows are declared, the network has not spoken yet.
    pub fn opened(rows: Vec<StoredTransfer>, outcomes: std::collections::BTreeMap<TransferId, OutboxOutcome>) -> (Self, Out) {
        let mut twin = Self::bare(1);
        let declarations = rows.iter().map(|row| (row.mutation.model.clone(), row.mutation.action.clone())).collect();
        twin.post(TransferInput::RegisterOperations { declarations });
        let out = twin.post(TransferInput::Restored { uploads: rows, outcomes });
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
            TransferInput::StepError { transfer_id: id, attempt_id, temporary: true },
            TransferInput::Completed { transfer_id: id, attempt_id, signed_id: None },
            TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: true },
            TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id } },
        ]
    }

    /// The owner after `Restored` and `RestoreReady`, with the network up.
    pub fn new() -> Self { Self::with_slots(1) }

    pub fn with_slots(slots: u32) -> Self {
        let mut twin = Self::bare(slots);
        twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default() });
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
        let mut out = Out::default();
        for emitted in take(&self.log) {
            match emitted {
                Emitted::Run(run) => out.runs.push(run),
                Emitted::Schedule { .. } => out.timers += 1,
                Emitted::Send { .. } | Emitted::Cancel(_) | Emitted::Broadcast(_) => {}
            }
        }
        for input in take(&self.store) {
            if let StoreInput::Commit(batch) = input {
                self.unanswered.push(batch.id);
                out.batches.push(batch);
            }
        }
        out.outbox = take(&self.outbox);
        out.rounds = take(&self.rounds);
        out
    }

    pub fn post(&mut self, input: TransferInput) -> Out {
        self.det.post(Address::<Transfers>::new(TRANSFER_AT), EpochStamp::Epoch(EPOCH), input);
        self.det.run_until_quiet(10_000).expect("the owner is quiet");
        self.collect()
    }

    pub fn state(&self) -> &TransferState { self.det.state(Address::<Spy<Transfers>>::new(TRANSFER_AT)).map(|spy: &SpyState<Transfers>| &spy.inner).expect("the owner state") }

    pub fn t(&self, id: TransferId) -> &Transfer { &self.state().transfers[&id] }

    pub fn stage(&self, id: TransferId) -> TransferStage { self.t(id).stage }

    pub fn reply(&mut self) -> ReplyId {
        self.reply += 1;
        self.reply
    }

    pub fn attempt(&self, id: TransferId) -> u64 { self.t(id).attempt_id }

    pub fn answer(&mut self, reply: impl FnOnce(BatchId) -> TransferInput) -> Out {
        let batch = self.unanswered.remove(0);
        self.post(reply(batch))
    }

    /// `Committed` for the oldest batch the Store has not answered.
    pub fn commit(&mut self) -> Out { self.answer(|batch| TransferInput::Committed(Committed { batch, revision: 1, tables: Vec::new(), models: Default::default() })) }

    pub fn reject(&mut self, reason: RejectReason) -> Out { self.answer(|batch| TransferInput::Rejected(Rejected { batch, reason })) }

    pub fn accepting(&mut self, key: &str, parts: u32) -> (TransferId, ReplyId, Out) {
        let reply = self.reply();
        let out = self.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(key, parts)), reply, now: Time(0) });
        let id = self.state().open_id(key).expect("an accepting transfer");
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
        self.post(TransferInput::CopyDone { transfer_id: id, attempt_id, size: 10, sha256: "h".into(), place: Location::Directory })
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
        self.post(TransferInput::StepError { transfer_id: id, attempt_id, temporary })
    }

    pub fn completed(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::Completed { transfer_id: id, attempt_id, signed_id: Some("signed".into()) })
    }

    pub fn complete_error(&mut self, id: TransferId, temporary: bool) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::CompleteError { transfer_id: id, attempt_id, temporary })
    }

    pub fn tick_retry(&mut self, id: TransferId) -> Out {
        let attempt_id = self.attempt(id);
        self.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id } })
    }

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
        (reply, self.post(TransferInput::Retry { key: key.into(), source, reply }))
    }

    pub fn outcome(&mut self, id: TransferId, cycle_id: u64) -> (ReplyId, Out) {
        let reply = self.reply();
        let receipt = Receipt { epoch: EPOCH, op_id: id, cycle_id, revision: 0 };
        (reply, self.post(TransferInput::Outcome { receipt, reply }))
    }

    pub fn mutation_outcome(&mut self, key: &str, outcome: OutboxOutcome) -> Out { self.post(TransferInput::MutationOutcome { key: key.into(), outcome }) }

    pub fn end_epoch(&mut self) -> Out {
        self.det.end_epoch(EPOCH, Cause::SessionReset);
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
    pub fn accepted_batch(&mut self, keys: &[&str], parts: u32) -> Vec<TransferId> {
        let reply = self.reply();
        self.post(TransferInput::AcceptBatch { epoch: EPOCH, items: keys.iter().map(|key| item(key, parts)).collect(), reply, now: Time(0) });
        let ids: Vec<TransferId> = keys.iter().map(|key| self.state().open_id(key).expect("an element")).collect();
        self.commit();
        for id in &ids {
            self.copy_done(*id);
        }
        self.commit();
        ids
    }

    /// `Mutating` of the first element of a batch: the owner has at least two slots.
    pub fn batch_mutating(&mut self, keys: &[&str]) -> Vec<TransferId> {
        let ids = self.accepted_batch(keys, 1);
        self.step_ok(ids[0], 1, None);
        self.commit();
        self.completed(ids[0]);
        self.commit();
        assert_eq!(self.stage(ids[0]), TransferStage::Mutating);
        ids
    }

    /// A `Ready` transfer behind a full slot: the network is up, the slot is held by another transfer.
    pub fn ready_behind(&mut self, key: &str) -> TransferId {
        self.accepted("busy:r:upload", 2);
        let id = self.accepted(key, 2);
        assert_eq!(self.stage(id), TransferStage::Ready);
        id
    }

    pub fn next_step(&self, id: TransferId) -> NextStep { self.t(id).next_step }

    pub fn fault(out: &Out, reply: ReplyId) -> Option<Fault> { out.replies(reply).into_iter().find_map(|answer| if let TransferAnswer::Err(fault) = answer { Some(fault) } else { None }) }

    /// The effects of the owner in a trace as a count of its outputs (a quiet step gives nothing).
    pub fn quiet(out: &Out) -> bool { out.runs.is_empty() && out.batches.is_empty() && out.outbox.is_empty() && out.rounds.is_empty() && out.timers == 0 }
}
