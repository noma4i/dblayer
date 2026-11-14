//! Harness of the owner-level tests of the `outbox` machine: the owner on `DetScheduler`, the twin of the reference, the book of what it sent.

use crate::schema::OnFailure;
use std::collections::BTreeMap;
use crate::owners::contract::{BatchId, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::session::{SessionCalls, SessionInput};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter};
use crate::spec::support::{new_log, Discard, Log, Sink};
use crate::owners::write::outbox::{self as o, CommitKind, NewOp, OpId, OpSource, Outbox, OutboxAnswer, OutboxEffect as Run, OutboxInput, OutboxPeers, OutboxState, Receipt, Stage, StoredOp, Why};
use crate::owners::write::transfer::{TransferInput, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, Effect, Effects, Epoch, EpochStamp, StateOwner, Time};
use std::any::Any;
use std::sync::PoisonError;
use crate::spec::support::committed_at;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RWhy { Rejected, Discarded }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum St { None, WaitingDeclaration, Submitting, Queued, PersistingAttempt, Sending, Landing, RollingBack(RWhy), MarkingFailed, MarkingRetry, Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RReason { Storage }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Policy { Rollback, Keep }

/// The fields of the owner state and of the input that the conditions of the table read.
#[derive(Clone, Copy, Debug)]
pub(super) struct Ctx {
    pub online: bool,
    pub row_free: bool,
    pub policy: Policy,
    /// `attempts + 1 < 40`.
    pub attempts_left: bool,
    pub transfer: bool,
}

impl Ctx {
    pub fn ready() -> Self { Self { online: true, row_free: true, policy: Policy::Rollback, attempts_left: true, transfer: false } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum In { EpochEnded(End), Closing }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Out { Confirmed, Rejected, Failed, Discarded, Ended(End) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum End { SessionEnded, Closed }


/// The slots of the peers and of the owner in every test: the peers first.
pub(super) const STORE_AT: usize = 0;
pub(super) const SESSION_AT: usize = 1;
pub(super) const TRANSFER_AT: usize = 2;
pub(super) const OUTBOX_AT: usize = 3;

/// What the owner gave out in a step, in the terms the rows of the table use: the executor effects (`Run`) and the inputs for the Store,
/// the Session and Transfer (`Send`), read from the typed inputs the `OutboxSpy` keeps. `Schedule` is a timer of the owner.
#[derive(Clone, Debug)]
pub(super) enum OutboxEffect {
    Store { op_id: Option<OpId>, batch: u64, kind: CommitKind },
    Send { op_id: OpId, request_id: u64, sequence: u64 },
    CancelRequest,
    Reply { reply: ReplyId, answer: OutboxAnswer },
    /// The owner asked the Store for the record of a receipt: `Twin` answers it from the batches the owner sent.
    Read { reply: ReplyId, op_id: OpId, cycle_id: u64 },
    TransferOutcome { outcome: o::OutboxOutcome },
    TransferOutcomes,
    Schedule,
    CancelTimer,
    Drained,
    Started { epoch: u64 },
    SnapshotRequest,
    RestoreClassified { epoch: u64 },
}

/// An input the owner sent to one of its peers.
#[derive(Clone, Debug)]
pub(super) enum Sent {
    Store(StoreBatch),
    Session(SessionInput),
    Transfer(TransferInput),
}

/// What the owner gave out in a step, typed, in the order of the effects.
#[derive(Clone, Debug)]
pub(super) enum Seen {
    Run(Run),
    Sent(Sent),
    Schedule(std::time::Duration),
    CancelTimer,
}

/// The owner `Outbox` with a log of everything its steps give out: `seen` is drained by the tests, `history` is never drained.
pub(super) struct OutboxSpy;

pub(super) struct OutboxSpyState {
    pub inner: OutboxState,
    pub seen: Log<Seen>,
    pub history: Log<Seen>,
}

impl StateOwner for OutboxSpy {
    const EPOCHED: bool = Outbox::EPOCHED;

    type State = OutboxSpyState;
    type Input = OutboxInput;
    type Effect = Run;

    fn step(state: &mut OutboxSpyState, input: OutboxInput, now: Time) -> Effects<Self> {
        let effects = Outbox::step(&mut state.inner, input, now);
        let mut seen = state.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let mut history = state.history.lock().unwrap_or_else(PoisonError::into_inner);
        let mut note = |found: Seen| { seen.push(found.clone()); history.push(found); };
        effects
            .into_iter()
            .map(|effect| match effect {
                Effect::Run(run) => {
                    note(Seen::Run(run.clone()));
                    Effect::Run(run)
                }
                Effect::Send(outbound) => {
                    let payload = &*outbound.payload as &dyn Any;
                    if let Some(StoreInput::Commit(batch)) = payload.downcast_ref::<StoreInput>() { note(Seen::Sent(Sent::Store(batch.clone()))); }
                    else if let Some(input) = payload.downcast_ref::<SessionInput>() { note(Seen::Sent(Sent::Session(input.clone()))); }
                    else if let Some(input) = payload.downcast_ref::<TransferInput>() { note(Seen::Sent(Sent::Transfer(input.clone()))); }
                    else if payload.downcast_ref::<StoreInput>().is_none() { panic!("a send of an unknown input: {:?}", outbound.payload); }
                    Effect::Send(outbound)
                }
                Effect::Schedule { id, after, input } => {
                    note(Seen::Schedule(after));
                    Effect::Schedule { id, after, input }
                }
                Effect::ScheduleAt { id, at, input } => {
                    note(Seen::Schedule(std::time::Duration::from_millis(at.0)));
                    Effect::ScheduleAt { id, at, input }
                }
                Effect::Backoff { id, policy, attempt, input } => {
                    note(Seen::Schedule(policy.delay_seeded(attempt, 0)));
                    Effect::Backoff { id, policy, attempt, input }
                }
                Effect::Cancel(id) => {
                    note(Seen::CancelTimer);
                    Effect::Cancel(id)
                }
                Effect::Broadcast(control) => Effect::Broadcast(control),
            })
            .collect()
    }

    fn epoch_started(epoch: Epoch) -> Option<OutboxInput> { Outbox::epoch_started(epoch) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { Outbox::epoch_ended(epoch, cause) }

    fn closing() -> OutboxInput { Outbox::closing() }
}

/// The op id of the first write of the batch that names one.
fn op_id_of(op: &StoreOp) -> Option<OpId> {
    match op {
        StoreOp::Intent { op_id, .. } | StoreOp::IntentOrigin { op_id, .. } | StoreOp::Effects { op_id, .. } | StoreOp::Attempt { op_id, .. } | StoreOp::Cause { op_id, .. } | StoreOp::Requeue { op_id } | StoreOp::RollBack { op_id } | StoreOp::RemoveIntent { op_id } | StoreOp::MarkError { op_id, .. } => Some(*op_id),
        _ => None,
    }
}

/// The kind of a batch from the ops it holds.
pub(super) fn kind_of(ops: &[StoreOp]) -> CommitKind {
    match ops.first() {
        Some(StoreOp::Intent { .. }) => return CommitKind::Intent,
        Some(StoreOp::RollBack { .. } | StoreOp::DiscardRestoreFailed { .. }) => return CommitKind::Rollback,
        _ => {}
    }
    if ops.iter().any(|op| matches!(op, StoreOp::MarkError { error: Some(_), .. })) { CommitKind::ErrorMark }
    else if ops.iter().any(|op| matches!(op, StoreOp::MarkError { .. })) { CommitKind::ClearMark }
    else if ops.iter().any(|op| matches!(op, StoreOp::Attempt { .. })) { CommitKind::Attempt }
    else {
        match ops.first() {
            Some(StoreOp::Requeue { .. }) => CommitKind::Requeue,
            Some(StoreOp::LocalRows { .. } | StoreOp::EphemeralRows { .. } | StoreOp::LocalPending { .. } | StoreOp::DetachFile { .. } | StoreOp::AttachFile { .. } | StoreOp::Planned { .. } | StoreOp::Show { .. }) => CommitKind::LocalWrite,
            Some(StoreOp::RemoveRows { .. }) => CommitKind::RemoveRow,
            _ => CommitKind::Land,
        }
    }
}

pub(super) fn lower(seen: Seen, state: Option<&OutboxState>) -> Option<OutboxEffect> {
    Some(match seen {
        Seen::Run(Run::Reply { reply, answer }) => OutboxEffect::Reply { reply, answer },
        Seen::Run(Run::Drained(_)) => OutboxEffect::Drained,
        Seen::Run(Run::Started { epoch }) => OutboxEffect::Started { epoch },
        Seen::Run(Run::SnapshotRequest { .. }) => OutboxEffect::SnapshotRequest,
        Seen::Run(Run::RestoreClassified { epoch }) => OutboxEffect::RestoreClassified { epoch },
        Seen::Run(Run::ReadOutcome { reply, op_id, cycle_id }) => OutboxEffect::Read { reply, op_id, cycle_id },
        Seen::Run(Run::Decode(_) | Run::Stale { .. }) => return None,
        Seen::Sent(Sent::Store(batch)) => {
            let kind = kind_of(&batch.ops);
            let op_id = matches!(kind, CommitKind::Intent | CommitKind::Attempt | CommitKind::Land | CommitKind::Rollback | CommitKind::ErrorMark | CommitKind::ClearMark | CommitKind::Requeue).then(|| batch.ops.iter().find_map(op_id_of)).flatten();
            OutboxEffect::Store { op_id, batch: batch.id, kind }
        }
        Seen::Sent(Sent::Session(SessionInput::Cancel { .. })) => OutboxEffect::CancelRequest,
        Seen::Sent(Sent::Session(SessionInput::Mutate { req_id, .. })) => {
            let operation = state.and_then(|state| state.ops.values().find(|operation| operation.request_id == req_id));
            OutboxEffect::Send { op_id: operation.map_or(req_id, |operation| operation.op_id), request_id: req_id, sequence: operation.map_or(0, |operation| operation.sequence) }
        }
        Seen::Sent(Sent::Session(_)) => return None,
        Seen::Sent(Sent::Transfer(TransferInput::MutationOutcomes { .. })) => OutboxEffect::TransferOutcomes,
        Seen::Sent(Sent::Transfer(TransferInput::MutationOutcome { outcome, .. })) => OutboxEffect::TransferOutcome { outcome },
        Seen::Sent(Sent::Transfer(_)) => return None,
        Seen::Schedule(_) => OutboxEffect::Schedule,
        Seen::CancelTimer => OutboxEffect::CancelTimer,
    })
}

pub(super) fn stale_marks(seen: &[Seen]) -> usize { seen.iter().filter(|found| matches!(found, Seen::Run(Run::Stale { .. }))).count() }

pub(super) fn timer_cancels(seen: &[Seen]) -> usize { seen.iter().filter(|found| matches!(found, Seen::CancelTimer)).count() }

pub(super) fn session_inputs(seen: &[Seen]) -> Vec<SessionInput> { seen.iter().filter_map(|found| match found { Seen::Sent(Sent::Session(input)) => Some(input.clone()), _ => None }).collect() }

pub(super) fn transfer_inputs(seen: &[Seen]) -> Vec<TransferInput> { seen.iter().filter_map(|found| match found { Seen::Sent(Sent::Transfer(input)) => Some(input.clone()), _ => None }).collect() }

pub(super) fn take_fx(log: &Log<Seen>, state: Option<&OutboxState>) -> Vec<OutboxEffect> { crate::spec::support::take(log).into_iter().filter_map(|seen| lower(seen, state)).collect() }

pub(super) fn state_of(det: &DetScheduler) -> Option<&OutboxState> { det.state(Address::<OutboxSpy>::new(OUTBOX_AT)).map(|spy: &OutboxSpyState| &spy.inner) }

pub(super) fn history_of(det: &DetScheduler) -> Vec<Seen> { det.state(Address::<OutboxSpy>::new(OUTBOX_AT)).map(|spy: &OutboxSpyState| crate::spec::support::snapshot(&spy.history)).unwrap_or_default() }

/// The owner with its three peers: the Store, the Session and Transfer keep what they receive.
pub(super) fn world(seed: u64) -> (DetScheduler, Log<Seen>, Address<OutboxSpy>) {
    let mut det = DetScheduler::new(seed);
    let peer = |det: &mut DetScheduler, name: &'static str| -> usize { det.add::<Sink<StoreWriter>>(name, new_log(), crate::spec::support::Silent).index() };
    let (store, session, transfer) = (peer(&mut det, "store"), det.add::<Sink<SessionCalls>>("session", new_log(), crate::spec::support::Silent).index(), det.add::<Sink<Transfers>>("transfer", new_log(), crate::spec::support::Silent).index());
    assert_eq!((store, session, transfer), (STORE_AT, SESSION_AT, TRANSFER_AT));
    let peers = OutboxPeers { store: Address::new(store), session: Some(Address::new(session)), transfer: Some(Address::new(transfer)) };
    let log = new_log();
    let addr = det.add::<OutboxSpy>("outbox", OutboxSpyState { inner: OutboxState::new(EPOCH, peers), seen: log.clone(), history: new_log() }, Discard);
    assert_eq!(addr.index(), OUTBOX_AT);
    (det, log, addr)
}

/// The rows of a server answer.
pub(super) fn server_rows() -> Vec<(String, Vec<crate::owners::store::RowValue>)> { vec![("Moment".into(), vec![crate::owners::store::RowValue { row_id: "r".into(), values: "{\"state\":\"server\"}".into() }])] }

/// The answer of the server to the operation `op_id`: the server rows and nothing else.
pub(super) fn response_of(op_id: OpId) -> OutboxInput { response_with(op_id, server_rows(), Vec::new()) }

pub(super) fn response_with(op_id: OpId, rows: Vec<(String, Vec<crate::owners::store::RowValue>)>, swaps: Vec<(String, String)>) -> OutboxInput { OutboxInput::Response { op_id, rows, swaps, stale: Vec::new(), result: None, missing: Vec::new(), errors: None } }

pub(super) fn response_errors(op_id: OpId, temporary: bool) -> OutboxInput { OutboxInput::Response { op_id, rows: Vec::new(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: Some(temporary) } }

/// The `Discard` of the row `r` of `Moment`, answered to `reply`.
pub(super) fn discard(action: Option<&str>, reply: ReplyId) -> OutboxInput { OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: action.map(Into::into), reply } }

pub(super) const EPOCH: u64 = 1;
pub(super) const SUBMIT_REPLY: ReplyId = 1;
pub(super) const DISCARD_REPLY: ReplyId = 800;
pub(super) const WAITER: ReplyId = 900;

pub(super) fn reject(reason: RReason) -> RejectReason {
    match reason {
        RReason::Storage => RejectReason::Storage("disk".into()),
    }
}

pub(super) fn stage_of(stage: Stage) -> St {
    match stage {
        Stage::Submitting => St::Submitting,
        Stage::Queued => St::Queued,
        Stage::Sending => St::Sending,
        Stage::Landing => St::Landing,
        Stage::RollingBack(Why::Rejected) => St::RollingBack(RWhy::Rejected),
        Stage::RollingBack(Why::Discarded) => St::RollingBack(RWhy::Discarded),
        Stage::WaitingDeclaration => St::WaitingDeclaration,
        Stage::PersistingAttempt => St::PersistingAttempt,
        Stage::MarkingFailed => St::MarkingFailed,
        Stage::MarkingRetry => St::MarkingRetry,
        Stage::Failed => St::Failed,
        Stage::Done => St::Done,
    }
}

pub(super) fn out_of(outcome: &o::OutboxOutcome) -> Out {
    match outcome {
        o::OutboxOutcome::Confirmed { .. } => Out::Confirmed,
        o::OutboxOutcome::Rejected { .. } => Out::Rejected,
        o::OutboxOutcome::Failed { .. } => Out::Failed,
        o::OutboxOutcome::Discarded => Out::Discarded,
        o::OutboxOutcome::Ended(crate::ffi::EndCause::Closed) => Out::Ended(End::Closed),
        o::OutboxOutcome::Ended(_) => Out::Ended(End::SessionEnded),
    }
}


pub(super) fn model_op(row: &str, policy: Policy, transfer: bool) -> NewOp { model_op_as(row, policy, transfer, "update") }

pub(super) fn model_op_as(row: &str, policy: Policy, transfer: bool, action: &str) -> NewOp {
    NewOp {
        model: "Moment".into(),
        row_id: row.into(),
        action: action.into(),
        on_failure: if policy == Policy::Keep { OnFailure::Keep } else { OnFailure::Rollback },
        origin: if transfer { OpSource::Transfer { transfer_id: 1, cycle_id: 1 } } else { OpSource::Api },
        expires_after: None,
        path: format!("/moments/{row}"),
        body: format!("{{\"row\":\"{row}\"}}"),
        effects: crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "Moment".into(), row_id: row.to_string(), values: "{\"state\":\"optimistic\"}".into() }], ..Default::default() },
    }
}

/// The owner with a recorder and the book of what it sent.
pub(super) struct Twin {
    pub(super) det: DetScheduler,
    pub(super) log: Log<Seen>,
    pub(super) batches: BTreeMap<OpId, BatchId>,
    pub(super) first_pair: BTreeMap<OpId, (RequestId, u64)>,
    /// The questions of the owner to Transfer that wait for an answer.
    asked: Vec<TransferInput>,
    pub(super) revision: i64,
    pub(super) reply: ReplyId,
    pub(super) follower: Option<OpId>,
    /// The effects of the last post.
    pub(super) last: Vec<OutboxEffect>,
    /// The Store batches of the last post, in the order sent.
    pub(super) store_batches: Vec<StoreBatch>,
    /// The ops of `store_batches`, in the order sent.
    pub(super) store_ops: Vec<StoreOp>,
    /// The pauses of the timers the last post set.
    pub(super) pauses: Vec<std::time::Duration>,
    /// Every Store batch of the last post with its id and ops, in the order sent.
    pub(super) stores: Vec<(BatchId, Vec<StoreOp>)>,
    /// The inputs the Session received in the last post, in the order sent.
    pub(super) session_sends: Vec<SessionInput>,
}

impl Twin {
    pub(super) fn bare() -> Self {
        let mut twin = Self::unannounced();
        twin.post(OutboxInput::Online(Online(true)));
        twin
    }

    pub(super) fn unannounced() -> Self {
        let (mut det, log, _) = world(7);
        det.start_epoch(EPOCH);
        det.run_until_quiet(10_000).expect("the owner is quiet");
        take_fx(&log, None);
        Self { det, log, batches: BTreeMap::new(), first_pair: BTreeMap::new(), asked: Vec::new(), revision: 10, reply: 10, follower: None, last: Vec::new(), store_batches: Vec::new(), store_ops: Vec::new(), pauses: Vec::new(), stores: Vec::new(), session_sends: Vec::new() }
    }

    /// The owner in the phase `Ready` with an empty queue.
    pub(super) fn new() -> Self {
        let mut twin = Self::bare();
        twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())], complete: false });
        twin.post(OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
        twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
        twin
    }

    pub(super) fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    pub(super) fn note(&mut self, effects: &[OutboxEffect]) {
        for effect in effects {
            match effect {
                OutboxEffect::Store { op_id: Some(id), batch, .. } => { self.batches.insert(*id, *batch); }
                OutboxEffect::Send { op_id, request_id, sequence } => { self.first_pair.entry(*op_id).or_insert((*request_id, *sequence)); }
                _ => {}
            }
        }
    }

    /// The server answers the operation `op_id` with the server rows.
    pub(super) fn respond(&mut self, op_id: OpId) -> Vec<OutboxEffect> { self.post(response_of(op_id)) }

    /// A call without an action waits for Transfer; the Transfer of the twin holds no transfer, so it names the smallest open operation of the row or none.
    pub(super) fn post(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        let mut effects = self.post_input(input);
        loop {
            let Some(asked) = self.asked.pop() else { self.last = effects.clone(); return effects };
            let TransferInput::SelectRow { rows, kind, reply, version, .. } = asked else { unreachable!("only SelectRow is kept in asked") };
            let asked_row = rows.first().cloned().unwrap_or_else(|| "r".to_string());
            let smallest = self.state().ops.values().find(|operation| operation.op.row_id == asked_row && operation.stage != Stage::Done).map(|operation| operation.op.action.clone());
            effects.extend(self.post_input(match smallest {
                Some(action) => OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: asked_row.clone(), action, kind, reply, version },
                None => OutboxInput::SelectedNone { reply, version },
            }));
        }
    }

    /// Posts `input` and returns the owner effects with every input sent to Transfer, without the auto-answer of `post`.
    pub(super) fn selector(&mut self, input: OutboxInput) -> (Vec<OutboxEffect>, Vec<TransferInput>) {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        let sends = transfer_inputs(&crate::spec::support::snapshot(&self.log));
        let effects = take_fx(&self.log, state_of(&self.det));
        (effects, sends)
    }

    fn post_input(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        let seen = crate::spec::support::snapshot(&self.log);
        self.store_batches = seen.iter().filter_map(|found| match found { Seen::Sent(Sent::Store(batch)) => Some(batch.clone()), _ => None }).collect();
        self.stores = self.store_batches.iter().map(|batch| (batch.id, batch.ops.clone())).collect();
        self.store_ops = self.store_batches.iter().flat_map(|batch| batch.ops.clone()).collect();
        self.pauses = seen.iter().filter_map(|found| match found { Seen::Schedule(after) => Some(*after), _ => None }).collect();
        self.asked.extend(transfer_inputs(&seen).into_iter().filter(|input| matches!(input, TransferInput::SelectRow { .. })));
        self.session_sends = session_inputs(&seen);
        let effects = take_fx(&self.log, state_of(&self.det));
        let effects = self.serve_reads(effects);
        self.note(&effects);
        effects
    }

    /// Every Store op the owner sent so far, in order.
    pub(super) fn sent_ops(&self) -> Vec<StoreOp> { self.history().into_iter().filter_map(|seen| match seen { Seen::Sent(Sent::Store(batch)) => Some(batch.ops), _ => None }).flatten().collect() }

    /// The Store double: a record of an operation is the first one written, a `Failed` one is replaced by the later outcome of its cycle.
    fn serve_reads(&self, effects: Vec<OutboxEffect>) -> Vec<OutboxEffect> {
        effects.into_iter().map(|effect| {
            let OutboxEffect::Read { reply, op_id, cycle_id } = effect else { return effect };
            let mut found: Option<o::OutboxOutcome> = None;
            for seen in self.history() {
                let Seen::Sent(Sent::Store(batch)) = seen else { continue };
                for op in batch.ops {
                    if let StoreOp::Outcome { owner: crate::owners::store::OutcomeOwner::Operation, id, cycle_id: cycle, outcome, .. } = op {
                        if (id, cycle) == (op_id, cycle_id) && found.as_ref().is_none_or(|known| matches!(known, o::OutboxOutcome::Failed { .. })) { found = Some(outcome); }
                    }
                }
            }
            OutboxEffect::Reply { reply, answer: found.map_or(OutboxAnswer::Err(o::Fault::NotFound), OutboxAnswer::Outcome) }
        }).collect()
    }

    pub(super) fn history(&self) -> Vec<Seen> { history_of(&self.det) }

    pub(super) fn state(&self) -> &OutboxState { state_of(&self.det).expect("the owner state") }

    /// An operation that is no longer in memory is `Done` when a batch removed its intent: that terminal batch held the record of its outcome.
    pub(super) fn stage(&self, id: OpId) -> Option<St> { self.state().stage_of(id).map(stage_of).or_else(|| self.history().iter().any(|seen| matches!(seen, Seen::Sent(Sent::Store(batch)) if batch.ops.iter().any(|op| matches!(op, StoreOp::RemoveIntent { op_id } if *op_id == id) || matches!(op, StoreOp::Outcome { owner: crate::owners::store::OutcomeOwner::Operation, id: written, .. } if *written == id)))).then_some(St::Done)) }

    pub(super) fn submit(&mut self, row: &str, policy: Policy, transfer: bool) -> OpId {
        let before = self.state().ops.keys().max().copied().unwrap_or(0);
        let reply = if before == 0 { SUBMIT_REPLY } else { self.reply += 1; self.reply };
        self.post(OutboxInput::Submit { epoch: EPOCH, op: model_op(row, policy, transfer), reply });
        *self.state().ops.keys().max().expect("the operation exists")
    }

    pub(super) fn submit_as(&mut self, row: &str, policy: Policy, transfer: bool, action: &str) {
        let reply = { self.reply += 1; self.reply };
        self.post(OutboxInput::Submit { epoch: EPOCH, op: model_op_as(row, policy, transfer, action), reply });
    }

    pub(super) fn committed(&mut self, id: OpId) -> Vec<OutboxEffect> { let batch = self.batches[&id]; self.commit_batch(batch) }

    pub(super) fn commit_batch(&mut self, batch: BatchId) -> Vec<OutboxEffect> {
        self.revision += 1;
        self.post(OutboxInput::Committed(committed_at(batch, self.revision, &["moments"])))
    }

    pub(super) fn rejected(&mut self, id: OpId, reason: RReason) -> Vec<OutboxEffect> { let batch = self.batches[&id]; self.reject_batch(batch, reason) }

    pub(super) fn reject_batch(&mut self, batch: BatchId, reason: RReason) -> Vec<OutboxEffect> {
        self.post(OutboxInput::Rejected(Rejected { batch, reason: reject(reason) }))
    }

    pub(super) fn receipt(&self, id: OpId, epoch: u64) -> Receipt { Receipt { epoch, op_id: id, cycle_id: 1, revision: 0, optimistic: None } }

    pub(super) fn waiter(&mut self, id: OpId) { self.post(OutboxInput::Outcome { receipt: self.receipt(id, EPOCH), reply: WAITER , discard_on_failure: false}); }
}

/// Brings the owner to `state` of the operation `id` (row `r`) under the conditions of `ctx`.
pub(super) fn reach(state: St, ctx: &Ctx) -> (Twin, OpId) {
    let mut twin = Twin::new();
    if state == St::None {
        return (twin, 0);
    }
    if state == St::WaitingDeclaration {
        let mut twin = Twin::bare();
        let stored = StoredOp { answered: None, op_id: 7, op: model_op("r", ctx.policy, ctx.transfer), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() };
        twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
        twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
        assert_eq!(twin.stage(7), Some(St::WaitingDeclaration), "the owner reached WaitingDeclaration");
        twin.waiter(7);
        return (twin, 7);
    }
    let policy = if matches!(state, St::Failed | St::MarkingFailed | St::MarkingRetry) { Policy::Keep } else { ctx.policy };
    let id = match state {
        St::Submitting | St::Queued => {
            if !ctx.row_free {
                let blocker = twin.submit("r", Policy::Rollback, false);
                twin.committed(blocker);
            }
            if !ctx.online {
                twin.post(OutboxInput::Online(Online(false)));
            }
            let id = twin.submit("r", policy, ctx.transfer);
            if state == St::Queued {
                twin.committed(id);
            }
            id
        }
        St::RollingBack(RWhy::Discarded) => {
            twin.post(OutboxInput::Online(Online(false)));
            let id = twin.submit("r", policy, ctx.transfer);
            twin.committed(id);
            twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: DISCARD_REPLY });
            twin.post(OutboxInput::Online(Online(ctx.online)));
            id
        }
        _ => {
            let id = twin.submit("r", policy, ctx.transfer);
            twin.committed(id);
            if !ctx.attempts_left {
                for _ in 0..39 {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
                    twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
                    twin.committed(id);
                }
            }
            match state {
                St::Landing => { twin.respond(id); }
                St::RollingBack(RWhy::Rejected) => { twin.post(OutboxInput::TransportError { op_id: id, temporary: false }); }
                St::PersistingAttempt => {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
                    twin.post(OutboxInput::RetryDue { op_id: id, cycle_id: 1 });
                }
                St::MarkingFailed => { twin.post(OutboxInput::TransportError { op_id: id, temporary: false }); }
                St::MarkingRetry => {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
                    twin.committed(id);
                    twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 77 });
                }
                St::Failed => {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
                    twin.committed(id);
                }
                St::Done => {
                    twin.respond(id);
                    twin.committed(id);
                }
                _ => {}
            }
            if !ctx.online {
                twin.post(OutboxInput::Online(Online(false)));
            }
            id
        }
    };
    if matches!(state, St::Landing | St::RollingBack(_)) {
        let follower = twin.submit("r", Policy::Rollback, false);
        twin.committed(follower);
        twin.follower = Some(follower);
    }
    assert_eq!(twin.stage(id), Some(state).filter(|state| *state != St::Done).or(Some(St::Done)), "the owner reached {state:?}");
    if state != St::Done {
        twin.waiter(id);
    }
    (twin, id)
}




pub(super) fn input_of(twin: &mut Twin, _id: OpId, _ctx: &Ctx, input: In) -> Vec<OutboxEffect> {
    match input {
        In::EpochEnded(end) => {
            take_fx(&twin.log, state_of(&twin.det));
            twin.det.end_epoch(EPOCH, match end { End::SessionEnded => Cause::SessionReset, End::Closed => Cause::Close });
            twin.run();
            take_fx(&twin.log, state_of(&twin.det))
        }
        In::Closing => {
            take_fx(&twin.log, state_of(&twin.det));
            twin.det.close();
            twin.run();
            take_fx(&twin.log, state_of(&twin.det))
        }
    }
}


pub(super) fn ready() -> Ctx { Ctx::ready() }

pub(super) fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }









pub(super) fn sends(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Send { .. })).count() }

pub(super) fn kinds(effects: &[OutboxEffect]) -> Vec<CommitKind> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Store { kind, .. } => Some(*kind), _ => None }).collect()
}

pub(super) fn answers(effects: &[OutboxEffect], reply: u64) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Reply { reply: found, answer } if *found == reply => Some(answer.clone()), _ => None }).collect()
}

pub(super) fn drained(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count() }

pub(super) fn stored(op_id: u64, row: &str) -> StoredOp {
    StoredOp { answered: None, op_id, op: model_op(row, Policy::Rollback, false), created_at: 0, error_mark: false, pending: None, attempts: 1, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() }
}

pub(super) fn declare(twin: &mut Twin) -> Vec<OutboxEffect> { twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())], complete: false }) }

/// Whether a Store op of the last post matches the pattern.
macro_rules! has_op {
    ($twin:expr, $($pattern:tt)+) => { $twin.store_ops.iter().any(|op| matches!(op, $($pattern)+)) };
}
pub(super) use has_op;

/// The Exhausted mark of the operation `id` in the last post: the saved error is `Exhausted`, or the `response_too_large` kind of the cause.
pub(super) fn assert_exhausted_mark(twin: &Twin, id: OpId, response_too_large: bool) {
    let expected = twin.state().ops[&id].error.as_ref().unwrap();
    if response_too_large { assert_eq!(serde_json::from_str::<serde_json::Value>(expected).unwrap()["kind"], "response_too_large:64"); }
    else { assert_eq!(expected, "Exhausted"); }
    assert!(has_op!(twin, StoreOp::MarkError { op_id, error: Some(error) } if *op_id == id && error == expected), "{:?}", twin.store_ops);
}
