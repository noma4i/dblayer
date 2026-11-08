//! Harness of the owner-level tests of the `outbox` machine: the owner on `DetScheduler`, the twin of the reference, the book of what it sent.

use crate::schema::OnFailure;
use std::collections::BTreeMap;
use super::ref_outbox::{step_of, Ctx, Eff, Fault as RFault, In, Obs, Out, Policy, Reason as RReason, St, Step, Why as RWhy};
use crate::owners::contract::{BatchId, Committed, ModelsReset, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::session::SessionCalls;
use crate::owners::store::StoreWriter;
use crate::spec::support::{new_log, number_after, Discard, Emitted, Log, Sink, Spy, SpyState};
use crate::owners::write::outbox::{self as o, CommitKind, Fault, NewOp, OpId, OpSource, Outbox, OutboxAnswer, OutboxEffect as Run, OutboxInput, OutboxPeers, OutboxState, Receipt, Stage, Why};
use crate::owners::write::transfer::Transfers;
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};
/// The slots of the peers and of the owner in every test: the peers first.
pub(super) const STORE_AT: usize = 0;
pub(super) const SESSION_AT: usize = 1;
pub(super) const TRANSFER_AT: usize = 2;
pub(super) const OUTBOX_AT: usize = 3;

/// What the owner gave out in a step, in the terms the rows of the table use: the executor effects (`Run`) and the inputs for the Store,
/// the Session and Transfer (`Send`), read from the typed `Debug` form the `Spy` keeps. `Schedule` is a timer of the owner.
#[derive(Clone, Debug)]
pub(super) enum OutboxEffect {
    Store { op_id: Option<OpId>, batch: u64, kind: CommitKind, generation: u64 },
    Send { op_id: OpId, request_id: u64, sequence: u64 },
    CancelRequest { op_id: OpId },
    Reply { reply: ReplyId, answer: OutboxAnswer },
    TransferOutcome { outcome: o::OutboxOutcome },
    TransferOutcomes { input: String },
    Schedule,
    Drained,
    Started { epoch: u64 },
    SnapshotRequest,
    RestoreClassified { epoch: u64 },
    Copy { id: u64, source: String },
}

pub(super) fn told(effects: &[OutboxEffect]) -> Vec<Eff> {
    effects.iter().filter_map(|effect| match effect {
        OutboxEffect::Started { .. } => Some(Eff::Started),
        OutboxEffect::SnapshotRequest => Some(Eff::SnapshotRequest),
        OutboxEffect::RestoreClassified { .. } => Some(Eff::RestoreClassified),
        _ => None,
    }).collect()
}

/// The kind of a batch from the ops it holds (`Debug` form of the typed ops).
pub(super) fn kind_in(input: &str) -> CommitKind {
    if input.contains("ops: [Intent {") { CommitKind::Intent }
    else if input.contains("ops: [RollBack {") || input.contains("ops: [DiscardRestoreFailed {") { CommitKind::Rollback }
    else if input.contains("MarkError { op_id") && input.contains("error: Some(") { CommitKind::ErrorMark }
    else if input.contains("MarkError { op_id") { CommitKind::ClearMark }
    else if input.contains("Attempt {") { CommitKind::Attempt }
    else if input.contains("ops: [Requeue {") { CommitKind::Requeue }
    else if input.contains("ops: [LocalRows {") || input.contains("ops: [EphemeralRows {") || input.contains("ops: [LocalPending {") { CommitKind::LocalWrite }
    else if input.contains("ops: [RemoveRows {") { CommitKind::RemoveRow }
    else { CommitKind::Land }
}

pub(super) fn outcome_named(text: &str) -> o::OutboxOutcome {
    if text.contains("Confirmed") { o::OutboxOutcome::Confirmed { revision: 0, result: None } }
    else if text.contains("Rejected") { o::OutboxOutcome::Rejected { error: String::new() } }
    else if text.contains("Failed") { o::OutboxOutcome::Failed { error: String::new() } }
    else if text.contains("Discarded") { o::OutboxOutcome::Discarded }
    else { o::OutboxOutcome::Interrupted }
}

pub(super) fn lower(emitted: Emitted<Run>, state: Option<&OutboxState>) -> Option<OutboxEffect> {
    Some(match emitted {
        Emitted::Run(Run::Reply { reply, answer }) => OutboxEffect::Reply { reply, answer },
        Emitted::Run(Run::Drained(_)) => OutboxEffect::Drained,
        Emitted::Run(Run::Started { epoch }) => OutboxEffect::Started { epoch },
        Emitted::Run(Run::SnapshotRequest { .. }) => OutboxEffect::SnapshotRequest,
        Emitted::Run(Run::RestoreClassified { epoch }) => OutboxEffect::RestoreClassified { epoch },
        Emitted::Run(Run::Copy { id, source }) => OutboxEffect::Copy { id, source },
        Emitted::Run(Run::Decode(_) | Run::Stale { .. }) => return None,
        Emitted::Send { to: STORE_AT, input } => {
            let kind = kind_in(&input);
            let op_id = matches!(kind, CommitKind::Intent | CommitKind::Attempt | CommitKind::Land | CommitKind::Rollback | CommitKind::ErrorMark | CommitKind::ClearMark | CommitKind::Requeue).then(|| number_after(&input, "op_id: ")).flatten();
            let generation = number_after(&input, "\"Moment\": ").unwrap_or(0);
            OutboxEffect::Store { op_id, batch: number_after(&input, "id: ")?, kind, generation }
        }
        Emitted::Send { to: SESSION_AT, input } if input.starts_with("Release") => return None,
        Emitted::Send { to: SESSION_AT, input } if input.starts_with("Cancel") => {
            let request_id = number_after(&input, "req_id: ")?;
            let operation = state.and_then(|state| state.ops.values().find(|operation| operation.request_id == request_id));
            OutboxEffect::CancelRequest { op_id: operation.map_or(request_id, |operation| operation.op_id) }
        }
        Emitted::Send { to: SESSION_AT, input } => {
            let request_id = number_after(&input, "req_id: ")?;
            let operation = state.and_then(|state| state.ops.values().find(|operation| operation.request_id == request_id));
            OutboxEffect::Send { op_id: operation.map_or(request_id, |operation| operation.op_id), request_id, sequence: operation.map_or(0, |operation| operation.sequence) }
        }
        Emitted::Send { to: TRANSFER_AT, input } if input.starts_with("MutationOutcomes") => OutboxEffect::TransferOutcomes { input },
        Emitted::Send { to: TRANSFER_AT, input } => OutboxEffect::TransferOutcome { outcome: outcome_named(&input) },
        Emitted::Send { to, .. } => panic!("a send to the slot {to}"),
        Emitted::Schedule { .. } => OutboxEffect::Schedule,
        Emitted::Cancel(_) | Emitted::Broadcast(_) => return None,
    })
}

pub(super) fn take_fx(log: &Log<Emitted<Run>>, state: Option<&OutboxState>) -> Vec<OutboxEffect> { crate::spec::support::take(log).into_iter().filter_map(|emitted| lower(emitted, state)).collect() }

pub(super) fn state_of(det: &DetScheduler) -> Option<&OutboxState> { det.state(Address::<Spy<Outbox>>::new(OUTBOX_AT)).map(|spy: &SpyState<Outbox>| &spy.inner) }

/// The owner with its three peers: the Store, the Session and Transfer keep what they receive.
pub(super) fn world(seed: u64) -> (DetScheduler, Log<Emitted<Run>>, Address<Spy<Outbox>>) {
    let mut det = DetScheduler::new(seed);
    let peer = |det: &mut DetScheduler, name: &'static str| -> usize { det.add::<Sink<StoreWriter>>(name, new_log(), crate::spec::support::Silent).index() };
    let (store, session, transfer) = (peer(&mut det, "store"), det.add::<Sink<SessionCalls>>("session", new_log(), crate::spec::support::Silent).index(), det.add::<Sink<Transfers>>("transfer", new_log(), crate::spec::support::Silent).index());
    assert_eq!((store, session, transfer), (STORE_AT, SESSION_AT, TRANSFER_AT));
    let peers = OutboxPeers { store: Address::new(store), session: Some(Address::new(session)), transfer: Some(Address::new(transfer)) };
    let log = new_log();
    let addr = det.add::<Spy<Outbox>>("outbox", SpyState::new(OutboxState::new(EPOCH, peers), &log), Discard);
    assert_eq!(addr.index(), OUTBOX_AT);
    (det, log, addr)
}

/// The rows of a server answer.
pub(super) fn server_rows() -> Vec<(String, Vec<crate::owners::store::RowValue>)> { vec![("Moment".into(), vec![crate::owners::store::RowValue { row_id: "r".into(), values: "{\"state\":\"server\"}".into() }])] }

pub(super) const EPOCH: u64 = 1;
pub(super) const SUBMIT_REPLY: ReplyId = 1;
pub(super) const DISCARD_REPLY: ReplyId = 800;
pub(super) const WAITER: ReplyId = 900;
pub(super) const NEW_GENERATION: u64 = 5;

pub(super) fn reject(reason: RReason) -> RejectReason {
    match reason {
        RReason::StaleGeneration => RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: NEW_GENERATION },
        RReason::NoSession => RejectReason::NoSession,
        RReason::Storage => RejectReason::Storage("disk".into()),
        RReason::Closed => RejectReason::Closed,
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
        o::OutboxOutcome::Interrupted => Out::Interrupted,
    }
}

pub(super) fn fault_of(fault: &Fault) -> RFault {
    match fault {
        Fault::NoSession => RFault::NoSession,
        Fault::NotFound => RFault::NotFound,
        Fault::InFlight => RFault::InFlight,
        Fault::Interrupted => RFault::Interrupted,
        Fault::Closed => RFault::Closed,
        Fault::RestoreFailed => RFault::RestoreFailed,
        Fault::StateLost => RFault::StateLost,
        Fault::Internal => RFault::Internal,
        Fault::Invalid | Fault::Copy(_) => panic!("a fault of Transfer"),
        Fault::Rejected(RejectReason::NoSession) => RFault::Reason(RReason::NoSession),
        Fault::Rejected(RejectReason::StaleGeneration { .. }) => RFault::Reason(RReason::StaleGeneration),
        Fault::Rejected(RejectReason::Storage(_)) => RFault::Reason(RReason::Storage),
        Fault::Rejected(RejectReason::Closed) => RFault::Reason(RReason::Closed),
        Fault::Rejected(RejectReason::Invalid) => panic!("the Outbox batch carries no rows"),
    }
}

pub(super) fn model_op(row: &str, policy: Policy, transfer: bool) -> NewOp {
    NewOp {
        model: "Moment".into(),
        row_id: row.into(),
        action: "update".into(),
        on_failure: if policy == Policy::Keep { OnFailure::Keep } else { OnFailure::Rollback },
        origin: if transfer { OpSource::Transfer { transfer_id: 1, cycle_id: 1 } } else { OpSource::Api },
        created_at: 0, expires_after: None,
        path: format!("/moments/{row}"),
        body: format!("{{\"row\":\"{row}\"}}"),
        effects: crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "Moment".into(), row_id: row.to_string(), values: "{\"state\":\"optimistic\"}".into() }], ..Default::default() },
    }
}

/// The owner with a recorder and the book of what it sent.
pub(super) struct Twin {
    pub(super) det: DetScheduler,
    pub(super) log: Log<Emitted<Run>>,
    /// What the owner gave out when it restored the intents (read by `open_check`).
    pub(super) kept: Vec<OutboxEffect>,
    pub(super) batches: BTreeMap<OpId, BatchId>,
    pub(super) first_pair: BTreeMap<OpId, (RequestId, u64)>,
    pub(super) revision: i64,
    pub(super) reply: ReplyId,
    pub(super) follower: Option<OpId>,
    /// The typed text of the last inputs the Store received (`StoreOp` names).
    pub(super) store_text: String,
    pub(super) session_text: String,
}

impl Twin {
    /// The owner in the phase `Restoring`: nothing restored yet.
    pub(super) fn bare() -> Self {
        let (mut det, log, _) = world(7);
        det.start_epoch(EPOCH);
        det.run_until_quiet(10_000).expect("the owner is quiet");
        take_fx(&log, None);
        Self { det, log, kept: Vec::new(), batches: BTreeMap::new(), first_pair: BTreeMap::new(), revision: 10, reply: 10, follower: None, store_text: String::new(), session_text: String::new() }
    }

    /// The owner in the phase `Ready` with an empty queue.
    pub(super) fn new() -> Self {
        let mut twin = Self::bare();
        twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into()), ("Moment".into(), "create".into()), ("Moment".into(), "destroy".into())] });
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

    pub(super) fn post(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        self.store_text = crate::spec::support::snapshot(&self.log).into_iter().filter_map(|emitted| match emitted { Emitted::Send { to: STORE_AT, input } => Some(input), _ => None }).collect::<Vec<_>>().join("\n");
        self.session_text = crate::spec::support::snapshot(&self.log).into_iter().filter_map(|emitted| match emitted { Emitted::Send { to: SESSION_AT, input } => Some(input), _ => None }).collect::<Vec<_>>().join("\n");
        let effects = take_fx(&self.log, state_of(&self.det));
        self.note(&effects);
        effects
    }

    pub(super) fn state(&self) -> &OutboxState { state_of(&self.det).expect("the owner state") }

    pub(super) fn stage(&self, id: OpId) -> Option<St> { self.state().stage_of(id).map(stage_of) }

    pub(super) fn submit(&mut self, row: &str, policy: Policy, transfer: bool) -> OpId {
        let before = self.state().ops.keys().max().copied().unwrap_or(0);
        let reply = if before == 0 { SUBMIT_REPLY } else { self.reply += 1; self.reply };
        self.post(OutboxInput::Submit { epoch: EPOCH, op: model_op(row, policy, transfer), reply });
        *self.state().ops.keys().max().expect("the operation exists")
    }

    pub(super) fn committed(&mut self, id: OpId) -> Vec<OutboxEffect> {
        self.revision += 1;
        let batch = self.batches[&id];
        self.post(OutboxInput::Committed(Committed { batch, revision: self.revision, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }))
    }

    pub(super) fn rejected(&mut self, id: OpId, reason: RReason) -> Vec<OutboxEffect> {
        let batch = self.batches[&id];
        self.post(OutboxInput::Rejected(Rejected { batch, reason: reject(reason) }))
    }

    pub(super) fn receipt(&self, id: OpId, epoch: u64) -> Receipt { Receipt { epoch, op_id: id, cycle_id: 1, revision: 0, optimistic: None, landing: None } }

    pub(super) fn waiter(&mut self, id: OpId) { self.post(OutboxInput::Outcome { receipt: self.receipt(id, EPOCH), reply: WAITER }); }
}

/// Brings the owner to `state` of the operation `id` (row `r`) under the conditions of `ctx`.
pub(super) fn reach(state: St, ctx: &Ctx) -> (Twin, OpId) {
    let mut twin = Twin::new();
    if state == St::None {
        return (twin, 0);
    }
    let policy = if state == St::Failed { Policy::Keep } else { ctx.policy };
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
                St::Landing => { twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }); }
                St::RollingBack(RWhy::Rejected) => { twin.post(OutboxInput::TransportError { op_id: id, temporary: false }); }
                St::Failed => {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: false });
                    twin.committed(id);
                }
                St::Done => {
                    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
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

/// What the owner did for the operation `id`, in the terms of the reference.
pub(super) struct Seen {
    pub(super) next: Option<St>,
    pub(super) obs: Vec<Obs>,
    pub(super) eff: Vec<Eff>,
}

pub(super) fn translate(twin: &Twin, id: OpId, effects: Vec<OutboxEffect>, scheduled: bool, resend: bool, outcome_call: bool) -> Seen {
    let mut obs = Vec::new();
    let mut eff = Vec::new();
    for effect in effects {
        match effect {
            OutboxEffect::Store { op_id: Some(op), kind, generation, .. } if op == id => eff.push(Eff::Store(match (kind, generation > 0) {
                (CommitKind::Attempt, _) => super::ref_outbox::Commit::Attempt,
                (CommitKind::Intent, false) => super::ref_outbox::Commit::Intent,
                (CommitKind::Intent, true) => super::ref_outbox::Commit::IntentNewBasis,
                (CommitKind::Land, false) => super::ref_outbox::Commit::Land,
                (CommitKind::Land, true) => super::ref_outbox::Commit::LandNewBasis,
                (CommitKind::Rollback, false) => super::ref_outbox::Commit::Rollback,
                (CommitKind::Rollback, true) => super::ref_outbox::Commit::RollbackNewBasis,
                (CommitKind::ErrorMark, _) => super::ref_outbox::Commit::ErrorMark,
                (CommitKind::ClearMark, _) => super::ref_outbox::Commit::ClearMark,
                (CommitKind::Requeue, _) => super::ref_outbox::Commit::Requeue,
                (CommitKind::LocalWrite | CommitKind::RemoveRow, _) => panic!("a local write in an operation row"),
            })),
            OutboxEffect::Store { .. } => {}
            OutboxEffect::Send { op_id, request_id, sequence } if op_id == id => {
                eff.push(if resend && twin.first_pair.get(&id) == Some(&(request_id, sequence)) { Eff::SendSamePair } else { Eff::Send });
            }
            OutboxEffect::Send { op_id, .. } if Some(op_id) == twin.follower => eff.push(Eff::SendNextOfRow),
            OutboxEffect::Send { .. } => {}
            OutboxEffect::CancelRequest { op_id } if op_id == id => eff.push(Eff::Cancel),
            OutboxEffect::CancelRequest { .. } => {}
            OutboxEffect::Reply { reply, answer } => {
                if reply == SUBMIT_REPLY && reply != WAITER && matches!(answer, OutboxAnswer::Err(Fault::Interrupted)) { continue; }
                if reply == DISCARD_REPLY && matches!(answer, OutboxAnswer::Ok | OutboxAnswer::Err(Fault::Interrupted)) { continue; }
                match answer {
                    OutboxAnswer::Receipt(_) => obs.push(Obs::Receipt),
                    OutboxAnswer::Outcome(outcome) if outcome_call => { let _ = outcome; obs.push(Obs::OutcomeNow) }
                    OutboxAnswer::Outcome(outcome) => obs.push(Obs::Outcome(out_of(&outcome))),
                    OutboxAnswer::Ok => obs.push(Obs::DiscardOk),
                    OutboxAnswer::Resolved(_) | OutboxAnswer::Written { .. } | OutboxAnswer::Transfer { .. } => {}
                    OutboxAnswer::Err(fault) => obs.push(Obs::Fails(fault_of(&fault))),
                }
            }
            OutboxEffect::Schedule => {}
            OutboxEffect::TransferOutcome { outcome, .. } => eff.push(Eff::TransferOutcome(out_of(&outcome))),
            OutboxEffect::TransferOutcomes { .. } => {}
            OutboxEffect::Drained => eff.push(Eff::Drained),
            OutboxEffect::Copy { .. } => {}
            OutboxEffect::Started { .. } => eff.push(Eff::Started),
            OutboxEffect::SnapshotRequest => eff.push(Eff::SnapshotRequest),
            OutboxEffect::RestoreClassified { .. } => eff.push(Eff::RestoreClassified),
        }
    }
    if scheduled {
        eff.push(Eff::ScheduleTick);
    }
    Seen { next: twin.stage(id), obs, eff }
}

/// The observable of the reference that an owner effect can show; the rest is read in the API tests and the property tests.
pub(super) fn visible(step: &Step, follower: bool) -> (Vec<Obs>, Vec<Eff>) {
    let obs = step.obs.iter().copied().filter(|obs| matches!(obs, Obs::Receipt | Obs::Fails(_) | Obs::Outcome(_) | Obs::DiscardOk | Obs::OutcomeNow)).collect();
    let mut eff: Vec<Eff> = Vec::new();
    for effect in &step.eff {
        let effect = match effect {
            Eff::ScheduleBackoffSend | Eff::ScheduleLanding => Eff::ScheduleTick,
            other => *other,
        };
        if effect == Eff::ScheduleTick && eff.contains(&Eff::ScheduleTick) { continue; }
        if effect == Eff::SendNextOfRow && !follower { continue; }
        eff.push(effect);
    }
    (obs, eff)
}

pub(super) fn input_of(twin: &mut Twin, id: OpId, ctx: &Ctx, input: In) -> Vec<OutboxEffect> {
    let other = id + 1000;
    let op_id = if ctx.ours { id } else { other };
    let epoch = if ctx.epoch_current { EPOCH } else { EPOCH + 1 };
    match input {
        In::Submit => twin.post(OutboxInput::Submit { epoch, op: model_op("r", ctx.policy, ctx.transfer), reply: SUBMIT_REPLY }),
        In::Committed => twin.committed(id),
        In::Rejected(reason) => twin.rejected(id, reason),
        In::RowFreed => {
            let blocker = 1;
            twin.post(OutboxInput::Response { op_id: blocker, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
            twin.committed(blocker)
        }
        In::Online(on) => twin.post(OutboxInput::Online(Online(on))),
        In::Discard => twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), action: None, reply: 801 }),
        In::Retry => twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply: 802 }),
        In::Outcome => twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, epoch), reply: 803 }),
        In::ModelsReset => twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec![if ctx.owns_model { "Moment".into() } else { "Chat".into() }] })),
        In::Response => twin.post(OutboxInput::Response { op_id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }),
        In::TransportError { temporary } => twin.post(OutboxInput::TransportError { op_id, temporary }),
        In::Dropped => twin.post(OutboxInput::Dropped { op_id }),
        In::Tick => twin.post(OutboxInput::RetryDue { op_id, cycle_id: 1 }),
        In::EpochEnded => {
            take_fx(&twin.log, state_of(&twin.det));
            twin.det.end_epoch(EPOCH, Cause::SessionReset);
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

pub(super) fn row_check(state: St, ctx: Ctx, input: In) {
    let after = if matches!(input, In::Online(true)) { Ctx { online: true, ..ctx } } else { ctx };
    let before = if matches!(input, In::Online(true)) { Ctx { online: false, ..ctx } } else { ctx };
    let want = step_of(state, &after, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}"));
    let (mut twin, id) = reach(state, &before);
    let ctx = before;
    let ops_before = twin.det.trace().len();
    let effects = input_of(&mut twin, id, &ctx, input);
    let scheduled = twin.det.trace()[ops_before..].iter().any(|entry| entry.effects.contains("Schedule"));
    let resend = matches!(input, In::Tick);
    let outcome_call = matches!(input, In::Outcome);
    let id_of_row = if state == St::None { *twin.state().ops.keys().max().unwrap_or(&0) } else { id };
    let seen = translate(&twin, id_of_row, effects, scheduled, resend, outcome_call);
    let (obs, eff) = visible(&want, twin.follower.is_some());
    let next = seen.next.unwrap_or(if matches!(want.next, St::None | St::Done) { want.next } else { St::None });
    assert_eq!(next, want.next, "{state:?} x {input:?}: the next state");
    assert_eq!(seen.obs, obs, "{state:?} x {input:?}: the replies");
    assert_eq!(seen.eff, eff, "{state:?} x {input:?}: the effects");
}

pub(super) fn ready() -> Ctx { Ctx::ready() }

pub(super) fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

pub(super) fn busy() -> Ctx { Ctx { row_free: false, ..Ctx::ready() } }

pub(super) fn stranger() -> Ctx { Ctx { ours: false, ..Ctx::ready() } }

pub(super) fn keep() -> Ctx { Ctx { policy: Policy::Keep, ..Ctx::ready() } }

pub(super) fn transfer() -> Ctx { Ctx { transfer: true, ..Ctx::ready() } }

pub(super) fn stale() -> Ctx { Ctx { epoch_current: false, ..Ctx::ready() } }

pub(super) fn rolling() -> St { St::RollingBack(RWhy::Rejected) }

pub(super) fn discarding() -> St { St::RollingBack(RWhy::Discarded) }


pub(super) fn sends_of(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Send { .. })).count() }
