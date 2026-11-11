//! Harness of the owner-level tests of the `outbox` machine: the owner on `DetScheduler`, the twin of the reference, the book of what it sent.

use crate::schema::OnFailure;
use std::collections::BTreeMap;
use crate::owners::contract::{BatchId, Committed, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::session::SessionCalls;
use crate::owners::store::StoreWriter;
use crate::spec::support::{new_log, number_after, Discard, Emitted, Log, Sink, Spy, SpyState};
use crate::owners::write::outbox::{self as o, CommitKind, NewOp, OpId, OpSource, Outbox, OutboxAnswer, OutboxEffect as Run, OutboxInput, OutboxPeers, OutboxState, Receipt, Stage, StoredOp, Why};
use crate::owners::write::transfer::Transfers;
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RWhy { Rejected, Discarded }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum St { None, WaitingDeclaration, Submitting, Queued, PersistingAttempt, Sending, Landing, RollingBack(RWhy), MarkingFailed, MarkingRetry, Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RReason { StaleGeneration, NoSession, Storage }

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
pub(super) enum In { Committed, EpochEnded(End), Closing }

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
/// the Session and Transfer (`Send`), read from the typed `Debug` form the `Spy` keeps. `Schedule` is a timer of the owner.
#[derive(Clone, Debug)]
pub(super) enum OutboxEffect {
    Store { op_id: Option<OpId>, batch: u64, kind: CommitKind, generation: u64 },
    Send { op_id: OpId, request_id: u64, sequence: u64 },
    CancelRequest,
    Reply { reply: ReplyId, answer: OutboxAnswer },
    TransferOutcome { outcome: o::OutboxOutcome },
    TransferOutcomes,
    Schedule,
    Drained,
    Started { epoch: u64 },
    SnapshotRequest,
    RestoreClassified { epoch: u64 },
}


/// The kind of a batch from the ops it holds (`Debug` form of the typed ops).
pub(super) fn kind_in(input: &str) -> CommitKind {
    if input.contains("ops: [Intent {") { CommitKind::Intent }
    else if input.contains("ops: [RollBack {") || input.contains("ops: [DiscardRestoreFailed {") { CommitKind::Rollback }
    else if input.contains("MarkError { op_id") && input.contains("error: Some(") { CommitKind::ErrorMark }
    else if input.contains("MarkError { op_id") { CommitKind::ClearMark }
    else if input.contains("Attempt {") { CommitKind::Attempt }
    else if input.contains("ops: [Requeue {") { CommitKind::Requeue }
    else if input.contains("ops: [LocalRows {") || input.contains("ops: [EphemeralRows {") || input.contains("ops: [LocalPending {") || input.contains("ops: [DetachFile {") || input.contains("ops: [AttachFile {") || input.contains("ops: [Planned {") || input.contains("ops: [Show {") { CommitKind::LocalWrite }
    else if input.contains("ops: [RemoveRows {") { CommitKind::RemoveRow }
    else { CommitKind::Land }
}

pub(super) fn outcome_named(text: &str) -> o::OutboxOutcome {
    if text.contains("Confirmed") { o::OutboxOutcome::Confirmed { revision: 0, result: None, row_id: None } }
    else if text.contains("Rejected") { o::OutboxOutcome::Rejected { error: String::new() } }
    else if text.contains("Failed") { o::OutboxOutcome::Failed { error: String::new() } }
    else if text.contains("Ended(Closed)") { o::OutboxOutcome::Ended(crate::ffi::EndCause::Closed) }
    else if text.contains("Ended(SessionEnded)") { o::OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded) }
    else if text.contains("Discarded") { o::OutboxOutcome::Discarded }
    else { panic!("unknown outbox outcome: {text}") }
}

pub(super) fn lower(emitted: Emitted<Run>, state: Option<&OutboxState>) -> Option<OutboxEffect> {
    Some(match emitted {
        Emitted::Run(Run::Reply { reply, answer }) => OutboxEffect::Reply { reply, answer },
        Emitted::Run(Run::Drained(_)) => OutboxEffect::Drained,
        Emitted::Run(Run::Started { epoch }) => OutboxEffect::Started { epoch },
        Emitted::Run(Run::SnapshotRequest { .. }) => OutboxEffect::SnapshotRequest,
        Emitted::Run(Run::RestoreClassified { epoch }) => OutboxEffect::RestoreClassified { epoch },
        Emitted::Run(Run::Decode(_) | Run::Stale { .. }) => return None,
        Emitted::Send { to: STORE_AT, input } => {
            let kind = kind_in(&input);
            let op_id = matches!(kind, CommitKind::Intent | CommitKind::Attempt | CommitKind::Land | CommitKind::Rollback | CommitKind::ErrorMark | CommitKind::ClearMark | CommitKind::Requeue).then(|| number_after(&input, "op_id: ")).flatten();
            let generation = number_after(&input, "\"Moment\": ").unwrap_or(0);
            OutboxEffect::Store { op_id, batch: number_after(&input, "id: ")?, kind, generation }
        }
        Emitted::Send { to: SESSION_AT, input } if input.starts_with("Release") => return None,
        Emitted::Send { to: SESSION_AT, input } if input.starts_with("Cancel") => {
            number_after(&input, "req_id: ")?;
            OutboxEffect::CancelRequest
        }
        Emitted::Send { to: SESSION_AT, input } => {
            let request_id = number_after(&input, "req_id: ")?;
            let operation = state.and_then(|state| state.ops.values().find(|operation| operation.request_id == request_id));
            OutboxEffect::Send { op_id: operation.map_or(request_id, |operation| operation.op_id), request_id, sequence: operation.map_or(0, |operation| operation.sequence) }
        }
        Emitted::Send { to: TRANSFER_AT, input } if input.starts_with("SelectRow") => return None,
        Emitted::Send { to: TRANSFER_AT, input } if input.starts_with("MutationOutcomes") => OutboxEffect::TransferOutcomes,
        Emitted::Send { to: TRANSFER_AT, input } if input.starts_with("MutationOutcome") => OutboxEffect::TransferOutcome { outcome: outcome_named(&input) },
        Emitted::Send { to: TRANSFER_AT, .. } => return None,
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
        RReason::StaleGeneration => RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".into(), NEW_GENERATION)].into() },
        RReason::NoSession => RejectReason::NoSession,
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
    pub(super) log: Log<Emitted<Run>>,
    pub(super) batches: BTreeMap<OpId, BatchId>,
    pub(super) first_pair: BTreeMap<OpId, (RequestId, u64)>,
    /// The questions of the owner to Transfer that wait for an answer.
    asked: Vec<String>,
    pub(super) revision: i64,
    pub(super) reply: ReplyId,
    pub(super) follower: Option<OpId>,
    /// The typed text of the last inputs the Store received (`StoreOp` names).
    pub(super) last: Vec<OutboxEffect>,
    pub(super) store_text: String,
    /// Every Store input of the last post with the id of its batch, in the order sent.
    pub(super) stores: Vec<(BatchId, String)>,
    pub(super) session_text: String,
}

impl Twin {
    /// The owner in the phase `Restoring`: nothing restored yet.
    pub(super) fn bare() -> Self {
        let (mut det, log, _) = world(7);
        det.start_epoch(EPOCH);
        det.run_until_quiet(10_000).expect("the owner is quiet");
        take_fx(&log, None);
        Self { det, log, batches: BTreeMap::new(), first_pair: BTreeMap::new(), asked: Vec::new(), revision: 10, reply: 10, follower: None, last: Vec::new(), store_text: String::new(), stores: Vec::new(), session_text: String::new() }
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

    /// A call without an action waits for Transfer; the Transfer of the twin holds no transfer, so it names the smallest open operation of the row or none.
    pub(super) fn post(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        let mut effects = self.post_input(input);
        loop {
            let Some(asked) = self.asked.pop() else { self.last = effects.clone(); return effects };
            let asked = &asked;
            let reply = number_after(asked, "reply: ").expect("the reply of the question");
            let version = number_after(asked, "version: ").expect("the version of the question");
            let kind = if asked.contains("kind: Retry") { o::SelectKind::Retry } else { o::SelectKind::Discard };
            let asked_row = asked.split("row_id: \"").nth(1).and_then(|rest| rest.split('"').next()).unwrap_or("r").to_string();
            let smallest = self.state().ops.values().find(|operation| operation.op.row_id == asked_row && operation.stage != Stage::Done).map(|operation| operation.op.action.clone());
            effects.extend(self.post_input(match smallest {
                Some(action) => OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: asked_row.clone(), action, kind, reply, version },
                None => OutboxInput::SelectedNone { reply, version },
            }));
        }
    }

    /// Posts `input` and returns the owner effects with the raw text of every send to Transfer, without the auto-answer of `post`.
    pub(super) fn selector(&mut self, input: OutboxInput) -> (Vec<OutboxEffect>, Vec<String>) {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        let sends = crate::spec::support::snapshot(&self.log).into_iter().filter_map(|emitted| match emitted { Emitted::Send { to: TRANSFER_AT, input } => Some(input), _ => None }).collect();
        let effects = take_fx(&self.log, state_of(&self.det));
        (effects, sends)
    }

    fn post_input(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        self.stores = crate::spec::support::snapshot(&self.log).into_iter().filter_map(|emitted| match emitted { Emitted::Send { to: STORE_AT, input } => Some((number_after(&input, "id: ")?, input)), _ => None }).collect();
        self.store_text = self.stores.iter().map(|(_, input)| input.as_str()).collect::<Vec<_>>().join("\n");
        self.asked.extend(crate::spec::support::snapshot(&self.log).into_iter().filter_map(|emitted| match emitted { Emitted::Send { to: TRANSFER_AT, input } if input.starts_with("SelectRow") => Some(input), _ => None }));
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

    pub(super) fn submit_as(&mut self, row: &str, policy: Policy, transfer: bool, action: &str) {
        let reply = { self.reply += 1; self.reply };
        self.post(OutboxInput::Submit { epoch: EPOCH, op: model_op_as(row, policy, transfer, action), reply });
    }

    pub(super) fn committed(&mut self, id: OpId) -> Vec<OutboxEffect> { let batch = self.batches[&id]; self.commit_batch(batch) }

    pub(super) fn commit_batch(&mut self, batch: BatchId) -> Vec<OutboxEffect> {
        self.revision += 1;
        self.post(OutboxInput::Committed(Committed { batch, revision: self.revision, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }))
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
                St::Landing => { twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None }); }
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




pub(super) fn input_of(twin: &mut Twin, id: OpId, _ctx: &Ctx, input: In) -> Vec<OutboxEffect> {
    match input {
        In::Committed => twin.committed(id),
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



pub(super) fn keep() -> Ctx { Ctx { policy: Policy::Keep, ..Ctx::ready() } }






pub(super) fn sends_of(effects: &[OutboxEffect]) -> usize { effects.iter().filter(|effect| matches!(effect, OutboxEffect::Send { .. })).count() }
