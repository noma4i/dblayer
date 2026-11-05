//! Owner-level tests of the `outbox` machine: the owner `Outbox` on `DetScheduler`, every row of the table and every property.
//! A row test brings the owner to the state of the row, feeds the input to the owner through its address and compares the stage,
//! the replies and the effects with the reference (`ref_outbox.rs`). Names are the `Test` column.

use crate::schema::OnFailure;
use std::collections::BTreeMap;

use super::ref_outbox::{open_step_of, step_of, write_step_of, Ctx, Eff, Fault as RFault, In, OCtx, Obs, Out, Policy, Reason as RReason, Restored, St, Step, WCtx, WEff, WIn, WObs, WSt, Why as RWhy};
use crate::owners::contract::{BatchId, Committed, ModelsReset, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::session::SessionCalls;
use crate::owners::store::StoreWriter;
use crate::owners::support::{new_log, number_after, text_after, Discard, Emitted, Log, Sink, Spy, SpyState};
use crate::owners::write::outbox::{self as o, CommitKind, Fault, LocalWriteKind, NewOp, OpId, Origin, Outbox, OutboxAnswer, OutboxEffect as Run, OutboxInput, OutboxPeers, OutboxState, Receipt, Stage, StoredOp, Why};
use crate::owners::write::transfer::Transfers;
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};

/// The slots of the peers and of the owner in every test: the peers first.
const STORE_AT: usize = 0;
const SESSION_AT: usize = 1;
const TRANSFER_AT: usize = 2;
const OUTBOX_AT: usize = 3;

/// What the owner gave out in a step, in the terms the rows of the table use: the executor effects (`Run`) and the inputs for the Store,
/// the Session and Transfer (`Send`), read from the typed `Debug` form the `Spy` keeps. `Schedule` is a timer of the owner.
#[derive(Clone, Debug)]
enum OutboxEffect {
    Store { op_id: Option<OpId>, batch: u64, kind: CommitKind, generation: u64 },
    Send { op_id: OpId, request_id: u64, sequence: u64 },
    CancelRequest { op_id: OpId },
    Reply { reply: ReplyId, answer: OutboxAnswer },
    TransferOutcome { key: String, outcome: o::OutboxOutcome },
    Schedule { id: u64, after_ms: u128 },
    Drained,
}

/// The kind of a batch from the ops it holds (`Debug` form of the typed ops).
fn kind_in(input: &str) -> CommitKind {
    if input.contains("ops: [Intent {") { CommitKind::Intent }
    else if input.contains("ops: [RollBack {") { CommitKind::Rollback }
    else if input.contains("MarkError { op_id") && input.contains("error: Some(") { CommitKind::ErrorMark }
    else if input.contains("MarkError { op_id") { CommitKind::ClearMark }
    else if input.contains("ops: [LocalRows {") { CommitKind::LocalWrite }
    else if input.contains("ops: [RemoveRows {") { CommitKind::RemoveRow }
    else { CommitKind::Land }
}

fn outcome_named(text: &str) -> o::OutboxOutcome {
    if text.contains("Confirmed") { o::OutboxOutcome::Confirmed { revision: 0 } }
    else if text.contains("Rejected") { o::OutboxOutcome::Rejected { error: String::new() } }
    else if text.contains("Failed") { o::OutboxOutcome::Failed { error: String::new() } }
    else if text.contains("Discarded") { o::OutboxOutcome::Discarded }
    else { o::OutboxOutcome::Interrupted }
}

fn lower(emitted: Emitted<Run>, state: Option<&OutboxState>) -> Option<OutboxEffect> {
    Some(match emitted {
        Emitted::Run(Run::Reply { reply, answer }) => OutboxEffect::Reply { reply, answer },
        Emitted::Run(Run::Drained(_)) => OutboxEffect::Drained,
        Emitted::Send { to: STORE_AT, input } => {
            let kind = kind_in(&input);
            let op_id = matches!(kind, CommitKind::Intent | CommitKind::Land | CommitKind::Rollback | CommitKind::ErrorMark | CommitKind::ClearMark).then(|| number_after(&input, "op_id: ")).flatten();
            let generation = number_after(&input, "\"Moment\": ").unwrap_or(0);
            OutboxEffect::Store { op_id, batch: number_after(&input, "id: ")?, kind, generation }
        }
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
        Emitted::Send { to: TRANSFER_AT, input } => OutboxEffect::TransferOutcome { key: text_after(&input, "key: ")?, outcome: outcome_named(&input) },
        Emitted::Send { to, .. } => panic!("a send to the slot {to}"),
        Emitted::Schedule { id, after, .. } => OutboxEffect::Schedule { id: id.0, after_ms: after.as_millis() },
        Emitted::Cancel(_) | Emitted::Broadcast(_) => return None,
    })
}

fn take_fx(log: &Log<Emitted<Run>>, state: Option<&OutboxState>) -> Vec<OutboxEffect> { crate::owners::support::take(log).into_iter().filter_map(|emitted| lower(emitted, state)).collect() }

fn state_of(det: &DetScheduler) -> Option<&OutboxState> { det.state(Address::<Spy<Outbox>>::new(OUTBOX_AT)).map(|spy: &SpyState<Outbox>| &spy.inner) }

/// The owner with its three peers: the Store, the Session and Transfer keep what they receive.
fn world(seed: u64) -> (DetScheduler, Log<Emitted<Run>>, Address<Spy<Outbox>>) {
    let mut det = DetScheduler::new(seed);
    let peer = |det: &mut DetScheduler, name: &'static str| -> usize { det.add::<Sink<StoreWriter>>(name, new_log(), crate::owners::support::Silent).index() };
    let (store, session, transfer) = (peer(&mut det, "store"), det.add::<Sink<SessionCalls>>("session", new_log(), crate::owners::support::Silent).index(), det.add::<Sink<Transfers>>("transfer", new_log(), crate::owners::support::Silent).index());
    assert_eq!((store, session, transfer), (STORE_AT, SESSION_AT, TRANSFER_AT));
    let peers = OutboxPeers { store: Address::new(store), session: Address::new(session), transfer: Some(Address::new(transfer)) };
    let log = new_log();
    let addr = det.add::<Spy<Outbox>>("outbox", SpyState::new(OutboxState::new(EPOCH, peers), &log), Discard);
    assert_eq!(addr.index(), OUTBOX_AT);
    (det, log, addr)
}

/// The rows of a server answer.
fn server_rows() -> Vec<(String, Vec<crate::owners::store::RowValue>)> { vec![("Moment".into(), vec![crate::owners::store::RowValue { row_id: "r".into(), values: "{\"state\":\"server\"}".into() }])] }

const EPOCH: u64 = 1;
const SUBMIT_REPLY: ReplyId = 1;
const DISCARD_REPLY: ReplyId = 800;
const WAITER: ReplyId = 900;
const NEW_GENERATION: u64 = 5;

fn reject(reason: RReason) -> RejectReason {
    match reason {
        RReason::StaleGeneration => RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: NEW_GENERATION },
        RReason::NoSession => RejectReason::NoSession,
        RReason::Storage => RejectReason::Storage("disk".into()),
        RReason::Closed => RejectReason::Closed,
    }
}

fn stage_of(stage: Stage) -> St {
    match stage {
        Stage::Submitting => St::Submitting,
        Stage::Queued => St::Queued,
        Stage::Sending => St::Sending,
        Stage::Landing => St::Landing,
        Stage::RollingBack(Why::Rejected) => St::RollingBack(RWhy::Rejected),
        Stage::RollingBack(Why::Discarded) => St::RollingBack(RWhy::Discarded),
        Stage::Failed => St::Failed,
        Stage::Done => St::Done,
    }
}

fn out_of(outcome: &o::OutboxOutcome) -> Out {
    match outcome {
        o::OutboxOutcome::Confirmed { .. } => Out::Confirmed,
        o::OutboxOutcome::Rejected { .. } => Out::Rejected,
        o::OutboxOutcome::Failed { .. } => Out::Failed,
        o::OutboxOutcome::Discarded => Out::Discarded,
        o::OutboxOutcome::Interrupted => Out::Interrupted,
    }
}

fn fault_of(fault: &Fault) -> RFault {
    match fault {
        Fault::NoSession => RFault::NoSession,
        Fault::NotFound => RFault::NotFound,
        Fault::InFlight => RFault::InFlight,
        Fault::Interrupted => RFault::Interrupted,
        Fault::Rejected(RejectReason::NoSession) => RFault::Reason(RReason::NoSession),
        Fault::Rejected(RejectReason::StaleGeneration { .. }) => RFault::Reason(RReason::StaleGeneration),
        Fault::Rejected(RejectReason::Storage(_)) => RFault::Reason(RReason::Storage),
        Fault::Rejected(RejectReason::Closed) => RFault::Reason(RReason::Closed),
    }
}

fn model_op(row: &str, policy: Policy, transfer: bool) -> NewOp {
    NewOp {
        model: "Moment".into(),
        row_id: row.into(),
        action: "update".into(),
        on_failure: if policy == Policy::Keep { OnFailure::Keep } else { OnFailure::Rollback },
        origin: if transfer { Origin::Transfer("k".into()) } else { Origin::Api },
        path: format!("/moments/{row}"),
        body: format!("{{\"row\":\"{row}\"}}"),
        effects: crate::owners::store::IntentEffects { rows: vec![crate::owners::store::EffectRow { model: "Moment".into(), row_id: row.to_string(), values: "{\"state\":\"optimistic\"}".into() }], ..Default::default() },
    }
}

/// The owner with a recorder and the book of what it sent.
struct Twin {
    det: DetScheduler,
    log: Log<Emitted<Run>>,
    /// What the owner gave out when it restored the intents (read by `open_check`).
    kept: Vec<OutboxEffect>,
    batches: BTreeMap<OpId, BatchId>,
    first_pair: BTreeMap<OpId, (RequestId, u64)>,
    revision: i64,
    reply: ReplyId,
    follower: Option<OpId>,
}

impl Twin {
    fn new() -> Self {
        let (mut det, log, _) = world(7);
        det.start_epoch(EPOCH);
        Self { det, log, kept: Vec::new(), batches: BTreeMap::new(), first_pair: BTreeMap::new(), revision: 10, reply: 10, follower: None }
    }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn note(&mut self, effects: &[OutboxEffect]) {
        for effect in effects {
            match effect {
                OutboxEffect::Store { op_id: Some(id), batch, .. } => { self.batches.insert(*id, *batch); }
                OutboxEffect::Send { op_id, request_id, sequence } => { self.first_pair.entry(*op_id).or_insert((*request_id, *sequence)); }
                _ => {}
            }
        }
    }

    fn post(&mut self, input: OutboxInput) -> Vec<OutboxEffect> {
        take_fx(&self.log, None);
        self.det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), input);
        self.run();
        let effects = take_fx(&self.log, state_of(&self.det));
        self.note(&effects);
        effects
    }

    fn state(&self) -> &OutboxState { state_of(&self.det).expect("the owner state") }

    fn stage(&self, id: OpId) -> Option<St> { self.state().stage_of(id).map(stage_of) }

    fn submit(&mut self, row: &str, policy: Policy, transfer: bool) -> OpId {
        let before = self.state().ops.keys().max().copied().unwrap_or(0);
        let reply = if before == 0 { SUBMIT_REPLY } else { self.reply += 1; self.reply };
        self.post(OutboxInput::Submit { epoch: EPOCH, op: model_op(row, policy, transfer), reply });
        *self.state().ops.keys().max().expect("the operation exists")
    }

    fn committed(&mut self, id: OpId) -> Vec<OutboxEffect> {
        self.revision += 1;
        let batch = self.batches[&id];
        self.post(OutboxInput::Committed(Committed { batch, revision: self.revision, tables: vec!["moments".into()], models: Default::default() }))
    }

    fn rejected(&mut self, id: OpId, reason: RReason) -> Vec<OutboxEffect> {
        let batch = self.batches[&id];
        self.post(OutboxInput::Rejected(Rejected { batch, reason: reject(reason) }))
    }

    fn receipt(&self, id: OpId, epoch: u64) -> Receipt { Receipt { epoch, op_id: id, revision: 0 } }

    fn waiter(&mut self, id: OpId) { self.post(OutboxInput::Outcome { receipt: self.receipt(id, EPOCH), reply: WAITER }); }
}

/// Brings the owner to `state` of the operation `id` (row `r`) under the conditions of `ctx`.
fn reach(state: St, ctx: &Ctx) -> (Twin, OpId) {
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
            twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: DISCARD_REPLY });
            twin.post(OutboxInput::Online(Online(ctx.online)));
            id
        }
        _ => {
            let id = twin.submit("r", policy, ctx.transfer);
            twin.committed(id);
            if !ctx.attempts_left {
                for _ in 0..39 {
                    twin.post(OutboxInput::TransportError { op_id: id, temporary: true });
                    twin.post(OutboxInput::RetryDue { op_id: id });
                }
            }
            match state {
                St::Landing => { twin.post(OutboxInput::Response { op_id: id, rows: server_rows() }); }
                St::RollingBack(RWhy::Rejected) | St::Failed => { twin.post(OutboxInput::TransportError { op_id: id, temporary: false }); }
                St::Done => {
                    twin.post(OutboxInput::Response { op_id: id, rows: server_rows() });
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
struct Seen {
    next: Option<St>,
    obs: Vec<Obs>,
    eff: Vec<Eff>,
}

fn translate(twin: &Twin, id: OpId, effects: Vec<OutboxEffect>, scheduled: bool, resend: bool, outcome_call: bool) -> Seen {
    let mut obs = Vec::new();
    let mut eff = Vec::new();
    for effect in effects {
        match effect {
            OutboxEffect::Store { op_id: Some(op), kind, generation, .. } if op == id => eff.push(Eff::Store(match (kind, generation > 0) {
                (CommitKind::Intent, false) => super::ref_outbox::Commit::Intent,
                (CommitKind::Intent, true) => super::ref_outbox::Commit::IntentNewBasis,
                (CommitKind::Land, false) => super::ref_outbox::Commit::Land,
                (CommitKind::Land, true) => super::ref_outbox::Commit::LandNewBasis,
                (CommitKind::Rollback, false) => super::ref_outbox::Commit::Rollback,
                (CommitKind::Rollback, true) => super::ref_outbox::Commit::RollbackNewBasis,
                (CommitKind::ErrorMark, _) => super::ref_outbox::Commit::ErrorMark,
                (CommitKind::ClearMark, _) => super::ref_outbox::Commit::ClearMark,
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
                    OutboxAnswer::Err(fault) => obs.push(Obs::Fails(fault_of(&fault))),
                }
            }
            OutboxEffect::Schedule { .. } => {}
            OutboxEffect::TransferOutcome { outcome, .. } => eff.push(Eff::TransferOutcome(out_of(&outcome))),
            OutboxEffect::Drained => eff.push(Eff::Drained),
        }
    }
    if scheduled {
        eff.push(Eff::ScheduleTick);
    }
    Seen { next: twin.stage(id), obs, eff }
}

/// The observable of the reference that an owner effect can show; the rest is read in the API tests and the property tests.
fn visible(step: &Step, follower: bool) -> (Vec<Obs>, Vec<Eff>) {
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

fn input_of(twin: &mut Twin, id: OpId, ctx: &Ctx, input: In) -> Vec<OutboxEffect> {
    let other = id + 1000;
    let op_id = if ctx.ours { id } else { other };
    let epoch = if ctx.epoch_current { EPOCH } else { EPOCH + 1 };
    match input {
        In::Submit => twin.post(OutboxInput::Submit { epoch, op: model_op("r", ctx.policy, ctx.transfer), reply: SUBMIT_REPLY }),
        In::Committed => twin.committed(id),
        In::Rejected(reason) => twin.rejected(id, reason),
        In::RowFreed => {
            let blocker = 1;
            twin.post(OutboxInput::Response { op_id: blocker, rows: server_rows() });
            twin.committed(blocker)
        }
        In::Online(on) => twin.post(OutboxInput::Online(Online(on))),
        In::Discard => twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 801 }),
        In::Retry => twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 802 }),
        In::Outcome => twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, epoch), reply: 803 }),
        In::ModelsReset => twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec![if ctx.owns_model { "Moment".into() } else { "Chat".into() }] })),
        In::Response => twin.post(OutboxInput::Response { op_id, rows: server_rows() }),
        In::TransportError { temporary } => twin.post(OutboxInput::TransportError { op_id, temporary }),
        In::Dropped => twin.post(OutboxInput::Dropped { op_id }),
        In::Tick => twin.post(OutboxInput::RetryDue { op_id }),
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

fn row_check(state: St, ctx: Ctx, input: In) {
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

fn ready() -> Ctx { Ctx::ready() }

fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

fn busy() -> Ctx { Ctx { row_free: false, ..Ctx::ready() } }

fn stranger() -> Ctx { Ctx { ours: false, ..Ctx::ready() } }

fn keep() -> Ctx { Ctx { policy: Policy::Keep, ..Ctx::ready() } }

fn transfer() -> Ctx { Ctx { transfer: true, ..Ctx::ready() } }

fn stale() -> Ctx { Ctx { epoch_current: false, ..Ctx::ready() } }

fn rolling() -> St { St::RollingBack(RWhy::Rejected) }

fn discarding() -> St { St::RollingBack(RWhy::Discarded) }

macro_rules! row {
    ($name:ident, $state:expr, $ctx:expr, $($input:expr),+) => {
        #[test]
        fn $name() { $( row_check($state, $ctx, $input); )+ }
    };
}

row!(out_submit, St::None, ready(), In::Submit);
row!(out_submit_stale, St::None, stale(), In::Submit);
row!(out_unknown_operation, St::None, ready(), In::Retry, In::Discard, In::Outcome);
row!(out_submitting_committed, St::Submitting, ready(), In::Committed);
row!(out_submitting_committed_queued, St::Submitting, offline(), In::Committed);
row!(out_submitting_stale_generation, St::Submitting, ready(), In::Rejected(RReason::StaleGeneration));
row!(out_submitting_discard, St::Submitting, ready(), In::Discard);
row!(out_submitting_other, St::Submitting, ready(), In::Online(false), In::ModelsReset, In::Response, In::TransportError { temporary: true }, In::Dropped, In::Tick);
row!(out_queued_send, St::Queued, offline(), In::Online(true));
row!(out_queued_wait, St::Queued, busy(), In::Online(true));
row!(out_queued_offline, St::Queued, offline(), In::Online(false));
row!(out_queued_discard, St::Queued, offline(), In::Discard);
row!(out_queued_retry, St::Queued, offline(), In::Retry);
row!(out_queued_models_reset, St::Queued, offline(), In::ModelsReset);
row!(out_queued_models_other, St::Queued, offline(), In::ModelsReset);
row!(out_queued_stray, St::Queued, offline(), In::Response, In::TransportError { temporary: true }, In::Dropped, In::Tick, In::Rejected(RReason::Storage));
row!(out_sending_response, St::Sending, ready(), In::Response);
row!(out_sending_stray, St::Sending, stranger(), In::Response, In::TransportError { temporary: true }, In::Dropped);
row!(out_sending_temp, St::Sending, ready(), In::TransportError { temporary: true }, In::Dropped);
row!(out_sending_temp_offline, St::Sending, offline(), In::TransportError { temporary: true }, In::Dropped);
row!(out_sending_exhausted_keep, St::Sending, Ctx { policy: Policy::Keep, attempts_left: false, ..Ctx::ready() }, In::TransportError { temporary: true }, In::Dropped);
row!(out_sending_reject_rollback, St::Sending, ready(), In::TransportError { temporary: false });
row!(out_sending_reject_keep, St::Sending, keep(), In::TransportError { temporary: false });
row!(out_sending_tick, St::Sending, ready(), In::Tick);
row!(out_sending_tick_offline, St::Sending, offline(), In::Tick);
row!(out_sending_offline, St::Sending, ready(), In::Online(false));
row!(out_sending_online, St::Sending, ready(), In::Online(true));
row!(out_sending_discard, St::Sending, ready(), In::Discard);
row!(out_sending_retry, St::Sending, ready(), In::Retry);
row!(out_sending_other, St::Sending, ready(), In::ModelsReset, In::Committed, In::Rejected(RReason::Storage));
row!(out_landing_committed, St::Landing, ready(), In::Committed);
row!(out_landing_committed_transfer, St::Landing, transfer(), In::Committed);
row!(out_landing_stale_generation, St::Landing, ready(), In::Rejected(RReason::StaleGeneration));
row!(out_landing_no_session, St::Landing, ready(), In::Rejected(RReason::NoSession), In::Rejected(RReason::Closed));
row!(out_landing_storage, St::Landing, ready(), In::Rejected(RReason::Storage));
row!(out_landing_tick, St::Landing, ready(), In::Tick);
row!(out_landing_discard, St::Landing, ready(), In::Discard);
row!(out_landing_retry, St::Landing, ready(), In::Retry);
row!(out_landing_other, St::Landing, ready(), In::Online(false), In::ModelsReset, In::Response, In::TransportError { temporary: true }, In::Dropped);
row!(out_rollback_committed_discard, discarding(), transfer(), In::Committed);
row!(out_rollback_committed, rolling(), transfer(), In::Committed);
row!(out_rollback_stale_generation, rolling(), ready(), In::Rejected(RReason::StaleGeneration));
row!(out_rollback_no_session, rolling(), ready(), In::Rejected(RReason::NoSession), In::Rejected(RReason::Closed));
row!(out_rollback_storage, rolling(), ready(), In::Rejected(RReason::Storage));
row!(out_rollback_tick, rolling(), ready(), In::Tick);
row!(out_rollback_discard, rolling(), ready(), In::Discard);
row!(out_rollback_retry, rolling(), ready(), In::Retry);
row!(out_rollback_other, rolling(), ready(), In::Online(false), In::ModelsReset, In::Response, In::TransportError { temporary: true }, In::Dropped);
row!(out_failed_retry, St::Failed, keep(), In::Retry);
row!(out_failed_retry_queued, St::Failed, Ctx { online: false, ..keep() }, In::Retry);
row!(out_failed_discard, St::Failed, keep(), In::Discard);
row!(out_failed_mark_result, St::Failed, keep(), In::Committed, In::Rejected(RReason::Storage));
row!(out_failed_other, St::Failed, keep(), In::Online(false), In::ModelsReset, In::Response, In::TransportError { temporary: true }, In::Dropped, In::Tick);
row!(out_epoch_ended, St::Sending, transfer(), In::EpochEnded);
row!(out_closing, St::Sending, ready(), In::Closing);
row!(out_closing_queued, St::Queued, offline(), In::Closing);
row!(out_outcome_stale, St::Sending, stale(), In::Outcome);
row!(out_outcome_done, St::Done, ready(), In::Outcome);
row!(out_outcome_wait, St::Sending, ready(), In::Outcome);
row!(out_done_refuses, St::Done, ready(), In::Discard, In::Retry);
row!(out_done_closing, St::Done, ready(), In::Closing);
row!(out_done, St::Done, ready(), In::Online(true), In::ModelsReset, In::Response, In::TransportError { temporary: true }, In::Dropped, In::Tick);

#[test]
fn out_submitting_rejected() {
    for reason in [RReason::NoSession, RReason::Closed, RReason::Storage] {
        row_check(St::Submitting, ready(), In::Rejected(reason));
    }
}

#[test]
fn out_submitting_retry() {
    let want = step_of(St::Submitting, &ready(), In::Retry).unwrap();
    let (mut twin, id) = reach(St::Submitting, &ready());
    let before = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 802 });
    let after = twin.committed(id);
    assert_eq!((want.next, want.obs.clone()), (St::Submitting, vec![Obs::Receipt]));
    assert!(before.iter().all(|effect| !matches!(effect, OutboxEffect::Reply { reply: 802, .. })), "no answer before Committed");
    assert!(after.iter().any(|effect| matches!(effect, OutboxEffect::Reply { reply: 802, answer: OutboxAnswer::Receipt(_) })), "the receipt after Committed");
}

#[test]
fn out_open_restores() { open_check(Restored::Plain, ready(), false); }

#[test]
fn out_open_restores_queued() { open_check(Restored::Plain, offline(), false); }

#[test]
fn out_open_restores_failed() { open_check(Restored::ErrorMark, ready(), false); }

#[test]
fn out_open_restores_transfer() {
    open_check(Restored::Transfer, ready(), false);
    let (mut twin, id) = restored(Restored::Transfer, &ready());
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows() });
    let effects = twin.committed(id);
    assert!(effects.iter().any(|effect| matches!(effect, OutboxEffect::TransferOutcome { outcome: o::OutboxOutcome::Confirmed { .. }, .. })), "{effects:?}");
}

#[test]
fn out_open_format_change() { open_check(Restored::FormatChange, ready(), false); }

#[test]
fn out_open_restores_pending() { open_check(Restored::Pending, ready(), true); }

fn restored(kind: Restored, ctx: &Ctx) -> (Twin, OpId) {
    let mut twin = Twin::new();
    if !ctx.online {
        twin.post(OutboxInput::Online(Online(false)));
    }
    let stored = StoredOp {
        op_id: 7,
        op: model_op("r", Policy::Rollback, kind == Restored::Transfer),
        error_mark: kind == Restored::ErrorMark,
        pending: (kind == Restored::Pending).then_some((41, 42)),
    };
    twin.first_pair.insert(7, (41, 7));
    twin.kept = twin.post(OutboxInput::Restored(vec![stored]));
    (twin, 7)
}

fn open_check(kind: Restored, ctx: Ctx, pair: bool) {
    let want = open_step_of(kind, &OCtx { online: ctx.online, row_free: ctx.row_free });
    let (twin, id) = restored(kind, &ctx);
    let effects = twin.kept.clone();
    let sends: Vec<_> = effects.iter().filter_map(|effect| match effect {
        OutboxEffect::Send { op_id, request_id, sequence } if *op_id == id => Some((*request_id, *sequence)),
        _ => None,
    }).collect();
    assert_eq!(twin.stage(id), Some(want.next), "the state after Restored");
    let sent = want.eff.iter().any(|effect| matches!(effect, Eff::Send | Eff::SendStoredPair));
    assert_eq!(sends.len(), usize::from(sent), "{effects:?}");
    if pair {
        assert_eq!(sends.first().map(|pair| pair.0), Some(41), "the first Send carries the stored request id");
    }
}

fn write_state(state: WSt, ctx: &WCtx) -> (Twin, ReplyId) {
    let mut twin = Twin::new();
    if state == WSt::Writing {
        twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: ctx.ephemeral_expiry.then_some(10) }, row: "w".into(), values: "{}".into(), reply: 50, now: Time(100) });
    }
    (twin, 50)
}

fn write_check(state: WSt, ctx: WCtx, input: WIn) {
    let want = write_step_of(state, &ctx, input).unwrap_or_else(|| panic!("no table cell for write {state:?} x {input:?}"));
    let (mut twin, reply) = write_state(state, &ctx);
    let before = twin.det.trace().len();
    let write_batch = twin.log.lock().unwrap().len();
    let _ = write_batch;
    let effects = match input {
        WIn::Write => twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: if ctx.epoch_current { EPOCH } else { EPOCH + 1 }, write: LocalWriteKind::SetEphemeral { expire_after: ctx.ephemeral_expiry.then_some(10) }, row: "w".into(), values: "{}".into(), reply, now: Time(100) }),
        WIn::Committed => twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], models: Default::default() })),
        WIn::Rejected(reason) => {
            twin.post(OutboxInput::Rejected(Rejected { batch: 1, reason: reject(reason) }))
        }
        WIn::EpochEnded => { twin.det.end_epoch(EPOCH, Cause::SessionReset); twin.run(); take_fx(&twin.log, state_of(&twin.det)) }
        WIn::Closing => { twin.det.close(); twin.run(); take_fx(&twin.log, state_of(&twin.det)) }
        WIn::Expire => {
            if ctx.renewed {
                twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: Some(1_000) }, row: "w".into(), values: "{}".into(), reply: 51, now: Time(105) });
                twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 12, tables: vec!["moments".into()], models: Default::default() }));
            }
            twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(110) })
        }
    };
    let scheduled = twin.det.trace()[before..].iter().any(|entry| entry.effects.contains("Schedule"));
    let mut obs = Vec::new();
    let mut eff = Vec::new();
    for effect in effects {
        match effect {
            OutboxEffect::Reply { answer: OutboxAnswer::Ok, .. } => obs.push(WObs::Ok),
            OutboxEffect::Reply { answer: OutboxAnswer::Err(fault), .. } => obs.push(WObs::Fails(fault_of(&fault))),
            OutboxEffect::Store { kind: CommitKind::LocalWrite, generation, .. } => eff.push(if generation > 0 { WEff::StoreWriteNewBasis } else { WEff::StoreWrite }),
            OutboxEffect::Store { kind: CommitKind::RemoveRow, .. } => eff.push(WEff::StoreRemoveRow),
            OutboxEffect::Drained | OutboxEffect::Schedule { .. } => {}
            other => panic!("an effect outside the write table: {other:?}"),
        }
    }
    if scheduled && state == WSt::Writing && matches!(input, WIn::Committed) {
        eff.push(WEff::ScheduleExpire);
    }
    let want_obs: Vec<WObs> = want.obs.iter().copied().filter(|obs| !matches!(obs, WObs::ValueVisibleAtRevision | WObs::RowGone)).collect();
    assert_eq!(obs, want_obs, "{state:?} x {input:?}: the replies");
    assert_eq!(eff, want.eff, "{state:?} x {input:?}: the effects");
}

#[test]
fn out_local_write() { write_check(WSt::None, WCtx::ready(), WIn::Write); }

#[test]
fn out_local_write_stale() { write_check(WSt::None, WCtx { epoch_current: false, ..WCtx::ready() }, WIn::Write); }

#[test]
fn out_local_write_committed() {
    write_check(WSt::Writing, WCtx::ready(), WIn::Committed);
    write_check(WSt::Writing, WCtx { ephemeral_expiry: true, ..WCtx::ready() }, WIn::Committed);
}

#[test]
fn out_local_write_stale_generation() { write_check(WSt::Writing, WCtx::ready(), WIn::Rejected(RReason::StaleGeneration)); }

#[test]
fn out_local_write_rejected() {
    for reason in [RReason::NoSession, RReason::Closed, RReason::Storage] {
        write_check(WSt::Writing, WCtx::ready(), WIn::Rejected(reason));
    }
}

#[test]
fn out_local_write_interrupted() {
    write_check(WSt::Writing, WCtx::ready(), WIn::EpochEnded);
    write_check(WSt::Writing, WCtx::ready(), WIn::Closing);
}

#[test]
fn out_ephemeral_expire() { write_check(WSt::None, WCtx::ready(), WIn::Expire); }

#[test]
fn out_ephemeral_expire_renewed() { write_check(WSt::None, WCtx { renewed: true, ..WCtx::ready() }, WIn::Expire); }

fn replies_of(effects: &[OutboxEffect], reply: ReplyId) -> Vec<OutboxAnswer> {
    effects.iter().filter_map(|effect| match effect {
        OutboxEffect::Reply { reply: id, answer } if *id == reply => Some(answer.clone()),
        _ => None,
    }).collect()
}

/// The `Waits` table: every call gets one answer on every terminal row.
#[test]
fn out_wait_submit() {
    let (mut twin, id) = reach(St::Submitting, &ready());
    let ended = { take_fx(&twin.log, state_of(&twin.det)); twin.det.end_epoch(EPOCH, Cause::SessionReset); twin.run(); take_fx(&twin.log, state_of(&twin.det)) };
    assert_eq!(replies_of(&ended, SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Interrupted)]);
    let (mut twin, id2) = reach(St::Submitting, &ready());
    let committed = twin.committed(id2);
    assert!(matches!(replies_of(&committed, SUBMIT_REPLY).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, id3) = reach(St::Submitting, &ready());
    let rejected = twin.rejected(id3, RReason::NoSession);
    assert_eq!(replies_of(&rejected, SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Rejected(RejectReason::NoSession))]);
    let (mut twin, _) = reach(St::Submitting, &ready());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), SUBMIT_REPLY), [OutboxAnswer::Err(Fault::Interrupted)]);
    let _ = id;
}

#[test]
fn out_wait_outcome() {
    for (state, input, want) in [(St::Landing, In::Committed, Out::Confirmed), (St::Sending, In::EpochEnded, Out::Interrupted), (St::Queued, In::Closing, Out::Interrupted)] {
        let ctx = if state == St::Queued { offline() } else { ready() };
        let (mut twin, id) = reach(state, &ctx);
        let effects = input_of(&mut twin, id, &ctx, input);
        let answers = replies_of(&effects, WAITER);
        assert_eq!(answers.len(), 1, "{state:?}: one answer");
        assert!(matches!(&answers[0], OutboxAnswer::Outcome(outcome) if out_of(outcome) == want), "{answers:?}");
    }
    let (mut twin, id) = reach(St::Sending, &ready());
    let effects = twin.post(OutboxInput::Outcome { receipt: twin.receipt(id, EPOCH + 1), reply: 77 });
    assert_eq!(replies_of(&effects, 77), [OutboxAnswer::Err(Fault::NoSession)]);
}

#[test]
fn out_wait_retry() {
    let (mut twin, _) = reach(St::Failed, &keep());
    let ok = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 61 });
    assert!(matches!(replies_of(&ok, 61).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, _) = reach(St::Done, &ready());
    let none = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 62 });
    assert_eq!(replies_of(&none, 62), [OutboxAnswer::Err(Fault::NotFound)]);
    let (mut twin, _) = reach(St::Sending, &ready());
    let busy = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 63 });
    assert!(matches!(replies_of(&busy, 63).as_slice(), [OutboxAnswer::Receipt(_)]));
    let (mut twin, _) = reach(St::Submitting, &ready());
    let pending = twin.post(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), reply: 64 });
    assert!(replies_of(&pending, 64).is_empty());
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), 64), [OutboxAnswer::Err(Fault::Interrupted)]);
}

#[test]
fn out_wait_discard() {
    let (mut twin, id) = reach(St::Queued, &offline());
    let queued = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 71 });
    assert!(replies_of(&queued, 71).is_empty(), "no answer before Committed");
    let committed = twin.committed(id);
    assert_eq!(replies_of(&committed, 71), [OutboxAnswer::Ok]);
    let (mut twin, _) = reach(St::Sending, &ready());
    let in_flight = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 72 });
    assert_eq!(replies_of(&in_flight, 72), [OutboxAnswer::Err(Fault::InFlight)]);
    let (mut twin, _) = reach(St::Done, &ready());
    let none = twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 73 });
    assert_eq!(replies_of(&none, 73), [OutboxAnswer::Err(Fault::NotFound)]);
    let (mut twin, _) = reach(St::Queued, &offline());
    twin.post(OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 74 });
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), 74), [OutboxAnswer::Err(Fault::Interrupted)]);
}

#[test]
fn out_wait_local_write() {
    let mut twin = Twin::new();
    let effects = twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 81, now: Time(0) });
    assert!(replies_of(&effects, 81).is_empty());
    let committed = twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], models: Default::default() }));
    assert_eq!(replies_of(&committed, 81), [OutboxAnswer::Ok]);
    let mut twin = Twin::new();
    twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::Attach, row: "w".into(), values: "{}".into(), reply: 82, now: Time(0) });
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(replies_of(&take_fx(&twin.log, state_of(&twin.det)), 82), [OutboxAnswer::Err(Fault::Interrupted)]);
}

#[test]
fn out_done_closing_without_operations() {
    let mut twin = Twin::new();
    take_fx(&twin.log, state_of(&twin.det));
    twin.det.close();
    twin.run();
    assert_eq!(take_fx(&twin.log, state_of(&twin.det)).iter().filter(|effect| matches!(effect, OutboxEffect::Drained)).count(), 1);
}

// Properties: the explorer walks the orders of the inputs of three sources.

use crate::owners::support::{Source, SourceInput, SourceState};
use crate::runtime::{explore, Limits};
use std::cell::RefCell;

thread_local! { static LOG: RefCell<Option<Log<Emitted<Run>>>> = const { RefCell::new(None) }; }

fn scripted(sources: Vec<Vec<OutboxInput>>) -> DetScheduler {
    let (mut det, log, _) = world(3);
    LOG.with(|cell| *cell.borrow_mut() = Some(log.clone()));
    let addr = Address::<Outbox>::new(OUTBOX_AT);
    let mut feeders = Vec::new();
    for script in sources {
        feeders.push(det.add::<Source<Outbox>>("source", SourceState { to: addr, script }, crate::owners::support::Silent));
    }
    det.start_epoch(EPOCH);
    for feeder in feeders {
        det.post(feeder, EpochStamp::Control, SourceInput::Go);
    }
    det
}

fn api_script(rows: &[&str]) -> Vec<OutboxInput> {
    rows.iter().enumerate().map(|(index, row)| OutboxInput::Submit { epoch: EPOCH, op: model_op(row, Policy::Rollback, false), reply: 100 + index as u64 }).collect()
}

fn net_script() -> Vec<OutboxInput> {
    vec![
        OutboxInput::Response { op_id: 1, rows: server_rows() },
        OutboxInput::TransportError { op_id: 2, temporary: false },
        OutboxInput::Dropped { op_id: 2 },
        OutboxInput::Online(Online(false)),
        OutboxInput::Online(Online(true)),
    ]
}

fn store_script() -> Vec<OutboxInput> {
    (1..=4).map(|batch| OutboxInput::Committed(Committed { batch, revision: 10 + batch as i64, tables: vec!["moments".into()], models: Default::default() })).collect()
}

fn effects_now() -> Vec<OutboxEffect> { LOG.with(|cell| cell.borrow().as_ref().map(|log| crate::owners::support::snapshot(log).into_iter().filter_map(|emitted| lower(emitted, None)).collect()).unwrap_or_default()) }

fn walk(sources: impl Fn() -> Vec<Vec<OutboxInput>>, check: impl FnMut(&DetScheduler) -> Result<(), String>) {
    let report = explore(|| scripted(sources()), Limits { depth: 6, max_traces: 4_000 }, check);
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 0);
}

#[test]
fn out_prop_outcome_once() {
    walk(|| vec![api_script(&["r", "r"]), net_script(), store_script()], |_| {
        let mut seen = BTreeMap::new();
        for effect in effects_now() {
            if let OutboxEffect::Reply { reply, answer: OutboxAnswer::Outcome(_) } = effect { *seen.entry(reply).or_insert(0) += 1; }
        }
        match seen.values().find(|count| **count > 1) { Some(count) => Err(format!("an outcome came {count} times")), None => Ok(()) }
    });
}

#[test]
fn out_prop_no_send_after_end() {
    let mut det = scripted(vec![api_script(&["r", "q"]), store_script()]);
    det.run_until_quiet(10_000).unwrap();
    crate::owners::support::take(&LOG.with(|cell| cell.borrow().clone().unwrap()));
    det.end_epoch(EPOCH, Cause::SessionReset);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Online(Online(true)));
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::RetryDue { op_id: 1 });
    det.run_until_quiet(10_000).unwrap();
    let after = effects_now();
    assert!(after.iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. } | OutboxEffect::Store { .. })), "{after:?}");
}

#[test]
fn out_prop_row_order() {
    walk(|| vec![api_script(&["r", "r", "r"]), net_script(), store_script()], |det| {
        let state = state_of(det).ok_or("no state")?;
        let sending: Vec<_> = state.ops.values().filter(|op| op.op.row_id == "r" && matches!(op.stage, Stage::Sending)).collect();
        if sending.len() > 1 { return Err("two operations of one row in the network".into()); }
        let busy: Vec<OpId> = state.ops.values().filter(|op| matches!(op.stage, Stage::Sending | Stage::Landing | Stage::RollingBack(_) | Stage::Failed | Stage::Submitting)).map(|op| op.op_id).collect();
        match (busy.first(), state.ops.values().filter(|op| op.stage == Stage::Sending).map(|op| op.op_id).min()) {
            (Some(first), Some(sending)) if sending > *first => Err("a later operation went out before an earlier one".into()),
            _ => Ok(()),
        }
    });
}

#[test]
fn out_prop_intent_kept() {
    walk(|| vec![api_script(&["r", "q"]), net_script(), store_script()], |det| {
        let state = state_of(det).ok_or("no state")?;
        let done_without_outcome = state.ops.values().find(|op| op.stage == Stage::Done && op.outcome.is_none());
        match done_without_outcome { Some(op) => Err(format!("operation {} is Done without an outcome", op.op_id)), None => Ok(()) }
    });
}

#[test]
fn out_prop_idle() {
    let mut det = scripted(vec![api_script(&["r"]), store_script()]);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Response { op_id: 1, rows: server_rows() });
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Outbox>::new(OUTBOX_AT), EpochStamp::Epoch(EPOCH), OutboxInput::Committed(Committed { batch: 2, revision: 20, tables: vec![], models: Default::default() }));
    det.run_until_quiet(10_000).unwrap();
    let before = det.trace().len();
    det.tick(Time(u64::MAX / 2));
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(det.trace().len(), before, "no operation waits: a tick wakes nothing");
}

#[test]
fn out_prop_confirmed_lands() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] })]], |_| {
        let effects = effects_now();
        let lands = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Land, .. })).count();
        let confirmed = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Confirmed { .. }), .. })).count();
        if confirmed > lands { Err("Confirmed without a landed batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_rollback_clean() {
    walk(|| vec![api_script(&["r"]), net_script(), store_script(), vec![OutboxInput::Discard { model: "Moment".into(), row_id: "r".into(), reply: 200 }]], |_| {
        let effects = effects_now();
        let rollbacks = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { kind: CommitKind::Rollback, .. })).count();
        let ended = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Reply { answer: OutboxAnswer::Outcome(o::OutboxOutcome::Rejected { .. } | o::OutboxOutcome::Discarded), .. })).count();
        if ended > rollbacks { Err("Rejected or Discarded without a rollback batch".into()) } else { Ok(()) }
    });
}

#[test]
fn out_prop_membership_revision() {
    let (mut twin, id) = reach(St::Landing, &ready());
    let before = twin.state().ops[&id].op.clone();
    let effects = twin.post(OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Other".into()] }));
    assert!(effects.iter().all(|effect| !matches!(effect, OutboxEffect::Store { .. })), "a reset of another model touches no batch");
    assert_eq!(twin.state().ops[&id].op, before);
}

#[test]
fn out_prop_dropped_resent() {
    let (mut twin, id) = reach(St::Sending, &ready());
    let first = twin.first_pair[&id];
    let mut sends = 1;
    for _ in 0..60 {
        twin.post(OutboxInput::Dropped { op_id: id });
        for effect in twin.post(OutboxInput::RetryDue { op_id: id }) {
            if let OutboxEffect::Send { request_id, sequence, .. } = effect {
                assert_eq!((request_id, sequence), first, "the same pair on every Send");
                sends += 1;
            }
        }
    }
    assert!(sends <= 40, "{sends} Send");
    assert_ne!(twin.stage(id), Some(St::Sending), "the attempts run out");
    assert!(twin.state().ops[&id].outcome.as_ref().map_or(true, |outcome| !matches!(outcome, o::OutboxOutcome::Interrupted)));
}

#[test]
fn out_prop_local_write_kept() {
    walk(|| vec![vec![OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetLocal, row: "w".into(), values: "{}".into(), reply: 300, now: Time(0) }], store_script(), vec![OutboxInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] })]], |_| {
        let answers = effects_now().iter().filter(|effect| matches!(effect, OutboxEffect::Reply { reply: 300, .. })).count();
        if answers > 1 { Err("a local write got two answers".into()) } else { Ok(()) }
    });
}
