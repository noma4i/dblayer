//! The `outbox` machine as code: one match arm per table row. No library code.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Why { Rejected, Discarded }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum St { None, Submitting, Queued, Sending, Landing, RollingBack(Why), Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reason { StaleGeneration, NoSession, Storage, Closed }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Policy { Rollback, Keep }

/// The fields of the owner state and of the input that the conditions of the table read.
#[derive(Clone, Copy, Debug)]
pub(super) struct Ctx {
    pub epoch_current: bool,
    pub online: bool,
    pub row_free: bool,
    pub policy: Policy,
    pub owns_model: bool,
    /// The `op_id` (or the row) of the input is the operation of the state.
    pub ours: bool,
    /// `attempts + 1 < 40`.
    pub attempts_left: bool,
    pub transfer: bool,
}

impl Ctx {
    pub fn ready() -> Self { Self { epoch_current: true, online: true, row_free: true, policy: Policy::Rollback, owns_model: true, ours: true, attempts_left: true, transfer: false } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum In {
    Submit,
    Committed,
    Rejected(Reason),
    RowFreed,
    Online(bool),
    Discard,
    Retry,
    Outcome,
    ModelsReset,
    Response,
    TransportError { temporary: bool },
    Dropped,
    Tick,
    EpochEnded,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Out { Confirmed, Rejected, Failed, Discarded, Interrupted }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault { NoSession, Reason(Reason), InFlight, NotFound, Interrupted }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Obs {
    Receipt,
    Fails(Fault),
    Outcome(Out),
    RowCarriesError,
    RowErrorCleared,
    OptimismVisibleAtRevision,
    OptimismGoneAtRevision,
    ResponseRowsVisibleAtRevision,
    OptimismErasedByReset,
    DiscardOk,
    /// `outcome` waits for the end of the operation.
    OutcomeWaits,
    /// `outcome` answers at once with the stored outcome.
    OutcomeNow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Commit { Intent, IntentNewBasis, Land, LandNewBasis, Rollback, RollbackNewBasis, ErrorMark, ClearMark }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Eff {
    Store(Commit),
    Send,
    /// The same `request_id` and `sequence` as the first `Send`.
    SendSamePair,
    /// The first `Send` after `open` carries the stored pair.
    SendStoredPair,
    Cancel,
    ScheduleTick,
    ScheduleLanding,
    ScheduleBackoffSend,
    Drained,
    SendNextOfRow,
    TransferOutcome(Out),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Step { pub next: St, pub obs: Vec<Obs>, pub eff: Vec<Eff> }

fn step(next: St, obs: &[Obs], eff: &[Eff]) -> Option<Step> { Some(Step { next, obs: obs.to_vec(), eff: eff.to_vec() }) }

fn goes_out(ctx: &Ctx) -> bool { ctx.online && ctx.row_free }

/// Ends an operation with `out`: the effects `before`, the outcome for the transfer owner when the origin is `Transfer{key}`, the effects `after`.
fn done_with(ctx: &Ctx, out: Out, obs: &[Obs], before: &[Eff], after: &[Eff]) -> Option<Step> {
    let mut eff = before.to_vec();
    if ctx.transfer { eff.push(Eff::TransferOutcome(out)); }
    eff.extend_from_slice(after);
    Some(Step { next: St::Done, obs: obs.to_vec(), eff })
}

pub fn step_of(state: St, ctx: &Ctx, input: In) -> Option<Step> {
    use Commit::*;
    use Eff::*;
    use Obs::*;
    use St::*;
    match (state, input) {
        (None, In::Submit) if ctx.epoch_current => step(Submitting, &[], &[Store(Intent)]),
        (None, In::Submit) => step(Done, &[Fails(Fault::NoSession)], &[]),
        (None, In::Retry | In::Discard | In::Outcome) => step(None, &[Fails(Fault::NotFound)], &[]),

        (Submitting, In::Committed) if goes_out(ctx) => step(Sending, &[Receipt, OptimismVisibleAtRevision], &[Send]),
        (Submitting, In::Committed) => step(Queued, &[Receipt, OptimismVisibleAtRevision], &[]),
        (Submitting, In::Rejected(Reason::StaleGeneration)) => step(Submitting, &[], &[Store(IntentNewBasis)]),
        (Submitting, In::Rejected(reason)) => step(Done, &[Fails(Fault::Reason(reason))], &[]),
        (Submitting, In::Discard) => step(Submitting, &[Fails(Fault::InFlight)], &[]),
        (Submitting, In::Retry) => step(Submitting, &[Receipt], &[]),
        (Submitting, In::Online(_) | In::ModelsReset | In::Response | In::TransportError { .. } | In::Dropped | In::Tick) => step(Submitting, &[], &[]),

        (Queued, In::Online(true) | In::RowFreed) if goes_out(ctx) => step(Sending, &[], &[Send]),
        (Queued, In::Online(true) | In::RowFreed) => step(Queued, &[], &[]),
        (Queued, In::Online(false)) => step(Queued, &[], &[]),
        (Queued, In::Discard) if ctx.ours => step(RollingBack(Why::Discarded), &[], &[Store(Rollback)]),
        (Queued, In::Retry) if ctx.ours => step(Queued, &[Receipt], &[]),
        (Queued, In::ModelsReset) if ctx.owns_model => step(Queued, &[OptimismErasedByReset], &[]),
        (Queued, In::ModelsReset) => step(Queued, &[], &[]),
        (Queued, In::Response | In::TransportError { .. } | In::Dropped | In::Tick | In::Rejected(_)) => step(Queued, &[], &[]),

        (Sending, In::Response) if ctx.ours => step(Landing, &[], &[Store(Land)]),
        (Sending, In::Response | In::TransportError { .. } | In::Dropped) if !ctx.ours => step(Sending, &[], &[]),
        (Sending, In::TransportError { temporary: true } | In::Dropped) if ctx.online && ctx.attempts_left => step(Sending, &[], &[ScheduleTick, ScheduleBackoffSend]),
        (Sending, In::TransportError { temporary: true } | In::Dropped) if !ctx.online => step(Queued, &[], &[]),
        (Sending, In::TransportError { temporary: true } | In::Dropped) if ctx.policy == Policy::Rollback => step(RollingBack(Why::Rejected), &[], &[Store(Rollback)]),
        (Sending, In::TransportError { temporary: true } | In::Dropped) => step(Failed, &[RowCarriesError, Outcome(Out::Failed)], &[Store(ErrorMark)]),
        (Sending, In::TransportError { temporary: false }) if ctx.policy == Policy::Rollback => step(RollingBack(Why::Rejected), &[], &[Store(Rollback)]),
        (Sending, In::TransportError { temporary: false }) => step(Failed, &[RowCarriesError, Outcome(Out::Failed)], &[Store(ErrorMark)]),
        (Sending, In::Tick) if ctx.ours && ctx.online => step(Sending, &[], &[SendSamePair]),
        (Sending, In::Tick) if ctx.ours => step(Queued, &[], &[]),
        (Sending, In::Tick) => step(Sending, &[], &[]),
        (Sending, In::Online(_)) => step(Sending, &[], &[]),
        (Sending, In::Discard) => step(Sending, &[Fails(Fault::InFlight)], &[]),
        (Sending, In::Retry) => step(Sending, &[Receipt], &[]),
        (Sending, In::ModelsReset | In::Committed | In::Rejected(_)) => step(Sending, &[], &[]),

        (Landing, In::Committed) => done_with(ctx, Out::Confirmed, &[Outcome(Out::Confirmed), ResponseRowsVisibleAtRevision], &[], &[SendNextOfRow]),
        (Landing, In::Rejected(Reason::StaleGeneration)) => step(Landing, &[], &[Store(LandNewBasis)]),
        (Landing, In::Rejected(Reason::NoSession | Reason::Closed)) => done_with(ctx, Out::Interrupted, &[Outcome(Out::Interrupted)], &[], &[]),
        (Landing, In::Rejected(Reason::Storage)) => step(Landing, &[], &[ScheduleLanding]),
        (Landing, In::Tick) => step(Landing, &[], &[Store(Land)]),
        (Landing, In::Discard) => step(Landing, &[Fails(Fault::InFlight)], &[]),
        (Landing, In::Retry) => step(Landing, &[Receipt], &[]),
        (Landing, In::Online(_) | In::ModelsReset | In::Response | In::TransportError { .. } | In::Dropped) => step(Landing, &[], &[]),

        (RollingBack(why), In::Committed) => {
            let out = if why == Why::Discarded { Out::Discarded } else { Out::Rejected };
            done_with(ctx, out, &[Outcome(out), OptimismGoneAtRevision], &[], &[SendNextOfRow])
        }
        (RollingBack(_), In::Rejected(Reason::StaleGeneration)) => step(state, &[], &[Store(RollbackNewBasis)]),
        (RollingBack(_), In::Rejected(Reason::NoSession | Reason::Closed)) => done_with(ctx, Out::Interrupted, &[Outcome(Out::Interrupted)], &[], &[]),
        (RollingBack(_), In::Rejected(Reason::Storage)) => step(state, &[], &[ScheduleTick]),
        (RollingBack(_), In::Tick) => step(state, &[], &[Store(Rollback)]),
        (RollingBack(_), In::Discard) => step(state, &[DiscardOk], &[]),
        (RollingBack(_), In::Retry) => step(state, &[Fails(Fault::InFlight)], &[]),
        (RollingBack(_), In::Online(_) | In::ModelsReset | In::Response | In::TransportError { .. } | In::Dropped) => step(state, &[], &[]),

        (Failed, In::Retry) if ctx.ours && goes_out(ctx) => step(Sending, &[RowErrorCleared, Receipt], &[Store(ClearMark), Send]),
        (Failed, In::Retry) if ctx.ours => step(Queued, &[RowErrorCleared, Receipt], &[Store(ClearMark)]),
        (Failed, In::Discard) if ctx.ours => step(RollingBack(Why::Discarded), &[], &[Store(Rollback)]),
        (Failed, In::Committed | In::Rejected(_)) => step(Failed, &[], &[]),
        (Failed, In::Online(_) | In::ModelsReset | In::Response | In::TransportError { .. } | In::Dropped | In::Tick) => step(Failed, &[], &[]),

        (Done, In::Discard | In::Retry) => step(Done, &[Fails(Fault::NotFound)], &[]),
        (Done, In::Outcome) if !ctx.epoch_current => step(Done, &[Fails(Fault::NoSession)], &[]),
        (Done, In::Outcome) => step(Done, &[OutcomeNow], &[]),
        (Done, In::Closing) => step(Done, &[], &[Drained]),
        (Done, _) => step(Done, &[], &[]),

        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::Outcome) if !ctx.epoch_current => step(state, &[Fails(Fault::NoSession)], &[]),
        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::Outcome) => step(state, &[OutcomeWaits], &[]),
        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::EpochEnded) if state == Sending => done_with(ctx, Out::Interrupted, &[Outcome(Out::Interrupted)], &[Cancel], &[]),
        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::EpochEnded) => done_with(ctx, Out::Interrupted, &[Outcome(Out::Interrupted)], &[], &[]),
        (Submitting | Sending | Landing | RollingBack(_), In::Closing) => step(Done, &[Outcome(Out::Interrupted)], &[Cancel, Drained]),
        (Queued | Failed, In::Closing) => step(Done, &[Outcome(Out::Interrupted)], &[Drained]),
        _ => Option::None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WSt { None, Writing, Done }

#[derive(Clone, Copy, Debug)]
pub(super) struct WCtx { pub epoch_current: bool, pub ephemeral_expiry: bool, pub renewed: bool }

impl WCtx {
    pub fn ready() -> Self { Self { epoch_current: true, ephemeral_expiry: false, renewed: false } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WIn { Write, Committed, Rejected(Reason), EpochEnded, Closing, Expire }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WObs { Ok, Fails(Fault), ValueVisibleAtRevision, RowGone }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WEff { StoreWrite, StoreWriteNewBasis, StoreRemoveRow, ScheduleExpire }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct WStep { pub next: WSt, pub obs: Vec<WObs>, pub eff: Vec<WEff> }

fn wstep(next: WSt, obs: &[WObs], eff: &[WEff]) -> Option<WStep> { Some(WStep { next, obs: obs.to_vec(), eff: eff.to_vec() }) }

pub fn write_step_of(state: WSt, ctx: &WCtx, input: WIn) -> Option<WStep> {
    use WEff::*;
    use WObs::*;
    match (state, input) {
        (WSt::None, WIn::Write) if ctx.epoch_current => wstep(WSt::Writing, &[], &[StoreWrite]),
        (WSt::None, WIn::Write) => wstep(WSt::Done, &[Fails(Fault::NoSession)], &[]),
        (WSt::Writing, WIn::Committed) if ctx.ephemeral_expiry => wstep(WSt::Done, &[Ok, ValueVisibleAtRevision], &[ScheduleExpire]),
        (WSt::Writing, WIn::Committed) => wstep(WSt::Done, &[Ok, ValueVisibleAtRevision], &[]),
        (WSt::Writing, WIn::Rejected(Reason::StaleGeneration)) => wstep(WSt::Writing, &[], &[StoreWriteNewBasis]),
        (WSt::Writing, WIn::Rejected(reason)) => wstep(WSt::Done, &[Fails(Fault::Reason(reason))], &[]),
        (WSt::Writing, WIn::EpochEnded | WIn::Closing) => wstep(WSt::Done, &[Fails(Fault::Interrupted)], &[]),
        (WSt::None, WIn::Expire) if !ctx.renewed => wstep(WSt::None, &[RowGone], &[StoreRemoveRow]),
        (WSt::None, WIn::Expire) => wstep(WSt::None, &[], &[]),
        _ => Option::None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Restored { Plain, ErrorMark, Transfer, FormatChange, Pending }

#[derive(Clone, Copy, Debug)]
pub(super) struct OCtx { pub online: bool, pub row_free: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OStep { pub next: St, pub obs: Vec<Obs>, pub eff: Vec<Eff> }

/// The opening table: the state of an operation after `Restored`.
pub fn open_step_of(kind: Restored, ctx: &OCtx) -> OStep {
    let out = ctx.online && ctx.row_free;
    let (next, obs, eff) = match (kind, out) {
        (Restored::ErrorMark, _) => (St::Failed, vec![Obs::RowCarriesError], vec![]),
        (Restored::Pending, true) => (St::Sending, vec![], vec![Eff::SendStoredPair]),
        (Restored::Pending, false) => (St::Queued, vec![], vec![]),
        (_, true) => (St::Sending, vec![], vec![Eff::Send]),
        (_, false) => (St::Queued, vec![], vec![]),
    };
    OStep { next, obs, eff }
}
