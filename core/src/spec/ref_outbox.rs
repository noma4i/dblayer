#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Why { Rejected, Discarded }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum St { None, Submitting, Queued, Sending, Landing, RollingBack(Why), Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reason { StaleGeneration, NoSession, Storage }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Policy { Rollback, Keep }

#[derive(Clone, Copy, Debug)]
pub(super) struct Ctx { pub epoch_current: bool, pub online: bool, pub row_free: bool, pub policy: Policy, pub owns_model: bool }

impl Ctx {
    pub fn ready() -> Self { Self { epoch_current: true, online: true, row_free: true, policy: Policy::Rollback, owns_model: true } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum In {
    Submit,
    Committed,
    Rejected(Reason),
    RowFreed,
    Online(bool),
    EpochStarted,
    Discard,
    Retry,
    ModelsReset,
    Response,
    TransportError { temporary: bool },
    EpochEnded,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Out { Confirmed, Rejected, Failed, Discarded, Interrupted }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault { NoSession, Reason(Reason), InFlight }

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Commit { Intent, Land, LandNewBasis, Rollback, RollbackNewBasis, ErrorMark, ClearMark }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Eff { Store(Commit), Send, Cancel, ScheduleTick, ScheduleLanding, ScheduleBackoffSend, Drained, SendNextOfRow }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Step { pub next: St, pub obs: Vec<Obs>, pub eff: Vec<Eff> }

fn step(next: St, obs: &[Obs], eff: &[Eff]) -> Option<Step> { Some(Step { next, obs: obs.to_vec(), eff: eff.to_vec() }) }

fn goes_out(ctx: &Ctx) -> bool { ctx.online && ctx.row_free }

pub fn step_of(state: St, ctx: &Ctx, input: In) -> Option<Step> {
    use Commit::*;
    use Eff::*;
    use Obs::*;
    use St::*;
    match (state, input) {
        (None, In::Submit) if ctx.epoch_current => step(Submitting, &[], &[Store(Intent)]),
        (None, In::Submit) => step(Done, &[Fails(Fault::NoSession)], &[]),
        (Submitting, In::Committed) if goes_out(ctx) => step(Sending, &[Receipt, OptimismVisibleAtRevision], &[Send]),
        (Submitting, In::Committed) => step(Queued, &[Receipt, OptimismVisibleAtRevision], &[]),
        (Submitting, In::Rejected(reason)) => step(Done, &[Fails(Fault::Reason(reason))], &[]),
        (Queued, In::Online(true) | In::RowFreed | In::EpochStarted) if goes_out(ctx) => step(Sending, &[], &[Send]),
        (Queued, In::Online(true) | In::RowFreed | In::EpochStarted) => step(Queued, &[], &[]),
        (Queued, In::Online(false)) => step(Queued, &[], &[]),
        (Queued, In::Discard) => step(RollingBack(Why::Discarded), &[], &[Store(Rollback)]),
        (Queued, In::ModelsReset) if ctx.owns_model => step(Queued, &[], &[]),
        (Queued, In::ModelsReset) => step(Queued, &[], &[]),
        (Sending, In::Response) => step(Landing, &[], &[Store(Land)]),
        (Sending, In::TransportError { temporary: true }) if ctx.online => step(Sending, &[], &[ScheduleTick, ScheduleBackoffSend]),
        (Sending, In::TransportError { temporary: false }) if ctx.policy == Policy::Rollback => step(RollingBack(Why::Rejected), &[], &[Store(Rollback)]),
        (Sending, In::TransportError { temporary: false }) => step(Failed, &[RowCarriesError, Outcome(Out::Failed)], &[Store(ErrorMark)]),
        (Sending, In::Online(true)) => step(Sending, &[], &[Send]),
        (Sending, In::Online(false)) => step(Sending, &[], &[Cancel]),
        (Sending, In::Discard) => step(Sending, &[Fails(Fault::InFlight)], &[]),
        (Landing, In::Committed) => step(Done, &[Outcome(Out::Confirmed), ResponseRowsVisibleAtRevision], &[SendNextOfRow]),
        (Landing, In::Rejected(Reason::StaleGeneration)) => step(Landing, &[], &[Store(LandNewBasis)]),
        (Landing, In::Rejected(Reason::NoSession)) => step(Done, &[Outcome(Out::Interrupted)], &[]),
        (Landing, In::Rejected(Reason::Storage)) => step(Landing, &[], &[ScheduleLanding]),
        (RollingBack(why), In::Committed) => {
            let out = if why == Why::Discarded { Out::Discarded } else { Out::Rejected };
            step(Done, &[Outcome(out), OptimismGoneAtRevision], &[SendNextOfRow])
        }
        (RollingBack(_), In::Rejected(Reason::StaleGeneration)) => step(state, &[], &[Store(RollbackNewBasis)]),
        (RollingBack(_), In::Rejected(Reason::NoSession)) => step(Done, &[Outcome(Out::Interrupted)], &[]),
        (Failed, In::Retry) => step(Queued, &[RowErrorCleared], &[Store(ClearMark), Send]),
        (Failed, In::Discard) => step(RollingBack(Why::Discarded), &[], &[Store(Rollback)]),
        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::EpochEnded) if state == Sending => step(Done, &[Outcome(Out::Interrupted)], &[Cancel]),
        (Submitting | Queued | Sending | Landing | RollingBack(_) | Failed, In::EpochEnded) => step(Done, &[Outcome(Out::Interrupted)], &[]),
        (Submitting | Sending | Landing | RollingBack(_), In::Closing) => step(Done, &[Outcome(Out::Interrupted)], &[Cancel, Drained]),
        (Queued | Failed, In::Closing) => step(state, &[Outcome(Out::Interrupted)], &[Cancel, Drained]),
        _ => Option::None,
    }
}
