#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum St { None, Copying, Ready, Uploading, Completing, Mutating, Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind { Single, Batch }

#[derive(Clone, Copy, Debug)]
pub struct Ctx { pub epoch_current: bool, pub parts_all_confirmed: bool, pub has_upload_id: bool, pub kind: Kind, pub transport_started: bool, pub online: bool, pub age_days: u64 }

impl Ctx {
    pub fn ready() -> Self { Self { epoch_current: true, parts_all_confirmed: false, has_upload_id: false, kind: Kind::Single, transport_started: false, online: true, age_days: 0 } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum In {
    Accept,
    CopyDone,
    CopyFailed,
    Cancel,
    Start,
    StepOk { parts_left: bool },
    StepError { temporary: bool },
    Online(bool),
    Completed,
    CompleteError,
    MutationOutcome { confirmed: bool },
    Retry,
    Discard,
    Expire,
    EpochEnded,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shown { Copying, Queued, Uploading, Failed, Gone }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Obs {
    Receipt,
    NoSession,
    Shows(Shown),
    CancelTrue,
    CancelFalse,
    CancelInFlight,
    Progress,
    Progress100,
    RowWithServerFile,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Eff {
    Copy,
    CopyCancelled,
    RoundNeeded,
    StoreSyncUpload,
    StartInOrder,
    TransportStep,
    NextStep,
    ScheduleBackoff,
    Abort,
    NoAbort,
    Complete,
    SubmitFinalMutation,
    DeleteLocalCopy,
    DeleteFileAndOperation,
    StepCancelled,
    TransportCancel,
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step { pub next: St, pub obs: Vec<Obs>, pub eff: Vec<Eff> }

fn step(next: St, obs: &[Obs], eff: &[Eff]) -> Option<Step> { Some(Step { next, obs: obs.to_vec(), eff: eff.to_vec() }) }

pub fn step_of(state: St, ctx: &Ctx, input: In) -> Option<Step> {
    use Eff::*;
    use Obs::*;
    use St::*;
    match (state, input) {
        (None, In::Accept) if ctx.epoch_current => step(Copying, &[Receipt, Shows(Shown::Copying)], &[Eff::Copy, RoundNeeded]),
        (None, In::Accept) => step(Done, &[NoSession], &[]),
        (Copying, In::CopyDone) => step(Ready, &[Shows(Shown::Queued)], &[StoreSyncUpload, StartInOrder]),
        (Copying, In::CopyFailed) => step(Failed, &[Shows(Shown::Failed)], &[]),
        (Copying, In::Cancel) => step(Done, &[CancelTrue], &[CopyCancelled]),
        (Ready, In::Start) => step(Uploading, &[Shows(Shown::Uploading)], &[TransportStep]),
        (Ready, In::Cancel) => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[DeleteFileAndOperation]),
        (Uploading, In::StepOk { parts_left: true }) => step(Uploading, &[Progress], &[NextStep]),
        (Uploading, In::StepOk { parts_left: false }) => step(Completing, &[Progress100], &[Eff::Complete]),
        (Uploading, In::StepError { temporary: true }) if ctx.online => step(Uploading, &[], &[ScheduleBackoff]),
        (Uploading, In::StepError { .. }) => step(Failed, &[Shows(Shown::Failed)], &[Abort]),
        (Uploading, In::Online(false)) => step(Ready, &[Shows(Shown::Queued)], &[StepCancelled]),
        (Uploading, In::Cancel) if ctx.kind == Kind::Single && !ctx.transport_started => step(Done, &[CancelTrue], &[Abort, DeleteFileAndOperation]),
        (Completing, In::Completed) => step(Mutating, &[], &[SubmitFinalMutation]),
        (Completing, In::CompleteError) => step(Failed, &[Shows(Shown::Failed)], &[NoAbort]),
        (Mutating, In::MutationOutcome { confirmed: true }) => step(Done, &[Shows(Shown::Gone), RowWithServerFile], &[DeleteLocalCopy]),
        (Mutating, In::MutationOutcome { confirmed: false }) => step(Failed, &[Shows(Shown::Failed)], &[]),
        (Mutating, In::Cancel) if ctx.kind == Kind::Single => step(Mutating, &[CancelFalse], &[]),
        (Mutating, In::Cancel) => step(Mutating, &[CancelInFlight], &[]),
        (Failed, In::Retry) if ctx.parts_all_confirmed => step(Completing, &[Shows(Shown::Uploading)], &[Eff::Complete]),
        (Failed, In::Retry) => step(Ready, &[Shows(Shown::Queued)], &[]),
        (Failed, In::Discard) if ctx.parts_all_confirmed || ctx.has_upload_id => step(Done, &[Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation]),
        (Failed, In::Expire) if ctx.age_days > 7 => step(Done, &[Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation]),
        (Copying | Ready | Uploading | Completing | Mutating | Failed, In::EpochEnded) => step(Done, &[Interrupted], &[TransportCancel]),
        (Copying | Ready | Uploading | Completing | Mutating, In::Closing) => step(Ready, &[Interrupted], &[TransportCancel, Drained]),
        _ => Option::None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Round { None, Requested(u64), Running { id: u32, epoch: u64, next: Option<u64> } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundIn { Needed(u64), Started(u32, u64), Refused(u64), Ended(u32, u64), EpochEnded(u64) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundStep { pub next: Round, pub schedule_job: Option<u64>, pub refused_error: bool }

pub fn round_step(state: Round, current_epoch: u64, input: RoundIn, next_has_open_transfers: bool) -> Option<RoundStep> {
    let out = |next, schedule_job, refused_error| Some(RoundStep { next, schedule_job, refused_error });
    match (state, input) {
        (Round::None, RoundIn::Needed(e)) if e == current_epoch => out(Round::Requested(e), Some(e), false),
        (Round::Requested(e), RoundIn::Started(r, started)) if started == current_epoch && e == started => out(Round::Running { id: r, epoch: started, next: None }, None, false),
        (Round::Requested(_), RoundIn::Refused(_)) => out(Round::None, None, true),
        (Round::Running { id, epoch, next }, RoundIn::Needed(e)) if e == epoch => out(Round::Running { id, epoch, next }, None, false),
        (Round::Running { id, epoch, .. }, RoundIn::Needed(e)) => out(Round::Running { id, epoch, next: Some(e) }, None, false),
        (Round::Running { id, epoch, next }, RoundIn::Ended(r, e)) if r == id && e == epoch => match next {
            Some(next_epoch) if next_has_open_transfers => out(Round::Requested(next_epoch), Some(next_epoch), false),
            _ => out(Round::None, None, false),
        },
        (Round::Running { .. }, RoundIn::Ended(..)) => out(state, None, false),
        (Round::Running { epoch, .. }, RoundIn::EpochEnded(e)) if e == epoch => out(state, None, false),
        _ => Option::None,
    }
}
