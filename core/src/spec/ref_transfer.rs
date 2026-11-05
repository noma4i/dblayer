//! The `transfer` machine as code: one match arm per table row. No library code.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum St { None, Copying, Ready, Uploading, Completing, Mutating, Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind { Single, Batch }

/// The fields of the owner state and of the input that the conditions of the table read.
#[derive(Clone, Copy, Debug)]
pub(super) struct Ctx {
    pub epoch_current: bool,
    pub parts_all_confirmed: bool,
    pub has_upload_id: bool,
    pub kind: Kind,
    pub online: bool,
    pub age_days: u64,
    /// A slot is free and the transfer is the first by `(seq, position)` among `Ready`.
    pub first_to_start: bool,
    /// `attempts + 1 < 50`.
    pub attempts_left: bool,
    /// The input names a part or a step that is not the current one.
    pub stray_step: bool,
}

impl Ctx {
    pub fn ready() -> Self { Self { epoch_current: true, parts_all_confirmed: false, has_upload_id: false, kind: Kind::Single, online: true, age_days: 0, first_to_start: true, attempts_left: true, stray_step: false } }
}

/// Inputs that a state ignores in the rows "other".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Noise { StepOk, StepError, Completed, CompleteError, MutationOutcome, TickRetry, OnlineTrue, OnlineFalse, SlotFreed }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum In {
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
    CompleteTemp,
    MutationOutcome { confirmed: bool },
    MutationDiscarded,
    MutationInterrupted,
    Retry,
    Discard,
    Expire,
    TickRetry,
    SlotFreed,
    EpochEnded,
    Closing,
    Noise(Noise),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Shown { Copying, Queued, Uploading, Failed, Gone }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Obs {
    Receipt,
    NoSession,
    NotFound,
    Shows(Shown),
    CancelTrue,
    CancelFalse,
    CancelInFlight,
    Progress,
    Progress100,
    RowWithServerFile,
    Interrupted,
    DiscardOk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Eff {
    Copy,
    CopyCancelled,
    RoundNeeded,
    StoreSyncUpload,
    StoreConfirmedParts,
    StoreCompleted,
    StoreFailed,
    StoreRetrying,
    StartInOrder,
    SlotFreed,
    TransportStep,
    NextStep,
    ScheduleBackoff,
    ScheduleExpire,
    Abort,
    Complete,
    SubmitFinalMutation,
    DiscardMutation,
    DeleteLocalCopy,
    DeleteFile,
    DeleteFileAndOperation,
    StepCancelled,
    TransportCancel,
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Step { pub next: St, pub obs: Vec<Obs>, pub eff: Vec<Eff> }

fn step(next: St, obs: &[Obs], eff: &[Eff]) -> Option<Step> { Some(Step { next, obs: obs.to_vec(), eff: eff.to_vec() }) }

pub fn step_of(state: St, ctx: &Ctx, input: In) -> Option<Step> {
    use Eff::*;
    use Obs::*;
    use St::*;
    let batch = ctx.kind == Kind::Batch;
    match (state, input) {
        (None, In::Accept) if ctx.epoch_current => step(Copying, &[Receipt, Shows(Shown::Copying)], &[Eff::Copy, RoundNeeded]),
        (None, In::Accept) => step(Done, &[NoSession], &[]),
        (None, In::Cancel | In::Retry | In::Discard) => step(None, &[NotFound], &[]),

        (Copying, In::CopyDone) => step(Ready, &[Shows(Shown::Queued)], &[StoreSyncUpload, StartInOrder]),
        (Copying, In::CopyFailed) => step(Failed, &[Shows(Shown::Failed)], &[StoreFailed]),
        (Copying, In::Cancel) => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[CopyCancelled, DeleteFile]),
        (Copying, In::Discard) => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[CopyCancelled, DeleteFile]),
        (Copying, In::Retry) => step(Copying, &[Receipt], &[]),
        (Copying, In::Noise(_)) => step(Copying, &[], &[]),

        (Ready, In::Start | In::SlotFreed | In::Online(true)) if ctx.online && ctx.first_to_start => step(Uploading, &[Shows(Shown::Uploading)], &[TransportStep]),
        (Ready, In::Start | In::SlotFreed | In::Online(true)) => step(Ready, &[], &[]),
        (Ready, In::Online(false)) => step(Ready, &[], &[]),
        (Ready, In::Cancel) => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[DeleteFileAndOperation, SlotFreed]),
        (Ready, In::Discard) => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[DeleteFileAndOperation, SlotFreed]),
        (Ready, In::Retry) => step(Ready, &[Receipt], &[]),
        (Ready, In::Noise(_)) => step(Ready, &[], &[]),

        (Uploading, In::StepOk { .. } | In::StepError { .. }) if ctx.stray_step => step(Uploading, &[], &[]),
        (Uploading, In::StepOk { parts_left: true }) => step(Uploading, &[Progress], &[StoreConfirmedParts, NextStep]),
        (Uploading, In::StepOk { parts_left: false }) => step(Completing, &[Progress100], &[StoreConfirmedParts, Eff::Complete]),
        (Uploading, In::StepError { temporary: true }) if ctx.online && ctx.attempts_left => step(Uploading, &[], &[ScheduleBackoff]),
        (Uploading, In::StepError { temporary: true }) if !ctx.online => step(Ready, &[Shows(Shown::Queued)], &[SlotFreed]),
        (Uploading, In::StepError { .. }) if ctx.has_upload_id => step(Failed, &[Shows(Shown::Failed)], &[Abort, StoreFailed, SlotFreed]),
        (Uploading, In::StepError { .. }) => step(Failed, &[Shows(Shown::Failed)], &[StoreFailed, SlotFreed]),
        (Uploading, In::TickRetry) if ctx.online => step(Uploading, &[], &[TransportStep]),
        (Uploading, In::TickRetry) => step(Ready, &[Shows(Shown::Queued)], &[SlotFreed]),
        (Uploading, In::Online(false)) => step(Ready, &[Shows(Shown::Queued)], &[StepCancelled, SlotFreed]),
        (Uploading, In::Online(true) | In::SlotFreed) => step(Uploading, &[], &[]),
        (Uploading, In::Cancel) if !batch && ctx.has_upload_id => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[StepCancelled, Abort, DeleteFileAndOperation, SlotFreed]),
        (Uploading, In::Cancel) if !batch => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[StepCancelled, DeleteFileAndOperation, SlotFreed]),
        (Uploading, In::Cancel) => step(Uploading, &[CancelInFlight], &[]),
        (Uploading, In::Discard) if ctx.has_upload_id => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[StepCancelled, Abort, DeleteFileAndOperation, SlotFreed]),
        (Uploading, In::Discard) => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[StepCancelled, DeleteFileAndOperation, SlotFreed]),
        (Uploading, In::Retry) => step(Uploading, &[Receipt], &[]),
        (Uploading, In::Noise(_) | In::Completed | In::CompleteError | In::CompleteTemp | In::MutationOutcome { .. }) => step(Uploading, &[], &[]),

        (Completing, In::Completed) => step(Mutating, &[], &[StoreCompleted, SubmitFinalMutation, SlotFreed]),
        (Completing, In::CompleteTemp) if ctx.online => step(Completing, &[], &[ScheduleBackoff]),
        (Completing, In::CompleteTemp) => step(Completing, &[], &[]),
        (Completing, In::CompleteError) => step(Failed, &[Shows(Shown::Failed)], &[StoreFailed, SlotFreed]),
        (Completing, In::TickRetry | In::Online(true)) if ctx.online => step(Completing, &[], &[Eff::Complete]),
        (Completing, In::TickRetry | In::Online(false)) => step(Completing, &[], &[]),
        (Completing, In::Cancel) if !batch => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation, SlotFreed]),
        (Completing, In::Cancel) => step(Completing, &[CancelInFlight], &[]),
        (Completing, In::Discard) => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation, SlotFreed]),
        (Completing, In::Retry) => step(Completing, &[Receipt], &[]),
        (Completing, In::Noise(_)) => step(Completing, &[], &[]),

        (Mutating, In::MutationOutcome { confirmed: true }) => step(Done, &[Shows(Shown::Gone), RowWithServerFile], &[DeleteLocalCopy]),
        (Mutating, In::MutationOutcome { confirmed: false }) => step(Failed, &[Shows(Shown::Failed)], &[StoreFailed]),
        (Mutating, In::MutationDiscarded) => step(Done, &[Shows(Shown::Gone)], &[DeleteFile]),
        (Mutating, In::MutationInterrupted) => step(Done, &[Shows(Shown::Gone)], &[]),
        (Mutating, In::Cancel) if !batch => step(Mutating, &[CancelFalse], &[]),
        (Mutating, In::Cancel) => step(Mutating, &[CancelInFlight], &[]),
        (Mutating, In::Discard) => step(Mutating, &[], &[DiscardMutation]),
        (Mutating, In::Retry) => step(Mutating, &[Receipt], &[]),
        (Mutating, In::Noise(_)) => step(Mutating, &[], &[]),

        (Failed, In::Retry) if ctx.parts_all_confirmed => step(Completing, &[Receipt, Shows(Shown::Uploading)], &[StoreRetrying, Eff::Complete]),
        (Failed, In::Retry) => step(Ready, &[Receipt, Shows(Shown::Queued)], &[StoreRetrying, SlotFreed]),
        (Failed, In::Discard) if ctx.has_upload_id => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation]),
        (Failed, In::Discard) => step(Done, &[DiscardOk, Shows(Shown::Gone)], &[DeleteFileAndOperation]),
        (Failed, In::Cancel) if ctx.has_upload_id => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation]),
        (Failed, In::Cancel) => step(Done, &[CancelTrue, Shows(Shown::Gone)], &[DeleteFileAndOperation]),
        (Failed, In::Online(true)) => step(Failed, &[], &[ScheduleExpire]),
        (Failed, In::Expire) if ctx.age_days > 7 && ctx.has_upload_id => step(Done, &[Shows(Shown::Gone)], &[Abort, DeleteFileAndOperation]),
        (Failed, In::Expire) if ctx.age_days > 7 => step(Done, &[Shows(Shown::Gone)], &[DeleteFileAndOperation]),
        (Failed, In::Expire) => step(Failed, &[], &[]),
        (Failed, In::Noise(_)) => step(Failed, &[], &[]),

        (Copying | Ready | Uploading | Completing | Mutating | Failed, In::EpochEnded) => step(Done, &[Interrupted], &[TransportCancel]),
        (Copying, In::Closing) => step(Done, &[Interrupted], &[CopyCancelled, DeleteFile, Drained]),
        (Ready | Uploading | Completing | Mutating | Failed, In::Closing) => step(Done, &[Interrupted], &[TransportCancel, Drained]),
        (Done, In::Closing) => step(Done, &[], &[Drained]),
        (Done, In::Cancel | In::Retry | In::Discard) => step(Done, &[Obs::NotFound], &[]),
        (Done, _) => step(Done, &[], &[]),
        _ => Option::None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Round { None, Requested(u64), Running { id: u32, epoch: u64, next: Option<u64> } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RoundIn { Needed(u64), Started(u32, u64), Refused(u64), Ended(u32, u64), EpochEnded(u64), Closing }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RoundStep { pub next: Round, pub schedule_job: Option<u64>, pub end_job: Option<u32>, pub refused_error: bool }

/// The Android round table. `current_epoch` is the epoch of the owner, `next_has_open_transfers` is the condition of `Running x RoundEnded`.
pub fn round_step(state: Round, current_epoch: u64, input: RoundIn, next_has_open_transfers: bool) -> Option<RoundStep> {
    let out = |next, schedule_job, end_job, refused_error| Some(RoundStep { next, schedule_job, end_job, refused_error });
    match (state, input) {
        (Round::None, RoundIn::Needed(e)) if e == current_epoch => out(Round::Requested(e), Some(e), None, false),
        (Round::None, RoundIn::Needed(_)) => out(state, None, None, false),
        (Round::Requested(e), RoundIn::Started(r, started)) if started == current_epoch && e == started => out(Round::Running { id: r, epoch: started, next: None }, None, None, false),
        (Round::Requested(_), RoundIn::Started(r, _)) => out(state, None, Some(r), false),
        (Round::Requested(_), RoundIn::Refused(_)) => out(Round::None, None, None, true),
        (Round::Requested(_), RoundIn::Needed(_)) => out(state, None, None, false),
        (Round::Running { id, epoch, next }, RoundIn::Needed(e)) if e == epoch => out(Round::Running { id, epoch, next }, None, None, false),
        (Round::Running { id, epoch, .. }, RoundIn::Needed(e)) => out(Round::Running { id, epoch, next: Some(e) }, None, None, false),
        (Round::Running { .. }, RoundIn::Started(..) | RoundIn::Refused(_)) => out(state, None, None, false),
        (Round::Running { id, epoch, next }, RoundIn::Ended(r, e)) if r == id && e == epoch => match next {
            Some(next_epoch) if next_has_open_transfers => out(Round::Requested(next_epoch), Some(next_epoch), None, false),
            _ => out(Round::None, None, None, false),
        },
        (Round::Running { .. }, RoundIn::Ended(..)) => out(state, None, None, false),
        (Round::Requested(_) | Round::Running { .. }, RoundIn::EpochEnded(_)) => out(state, None, None, false),
        (Round::Running { id, .. }, RoundIn::Closing) => out(Round::None, None, Some(id), false),
        (_, RoundIn::Closing) => out(Round::None, None, None, false),
        _ => Option::None,
    }
}
