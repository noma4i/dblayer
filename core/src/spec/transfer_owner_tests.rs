//! Owner-level tests of `specs/machines/transfer.md`: the owner `Transfers` on `DetScheduler`, every row of the tables and every property.
//! A row test brings the owner to the state of the row, feeds the input through its address and compares the stage, the replies and the
//! effects with the reference (`ref_transfer.rs`). Names are the `Test` column.

use super::ref_transfer::{round_step, step_of, Ctx, Eff, In, Kind, Noise, Obs, Round, RoundIn, Shown, St, Step};
use crate::owners::contract::{EpochStarted, Online};
use crate::owners::outbox::{Fault, OutboxOutcome};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::owners::transfer::{self as t, RoundState, Source, StoreKind, StoredTransfer, TransferAnswer, TransferEffect, TransferInput, TransferStage, TransferState, TransferTick, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use crate::transport::NetworkPurpose;

const EPOCH: u64 = 1;
const KEY: &str = "k1";
const PARTS: u32 = 3;
const DAY: u64 = 86_400_000;

fn stage_of(stage: TransferStage) -> St {
    match stage {
        TransferStage::Copying => St::Copying,
        TransferStage::Ready => St::Ready,
        TransferStage::Uploading => St::Uploading,
        TransferStage::Completing => St::Completing,
        TransferStage::Mutating => St::Mutating,
        TransferStage::Failed => St::Failed,
        TransferStage::Done => St::Done,
    }
}

fn source() -> Source { Source { path: "a.mp4".into(), parts: PARTS } }

struct Twin {
    det: DetScheduler,
    addr: Address<Transfers>,
    log: Log<TransferEffect>,
    reply: u64,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(7);
        let log = new_log();
        let addr = det.add::<Transfers>("transfer", TransferState::new(EPOCH, 1), Recorder::<Transfers>::new(&log));
        det.start_epoch(EPOCH);
        Self { det, addr, log, reply: 100 }
    }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: TransferInput) -> Vec<TransferEffect> {
        take(&self.log);
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
        take(&self.log)
    }

    fn control(&mut self, input: TransferInput) -> Vec<TransferEffect> {
        take(&self.log);
        self.det.post(self.addr, EpochStamp::Control, input);
        self.run();
        take(&self.log)
    }

    fn state(&self) -> &TransferState { self.det.state(self.addr).expect("the owner state") }

    fn stage(&self, key: &str) -> St { self.state().stage_of(key).map_or(St::Done, stage_of) }

    fn reply(&mut self) -> u64 {
        self.reply += 1;
        self.reply
    }

    fn accept(&mut self, key: &str, batch: bool) -> Vec<TransferEffect> {
        let reply = self.reply();
        self.post(TransferInput::Accept { epoch: EPOCH, key: key.into(), source: source(), batch, reply, now: Time(0) })
    }
}

fn stored(key: &str, ctx: &Ctx, failed: bool, completed: bool, confirmed: u32) -> StoredTransfer {
    StoredTransfer {
        key: key.into(),
        seq: 1,
        source: source(),
        batch: ctx.kind == Kind::Batch,
        upload_id: ctx.has_upload_id.then(|| "u1".to_string()),
        confirmed_parts: confirmed,
        completed,
        failed: failed.then(|| "e".to_string()),
        file_present: true,
        created_at: Time(0),
    }
}

/// Brings the owner to `state` of the transfer `k1` under the conditions of `ctx`.
fn reach(state: St, ctx: &Ctx) -> Twin {
    let mut twin = Twin::new();
    let batch = ctx.kind == Kind::Batch;
    let last_part = if ctx.parts_all_confirmed { PARTS - 1 } else { 0 };
    match state {
        St::None => {}
        St::Copying => { twin.accept(KEY, batch); }
        St::Ready => {
            twin.post(TransferInput::Online(Online(false)));
            if !ctx.first_to_start {
                twin.accept("k0", false);
                twin.post(TransferInput::CopyDone { key: "k0".into() });
            }
            twin.accept(KEY, batch);
            twin.post(TransferInput::CopyDone { key: KEY.into() });
        }
        St::Uploading => {
            twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, false, last_part)]));
            if !ctx.attempts_left {
                for _ in 0..49 {
                    twin.post(TransferInput::StepError { key: KEY.into(), part: Some(1), temporary: true });
                    twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key: KEY.into() } });
                }
            }
        }
        St::Completing => { twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, false, PARTS)])); }
        St::Mutating => { twin.post(TransferInput::Restored(vec![stored(KEY, ctx, false, true, PARTS)])); }
        St::Failed => {
            let confirmed = if ctx.parts_all_confirmed { PARTS } else { 0 };
            twin.post(TransferInput::Restored(vec![stored(KEY, ctx, true, false, confirmed)]));
        }
        St::Done => {
            twin.accept(KEY, batch);
            let reply = twin.reply();
            twin.post(TransferInput::Discard { key: KEY.into(), reply });
        }
    }
    if !ctx.online && state != St::Ready {
        twin.post(TransferInput::Online(Online(false)));
    }
    if state != St::None {
        assert_eq!(twin.stage(KEY), if state == St::Done { St::Done } else { state }, "the owner reached {state:?}");
    }
    twin
}

fn input_of(twin: &mut Twin, ctx: &Ctx, input: In) -> Vec<TransferEffect> {
    let key: String = KEY.into();
    let part = if ctx.stray_step { Some(99) } else { Some(if ctx.parts_all_confirmed { PARTS } else { 1 }) };
    let reply = twin.reply();
    match input {
        In::Accept => {
            let epoch = if ctx.epoch_current { EPOCH } else { EPOCH + 1 };
            twin.post(TransferInput::Accept { epoch, key, source: source(), batch: ctx.kind == Kind::Batch, reply, now: Time(0) })
        }
        In::CopyDone => twin.post(TransferInput::CopyDone { key }),
        In::CopyFailed => twin.post(TransferInput::CopyFailed { key, error: "e".into() }),
        In::Cancel => twin.post(TransferInput::Cancel { key, reply }),
        In::Start => twin.post(TransferInput::Online(Online(true))),
        In::StepOk { parts_left } => twin.post(TransferInput::StepOk { key, part: if ctx.stray_step { Some(99) } else if parts_left { Some(1) } else { Some(PARTS) } }),
        In::StepError { temporary } => twin.post(TransferInput::StepError { key, part, temporary }),
        In::Online(on) => twin.post(TransferInput::Online(Online(on))),
        In::Completed => twin.post(TransferInput::Completed { key }),
        In::CompleteError => twin.post(TransferInput::CompleteError { key, temporary: false }),
        In::CompleteTemp => twin.post(TransferInput::CompleteError { key, temporary: true }),
        In::MutationOutcome { confirmed } => twin.post(TransferInput::MutationOutcome { key, outcome: if confirmed { OutboxOutcome::Confirmed { revision: 1 } } else { OutboxOutcome::Rejected { error: "e".into() } } }),
        In::MutationDiscarded => twin.post(TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Discarded }),
        In::MutationInterrupted => twin.post(TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Interrupted }),
        In::Retry => twin.post(TransferInput::Retry { key, reply }),
        In::Discard => twin.post(TransferInput::Discard { key, reply }),
        In::Expire => twin.post(TransferInput::Tick { now: Time(ctx.age_days * DAY), kind: TransferTick::Expire }),
        In::TickRetry => twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key } }),
        In::SlotFreed => twin.post(TransferInput::SlotFreed),
        In::EpochEnded => {
            take(&twin.log);
            twin.det.end_epoch(EPOCH, Cause::SessionReset);
            twin.run();
            take(&twin.log)
        }
        In::Closing => {
            take(&twin.log);
            twin.det.close();
            twin.run();
            take(&twin.log)
        }
        In::Noise(noise) => twin.post(match noise {
            Noise::StepOk => TransferInput::StepOk { key, part: Some(1) },
            Noise::StepError => TransferInput::StepError { key, part: Some(1), temporary: true },
            Noise::Completed => TransferInput::Completed { key },
            Noise::CompleteError => TransferInput::CompleteError { key, temporary: false },
            Noise::MutationOutcome => TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Confirmed { revision: 1 } },
            Noise::TickRetry => TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key } },
            Noise::OnlineTrue => TransferInput::Online(Online(true)),
            Noise::OnlineFalse => TransferInput::Online(Online(false)),
            Noise::SlotFreed => TransferInput::SlotFreed,
        }),
    }
}

fn shown_of(shown: &t::Shown) -> Shown {
    match shown {
        t::Shown::Copying => Shown::Copying,
        t::Shown::Queued => Shown::Queued,
        t::Shown::Uploading { .. } => Shown::Uploading,
        t::Shown::Failed { .. } => Shown::Failed,
        t::Shown::Gone => Shown::Gone,
    }
}

fn translate(input: In, effects: Vec<TransferEffect>, scheduled: bool) -> (Vec<Obs>, Vec<Eff>) {
    let mut obs = Vec::new();
    let mut eff: Vec<Eff> = Vec::new();
    for effect in effects {
        match effect {
            TransferEffect::Copy { .. } => eff.push(Eff::Copy),
            TransferEffect::CopyCancel { .. } => eff.push(Eff::CopyCancelled),
            TransferEffect::Store { kind, .. } => eff.push(match kind {
                StoreKind::SyncUpload => Eff::StoreSyncUpload,
                StoreKind::ConfirmedParts => Eff::StoreConfirmedParts,
                StoreKind::Completed => Eff::StoreCompleted,
                StoreKind::Failed => Eff::StoreFailed,
                StoreKind::Retrying => Eff::StoreRetrying,
            }),
            TransferEffect::Step { purpose, .. } => {
                assert_eq!(purpose, NetworkPurpose::Transfer, "P-XFER-03: a step carries the purpose Transfer");
                eff.push(if matches!(input, In::StepOk { .. }) { Eff::NextStep } else { Eff::TransportStep });
            }
            TransferEffect::Complete { .. } => eff.push(Eff::Complete),
            TransferEffect::Abort { .. } => eff.push(Eff::Abort),
            TransferEffect::CancelStep { .. } => eff.push(if matches!(input, In::EpochEnded | In::Closing) { Eff::TransportCancel } else { Eff::StepCancelled }),
            TransferEffect::DeleteFile { .. } => eff.push(Eff::DeleteFile),
            TransferEffect::DeleteOperation { .. } => {
                if eff.last() == Some(&Eff::DeleteFile) { eff.pop(); }
                eff.push(Eff::DeleteFileAndOperation);
            }
            TransferEffect::DeleteLocalCopy { .. } => eff.push(Eff::DeleteLocalCopy),
            TransferEffect::SubmitMutation { .. } => eff.push(Eff::SubmitFinalMutation),
            TransferEffect::DiscardMutation { .. } => eff.push(Eff::DiscardMutation),
            TransferEffect::Reply { answer, .. } => obs.push(match answer {
                TransferAnswer::Receipt(_) => Obs::Receipt,
                TransferAnswer::Ok => Obs::DiscardOk,
                TransferAnswer::Cancelled(true) => Obs::CancelTrue,
                TransferAnswer::Cancelled(false) => Obs::CancelFalse,
                TransferAnswer::Err(Fault::InFlight) => Obs::CancelInFlight,
                TransferAnswer::Err(Fault::NotFound) => Obs::NotFound,
                TransferAnswer::Err(Fault::NoSession) => Obs::NoSession,
                TransferAnswer::Err(other) => panic!("a fault outside the table: {other:?}"),
            }),
            TransferEffect::Shows { shown, .. } => obs.push(Obs::Shows(shown_of(&shown))),
            TransferEffect::Refused { .. } | TransferEffect::EndJob { .. } => {}
            TransferEffect::ScheduleJob { .. } => eff.push(Eff::RoundNeeded),
            TransferEffect::Drained(_) => eff.push(Eff::Drained),
        }
    }
    if scheduled {
        eff.push(if matches!(input, In::Online(true)) { Eff::ScheduleExpire } else { Eff::ScheduleBackoff });
    }
    (obs, eff)
}

/// The part of the reference that the owner shows: `SlotFreed` and `StartInOrder` are inside the owner, progress is the `Shows` value.
fn visible(step: &Step) -> (Vec<Obs>, Vec<Eff>) {
    let obs = step.obs.iter().copied().filter(|obs| !matches!(obs, Obs::Progress | Obs::Progress100 | Obs::Interrupted | Obs::RowWithServerFile)).collect();
    let eff = step.eff.iter().copied().filter(|eff| !matches!(eff, Eff::SlotFreed | Eff::StartInOrder)).collect();
    (obs, eff)
}

fn sorted(mut obs: Vec<Obs>) -> Vec<String> {
    let mut named: Vec<String> = obs.drain(..).map(|obs| format!("{obs:?}")).collect();
    named.sort();
    named
}

fn row_check(state: St, ctx: Ctx, input: In) {
    let want = step_of(state, &ctx, input).unwrap_or_else(|| panic!("no table cell for {state:?} x {input:?}"));
    let mut twin = reach(state, &ctx);
    let before = twin.det.trace().len();
    let effects = input_of(&mut twin, &ctx, input);
    let scheduled = twin.det.trace()[before..].iter().any(|entry| entry.effects.contains("Schedule"));
    let (obs, eff) = translate(input, effects, scheduled);
    let (want_obs, want_eff) = visible(&want);
    assert_eq!(twin.stage(KEY), want.next, "{state:?} x {input:?}: the next state");
    assert_eq!(sorted(obs), sorted(want_obs), "{state:?} x {input:?}: the replies and the values");
    assert_eq!(eff, want_eff, "{state:?} x {input:?}: the effects");
}

fn ready() -> Ctx { Ctx::ready() }

fn multipart() -> Ctx { Ctx { has_upload_id: true, ..Ctx::ready() } }

fn batch() -> Ctx { Ctx { kind: Kind::Batch, ..Ctx::ready() } }

fn offline() -> Ctx { Ctx { online: false, ..Ctx::ready() } }

fn queued_behind() -> Ctx { Ctx { first_to_start: false, ..Ctx::ready() } }

fn all_parts() -> Ctx { Ctx { parts_all_confirmed: true, ..multipart() } }

fn stray() -> Ctx { Ctx { stray_step: true, ..Ctx::ready() } }

macro_rules! row {
    ($name:ident, $state:expr, $ctx:expr, $($input:expr),+) => {
        #[test]
        fn $name() { $( row_check($state, $ctx, $input); )+ }
    };
}

row!(xfer_accept, St::None, ready(), In::Accept);
row!(xfer_accept_stale, St::None, Ctx { epoch_current: false, ..ready() }, In::Accept);
row!(xfer_unknown_key, St::None, ready(), In::Cancel, In::Retry, In::Discard);
row!(xfer_copy_done, St::Copying, ready(), In::CopyDone);
row!(xfer_copy_failed, St::Copying, ready(), In::CopyFailed);
row!(xfer_copy_cancel, St::Copying, ready(), In::Cancel, In::Discard);
row!(xfer_copy_retry, St::Copying, ready(), In::Retry);
row!(xfer_copy_other, St::Copying, ready(), In::Noise(Noise::StepOk), In::Noise(Noise::StepError), In::Noise(Noise::Completed), In::Noise(Noise::CompleteError), In::Noise(Noise::MutationOutcome), In::Noise(Noise::TickRetry), In::Noise(Noise::OnlineTrue), In::Noise(Noise::SlotFreed));
row!(xfer_ready_start, St::Ready, ready(), In::Start);
row!(xfer_ready_wait, St::Ready, queued_behind(), In::Start);
row!(xfer_ready_offline, St::Ready, offline(), In::Online(false));
row!(xfer_ready_cancel, St::Ready, ready(), In::Cancel, In::Discard);
row!(xfer_ready_retry, St::Ready, ready(), In::Retry);
row!(xfer_ready_other, St::Ready, ready(), In::Noise(Noise::StepOk), In::Noise(Noise::StepError), In::Noise(Noise::Completed), In::Noise(Noise::CompleteError), In::Noise(Noise::MutationOutcome), In::Noise(Noise::TickRetry));
row!(xfer_step_ok, St::Uploading, multipart(), In::StepOk { parts_left: true });
row!(xfer_all_parts, St::Uploading, all_parts(), In::StepOk { parts_left: false });
row!(xfer_step_temp, St::Uploading, ready(), In::StepError { temporary: true });
row!(xfer_step_fail, St::Uploading, multipart(), In::StepError { temporary: false });
row!(xfer_step_fail_exhausted, St::Uploading, Ctx { attempts_left: false, ..multipart() }, In::StepError { temporary: true });
row!(xfer_step_tick, St::Uploading, ready(), In::TickRetry);
row!(xfer_step_offline, St::Uploading, ready(), In::Online(false));
row!(xfer_step_online, St::Uploading, ready(), In::Online(true), In::SlotFreed);
row!(xfer_uploading_cancel, St::Uploading, multipart(), In::Cancel);
row!(xfer_uploading_cancel_batch, St::Uploading, batch(), In::Cancel);
row!(xfer_uploading_discard, St::Uploading, multipart(), In::Discard);
row!(xfer_uploading_retry, St::Uploading, ready(), In::Retry);
row!(xfer_uploading_stray, St::Uploading, stray(), In::StepOk { parts_left: true }, In::StepError { temporary: true });
row!(xfer_uploading_other, St::Uploading, ready(), In::Noise(Noise::Completed), In::Noise(Noise::CompleteError), In::Noise(Noise::MutationOutcome));
row!(xfer_completed, St::Completing, multipart(), In::Completed);
row!(xfer_complete_temp, St::Completing, multipart(), In::CompleteTemp);
row!(xfer_complete_temp_offline, St::Completing, Ctx { online: false, ..multipart() }, In::CompleteTemp);
row!(xfer_complete_fail, St::Completing, multipart(), In::CompleteError);
row!(xfer_complete_tick, St::Completing, multipart(), In::TickRetry, In::Online(true));
row!(xfer_complete_offline, St::Completing, Ctx { online: false, ..multipart() }, In::TickRetry, In::Online(false));
row!(xfer_completing_cancel, St::Completing, multipart(), In::Cancel);
row!(xfer_completing_cancel_batch, St::Completing, Ctx { kind: Kind::Batch, ..multipart() }, In::Cancel);
row!(xfer_completing_discard, St::Completing, multipart(), In::Discard);
row!(xfer_completing_retry, St::Completing, multipart(), In::Retry);
row!(xfer_completing_other, St::Completing, multipart(), In::Noise(Noise::StepOk), In::Noise(Noise::StepError), In::Noise(Noise::MutationOutcome), In::Noise(Noise::SlotFreed));
row!(xfer_mutation_confirmed, St::Mutating, ready(), In::MutationOutcome { confirmed: true });
row!(xfer_mutation_failed, St::Mutating, ready(), In::MutationOutcome { confirmed: false });
row!(xfer_mutation_discarded, St::Mutating, ready(), In::MutationDiscarded);
row!(xfer_mutation_interrupted, St::Mutating, ready(), In::MutationInterrupted);
row!(xfer_mutating_cancel, St::Mutating, ready(), In::Cancel);
row!(xfer_mutating_cancel_batch, St::Mutating, batch(), In::Cancel);
row!(xfer_mutating_discard, St::Mutating, ready(), In::Discard);
row!(xfer_mutating_retry, St::Mutating, ready(), In::Retry);
row!(xfer_mutating_other, St::Mutating, ready(), In::Noise(Noise::StepOk), In::Noise(Noise::StepError), In::Noise(Noise::Completed), In::Noise(Noise::CompleteError), In::Noise(Noise::TickRetry), In::Noise(Noise::OnlineFalse), In::Noise(Noise::SlotFreed));
row!(xfer_retry_complete, St::Failed, all_parts(), In::Retry);
row!(xfer_retry, St::Failed, ready(), In::Retry);
row!(xfer_discard, St::Failed, multipart(), In::Discard, In::Cancel);
row!(xfer_discard_no_upload, St::Failed, ready(), In::Discard, In::Cancel);
row!(xfer_failed_online, St::Failed, ready(), In::Online(true));
row!(xfer_expired, St::Failed, Ctx { age_days: 8, ..multipart() }, In::Expire);
row!(xfer_not_expired, St::Failed, Ctx { age_days: 7, ..multipart() }, In::Expire);
row!(xfer_failed_other, St::Failed, ready(), In::Noise(Noise::OnlineFalse), In::Noise(Noise::TickRetry), In::Noise(Noise::StepOk), In::Noise(Noise::StepError), In::Noise(Noise::Completed), In::Noise(Noise::CompleteError), In::Noise(Noise::MutationOutcome), In::Noise(Noise::SlotFreed));
row!(xfer_epoch_ended, St::Uploading, multipart(), In::EpochEnded);
row!(xfer_closing_copying, St::Copying, ready(), In::Closing);
row!(xfer_closing, St::Uploading, multipart(), In::Closing);
row!(xfer_done_closing, St::Done, ready(), In::Closing);
row!(xfer_done, St::Done, ready(), In::Cancel, In::Retry, In::Discard, In::Noise(Noise::StepOk));

/// `Uploading x StepError/Tick(retry)` with `not online` is a dead cell: `Online(false)` moves `Uploading` to `Ready` first (hole H-XFER-02).
#[test]
fn xfer_step_temp_offline() {
    let want = step_of(St::Uploading, &Ctx { online: false, ..ready() }, In::StepError { temporary: true }).unwrap();
    let mut twin = reach(St::Uploading, &ready());
    twin.post(TransferInput::Online(Online(false)));
    assert_eq!(twin.stage(KEY), want.next, "after Online(false) the transfer is Ready");
    let effects = twin.post(TransferInput::StepError { key: KEY.into(), part: Some(1), temporary: true });
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Step { .. } | TransferEffect::Abort { .. })), "{effects:?}");
}

#[test]
fn xfer_step_tick_offline() {
    let want = step_of(St::Uploading, &Ctx { online: false, ..ready() }, In::TickRetry).unwrap();
    let mut twin = reach(St::Uploading, &ready());
    twin.post(TransferInput::Online(Online(false)));
    assert_eq!(twin.stage(KEY), want.next);
    let effects = twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key: KEY.into() } });
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Step { .. })), "{effects:?}");
}

#[test]
fn xfer_open_ready() {
    let ctx = Ctx { online: false, ..ready() };
    let mut twin = Twin::new();
    twin.post(TransferInput::Online(Online(false)));
    let effects = twin.post(TransferInput::Restored(vec![stored(KEY, &ctx, false, false, 1)]));
    assert_eq!(twin.stage(KEY), St::Ready);
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Queued, .. })));
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::ScheduleJob { epoch: EPOCH })));
}

#[test]
fn xfer_open_completing() {
    let mut twin = Twin::new();
    let effects = twin.post(TransferInput::Restored(vec![stored(KEY, &multipart(), false, false, PARTS)]));
    assert_eq!(twin.stage(KEY), St::Completing);
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Complete { .. })), "complete goes at once when online");
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Uploading { progress: 100 }, .. })));
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::ScheduleJob { .. })));
}

#[test]
fn xfer_open_mutating() {
    let mut twin = Twin::new();
    let effects = twin.post(TransferInput::Restored(vec![stored(KEY, &multipart(), false, true, PARTS)]));
    assert_eq!(twin.stage(KEY), St::Mutating);
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Complete { .. } | TransferEffect::Step { .. })), "{effects:?}");
}

#[test]
fn xfer_open_failed() {
    let mut twin = Twin::new();
    let effects = twin.post(TransferInput::Restored(vec![stored(KEY, &multipart(), true, false, 1)]));
    assert_eq!(twin.stage(KEY), St::Failed);
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { .. }, .. })));
    take(&twin.log);
    twin.det.tick(Time(8 * DAY));
    twin.run();
    assert_eq!(twin.stage(KEY), St::Done, "the expiry tick requested at open ends a failed transfer older than 7 days");
}

#[test]
fn xfer_open_missing_file() {
    let mut twin = Twin::new();
    let mut row = stored(KEY, &multipart(), false, false, 1);
    row.file_present = false;
    let effects = twin.post(TransferInput::Restored(vec![row]));
    assert_eq!(twin.stage(KEY), St::Failed);
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { error }, .. } if error == "missing_file")), "{effects:?}");
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Store { kind: StoreKind::Failed, .. })));
}

fn round_state(round: RoundState) -> Round {
    match round {
        RoundState::None => Round::None,
        RoundState::Requested { epoch } => Round::Requested(epoch),
        RoundState::Running { round_id, epoch, next } => Round::Running { id: round_id, epoch, next },
    }
}

fn round_in(twin: &mut Twin, input: RoundIn) -> Vec<TransferEffect> {
    match input {
        RoundIn::Needed(epoch) => twin.control(TransferInput::RoundNeeded { epoch }),
        RoundIn::Started(round_id, epoch) => twin.control(TransferInput::RoundStarted { round_id, epoch }),
        RoundIn::Refused(epoch) => twin.control(TransferInput::RoundRefused { epoch }),
        RoundIn::Ended(round_id, epoch) => twin.control(TransferInput::RoundEnded { round_id, epoch }),
        RoundIn::EpochEnded(epoch) => {
            take(&twin.log);
            twin.det.end_epoch(epoch, Cause::SessionReset);
            twin.run();
            take(&twin.log)
        }
        RoundIn::Closing => {
            take(&twin.log);
            twin.det.close();
            twin.run();
            take(&twin.log)
        }
    }
}

/// A round in the state `from`; the owner is in epoch 1, a transfer of epoch 2 is open when `next_open`.
fn round_reach(from: Round, next_open: bool) -> Twin {
    let mut twin = Twin::new();
    twin.post(TransferInput::Online(Online(false)));
    match from {
        Round::None => {}
        Round::Requested(_) => { round_in(&mut twin, RoundIn::Needed(EPOCH)); }
        Round::Running { id, next, .. } => {
            round_in(&mut twin, RoundIn::Needed(EPOCH));
            round_in(&mut twin, RoundIn::Started(id, EPOCH));
            if let Some(next_epoch) = next {
                twin.control(TransferInput::EpochStarted(EpochStarted { epoch: next_epoch, identity: Some(1) }));
                if next_open {
                    let reply = twin.reply();
                    take(&twin.log);
                    twin.det.start_epoch(next_epoch);
                    twin.det.post(twin.addr, EpochStamp::Epoch(next_epoch), TransferInput::Accept { epoch: next_epoch, key: "n1".into(), source: source(), batch: false, reply, now: Time(0) });
                    twin.run();
                }
                round_in(&mut twin, RoundIn::Needed(next_epoch));
            }
        }
    }
    twin
}

fn round_check(from: Round, input: RoundIn, next_open: bool) {
    let epoch = if matches!(from, Round::Running { next: Some(_), .. }) && next_open { EPOCH + 1 } else { EPOCH };
    let want = round_step(from, epoch, input, next_open).unwrap_or_else(|| panic!("no table cell for {from:?} x {input:?}"));
    let mut twin = round_reach(from, next_open);
    assert_eq!(round_state(twin.state().round), from, "the round reached {from:?}");
    let effects = round_in(&mut twin, input);
    assert_eq!(round_state(twin.state().round), want.next, "{from:?} x {input:?}: the next round state");
    let scheduled = effects.iter().find_map(|effect| if let TransferEffect::ScheduleJob { epoch } = effect { Some(*epoch) } else { None });
    let ended = effects.iter().find_map(|effect| if let TransferEffect::EndJob { round_id } = effect { Some(*round_id) } else { None });
    let refused = effects.iter().any(|effect| matches!(effect, TransferEffect::Refused { .. }));
    assert_eq!((scheduled, ended, refused), (want.schedule_job, want.end_job, want.refused_error), "{from:?} x {input:?}: the platform effects");
}

fn running() -> Round { Round::Running { id: 7, epoch: EPOCH, next: None } }

fn running_next() -> Round { Round::Running { id: 7, epoch: EPOCH, next: Some(2) } }

#[test]
fn xfer_round_request() { round_check(Round::None, RoundIn::Needed(EPOCH), false); }

#[test]
fn xfer_round_request_stale() { round_check(Round::None, RoundIn::Needed(EPOCH + 5), false); }

#[test]
fn xfer_round_started() { round_check(Round::Requested(EPOCH), RoundIn::Started(7, EPOCH), false); }

#[test]
fn xfer_round_started_stale() { round_check(Round::Requested(EPOCH), RoundIn::Started(7, EPOCH + 1), false); }

#[test]
fn xfer_round_refused() {
    round_check(Round::Requested(EPOCH), RoundIn::Refused(EPOCH), false);
    let mut twin = Twin::new();
    twin.post(TransferInput::Online(Online(false)));
    for key in ["a", "b"] {
        twin.accept(key, false);
        twin.post(TransferInput::CopyDone { key: key.into() });
    }
    let effects = round_in(&mut twin, RoundIn::Refused(EPOCH));
    let refused: Vec<_> = effects.iter().filter_map(|effect| if let TransferEffect::Refused { key } = effect { Some(key.clone()) } else { None }).collect();
    assert_eq!(refused, ["a".to_string()], "the error lands on the first accepted transfer by (seq, position)");
}

#[test]
fn xfer_round_needed_requested() { round_check(Round::Requested(EPOCH), RoundIn::Needed(EPOCH), false); }

#[test]
fn xfer_round_needed_running() { round_check(running(), RoundIn::Needed(EPOCH), false); }

#[test]
fn xfer_round_needed_new_epoch() { round_check(running(), RoundIn::Needed(2), false); }

#[test]
fn xfer_round_started_during_running() {
    round_check(running(), RoundIn::Started(8, EPOCH), false);
    round_check(running(), RoundIn::Refused(EPOCH + 1), false);
}

#[test]
fn xfer_round_end_next() { round_check(running_next(), RoundIn::Ended(7, EPOCH), true); }

#[test]
fn xfer_round_end() { round_check(running(), RoundIn::Ended(7, EPOCH), false); }

#[test]
fn xfer_round_end_stale() { round_check(running(), RoundIn::Ended(8, EPOCH), false); }

#[test]
fn xfer_round_epoch_ended() {
    round_check(Round::Requested(EPOCH), RoundIn::EpochEnded(EPOCH), false);
    round_check(running(), RoundIn::EpochEnded(EPOCH), false);
}

#[test]
fn xfer_round_closing() {
    round_check(running(), RoundIn::Closing, false);
    round_check(Round::Requested(EPOCH), RoundIn::Closing, false);
}

#[test]
fn xfer_done_closing_without_transfers() {
    let mut twin = Twin::new();
    take(&twin.log);
    twin.det.close();
    twin.run();
    assert_eq!(take(&twin.log).iter().filter(|effect| matches!(effect, TransferEffect::Drained(_))).count(), 1);
}

fn replies_of(effects: &[TransferEffect], reply: u64) -> Vec<TransferAnswer> {
    effects.iter().filter_map(|effect| match effect {
        TransferEffect::Reply { reply: id, answer } if *id == reply => Some(answer.clone()),
        _ => None,
    }).collect()
}

#[test]
fn xfer_wait_accept() {
    let mut twin = Twin::new();
    let effects = twin.post(TransferInput::Accept { epoch: EPOCH, key: KEY.into(), source: source(), batch: false, reply: 1, now: Time(0) });
    assert!(matches!(replies_of(&effects, 1).as_slice(), [TransferAnswer::Receipt(_)]));
    let stale = twin.post(TransferInput::Accept { epoch: EPOCH + 1, key: "x".into(), source: source(), batch: false, reply: 2, now: Time(0) });
    assert_eq!(replies_of(&stale, 2), [TransferAnswer::Err(Fault::NoSession)]);
    assert_eq!(twin.stage("x"), St::Done, "a stale accept leaves no transfer");
}

#[test]
fn xfer_wait_accept_batch() {
    let mut twin = Twin::new();
    let mut answered = Vec::new();
    for (index, key) in ["b1", "b2", "b3"].iter().enumerate() {
        let effects = twin.post(TransferInput::Accept { epoch: EPOCH, key: (*key).into(), source: source(), batch: true, reply: 10 + index as u64, now: Time(0) });
        answered.push(replies_of(&effects, 10 + index as u64));
    }
    assert!(answered.iter().all(|answers| matches!(answers.as_slice(), [TransferAnswer::Receipt(_)])));
    assert!(twin.state().transfers.values().all(|transfer| transfer.batch));
    take(&twin.log);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert!(["b1", "b2", "b3"].iter().all(|key| twin.stage(key) == St::Done), "accepted items end Interrupted with the epoch");
}

#[test]
fn xfer_wait_cancel() {
    let mut twin = reach(St::Ready, &ready());
    let ok = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 21 });
    assert_eq!(replies_of(&ok, 21), [TransferAnswer::Cancelled(true)]);
    let mut twin = reach(St::Mutating, &ready());
    let late = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 22 });
    assert_eq!(replies_of(&late, 22), [TransferAnswer::Cancelled(false)]);
    let mut twin = reach(St::Uploading, &batch());
    let blocked = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 23 });
    assert_eq!(replies_of(&blocked, 23), [TransferAnswer::Err(Fault::InFlight)]);
    let none = Twin::new().post(TransferInput::Cancel { key: "none".into(), reply: 24 });
    assert_eq!(replies_of(&none, 24), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_retry() {
    let mut twin = reach(St::Failed, &ready());
    let ok = twin.post(TransferInput::Retry { key: KEY.into(), reply: 31 });
    assert!(matches!(replies_of(&ok, 31).as_slice(), [TransferAnswer::Receipt(_)]));
    let none = Twin::new().post(TransferInput::Retry { key: "none".into(), reply: 32 });
    assert_eq!(replies_of(&none, 32), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_discard() {
    let mut twin = reach(St::Failed, &ready());
    let ok = twin.post(TransferInput::Discard { key: KEY.into(), reply: 41 });
    assert_eq!(replies_of(&ok, 41), [TransferAnswer::Ok]);
    let none = Twin::new().post(TransferInput::Discard { key: "none".into(), reply: 42 });
    assert_eq!(replies_of(&none, 42), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_outcome() {
    let mut twin = reach(St::Mutating, &ready());
    let effects = twin.post(TransferInput::MutationOutcome { key: KEY.into(), outcome: OutboxOutcome::Confirmed { revision: 3 } });
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Gone, .. })));
    let mut twin = reach(St::Mutating, &ready());
    let effects = twin.post(TransferInput::MutationOutcome { key: KEY.into(), outcome: OutboxOutcome::Failed { error: "e".into() } });
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { .. }, .. })));
}

// Properties: the explorer walks the orders of the inputs of three sources.

use crate::owners::support::{snapshot, Silent, Source as Feeder, SourceInput, SourceState};
use crate::runtime::{explore, Limits};
use std::cell::RefCell;

thread_local! { static LOG: RefCell<Option<Log<TransferEffect>>> = const { RefCell::new(None) }; }

fn scripted(sources: Vec<Vec<TransferInput>>, slots: u32) -> DetScheduler {
    let mut det = DetScheduler::new(3);
    let log = new_log();
    LOG.with(|cell| *cell.borrow_mut() = Some(log.clone()));
    let addr = det.add::<Transfers>("transfer", TransferState::new(EPOCH, slots), Recorder::<Transfers>::new(&log));
    let feeders: Vec<_> = sources.into_iter().map(|script| det.add::<Feeder<Transfers>>("source", SourceState { to: addr, script }, Silent)).collect();
    det.start_epoch(EPOCH);
    for feeder in feeders {
        det.post(feeder, EpochStamp::Control, SourceInput::Go);
    }
    det
}

fn accepts(keys: &[&str]) -> Vec<TransferInput> {
    keys.iter().enumerate().map(|(index, key)| TransferInput::Accept { epoch: EPOCH, key: (*key).into(), source: source(), batch: false, reply: 100 + index as u64, now: Time(0) }).collect()
}

fn copies(keys: &[&str]) -> Vec<TransferInput> { keys.iter().map(|key| TransferInput::CopyDone { key: (*key).into() }).collect() }

fn net(keys: &[&str]) -> Vec<TransferInput> {
    let mut script = Vec::new();
    for key in keys {
        script.push(TransferInput::StepOk { key: (*key).into(), part: Some(1) });
        script.push(TransferInput::StepError { key: (*key).into(), part: Some(1), temporary: false });
        script.push(TransferInput::Completed { key: (*key).into() });
    }
    script.push(TransferInput::Online(Online(false)));
    script.push(TransferInput::Online(Online(true)));
    script
}

fn walk(sources: impl Fn() -> Vec<Vec<TransferInput>>, slots: u32, check: impl FnMut(&DetScheduler) -> Result<(), String>) {
    let report = explore(|| scripted(sources(), slots), Limits { depth: 6, max_traces: 4_000 }, check);
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 0);
}

fn state_of(det: &DetScheduler) -> Result<&TransferState, String> { det.state(Address::<Transfers>::new(0)).ok_or_else(|| "no state".to_string()) }

fn effects_now() -> Vec<TransferEffect> { LOG.with(|cell| cell.borrow().as_ref().map(snapshot).unwrap_or_default()) }

#[test]
fn xfer_prop_outcome() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a", "b"])], 1, |_| {
        let effects = effects_now();
        let receipts = effects.iter().filter(|effect| matches!(effect, TransferEffect::Reply { answer: TransferAnswer::Receipt(_), .. })).count();
        let gone = effects.iter().filter(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Gone, .. })).count();
        if gone > receipts { Err("a transfer ended that was never accepted".into()) } else { Ok(()) }
    });
}

#[test]
fn xfer_prop_abort_rule() {
    walk(|| vec![accepts(&["a"]), copies(&["a"]), net(&["a"])], 1, |_| {
        let aborts = effects_now().iter().filter(|effect| matches!(effect, TransferEffect::Abort { .. })).count();
        if aborts > 1 { Err("abort was called twice".into()) } else { Ok(()) }
    });
    let mut twin = reach(St::Failed, &all_parts());
    let effects = twin.post(TransferInput::Retry { key: KEY.into(), reply: 1 });
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Abort { .. })), "retry after CompleteError does not abort");
}

#[test]
fn xfer_prop_purpose() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a"])], 2, |_| {
        match effects_now().iter().find(|effect| matches!(effect, TransferEffect::Step { purpose, .. } if *purpose != NetworkPurpose::Transfer)) {
            Some(effect) => Err(format!("a step without the purpose Transfer: {effect:?}")),
            None => Ok(()),
        }
    });
}

#[test]
fn xfer_prop_epoch() {
    let mut det = scripted(vec![accepts(&["a"]), copies(&["a"])], 1);
    det.run_until_quiet(10_000).unwrap();
    det.end_epoch(EPOCH, Cause::SessionReset);
    det.run_until_quiet(10_000).unwrap();
    take(&LOG.with(|cell| cell.borrow().clone().unwrap()));
    for input in [TransferInput::Online(Online(true)), TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key: "a".into() } }, TransferInput::SlotFreed] {
        det.post(Address::<Transfers>::new(0), EpochStamp::Epoch(EPOCH), input);
    }
    det.run_until_quiet(10_000).unwrap();
    let after = effects_now();
    assert!(after.iter().all(|effect| !matches!(effect, TransferEffect::Step { .. } | TransferEffect::Complete { .. } | TransferEffect::Abort { .. })), "{after:?}");
}

#[test]
fn xfer_prop_round_covers() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a"]), vec![TransferInput::RoundStarted { round_id: 7, epoch: EPOCH }, TransferInput::RoundEnded { round_id: 7, epoch: EPOCH }]], 1, |det| {
        let state = state_of(det)?;
        let open = state.transfers.values().any(|transfer| transfer.epoch == EPOCH && matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading | TransferStage::Completing | TransferStage::Mutating));
        match (open, state.round) {
            (true, RoundState::None) if !effects_now().iter().any(|effect| matches!(effect, TransferEffect::ScheduleJob { epoch: EPOCH })) => Err("an open transfer without a requested round".into()),
            _ => Ok(()),
        }
    });
}

#[test]
fn xfer_prop_order() {
    walk(|| vec![accepts(&["a", "b", "c"]), copies(&["a", "b", "c"]), net(&["a", "b", "c"])], 1, |det| {
        let state = state_of(det)?;
        let active = state.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing)).count();
        if active > state.slots as usize { return Err(format!("{active} transfers hold {} slots", state.slots)); }
        let mut started: Vec<&str> = Vec::new();
        for effect in effects_now() {
            if let TransferEffect::Step { key, .. } = &effect {
                if !started.contains(&key.as_str()) { started.push(Box::leak(key.clone().into_boxed_str())); }
            }
        }
        let mut sorted = started.clone();
        sorted.sort();
        if started == sorted { Ok(()) } else { Err(format!("starts out of order: {started:?}")) }
    });
}

#[test]
fn xfer_prop_idle() {
    let mut det = scripted(vec![accepts(&["a"]), copies(&["a"])], 1);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Transfers>::new(0), EpochStamp::Epoch(EPOCH), TransferInput::Cancel { key: "a".into(), reply: 9 });
    det.run_until_quiet(10_000).unwrap();
    let before = det.trace().len();
    det.tick(Time(u64::MAX / 2));
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(det.trace().len(), before, "no transfer waits: a tick wakes nothing");
}

#[test]
fn xfer_prop_file_kept() {
    let mut det = scripted(vec![accepts(&["a"]), copies(&["a"])], 1);
    det.run_until_quiet(10_000).unwrap();
    take(&LOG.with(|cell| cell.borrow().clone().unwrap()));
    det.close();
    det.run_until_quiet(10_000).unwrap();
    det.end_epoch(EPOCH, Cause::SessionReset);
    det.run_until_quiet(10_000).unwrap();
    let effects = effects_now();
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::DeleteFile { .. } | TransferEffect::DeleteOperation { .. })), "{effects:?}");
}

#[test]
fn xfer_prop_batch_all_or_none() {
    let mut twin = Twin::new();
    for key in ["b1", "b2"] {
        twin.accept(key, true);
    }
    twin.post(TransferInput::Restored(vec![stored("b3", &batch(), false, false, 0)]));
    let batch_keys: Vec<_> = twin.state().transfers.values().filter(|transfer| transfer.batch).map(|transfer| transfer.key.clone()).collect();
    assert_eq!(batch_keys.len(), 3);
    for key in &batch_keys {
        let effects = twin.post(TransferInput::Cancel { key: key.clone(), reply: 7 });
        let in_flight = matches!(twin.state().stage_of(key), Some(TransferStage::Uploading | TransferStage::Completing));
        if in_flight {
            assert_eq!(replies_of(&effects, 7), [TransferAnswer::Err(Fault::InFlight)]);
        }
    }
}
