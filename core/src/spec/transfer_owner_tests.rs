//! Owner-level tests of the `transfer` machine: the rows of the table of a transfer, each against the reference (`ref_transfer.rs`). Names are the `Test` column.



use super::ref_transfer::{step_of, Ctx, In, Kind, Noise, St};
use crate::owners::contract::{Online};





use crate::owners::write::transfer::{self as t, StoreKind, TransferInput, TransferTick};
use crate::runtime::{Time};

use super::transfer_harness::*;

#[test]
fn xfer_accept() { row_check(St::None, ready(), In::Accept); }

#[test]
fn xfer_accept_stale() { row_check(St::None, Ctx { epoch_current: false, ..ready() }, In::Accept); }

#[test]
fn xfer_unknown_key() { row_check(St::None, ready(), In::Cancel); row_check(St::None, ready(), In::Retry); row_check(St::None, ready(), In::Discard); }

#[test]
fn xfer_copy_done() { row_check(St::Copying, ready(), In::CopyDone); }

#[test]
fn xfer_copy_failed() { row_check(St::Copying, ready(), In::CopyFailed); }

#[test]
fn xfer_copy_cancel() { row_check(St::Copying, ready(), In::Cancel); row_check(St::Copying, ready(), In::Discard); }

#[test]
fn xfer_copy_retry() { row_check(St::Copying, ready(), In::Retry); }

#[test]
fn xfer_copy_other() { row_check(St::Copying, ready(), In::Noise(Noise::StepOk)); row_check(St::Copying, ready(), In::Noise(Noise::StepError)); row_check(St::Copying, ready(), In::Noise(Noise::Completed)); row_check(St::Copying, ready(), In::Noise(Noise::CompleteError)); row_check(St::Copying, ready(), In::Noise(Noise::MutationOutcome)); row_check(St::Copying, ready(), In::Noise(Noise::TickRetry)); row_check(St::Copying, ready(), In::Noise(Noise::OnlineTrue)); row_check(St::Copying, ready(), In::Noise(Noise::SlotFreed)); }

#[test]
fn xfer_ready_start() { row_check(St::Ready, ready(), In::Start); }

#[test]
fn xfer_ready_wait() { row_check(St::Ready, queued_behind(), In::Start); }

#[test]
fn xfer_ready_offline() { row_check(St::Ready, offline(), In::Online(false)); }

#[test]
fn xfer_ready_cancel() { row_check(St::Ready, ready(), In::Cancel); row_check(St::Ready, ready(), In::Discard); }

#[test]
fn xfer_ready_retry() { row_check(St::Ready, ready(), In::Retry); }

#[test]
fn xfer_ready_other() { row_check(St::Ready, ready(), In::Noise(Noise::StepOk)); row_check(St::Ready, ready(), In::Noise(Noise::StepError)); row_check(St::Ready, ready(), In::Noise(Noise::Completed)); row_check(St::Ready, ready(), In::Noise(Noise::CompleteError)); row_check(St::Ready, ready(), In::Noise(Noise::MutationOutcome)); row_check(St::Ready, ready(), In::Noise(Noise::TickRetry)); }

#[test]
fn xfer_step_ok() { row_check(St::Uploading, multipart(), In::StepOk { parts_left: true }); }

#[test]
fn xfer_all_parts() { row_check(St::Uploading, all_parts(), In::StepOk { parts_left: false }); }

#[test]
fn xfer_step_temp() { row_check(St::Uploading, ready(), In::StepError { temporary: true }); }

#[test]
fn xfer_step_fail() { row_check(St::Uploading, multipart(), In::StepError { temporary: false }); }

#[test]
fn xfer_step_fail_exhausted() { row_check(St::Uploading, Ctx { attempts_left: false, ..multipart() }, In::StepError { temporary: true }); }

#[test]
fn xfer_step_tick() { row_check(St::Uploading, ready(), In::TickRetry); }

#[test]
fn xfer_step_offline() { row_check(St::Uploading, ready(), In::Online(false)); }

#[test]
fn xfer_step_online() { row_check(St::Uploading, ready(), In::Online(true)); row_check(St::Uploading, ready(), In::SlotFreed); }

#[test]
fn xfer_uploading_cancel() { row_check(St::Uploading, multipart(), In::Cancel); }

#[test]
fn xfer_uploading_cancel_batch() { row_check(St::Uploading, batch(), In::Cancel); }

#[test]
fn xfer_uploading_discard() { row_check(St::Uploading, multipart(), In::Discard); }

#[test]
fn xfer_uploading_retry() { row_check(St::Uploading, ready(), In::Retry); }

#[test]
fn xfer_uploading_stray() { row_check(St::Uploading, stray(), In::StepOk { parts_left: true }); row_check(St::Uploading, stray(), In::StepError { temporary: true }); }

#[test]
fn xfer_uploading_other() { row_check(St::Uploading, ready(), In::Noise(Noise::Completed)); row_check(St::Uploading, ready(), In::Noise(Noise::CompleteError)); row_check(St::Uploading, ready(), In::Noise(Noise::MutationOutcome)); }

#[test]
fn xfer_completed() { row_check(St::Completing, multipart(), In::Completed); }

#[test]
fn xfer_complete_temp() { row_check(St::Completing, multipart(), In::CompleteTemp); }

#[test]
fn xfer_complete_temp_offline() { row_check(St::Completing, Ctx { online: false, ..multipart() }, In::CompleteTemp); }

#[test]
fn xfer_complete_fail() { row_check(St::Completing, multipart(), In::CompleteError); }

#[test]
fn xfer_complete_tick() { row_check(St::Completing, multipart(), In::TickRetry); row_check(St::Completing, multipart(), In::Online(true)); }

#[test]
fn xfer_complete_offline() { row_check(St::Completing, Ctx { online: false, ..multipart() }, In::TickRetry); row_check(St::Completing, Ctx { online: false, ..multipart() }, In::Online(false)); }

#[test]
fn xfer_completing_cancel() { row_check(St::Completing, multipart(), In::Cancel); }

#[test]
fn xfer_completing_cancel_batch() { row_check(St::Completing, Ctx { kind: Kind::Batch, ..multipart() }, In::Cancel); }

#[test]
fn xfer_completing_discard() { row_check(St::Completing, multipart(), In::Discard); }

#[test]
fn xfer_completing_retry() { row_check(St::Completing, multipart(), In::Retry); }

#[test]
fn xfer_completing_other() { row_check(St::Completing, multipart(), In::Noise(Noise::StepOk)); row_check(St::Completing, multipart(), In::Noise(Noise::StepError)); row_check(St::Completing, multipart(), In::Noise(Noise::MutationOutcome)); row_check(St::Completing, multipart(), In::Noise(Noise::SlotFreed)); }

#[test]
fn xfer_mutation_confirmed() { row_check(St::Mutating, ready(), In::MutationOutcome { confirmed: true }); }

#[test]
fn xfer_mutation_failed() { row_check(St::Mutating, ready(), In::MutationOutcome { confirmed: false }); }

#[test]
fn xfer_mutation_discarded() { row_check(St::Mutating, ready(), In::MutationDiscarded); }

#[test]
fn xfer_mutation_interrupted() { row_check(St::Mutating, ready(), In::MutationInterrupted); }

#[test]
fn xfer_mutating_cancel() { row_check(St::Mutating, ready(), In::Cancel); }

#[test]
fn xfer_mutating_cancel_batch() { row_check(St::Mutating, batch(), In::Cancel); }

#[test]
fn xfer_mutating_discard() { row_check(St::Mutating, ready(), In::Discard); }

#[test]
fn xfer_mutating_retry() { row_check(St::Mutating, ready(), In::Retry); }

#[test]
fn xfer_mutating_other() { row_check(St::Mutating, ready(), In::Noise(Noise::StepOk)); row_check(St::Mutating, ready(), In::Noise(Noise::StepError)); row_check(St::Mutating, ready(), In::Noise(Noise::Completed)); row_check(St::Mutating, ready(), In::Noise(Noise::CompleteError)); row_check(St::Mutating, ready(), In::Noise(Noise::TickRetry)); row_check(St::Mutating, ready(), In::Noise(Noise::OnlineFalse)); row_check(St::Mutating, ready(), In::Noise(Noise::SlotFreed)); }

#[test]
fn xfer_retry_complete() { row_check(St::Failed, all_parts(), In::Retry); }

#[test]
fn xfer_retry() { row_check(St::Failed, ready(), In::Retry); }

#[test]
fn xfer_discard() { row_check(St::Failed, multipart(), In::Discard); row_check(St::Failed, multipart(), In::Cancel); }

#[test]
fn xfer_discard_no_upload() { row_check(St::Failed, ready(), In::Discard); row_check(St::Failed, ready(), In::Cancel); }

#[test]
fn xfer_failed_online() { row_check(St::Failed, ready(), In::Online(true)); }

#[test]
fn xfer_expired() { row_check(St::Failed, Ctx { age_days: 8, ..multipart() }, In::Expire); }

#[test]
fn xfer_not_expired() { row_check(St::Failed, Ctx { age_days: 7, ..multipart() }, In::Expire); }

#[test]
fn xfer_failed_other() { row_check(St::Failed, ready(), In::Noise(Noise::OnlineFalse)); row_check(St::Failed, ready(), In::Noise(Noise::TickRetry)); row_check(St::Failed, ready(), In::Noise(Noise::StepOk)); row_check(St::Failed, ready(), In::Noise(Noise::StepError)); row_check(St::Failed, ready(), In::Noise(Noise::Completed)); row_check(St::Failed, ready(), In::Noise(Noise::CompleteError)); row_check(St::Failed, ready(), In::Noise(Noise::MutationOutcome)); row_check(St::Failed, ready(), In::Noise(Noise::SlotFreed)); }

#[test]
fn xfer_epoch_ended() { row_check(St::Uploading, multipart(), In::EpochEnded); }

#[test]
fn xfer_closing_copying() { row_check(St::Copying, ready(), In::Closing); }

#[test]
fn xfer_closing() { row_check(St::Uploading, multipart(), In::Closing); }

#[test]
fn xfer_done_closing() { row_check(St::Done, ready(), In::Closing); }

#[test]
fn xfer_done() { row_check(St::Done, ready(), In::Cancel); row_check(St::Done, ready(), In::Retry); row_check(St::Done, ready(), In::Discard); row_check(St::Done, ready(), In::Noise(Noise::StepOk)); }


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
    twin.det.tick(Time(8 * DAY));
    let effects = twin.post(TransferInput::Restored(vec![stored(KEY, &multipart(), true, false, 1)]));
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { .. }, .. })));
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
