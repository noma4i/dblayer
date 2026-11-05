//! Owner-level tests of the `transfer` machine: the rows of the background round.



use super::ref_transfer::{round_step, Round, RoundIn};
use crate::owners::contract::{EpochStarted, Online};

use crate::files::Location;



use crate::owners::write::transfer::{RoundState, TransferInput};
use crate::runtime::{Cause, EpochStamp, Time};

use super::transfer_harness::*;

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
            take_fx(&twin.log);
            twin.det.end_epoch(epoch, Cause::SessionReset);
            twin.run();
            take_fx(&twin.log)
        }
        RoundIn::Closing => {
            take_fx(&twin.log);
            twin.det.close();
            twin.run();
            take_fx(&twin.log)
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
                twin.control(TransferInput::EpochStarted(EpochStarted { epoch: next_epoch, identity: Some(1), generation: 0 }));
                if next_open {
                    let reply = twin.reply();
                    take_fx(&twin.log);
                    twin.det.start_epoch(next_epoch);
                    twin.det.post(twin.addr, EpochStamp::Epoch(next_epoch), TransferInput::Accept { epoch: next_epoch, key: "n1".into(), source: source(), batch: false, reply, now: Time(0), mutation: mutation(), steps: steps() });
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
    if matches!(input, RoundIn::Refused(_)) {
        twin.accept("a", false);
        twin.post(TransferInput::CopyDone { key: "a".into(), size: 1, sha256: "h".into(), place: Location::Directory });
    }
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
        twin.post(TransferInput::CopyDone { key: key.into(), size: 1, sha256: "h".into(), place: Location::Directory });
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
    take_fx(&twin.log);
    twin.det.close();
    twin.run();
    assert_eq!(take_fx(&twin.log).iter().filter(|effect| matches!(effect, TransferEffect::Drained)).count(), 1);
}
