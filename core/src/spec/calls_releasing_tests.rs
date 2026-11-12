//! Owner-level tests of the `calls` owner: a call in `Releasing` ends with the reader's cancel or a new epoch and leaves nothing to the next call (D-348).

use crate::owners::calls::{CallKind, Calls, CallsEffect, CallsInput, CallsState};
use crate::runtime::Time;

const EPOCH: u64 = 1;

fn step(state: &mut CallsState, input: CallsInput) -> Vec<CallsEffect> {
    use crate::runtime::{Effect, StateOwner};
    Calls::step(state, input, Time(0)).into_iter().filter_map(|effect| if let Effect::Run(effect) = effect { Some(effect) } else { None }).collect()
}

fn run(epoch: u64, reply: u64) -> CallsInput { CallsInput::Run { epoch, operation: "Change".into(), variables: "{}".into(), reply } }

fn releasing() -> CallsState {
    let mut state = CallsState::new(EPOCH, Some(true), &[("Change", CallKind::Mutate)]);
    let sent = request_id_of(&step(&mut state, run(EPOCH, 1)));
    assert_eq!(step(&mut state, CallsInput::Deadline { call_id: 1 }), vec![CallsEffect::Release { request_id: sent }], "the first deadline of a sent mutation releases its pair");
    state
}

fn call_id_of(effects: &[CallsEffect]) -> u64 {
    effects.iter().find_map(|effect| if let CallsEffect::Request { call_id, .. } = effect { Some(*call_id) } else { None }).expect("the run sends a request")
}

fn request_id_of(effects: &[CallsEffect]) -> u64 {
    effects.iter().find_map(|effect| if let CallsEffect::Request { request_id, .. } = effect { Some(*request_id) } else { None }).expect("the run sends a request")
}

/// A call in `Releasing` that the reader cancels or a new epoch drops: Session hears nothing more about the old pair, the next call with the same reply id is an ordinary call (its first deadline releases it once, no answer).
#[test]
fn call_releasing_caller_cancel_and_epoch_activated() {
    let mut cancelled = releasing();
    assert_eq!(step(&mut cancelled, CallsInput::CallerCancel { reply: 1 }), Vec::<CallsEffect>::new(), "the Release of this pair is out already");
    assert_eq!(step(&mut cancelled, CallsInput::Dropped { call_id: 1 }), Vec::<CallsEffect>::new(), "the late verdict of the cancelled call has no reader");
    let started = step(&mut cancelled, run(EPOCH, 1));
    let (call, next) = (call_id_of(&started), request_id_of(&started));
    assert_eq!(step(&mut cancelled, CallsInput::Deadline { call_id: call }), vec![CallsEffect::Release { request_id: next }], "after the cancel: an ordinary first deadline, no answer");

    let mut activated = releasing();
    assert_eq!(step(&mut activated, CallsInput::ActivateEpoch(EPOCH + 1)), vec![CallsEffect::SnapshotRequest { epoch: EPOCH + 1 }], "the new epoch only asks for its snapshot");
    assert_eq!(step(&mut activated, CallsInput::Dropped { call_id: 1 }), Vec::<CallsEffect>::new(), "the late verdict of the old epoch has no reader");
    step(&mut activated, CallsInput::Snapshot(crate::owners::contract::Online(true)));
    let started = step(&mut activated, run(EPOCH + 1, 1));
    let (call, next) = (call_id_of(&started), request_id_of(&started));
    assert_eq!(step(&mut activated, CallsInput::Deadline { call_id: call }), vec![CallsEffect::Release { request_id: next }], "new epoch: an ordinary first deadline, no answer");
}
