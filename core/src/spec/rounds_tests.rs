//! Owner-level tests of the owner `Rounds`: the background round of the system job, the states of a job and the requests of the platform.

use std::collections::BTreeSet;

use crate::owners::contract::{EpochEnded, EpochStarted};
use crate::owners::write::rounds::{Round, Rounds, RoundsEffect, RoundsInput, RoundsState};
use crate::owners::write::transfer::Transfers;
use crate::runtime::{Address, Cause, Effect, StateOwner};

fn state(epoch: u64) -> RoundsState {
    let mut state = RoundsState::new(Address::<Transfers>::new(2));
    state.current_epoch = epoch;
    state.sink = true;
    state
}

fn busy(state: &mut RoundsState, epoch: u64, ids: &[u64]) { state.active.insert(epoch, ids.iter().copied().collect::<BTreeSet<u64>>()); }

/// The effects of one step: the runs, and the `Debug` form of the inputs sent to Transfer.
fn step(state: &mut RoundsState, input: RoundsInput) -> (Vec<RoundsEffect>, Vec<String>) {
    let mut runs = Vec::new();
    let mut sends = Vec::new();
    for effect in Rounds::step(state, input) {
        match effect {
            Effect::Run(run) => runs.push(run),
            Effect::Send(outbound) => sends.push(format!("{:?}", outbound.payload)),
            other => panic!("an effect outside the table: {other:?}"),
        }
    }
    (runs, sends)
}

fn jobs(runs: &[RoundsEffect]) -> Vec<(u64, u64)> { runs.iter().filter_map(|run| if let RoundsEffect::ScheduleJob { request_id, epoch } = run { Some((*request_id, *epoch)) } else { None }).collect() }

fn ended_jobs(runs: &[RoundsEffect]) -> Vec<u32> { runs.iter().filter_map(|run| if let RoundsEffect::EndJob { round_id } = run { Some(*round_id) } else { None }).collect() }

fn requested(state: &mut RoundsState, epoch: u64, ids: &[u64]) -> u64 {
    let (runs, _) = step(state, RoundsInput::Needed { epoch, ids: ids.to_vec() });
    jobs(&runs)[0].0
}

fn running(state: &mut RoundsState, epoch: u64, round_id: u32) {
    let request_id = requested(state, epoch, &[10]);
    step(state, RoundsInput::RoundStarted { request_id, round_id, epoch });
    assert!(matches!(state.round, Round::Running { .. }));
}

#[test]
fn xfer_round_request_stale() {
    let mut state = state(1);
    let (runs, sends) = step(&mut state, RoundsInput::Needed { epoch: 2, ids: vec![10] });
    assert_eq!((runs.len(), sends.len(), state.round, state.active.is_empty()), (0, 0, Round::None, true));
}

#[test]
fn xfer_round_request() {
    let mut state = state(1);
    let (runs, _) = step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![10] });
    assert_eq!((jobs(&runs), state.round, state.active[&1].len()), (vec![(1, 1)], Round::Requested { epoch: 1, request_id: 1 }, 1));
}

#[test]
fn xfer_round_unexpected_started() {
    for round in [Round::None, Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }] {
        let mut state = state(1);
        state.round = round;
        let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 7, round_id: 3, epoch: 1 });
        assert_eq!((ended_jobs(&runs), state.round), (vec![3], round), "{round:?}");
    }
}

#[test]
fn xfer_round_foreground() {
    let mut state = state(1);
    busy(&mut state, 1, &[10]);
    let (runs, _) = step(&mut state, RoundsInput::Foreground);
    assert_eq!((jobs(&runs), matches!(state.round, Round::Requested { epoch: 1, .. })), (vec![(1, 1)], true));
}

#[test]
fn xfer_round_foreground_idle() {
    let mut state = state(1);
    let (runs, sends) = step(&mut state, RoundsInput::Foreground);
    assert_eq!((runs.len(), sends.len(), state.round), (0, 0, Round::None));
}

#[test]
fn xfer_round_idle() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    busy(&mut state, 1, &[10, 11]);
    let before = state.round;
    let (runs, _) = step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
    assert_eq!((runs.len(), state.round, state.active[&1].iter().copied().collect::<Vec<_>>()), (0, before, vec![11]));
}

#[test]
fn xfer_round_started() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id, round_id: 5, epoch: 1 });
    assert_eq!((runs.len(), state.round), (0, Round::Running { request_id, round_id: 5, epoch: 1, next: None, ended: false }));
}

#[test]
fn xfer_round_started_stale() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: request_id + 9, round_id: 6, epoch: 1 });
    assert_eq!((ended_jobs(&runs), state.round), (vec![6], Round::Requested { epoch: 1, request_id }));
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id, round_id: 7, epoch: 2 });
    assert_eq!((ended_jobs(&runs), state.round), (vec![7], Round::Requested { epoch: 1, request_id }), "another epoch is a stray job too");
}

#[test]
fn xfer_round_refused() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    let (runs, sends) = step(&mut state, RoundsInput::RoundRefused { request_id, epoch: 1, reason: "denied".into() });
    assert_eq!((runs.len(), state.round), (0, Round::Refused { epoch: 1 }), "the transfers go on in the process");
    assert!(sends.len() == 1 && sends[0].contains("RoundStatus") && sends[0].contains("denied") && sends[0].contains("10"), "{sends:?}");
}

#[test]
fn xfer_round_refused_stale() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    let (runs, sends) = step(&mut state, RoundsInput::RoundRefused { request_id: request_id + 1, epoch: 1, reason: "old".into() });
    assert_eq!((runs.len(), sends.len(), state.round), (0, 0, Round::Requested { epoch: 1, request_id }));
    let mut other = self::state(1);
    running(&mut other, 1, 5);
    let (runs, sends) = step(&mut other, RoundsInput::RoundRefused { request_id, epoch: 1, reason: "old".into() });
    assert_eq!((runs.len(), sends.len()), (0, 0));
}

#[test]
fn xfer_round_refused_resume() {
    for resume in [RoundsInput::Foreground, RoundsInput::Needed { epoch: 1, ids: vec![11] }] {
        let mut state = state(1);
        let request_id = requested(&mut state, 1, &[10]);
        step(&mut state, RoundsInput::RoundRefused { request_id, epoch: 1, reason: "denied".into() });
        let (runs, sends) = step(&mut state, resume);
        assert_eq!((jobs(&runs).len(), matches!(state.round, Round::Requested { epoch: 1, .. })), (1, true));
        assert!(sends.len() == 1 && sends[0].contains("RoundStatus") && sends[0].contains("refused: None"), "the refusal is taken off: {sends:?}");
    }
}

#[test]
fn xfer_round_needed_requested() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    let (runs, _) = step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![11] });
    assert_eq!((runs.len(), state.round, state.active[&1].len()), (0, Round::Requested { epoch: 1, request_id }, 2));
    step(&mut state, RoundsInput::RoundStarted { request_id, round_id: 5, epoch: 1 });
    let (runs, _) = step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![12] });
    assert_eq!((runs.len(), state.active[&1].len()), (0, 3));
}

#[test]
fn xfer_round_requested_new_epoch() {
    let mut state = state(1);
    let old = requested(&mut state, 1, &[10]);
    step(&mut state, RoundsInput::EpochStarted(EpochStarted { epoch: 2, identity: None, generation: 0 }));
    let (runs, _) = step(&mut state, RoundsInput::Needed { epoch: 2, ids: vec![20] });
    let asked = jobs(&runs);
    assert!(asked.len() == 1 && asked[0].1 == 2 && asked[0].0 != old, "{asked:?}");
    let (stale, _) = step(&mut state, RoundsInput::RoundStarted { request_id: old, round_id: 8, epoch: 1 });
    assert_eq!(ended_jobs(&stale), vec![8], "the answer to the old request is a stray job");
}

#[test]
fn xfer_round_needed_new_epoch() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    step(&mut state, RoundsInput::EpochStarted(EpochStarted { epoch: 2, identity: None, generation: 0 }));
    let (runs, _) = step(&mut state, RoundsInput::Needed { epoch: 2, ids: vec![20] });
    assert_eq!((runs.len(), state.round), (0, Round::Running { request_id: 1, round_id: 5, epoch: 1, next: Some(2), ended: false }));
}

#[test]
fn xfer_round_refused_new_epoch() {
    for round in [Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }] {
        let mut state = state(2);
        state.round = round;
        let (runs, sends) = step(&mut state, RoundsInput::Needed { epoch: 2, ids: vec![20] });
        assert_eq!((jobs(&runs).len(), sends.len(), matches!(state.round, Round::Requested { epoch: 2, .. })), (1, 0, true), "{round:?}");
    }
}

#[test]
fn xfer_round_started_during_running() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    let before = state.round;
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 99, round_id: 9, epoch: 1 });
    assert_eq!((ended_jobs(&runs), state.round), (vec![9], before));
    let (runs, sends) = step(&mut state, RoundsInput::RoundRefused { request_id: 99, epoch: 1, reason: "late".into() });
    assert_eq!((runs.len(), sends.len(), state.round), (0, 0, before));
}

#[test]
fn xfer_round_end_next() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    step(&mut state, RoundsInput::EpochStarted(EpochStarted { epoch: 2, identity: None, generation: 0 }));
    step(&mut state, RoundsInput::Needed { epoch: 2, ids: vec![20] });
    let (runs, _) = step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 });
    assert_eq!((jobs(&runs).iter().map(|job| job.1).collect::<Vec<_>>(), matches!(state.round, Round::Requested { epoch: 2, .. })), (vec![2], true));
}

#[test]
fn xfer_round_end_stopped() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    let (runs, _) = step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 });
    assert_eq!((runs.len(), state.round), (0, Round::Stopped { epoch: 1 }), "the platform stopped the job: the next event resumes it");
}

#[test]
fn xfer_round_end() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
    let (runs, _) = step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 });
    assert_eq!((runs.len(), state.round), (0, Round::None));
}

#[test]
fn xfer_round_end_stale() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    let before = state.round;
    for (round_id, epoch) in [(6, 1), (5, 2)] {
        let (runs, _) = step(&mut state, RoundsInput::RoundEnded { round_id, epoch });
        assert_eq!((runs.len(), state.round), (0, before));
    }
}

#[test]
fn xfer_round_stopped_resume() {
    for resume in [RoundsInput::Foreground, RoundsInput::Needed { epoch: 1, ids: vec![11] }] {
        let mut state = state(1);
        busy(&mut state, 1, &[10]);
        state.round = Round::Stopped { epoch: 1 };
        let (runs, sends) = step(&mut state, resume);
        assert_eq!((jobs(&runs).len(), sends.len(), matches!(state.round, Round::Requested { epoch: 1, .. })), (1, 0, true));
    }
}

#[test]
fn xfer_round_epoch_ended() {
    for round in [Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }] {
        let mut state = state(1);
        busy(&mut state, 1, &[10]);
        state.round = round;
        step(&mut state, RoundsInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }));
        assert_eq!((state.round, state.active.contains_key(&1)), (Round::None, false), "{round:?}");
    }
    let mut state = state(1);
    let old = requested(&mut state, 1, &[10]);
    step(&mut state, RoundsInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }));
    assert_eq!(state.round, Round::None);
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: old, round_id: 4, epoch: 1 });
    assert_eq!(ended_jobs(&runs), vec![4]);
}

#[test]
fn xfer_round_running_epoch_ended() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    step(&mut state, RoundsInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }));
    assert_eq!(state.round, Round::Running { request_id: 1, round_id: 5, epoch: 1, next: None, ended: true });
    step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 });
    assert_eq!(state.round, Round::None, "the platform ends the job itself");
}

#[test]
fn xfer_round_epoch_started() {
    let mut state = state(1);
    let (runs, _) = step(&mut state, RoundsInput::EpochStarted(EpochStarted { epoch: 3, identity: None, generation: 0 }));
    assert_eq!((runs.len(), state.current_epoch, state.round), (0, 3, Round::None));
}

#[test]
fn xfer_round_closing() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    let (runs, _) = step(&mut state, RoundsInput::Closing);
    assert_eq!((ended_jobs(&runs), runs.iter().filter(|run| matches!(run, RoundsEffect::Drained(_))).count(), state.round), (vec![5], 1, Round::Closed));
    let (runs, _) = step(&mut state, RoundsInput::Closing);
    assert!(runs.is_empty(), "Drained goes once");
}

#[test]
fn xfer_round_closed_started() {
    let mut state = state(1);
    step(&mut state, RoundsInput::Closing);
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 1, round_id: 9, epoch: 1 });
    assert_eq!((ended_jobs(&runs), state.round), (vec![9], Round::Closed), "a job after the close is ended");
}

#[test]
fn xfer_round_closed() {
    let mut state = state(1);
    step(&mut state, RoundsInput::Closing);
    for input in [RoundsInput::Needed { epoch: 1, ids: vec![1] }, RoundsInput::Foreground, RoundsInput::Idle { epoch: 1, transfer_id: 1 }, RoundsInput::RoundEnded { round_id: 1, epoch: 1 }, RoundsInput::RoundRefused { request_id: 1, epoch: 1, reason: "x".into() }] {
        let (runs, sends) = step(&mut state, input);
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, Round::Closed));
    }
}

#[test]
fn xfer_prop_round_ids() {
    let rounds = [Round::None, Round::Requested { epoch: 1, request_id: 5 }, Round::Running { request_id: 6, round_id: 7, epoch: 1, next: None, ended: false }, Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }];
    for round in rounds {
        let mut state = state(1);
        busy(&mut state, 1, &[10]);
        state.round = round;
        let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 99, round_id: 42, epoch: 1 });
        assert_eq!((ended_jobs(&runs), state.round), (vec![42], round), "{round:?}: a foreign job is ended and the state stays");
        let (runs, sends) = step(&mut state, RoundsInput::RoundRefused { request_id: 99, epoch: 1, reason: "foreign".into() });
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, round), "{round:?}");
        let (runs, sends) = step(&mut state, RoundsInput::RoundEnded { round_id: 99, epoch: 1 });
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, round), "{round:?}");
    }
}

#[test]
fn xfer_round_foreground_busy() {
    for round in [Round::Requested { epoch: 1, request_id: 4 }, Round::Running { request_id: 2, round_id: 3, epoch: 1, next: None, ended: false }] {
        let mut state = state(1);
        busy(&mut state, 1, &[10]);
        state.round = round;
        let (runs, sends) = step(&mut state, RoundsInput::Foreground);
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, round), "{round:?}");
    }
}

#[test]
fn xfer_round_end_unknown() {
    for round in [Round::None, Round::Requested { epoch: 1, request_id: 4 }, Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }] {
        let mut state = state(1);
        busy(&mut state, 1, &[10]);
        state.round = round;
        let (runs, sends) = step(&mut state, RoundsInput::RoundEnded { round_id: 9, epoch: 1 });
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, round), "{round:?}");
    }
}

#[test]
fn xfer_round_epoch_ended_other() {
    let mut state = state(2);
    busy(&mut state, 2, &[10]);
    busy(&mut state, 3, &[11]);
    state.round = Round::Running { request_id: 2, round_id: 3, epoch: 2, next: Some(3), ended: false };
    let (runs, sends) = step(&mut state, RoundsInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }));
    assert_eq!((runs.len(), sends.len(), state.active.contains_key(&3)), (0, 0, false), "the ids of the other epoch go");
    assert_eq!(state.round, Round::Running { request_id: 2, round_id: 3, epoch: 2, next: None, ended: false }, "the next epoch that ended is not asked for");
}

#[test]
fn xfer_round_no_sink() {
    let mut state = RoundsState::new(Address::<Transfers>::new(2));
    state.current_epoch = 1;
    let (runs, sends) = step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![10] });
    assert_eq!((runs.len(), sends.len(), state.round, state.active[&1].len()), (0, 0, Round::None, 1), "the need is kept, no job is asked for");
}

#[test]
fn xfer_round_foreground_no_sink() {
    let mut state = RoundsState::new(Address::<Transfers>::new(2));
    state.current_epoch = 1;
    busy(&mut state, 1, &[10]);
    let (runs, _) = step(&mut state, RoundsInput::Foreground);
    assert_eq!((runs.len(), state.round), (0, Round::None));
}

#[test]
fn xfer_round_sink_set() {
    let mut state = RoundsState::new(Address::<Transfers>::new(2));
    state.current_epoch = 1;
    step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![10] });
    let (runs, _) = step(&mut state, RoundsInput::JobsSinkSet);
    assert!(state.sink);
    let asked = jobs(&runs);
    assert_eq!((asked.len(), asked[0].1, state.round), (1, 1, Round::Requested { epoch: 1, request_id: asked[0].0 }));
}

#[test]
fn xfer_round_sink_set_idle() {
    let mut idle = RoundsState::new(Address::<Transfers>::new(2));
    idle.current_epoch = 1;
    let (runs, _) = step(&mut idle, RoundsInput::JobsSinkSet);
    assert_eq!((runs.len(), idle.sink, idle.round), (0, true, Round::None), "nothing waits");
    let mut set = state(1);
    let request_id = requested(&mut set, 1, &[10]);
    let (runs, _) = step(&mut set, RoundsInput::JobsSinkSet);
    assert_eq!((runs.len(), set.round), (0, Round::Requested { epoch: 1, request_id }), "a second call changes nothing");
}

#[test]
fn a_running_round_ends_its_job_when_the_last_transfer_of_its_epoch_goes_idle() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    busy(&mut state, 1, &[10, 11]);
    let (first, _) = step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
    let (last, _) = step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 11 });
    assert_eq!((ended_jobs(&first), ended_jobs(&last)), (Vec::<u32>::new(), vec![5]));
}

#[test]
fn a_job_that_starts_with_nothing_unfinished_ends_at_once() {
    let mut state = state(1);
    let request_id = requested(&mut state, 1, &[10]);
    step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
    let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id, round_id: 5, epoch: 1 });
    assert_eq!(ended_jobs(&runs), vec![5]);
}

/// The two ways a round ends by itself (D-86): the last transfer of a running round goes idle, or a job starts when nothing is unfinished. Both leave the state at none.
fn ended_by_itself(prefix: usize) -> RoundsState {
    let mut state = state(1);
    if prefix == 0 {
        running(&mut state, 1, 5);
        let (runs, _) = step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
        assert_eq!(ended_jobs(&runs), vec![5]);
    } else {
        let request_id = requested(&mut state, 1, &[10]);
        step(&mut state, RoundsInput::Idle { epoch: 1, transfer_id: 10 });
        let (runs, _) = step(&mut state, RoundsInput::RoundStarted { request_id, round_id: 5, epoch: 1 });
        assert_eq!(ended_jobs(&runs), vec![5]);
    }
    assert_eq!(state.round, Round::None, "the round that ended its job is none");
    state
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum After {
    Needed,
    Ended,
    Started,
}

/// P-XFER-42 (D-193, xfer_round_reuse_after_idle): after the round ended its own job, a new need gets a new job in every order of the late callbacks of the old round, and the old round never changes the new request.
#[test]
fn xfer_round_reuse_after_idle() {
    for prefix in 0..2 {
        for order in crate::test_wait::permutations(&[After::Needed, After::Ended, After::Started]) {
            let mut state = ended_by_itself(prefix);
            for step_name in &order {
                let (runs, sends) = match step_name {
                    After::Needed => step(&mut state, RoundsInput::Needed { epoch: 1, ids: vec![20] }),
                    After::Ended => step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 }),
                    After::Started => step(&mut state, RoundsInput::RoundStarted { request_id: 1, round_id: 5, epoch: 1 }),
                };
                let case = format!("prefix {prefix}, order {order:?}, step {step_name:?}");
                assert_eq!(sends.len(), 0, "{case}");
                match step_name {
                    After::Needed => assert_eq!((jobs(&runs), ended_jobs(&runs)), (vec![(2, 1)], Vec::<u32>::new()), "{case}"),
                    After::Ended => assert!(runs.is_empty(), "{case}"),
                    After::Started => assert_eq!(ended_jobs(&runs), vec![5], "{case}"),
                }
            }
            assert_eq!(state.round, Round::Requested { epoch: 1, request_id: 2 }, "the new need holds its request in {order:?}");
        }
    }
}

/// xfer_round_started_duplicate (D-193): the repeated start of the running identity changes nothing; a start that differs in the round id ends only its own job.
#[test]
fn xfer_round_started_duplicate() {
    let mut state = state(1);
    running(&mut state, 1, 5);
    let running_round = state.round;
    let (duplicate, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 1, round_id: 5, epoch: 1 });
    assert_eq!((duplicate.len(), state.round), (0, running_round), "the duplicate ends no job");
    let (other, _) = step(&mut state, RoundsInput::RoundStarted { request_id: 1, round_id: 6, epoch: 1 });
    assert_eq!((ended_jobs(&other), state.round), (vec![6], running_round), "a foreign round id ends only its own job");
}

/// boundary_round_orders (P-BND-08, D-176): whatever the order of a start, an end and a refusal of a foreign round or epoch, the running round stays and only a foreign job is ended.
#[test]
fn boundary_round_orders() {
    let foreign = [
        RoundsInput::RoundStarted { request_id: 99, round_id: 9, epoch: 1 },
        RoundsInput::RoundEnded { round_id: 6, epoch: 1 },
        RoundsInput::RoundEnded { round_id: 5, epoch: 2 },
        RoundsInput::RoundRefused { request_id: 99, epoch: 1, reason: "foreign".into() },
    ];
    let orders = crate::test_wait::permutations(&[0, 1, 2, 3]);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let mut state = state(1);
        running(&mut state, 1, 5);
        let before = state.round;
        for at in &order {
            let (runs, sends) = step(&mut state, foreign[*at].clone());
            let expected_ended = if *at == 0 { vec![9] } else { Vec::new() };
            assert_eq!((ended_jobs(&runs), runs.len() - expected_ended.len(), sends.len(), state.round), (expected_ended, 0, 0, before), "{:?} in {order:?}", foreign[*at]);
        }
        let (runs, _) = step(&mut state, RoundsInput::RoundEnded { round_id: 5, epoch: 1 });
        assert_eq!((runs.len(), state.round), (0, Round::Stopped { epoch: 1 }), "the own end of the round still ends it in {order:?}");
    }
}

#[test]
fn xfer_round_resume_old_epoch() {
    for round in [Round::Refused { epoch: 1 }, Round::Stopped { epoch: 1 }] {
        let mut state = state(2);
        state.round = round;
        busy(&mut state, 1, &[10]);
        let (runs, sends) = step(&mut state, RoundsInput::Foreground);
        assert_eq!((runs.len(), sends.len(), state.round), (0, 0, round), "{round:?}: a round of an old epoch is not resumed");
    }
}
