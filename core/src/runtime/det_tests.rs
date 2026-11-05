use std::collections::HashSet;

use super::testing::*;
use super::*;

fn single(seed: u64) -> (DetScheduler, Address<Toy>) {
    let mut det = DetScheduler::new(seed);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
    (det, toy)
}

#[test]
fn step_is_a_function_of_state_and_input_only() {
    let mut first = ToyState::default();
    let mut second = first.clone();
    let left = Toy::step(&mut first, ToyInput::WorkRequest(3));
    let right = Toy::step(&mut second, ToyInput::WorkRequest(3));
    assert_eq!(format!("{left:?}"), format!("{right:?}"));
    assert_eq!(first.results, second.results);
}

#[test]
fn one_input_per_step_and_effect_result_returns_as_input() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.send(toy, Stamp::Epoch(1), ToyInput::WorkRequest(7));
    assert!(det.step());
    assert_eq!(det.trace().len(), 1);
    assert!(det.state(toy).results.is_empty());
    assert_eq!(det.ready(), vec![0]);
    assert!(det.step());
    assert_eq!(det.trace().len(), 2);
    assert_eq!(det.state(toy).results, vec![14]);
    assert!(det.ready().is_empty());
}

#[test]
fn owners_exchange_values_and_share_no_state() {
    let (mut det, ledger) = race(3);
    assert_eq!(det.run_until_quiet(20), Ok(4));
    let state = det.state(ledger);
    assert_eq!(state.total + state.lost, 5);
    assert!(state.sealed);
}

#[test]
fn input_of_another_epoch_never_reaches_step() {
    let (mut det, toy) = single(1);
    det.start_epoch(2);
    det.send(toy, Stamp::Epoch(1), ToyInput::Deposit(5));
    det.send(toy, Stamp::Epoch(2), ToyInput::Deposit(3));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).total, 3);
    assert_eq!(det.dropped(toy), 1);
    assert!(det.trace().iter().any(|entry| entry.dropped && entry.input == "Deposit(5)"));
}

#[test]
fn effect_result_of_replaced_epoch_is_dropped() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.send(toy, Stamp::Epoch(1), ToyInput::WorkRequest(4));
    det.step();
    det.start_epoch(2);
    det.run_until_quiet(10).unwrap();
    assert!(det.state(toy).results.is_empty());
    assert_eq!(det.dropped(toy), 1);
}

#[test]
fn epoch_end_reaches_owner_then_drops_later_inputs_and_schedule() {
    let (mut det, toy) = single(1);
    det.start_epoch(2);
    det.send(toy, Stamp::Epoch(2), ToyInput::ScheduleAt(1, Time(10), 5));
    det.run_until_quiet(10).unwrap();
    det.end_epoch(2, Cause::SessionReset);
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).ended, vec![(2, Cause::SessionReset)]);
    det.send(toy, Stamp::Epoch(2), ToyInput::Deposit(1));
    det.tick(Time(100));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).total, 0);
    assert!(det.state(toy).fired.is_empty());
    assert_eq!(det.dropped(toy), 1);
}

#[test]
fn schedule_fires_only_after_tick_reaches_its_time() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.send(toy, Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(100), 9));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(99));
    assert!(det.ready().is_empty());
    assert!(det.state(toy).fired.is_empty());
    det.tick(Time(100));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).fired, vec![9]);
    assert_eq!(det.state(toy).ticks, vec![Time(100)]);
}

#[test]
fn cancelled_schedule_never_fires() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.send(toy, Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(100), 9));
    det.send(toy, Stamp::Epoch(1), ToyInput::CancelSchedule(1));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(500));
    assert!(det.ready().is_empty());
    assert!(det.state(toy).fired.is_empty());
}

#[test]
fn idle_scheduler_has_no_work_and_tick_creates_none() {
    let (mut det, _toy) = single(1);
    det.start_epoch(1);
    assert!(det.ready().is_empty());
    det.tick(Time(1_000_000));
    assert!(det.ready().is_empty());
    assert!(!det.step());
    assert!(det.trace().is_empty());
}

#[test]
fn same_seed_gives_same_trace_and_seeds_differ() {
    let run = |seed| {
        let (mut det, _) = race(seed);
        det.run_until_quiet(20).unwrap();
        det.trace().to_vec()
    };
    let mut distinct = HashSet::new();
    for seed in 0..32 {
        assert_eq!(run(seed), run(seed));
        distinct.insert(format!("{:?}", run(seed)));
    }
    assert!(distinct.len() >= 2);
}

#[test]
fn trace_records_owner_input_and_effects() {
    let (mut det, _) = race(5);
    det.run_until_quiet(20).unwrap();
    let go = det.trace().iter().find(|entry| entry.owner == "depositor").unwrap();
    assert_eq!(go.input, "Go");
    assert!(go.effects.contains("Deposit(5)"));
}

#[test]
fn run_until_quiet_reports_the_step_limit() {
    let (mut det, _) = race(1);
    assert_eq!(det.run_until_quiet(2), Err(2));
}

#[test]
fn every_open_id_gets_one_terminal_outcome_on_epoch_end_and_close() {
    let outcomes = Outcomes::<u32>::new();
    let mut det = DetScheduler::new(1);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner { outcomes: Some(outcomes.clone()), report: None });
    det.start_epoch(1);
    let mut first = outcomes.register(1);
    let mut second = outcomes.register(1);
    let mut third = outcomes.register(1);
    det.send(toy, Stamp::Epoch(1), ToyInput::Resolve(first.id(), 11));
    det.end_epoch(1, Cause::ServerEnded);
    det.run_until_quiet(10).unwrap();
    assert_eq!(first.try_take(), Some(Outcome::Value(11)));
    assert_eq!(second.try_take(), Some(Outcome::EpochEnded(Cause::ServerEnded)));
    assert_eq!(third.try_take(), Some(Outcome::EpochEnded(Cause::ServerEnded)));
    assert_eq!((first.try_take(), second.try_take(), third.try_take()), (None, None, None));
    det.start_epoch(2);
    let mut fourth = outcomes.register(2);
    det.close();
    det.run_until_quiet(10).unwrap();
    assert_eq!(fourth.try_take(), Some(Outcome::Closing));
    assert_eq!(fourth.try_take(), None);
    assert_eq!(outcomes.open_count(), 0);
}
