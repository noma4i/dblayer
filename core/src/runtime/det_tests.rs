use std::collections::HashSet;

use super::toys_tests::*;
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
    det.post(toy, EpochStamp::Epoch(1), ToyInput::WorkRequest(7));
    assert!(det.step());
    assert_eq!(det.trace().len(), 1);
    assert!(det.state(toy).unwrap().results.is_empty());
    assert_eq!(det.choices(), vec![Choice::Arrive(0)], "the answer of the executor is a move of its own");
    assert!(det.step());
    assert_eq!(det.choices(), vec![Choice::Step(0)]);
    assert!(det.step());
    assert_eq!(det.trace().len(), 2);
    assert_eq!(det.state(toy).unwrap().results, vec![14]);
    assert!(det.ready().is_empty());
}

#[test]
fn an_answer_of_the_executor_in_the_inbox_makes_the_owner_ready_and_runs_a_step() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::WorkRequest(7));
    assert!(det.step());
    assert_eq!(det.ready(), vec![0], "the answer that came through Back is a pending input of the owner");
    assert!(det.state(toy).unwrap().results.is_empty(), "the answer did not run a step yet");
    assert_eq!(det.run_until_quiet(5), Ok(1), "the answer is one more step");
    assert_eq!(det.state(toy).unwrap().results, vec![14]);
    assert!(det.ready().is_empty());
}

#[test]
fn owners_exchange_values_and_share_no_state() {
    let (mut det, ledger) = race(3);
    assert_eq!(det.run_until_quiet(20), Ok(4));
    let state = det.state(ledger).unwrap();
    assert_eq!(state.total + state.lost, 5);
    assert!(state.sealed);
}

#[test]
fn input_of_another_epoch_never_reaches_step() {
    let (mut det, toy) = single(1);
    det.start_epoch(2);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Deposit(5));
    det.post(toy, EpochStamp::Epoch(2), ToyInput::Deposit(3));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).unwrap().total, 3);
    assert_eq!(det.dropped(toy), 1);
    assert!(det.trace().iter().any(|entry| entry.dropped && entry.input == "Deposit(5)"));
}

#[test]
fn effect_result_of_replaced_epoch_is_dropped() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::WorkRequest(4));
    det.step();
    det.start_epoch(2);
    det.run_until_quiet(10).unwrap();
    assert!(det.state(toy).unwrap().results.is_empty());
    assert_eq!(det.dropped(toy), 1);
}

#[test]
fn epoch_end_reaches_owner_then_drops_later_inputs_and_schedule() {
    let (mut det, toy) = single(1);
    det.start_epoch(2);
    det.post(toy, EpochStamp::Epoch(2), ToyInput::ScheduleAt(1, Time(10), 5));
    det.run_until_quiet(10).unwrap();
    det.end_epoch(2, Cause::SessionReset);
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).unwrap().ended, vec![(2, Cause::SessionReset)]);
    det.post(toy, EpochStamp::Epoch(2), ToyInput::Deposit(1));
    det.tick(Time(100));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).unwrap().total, 0);
    assert!(det.state(toy).unwrap().fired.is_empty());
    assert_eq!(det.dropped(toy), 1);
}

#[test]
fn schedule_fires_only_after_tick_reaches_its_time() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(100), 9));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(99));
    assert!(det.ready().is_empty());
    assert!(det.state(toy).unwrap().fired.is_empty());
    det.tick(Time(100));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).unwrap().fired, vec![9]);
    assert_eq!(det.state(toy).unwrap().ticks, vec![Time(100)]);
}

#[test]
fn cancelled_schedule_never_fires() {
    let (mut det, toy) = single(1);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(100), 9));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::CancelSchedule(1));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(500));
    assert!(det.ready().is_empty());
    assert!(det.state(toy).unwrap().fired.is_empty());
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
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner { outcomes: Some(outcomes.clone()), ..ToyRunner::default() });
    det.start_epoch(1);
    let mut first = outcomes.register(1);
    let mut second = outcomes.register(1);
    let mut third = outcomes.register(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Resolve(first.id(), 11));
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

#[test]
fn message_from_a_tick_carries_the_owner_epoch_and_a_receiver_of_another_epoch_drops_it() {
    for (receiver_epoch, total, dropped) in [(1, 5, 0), (2, 0, 1)] {
        let mut det = DetScheduler::new(1);
        let receiver = det.add::<Toy>("receiver", ToyState::default(), ToyRunner::default());
        let ticker = det.add::<Toy>("ticker", ToyState { on_tick: Some((receiver, ToyInput::Deposit(5))), ..ToyState::default() }, ToyRunner::default());
        det.start_epoch(1);
        det.start_epoch_for(receiver, receiver_epoch);
        det.post(ticker, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 1));
        det.run_until_quiet(10).unwrap();
        det.tick(Time(10));
        det.run_until_quiet(10).unwrap();
        assert_eq!(det.state(ticker).unwrap().ticks, vec![Time(10)]);
        assert_eq!(det.state(receiver).unwrap().total, total);
        assert_eq!(det.dropped(receiver), dropped);
    }
}

#[test]
fn owner_with_epochs_keeps_sending_after_epoch_end_and_epochless_receiver_admits_any_stamp() {
    let mut det = DetScheduler::new(1);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    let sender = det.add::<Toy>("sender", ToyState { tell: Some((sink, 9)), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch_for(sender, 1);
    det.end_epoch(1, Cause::Close);
    det.run_until_quiet(10).unwrap();
    det.post(sender, EpochStamp::Control, ToyInput::Tell);
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(sink).unwrap(), &vec![9]);
    assert_eq!(det.dropped(sink), 0);

    let receiver = det.add::<Toy>("receiver", ToyState::default(), ToyRunner::default());
    let relay = det.add::<Toy>("relay", ToyState { on_go: Some((receiver, ToyInput::Deposit(2))), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch_for(relay, 1);
    det.start_epoch_for(receiver, 2);
    det.end_epoch(1, Cause::Close);
    det.run_until_quiet(10).unwrap();
    let before = det.dropped(receiver);
    det.post(relay, EpochStamp::Control, ToyInput::Go);
    det.run_until_quiet(10).unwrap();
    assert_eq!((det.state(receiver).unwrap().total, det.dropped(receiver) - before), (0, 1));
}

#[test]
fn owner_without_epochs_runs_step_for_input_stamped_with_epoch_7() {
    let mut det = DetScheduler::new(1);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    det.post(sink, EpochStamp::Epoch(7), SinkInput::Note(3));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(sink).unwrap(), &vec![3]);
    assert_eq!(det.dropped(sink), 0);
}

#[test]
fn owner_with_epochs_before_its_first_epoch_stamps_epoch_0_and_receiver_of_epoch_1_drops_it() {
    let mut det = DetScheduler::new(1);
    let receiver = det.add::<Toy>("receiver", ToyState::default(), ToyRunner::default());
    let relay = det.add::<Toy>("relay", ToyState { on_go: Some((receiver, ToyInput::Deposit(2))), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch_for(receiver, 1);
    det.post(relay, EpochStamp::Control, ToyInput::Go);
    det.run_until_quiet(10).unwrap();
    assert_eq!((det.state(receiver).unwrap().total, det.dropped(receiver)), (0, 1));
    assert!(det.trace().iter().any(|entry| entry.owner == "receiver" && entry.dropped && entry.stamp == EpochStamp::Epoch(0)));
}

#[test]
fn control_input_from_an_owner_without_epochs_reaches_a_receiver_with_an_epoch() {
    let mut det = DetScheduler::new(1);
    let receiver = det.add::<Toy>("receiver", ToyState::default(), ToyRunner::default());
    let hub = det.add::<Hub>("hub", (), HubRunner);
    det.start_epoch_for(receiver, 4);
    det.post(hub, EpochStamp::Control, HubInput::Probe(receiver));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(receiver).unwrap().total, 1);
}

#[test]
fn lifecycle_broadcast_reaches_every_owner_on_the_scheduler() {
    let mut det = DetScheduler::new(7);
    let hub = det.add::<Hub>("hub", (), HubRunner);
    let first = det.add::<Toy>("first", ToyState::default(), ToyRunner::default());
    let second = det.add::<Toy>("second", ToyState::default(), ToyRunner::default());
    det.post(hub, EpochStamp::Control, HubInput::Begin(1));
    det.post(hub, EpochStamp::Control, HubInput::Finish(1, Cause::SessionReset));
    det.post(hub, EpochStamp::Control, HubInput::Shutdown);
    det.run_until_quiet(30).unwrap();
    for toy in [first, second] {
        assert_eq!(det.state(toy).unwrap().ended, vec![(1, Cause::SessionReset)]);
        assert!(det.state(toy).unwrap().closed);
    }
}

fn due_after(seed: u64, attempt: u32) -> u64 {
    let (mut det, toy) = single(seed);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Retry(attempt));
    det.run_until_quiet(5).unwrap();
    (0..=40_000).find(|at| {
        det.tick(Time(*at));
        det.choices().contains(&Choice::Fire(0))
    }).unwrap()
}

#[test]
fn runtime_applies_the_jitter_of_a_backoff_from_its_seed() {
    let delays: Vec<u64> = (0..16).map(|seed| due_after(seed, 0)).collect();
    assert!(delays.iter().all(|delay| (800..=1000).contains(delay)), "attempt 0 of 1 s with jitter 0.2 lies in [800, 1000] ms: {delays:?}");
    assert!(delays.iter().collect::<HashSet<_>>().len() >= 4, "the seed spreads the retries: {delays:?}");
    assert_eq!(due_after(3, 2), due_after(3, 2), "one seed gives one delay");
    assert!((3200..=4000).contains(&due_after(3, 2)), "attempt 2 doubles twice");
}

#[test]
fn a_panic_in_a_step_tells_lifecycle_and_the_owner_keeps_taking_inputs() {
    let mut det = DetScheduler::new(1);
    let watch = det.add::<DeathLog>("deaths", Vec::new(), DeathLogRunner);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
    det.on_executor_failed(watch);
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(7));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Crash);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Deposit(2));
    det.run_until_quiet(20).unwrap();
    assert_eq!(det.state(watch).unwrap(), &vec!["toy".to_string()]);
    assert_eq!(det.state(toy).unwrap().failed, vec![7], "the id without a result got its failure cell");
    assert_eq!(det.state(toy).unwrap().total, 2);
}
