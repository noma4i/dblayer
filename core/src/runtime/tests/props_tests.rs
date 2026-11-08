use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Mutex, PoisonError};

use super::toys_tests::*;
use super::*;


fn prop(property: &'static str, domain: &'static str, cells: &'static str, depth: usize, threaded_public_test: &'static str) -> Property {
    Property { property, domain, cells, depth, max_traces: 400_000, seeds: 16, threaded_public_test }
}

fn inputs(det: &DetScheduler, wanted: &[&str]) -> Vec<String> { det.trace().iter().filter(|entry| wanted.contains(&entry.input.as_str())).map(|entry| entry.input.clone()).collect() }

fn timers() -> (DetScheduler, Address<Toy>, Address<Toy>) {
    let mut det = DetScheduler::new(0);
    let left = det.add::<Toy>("left", ToyState::default(), ToyRunner::default());
    let right = det.add::<Toy>("right", ToyState::default(), ToyRunner::default());
    let sender = det.add::<Toy>("sender", ToyState { on_go: Some((left, ToyInput::Note(7))), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.post(left, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 1));
    det.post(right, EpochStamp::Epoch(1), ToyInput::ScheduleAt(2, Time(10), 2));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(10));
    det.post(sender, EpochStamp::Epoch(1), ToyInput::Go);
    (det, left, right)
}

#[test]
fn rt_prop_timer_orders() {
    let (det, _, _) = timers();
    assert!(det.choices().contains(&Choice::Fire(0)) && det.choices().contains(&Choice::Fire(1)), "every owner with a due entry offers its own Fire");

    let seen = RefCell::new(BTreeSet::new());
    let manifest = prop("P-RT-01", "Schedule x Send x Fire", "R5 Schedule, R9 FireNextDue", 14, "rt_prop_timer_orders").explore(
        || timers().0,
        |det| {
            if det.choices().is_empty() {
                seen.borrow_mut().insert(inputs(det, &["Fire(1)", "Fire(2)", "Note(7)"]));
            }
            Ok(())
        },
    );
    manifest.assert_complete();
    assert_eq!(seen.borrow().len(), 6, "{}: every order of two due entries and a Send was visited", manifest.line());

    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let left = builder.add::<Toy>("left", ToyState::default(), ToyRunner { report: Some(report.clone()), ..ToyRunner::default() });
    let right = builder.add::<Toy>("right", ToyState::default(), ToyRunner { report: Some(report), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(right, EpochStamp::Epoch(1), ToyInput::ScheduleAt(2, Time(70), 2));
    runtime.post(left, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(30), 1));
    let order: Vec<String> = (0..2).map(|_| format!("Fire({})", crate::test_wait::recv_within(&fired))).collect();
    let projected: Vec<String> = seen.borrow().iter().next().unwrap().iter().filter(|input| input.starts_with("Fire")).cloned().collect();
    assert!(seen.borrow().iter().any(|model| model.iter().filter(|input| input.starts_with("Fire")).cloned().collect::<Vec<_>>() == order), "the order of the clock {order:?} is an order of the model, e.g. {projected:?}");
}

#[test]
fn a_due_entry_is_one_input_and_the_next_one_waits_for_its_own_choice() {
    let mut det = DetScheduler::new(0);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 1));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(2, Time(10), 2));
    det.run_until_quiet(10).unwrap();
    det.tick(Time(10));
    assert_eq!(det.choices(), vec![Choice::Fire(0)]);
    assert!(det.step_choice(0));
    assert_eq!(det.choices(), vec![Choice::Step(0), Choice::Fire(0)], "the second entry is still in the schedule");
    assert!(det.step_choice(1));
    det.run_until_quiet(10).unwrap();
    assert_eq!(det.state(toy).unwrap().fired, vec![1, 2]);
}

#[test]
fn rt_prop_effect_order() {
    let mut det = DetScheduler::new(0);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let toy = det.add::<Toy>("toy", ToyState { tell: Some((sink, 0)), ..ToyState::default() }, ToyRunner { calls: Some(calls.clone()), ..ToyRunner::default() });
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![1, 2, 3]));
    det.run_until_quiet(10).unwrap();
    let label = &det.trace().iter().find(|entry| entry.input.starts_with("Burst(")).unwrap().effects;
    let positions: Vec<usize> = ["Report(1)", "Note(1)", "Report(2)", "Note(2)", "Report(3)", "Note(3)"].iter().map(|part| label.find(part).unwrap()).collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "the step lists its effects in the order of the table: {label}");
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(det.state(sink).unwrap(), &vec![1, 2, 3]);

    let build = || {
        let mut det = DetScheduler::new(0);
        let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
        let toy = det.add::<Toy>("toy", ToyState { tell: Some((sink, 0)), ..ToyState::default() }, ToyRunner::default());
        det.start_epoch(1);
        det.run_until_quiet(10).unwrap();
        det.post(toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![1, 2, 3]));
        det.inject(0, toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![4, 5]));
        det
    };
    let sink = Address::<Sink>::new(0);
    let manifest = prop("P-RT-03", "Send order of one step", "R2 effects of a step", 12, "rt_prop_effect_order").explore(build, |det| {
        let seen = det.state(sink).unwrap();
        let first: Vec<u32> = seen.iter().copied().filter(|n| *n <= 3).collect();
        let second: Vec<u32> = seen.iter().copied().filter(|n| *n >= 4).collect();
        if first == [1, 2, 3][..first.len()] && second == [4, 5][..second.len()] { Ok(()) } else { Err(format!("the Sends of one step arrived out of order: {seen:?}")) }
    });
    manifest.assert_complete();

    let (report, _fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let sink = builder.add::<Sink>("sink", Vec::new(), SinkRunner);
    let toy = builder.add::<Toy>("toy", ToyState { tell: Some((sink, 0)), ..ToyState::default() }, ToyRunner { report: Some(report), ..ToyRunner::default() });
    builder.record();
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![1, 2, 3]));
    runtime.barrier();
    let history = runtime.history(sink);
    assert_eq!(history.iter().map(|entry| entry.input.as_str()).filter(|input| input.starts_with("Note(")).collect::<Vec<_>>(), vec!["Note(1)", "Note(2)", "Note(3)"]);
}

fn epochs() -> (DetScheduler, [Address<Toy>; 2]) {
    let mut det = DetScheduler::new(0);
    let hub = det.add::<Hub>("hub", (), HubRunner);
    let first = det.add::<Toy>("first", ToyState::default(), ToyRunner::default());
    let second = det.add::<Toy>("second", ToyState::default(), ToyRunner::default());
    det.start_epoch_for(first, 1);
    det.start_epoch_for(second, 1);
    det.run_until_quiet(10).unwrap();
    det.post(hub, EpochStamp::Control, HubInput::Finish(1, Cause::SessionReset));
    det.post(hub, EpochStamp::Control, HubInput::Begin(2));
    det.inject(0, first, EpochStamp::Epoch(1), ToyInput::Deposit(4));
    (det, [first, second])
}

#[test]
fn rt_prop_epoch_queue() {
    let toys = epochs().1;
    let manifest = prop("P-RT-04", "End(e) x Start(e+1) x input of e", "R4 queue of control inputs", 20, "rt_prop_epoch_queue").explore_parallel(
        || epochs().0,
        |det| {
            for toy in toys {
                let state = det.state(toy).unwrap();
                if state.ended.len() > 1 || state.ended.iter().any(|ended| *ended != (1, Cause::SessionReset)) {
                    return Err(format!("{:?}", state.ended));
                }
                if state.started.contains(&2) && state.ended != [(1, Cause::SessionReset)] {
                    return Err(format!("Start(2) preceded End(1): started={:?} ended={:?}", state.started, state.ended));
                }
            }
            if det.choices().is_empty() && toys.iter().any(|toy| det.state(*toy).unwrap().ended.len() != 1) {
                return Err("EpochEnded(1) was dropped before the owner took it".to_string());
            }
            if det.choices().is_empty() && toys.iter().any(|toy| det.state(*toy).unwrap().started != [1, 2]) {
                return Err("the owner did not step through both starts".to_string());
            }
            Ok(())
        },
    );
    manifest.assert_complete();

    let (report, reports) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let hub = builder.add::<Hub>("hub", (), HubRunner);
    builder.add::<Toy>("toy", ToyState::default(), ToyRunner { report: Some(report), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.post(hub, EpochStamp::Control, HubInput::Begin(1));
    for epoch in 1..=200 {
        runtime.post(hub, EpochStamp::Control, HubInput::Finish(epoch, Cause::SessionReset));
        runtime.post(hub, EpochStamp::Control, HubInput::Begin(epoch + 1));
    }
    let ended: Vec<u32> = (0..200).map(|_| crate::test_wait::recv_within(&reports)).collect();
    assert_eq!(ended, (1..=200).collect::<Vec<u32>>(), "every EpochEnded(e) made its step before EpochStarted(e+1)");
}

fn revocation() -> DetScheduler {
    let pending = ParkedBacks::default();
    let mut det = DetScheduler::new(0);
    let hub = det.add::<Hub>("hub", (), HubRunner);
    let pump = det.add::<Toy>("pump", ToyState::default(), ToyRunner { pending: Some(pending.clone()), ..ToyRunner::default() });
    let data = det.add::<Toy>("data", ToyState { pump: Some(pump), ..ToyState::default() }, ToyRunner { pending: Some(pending), ..ToyRunner::default() });
    det.start_epoch(1);
    det.run_until_quiet(10).unwrap();
    det.post(data, EpochStamp::Epoch(1), ToyInput::WorkAsync(4));
    det.inject(0, hub, EpochStamp::Control, HubInput::Finish(1, Cause::ServerEnded));
    det
}

#[test]
fn rt_prop_revoke_boundary() {
    let manifest = prop("P-RT-05", "late callback x EpochEnded", "R4 boundary of the revocation", 14, "rt_prop_revoke_boundary").explore(revocation, |det| {
        let steps = det.trace();
        let ended: Vec<Option<usize>> = ["pump", "data"].iter().map(|owner| steps.iter().position(|entry| entry.owner == *owner && entry.input.starts_with("EpochEnded(1"))).collect();
        let Some(boundary) = ended.iter().copied().collect::<Option<Vec<usize>>>().map(|positions| positions.into_iter().max().unwrap_or(0)) else { return Ok(()) };
        match steps[boundary + 1..].iter().find(|entry| !entry.dropped && entry.stamp == EpochStamp::Epoch(1) && entry.effects.contains("Report")) {
            Some(entry) => Err(format!("a data effect of epoch 1 after the boundary: {entry:?}")),
            None => Ok(()),
        }
    });
    manifest.assert_complete();
}

struct World {
    outcomes: Outcomes<u32>,
    waiters: Vec<Waiter<u32>>,
    taken: Vec<Vec<Outcome<u32>>>,
}

fn outcome_world(world: &Rc<RefCell<Option<World>>>) -> DetScheduler {
    let outcomes = Outcomes::<u32>::new();
    let waiters = vec![outcomes.register(1), outcomes.register(1)];
    let mut det = DetScheduler::new(0);
    let hub = det.add::<Hub>("hub", (), HubRunner);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner { outcomes: Some(outcomes.clone()), ..ToyRunner::default() });
    det.start_epoch(1);
    det.run_until_quiet(10).unwrap();
    det.inject(0, toy, EpochStamp::Epoch(1), ToyInput::Resolve(waiters[0].id(), 11));
    det.inject(1, hub, EpochStamp::Control, HubInput::Finish(1, Cause::ServerEnded));
    det.inject(2, hub, EpochStamp::Control, HubInput::Shutdown);
    *world.borrow_mut() = Some(World { outcomes, taken: vec![Vec::new(); waiters.len()], waiters });
    det
}

#[test]
fn rt_prop_outcome() {
    let world = Rc::new(RefCell::new(None));
    let manifest = prop("P-RT-06", "settle x EpochEnded x Closing", "R6 outcome of an id", 14, "rt_prop_outcome").explore(
        || outcome_world(&world),
        |det| {
            let mut guard = world.borrow_mut();
            let world = guard.as_mut().unwrap();
            for (index, waiter) in world.waiters.iter_mut().enumerate() {
                world.taken[index].extend(waiter.try_take());
                if world.taken[index].len() > 1 {
                    return Err(format!("the id {index} got {:?}", world.taken[index]));
                }
                if world.taken[index].first() == Some(&Outcome::Value(11)) && index != 0 {
                    return Err("a value reached an id that nobody settled".to_string());
                }
            }
            if det.choices().is_empty() && world.taken.iter().any(Vec::is_empty) {
                return Err(format!("an id stayed without an outcome: {:?}", world.taken));
            }
            if det.choices().is_empty() && world.outcomes.open_count() != 0 {
                return Err("a wait stayed open".to_string());
            }
            Ok(())
        },
    );
    manifest.assert_complete();

    let outcomes = Outcomes::<u32>::new();
    let mut lost = outcomes.register(1);
    drop(outcomes);
    assert_eq!(lost.try_take(), Some(Outcome::Internal), "a wait whose registry is gone is an internal error, not a close");
}

#[tokio::test]
async fn a_lost_wait_ends_with_internal_not_closing() {
    let outcomes = Outcomes::<u32>::new();
    let waiter = outcomes.register(1);
    drop(outcomes);
    assert_eq!(crate::test_wait::within(waiter.wait()).await, Outcome::Internal);
}

fn fan() -> DetScheduler {
    let mut det = DetScheduler::new(0);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    let relay = det.add::<Toy>("relay", ToyState { tell: Some((sink, 5)), ..ToyState::default() }, ToyRunner::default());
    let producer = det.add::<Toy>("producer", ToyState { on_go: Some((relay, ToyInput::Tell)), tell: Some((sink, 9)), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.run_until_quiet(10).unwrap();
    det.post(producer, EpochStamp::Epoch(1), ToyInput::Fan(9));
    det
}

#[test]
fn rt_prop_refinement() {
    let sink = Address::<Sink>::new(0);
    let seen = RefCell::new(BTreeSet::new());
    let manifest = prop("P-RT-07", "step of B between two Sends of A", "R2 issue order, R9 Deliver", 12, "rt_prop_refinement").explore(fan, |det| {
        if det.choices().is_empty() {
            seen.borrow_mut().insert(det.state(sink).unwrap().clone());
        }
        Ok(())
    });
    manifest.assert_complete();
    assert_eq!(*seen.borrow(), BTreeSet::from([vec![5, 9], vec![9, 5]]), "B answered between the two Sends of A in one history and after both in the other");

    for _ in 0..100 {
        let (report, _unused) = mpsc::channel();
        let mut builder = RuntimeBuilder::for_tests();
        let sink = builder.add::<Sink>("sink", Vec::new(), SinkRunner);
        let relay = builder.add::<Toy>("relay", ToyState { tell: Some((sink, 5)), ..ToyState::default() }, ToyRunner { report: Some(report), ..ToyRunner::default() });
        let producer = builder.add::<Toy>("producer", ToyState { on_go: Some((relay, ToyInput::Tell)), tell: Some((sink, 9)), ..ToyState::default() }, ToyRunner::default());
        builder.record();
        let runtime = builder.start().unwrap();
        runtime.start_epoch(1);
        runtime.post(producer, EpochStamp::Epoch(1), ToyInput::Fan(9));
        runtime.barrier();
        let order: Vec<u32> = runtime.history(sink).iter().filter(|entry| entry.input.starts_with("Note(")).map(|entry| entry.input.trim_start_matches("Note(").trim_end_matches(')').parse().unwrap()).collect();
        assert!(seen.borrow().contains(&order), "the threads produced {order:?}, which the model does not allow");
    }
}

fn chain() -> DetScheduler {
    let mut det = DetScheduler::new(0);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    let last = det.add::<Toy>("last", ToyState { tell: Some((sink, 3)), ..ToyState::default() }, ToyRunner::default());
    let middle = det.add::<Toy>("middle", ToyState { on_go: Some((last, ToyInput::Tell)), ..ToyState::default() }, ToyRunner::default());
    let first = det.add::<Toy>("first", ToyState { on_go: Some((middle, ToyInput::Go)), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.run_until_quiet(10).unwrap();
    det.post(first, EpochStamp::Epoch(1), ToyInput::Go);
    det
}

#[test]
fn rt_prop_det_equals_threaded() {
    let mut det = chain();
    det.run_until_quiet(20).unwrap();
    let mut builder = RuntimeBuilder::for_tests();
    let sink = builder.add::<Sink>("sink", Vec::new(), SinkRunner);
    let last = builder.add::<Toy>("last", ToyState { tell: Some((sink, 3)), ..ToyState::default() }, ToyRunner::default());
    let middle = builder.add::<Toy>("middle", ToyState { on_go: Some((last, ToyInput::Tell)), ..ToyState::default() }, ToyRunner::default());
    let first = builder.add::<Toy>("first", ToyState { on_go: Some((middle, ToyInput::Go)), ..ToyState::default() }, ToyRunner::default());
    builder.record();
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(first, EpochStamp::Epoch(1), ToyInput::Go);
    runtime.barrier();
    for (name, address_index) in [("sink", sink.index()), ("last", last.index()), ("middle", middle.index()), ("first", first.index())] {
        let modeled: Vec<&TraceEntry> = det.trace().iter().filter(|entry| entry.slot == address_index).collect();
        let threaded = match name {
            "sink" => runtime.history(sink),
            "last" => runtime.history(last),
            "middle" => runtime.history(middle),
            _ => runtime.history(first),
        };
        assert_eq!(threaded.iter().collect::<Vec<_>>(), modeled, "{name}: the same inputs in the same order with the same effects");
    }
    assert_eq!(det.state(Address::<Sink>::new(0)).unwrap(), &vec![3]);

    let allowed = RefCell::new(BTreeSet::new());
    let ledger = Address::<Toy>::new(0);
    let manifest = prop("P-RT-02", "Deposit x Seal at one ledger", "R9 equals Runtime", 10, "rt_prop_det_equals_threaded").explore(|| race(0).0, |det| {
        if det.choices().is_empty() {
            allowed.borrow_mut().insert(det.history(ledger).iter().map(|entry| entry.input.clone()).collect::<Vec<_>>());
        }
        Ok(())
    });
    manifest.assert_complete();
    assert_eq!(allowed.borrow().len(), 2);
    for _ in 0..200 {
        let mut builder = RuntimeBuilder::for_tests();
        let ledger = builder.add::<Toy>("ledger", ToyState::default(), ToyRunner::default());
        let depositor = builder.add::<Toy>("depositor", ToyState { on_go: Some((ledger, ToyInput::Deposit(5))), ..ToyState::default() }, ToyRunner::default());
        let sealer = builder.add::<Toy>("sealer", ToyState { on_go: Some((ledger, ToyInput::Seal)), ..ToyState::default() }, ToyRunner::default());
        builder.record();
        let runtime = builder.start().unwrap();
        runtime.start_epoch(1);
        runtime.post(depositor, EpochStamp::Epoch(1), ToyInput::Go);
        runtime.post(sealer, EpochStamp::Epoch(1), ToyInput::Go);
        runtime.barrier();
        let history: Vec<String> = runtime.history(ledger).iter().map(|entry| entry.input.clone()).collect();
        assert!(allowed.borrow().contains(&history), "the threads produced {history:?}, which the model does not allow");
    }
}

#[test]
fn rt_prop_no_lock_on_effect() {
    let held = Arc::new(Mutex::new(Vec::new()));
    let mut det = DetScheduler::new(0);
    let sink = det.add::<Sink>("sink", Vec::new(), SinkRunner);
    let toy = det.add::<Toy>("toy", ToyState { tell: Some((sink, 0)), ..ToyState::default() }, ToyRunner { held: Some(held.clone()), ..ToyRunner::default() });
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![1, 2]));
    det.run_until_quiet(10).unwrap();

    let mut builder = RuntimeBuilder::for_tests();
    let toy = builder.add::<Toy>("toy", ToyState::default(), ToyRunner { held: Some(held.clone()), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::Burst(vec![3, 4]));
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::WorkRequest(5));
    runtime.barrier();
    assert_eq!(*held.lock().unwrap_or_else(PoisonError::into_inner), Vec::<&str>::new(), "no library lock is held while a runner runs");

    let guard = Mutex::new(0u8);
    let _held = crate::lock::lock(&guard);
    let pushed = std::panic::catch_unwind(|| crate::lock::effect(|| ()));
    assert!(pushed.is_err(), "the detector sees a lock held at the boundary of an effect");
}

#[test]
fn rt_prop_start_close() {
    let mut det = DetScheduler::new(0);
    let watch = det.add::<DeathLog>("deaths", Vec::new(), DeathLogRunner);
    let bare = det.add::<Bare>("bare", (), BareRunner);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
    det.on_executor_failed(watch);
    det.start_epoch(1);
    det.post(bare, EpochStamp::Control, BareInput::Boom);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(1));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(2));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(BOOM));
    det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(5));
    det.run_until_quiet(30).unwrap();
    let mut told = det.state(watch).unwrap().clone();
    told.sort();
    assert_eq!(told, vec!["bare".to_string(), "toy".to_string()], "Lifecycle learns about both executors, also the one whose owner has no failure cell");
    let mut failed = det.state(toy).unwrap().failed.clone();
    failed.sort_unstable();
    assert_eq!(failed, vec![1, 2, 5, BOOM], "every id without a result got its failure cell, also the one asked after the death");

    let mut builder = RuntimeBuilder::for_tests();
    let watch = builder.add::<DeathLog>("deaths", Vec::new(), DeathLogRunner);
    let bare = builder.add::<Bare>("bare", (), BareRunner);
    let (report, reports) = mpsc::channel();
    let outcomes = Outcomes::<u32>::new();
    let dying = builder.add::<Toy>("dying", ToyState::default(), ToyRunner::default());
    builder.add::<Toy>("closing", ToyState::default(), ToyRunner { outcomes: Some(outcomes.clone()), report: Some(report), ..ToyRunner::default() });
    builder.on_executor_failed(watch);
    builder.record();
    let runtime = builder.start().unwrap();
    let alive = runtime.alive();
    let mut open = outcomes.register(1);
    runtime.start_epoch(1);
    runtime.post(bare, EpochStamp::Control, BareInput::Boom);
    runtime.post(dying, EpochStamp::Epoch(1), ToyInput::Ask(BOOM));
    runtime.barrier();
    assert_eq!(runtime.history(watch).iter().filter(|entry| !entry.input.starts_with("EpochStarted(")).count(), 2, "the threads told Lifecycle about both executors");
    runtime.close();
    assert_eq!(crate::test_wait::recv_within(&reports), CLOSED);
    assert_eq!(open.try_take(), Some(Outcome::Closing), "a wait that nobody answered ends with the close");
    assert_eq!(alive.load(Ordering::SeqCst), 4);
    drop(runtime);
    assert_eq!(alive.load(Ordering::SeqCst), 0, "the threads of the owners ended with the runtime");
}

fn due_against_cancel() -> DetScheduler {
    let mut det = DetScheduler::new(0);
    let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
    det.start_epoch(1);
    det.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 7));
    det.run_until_quiet(5).unwrap();
    det.tick(Time(10));
    det.inject(0, toy, EpochStamp::Epoch(1), ToyInput::CancelSchedule(1));
    det
}

#[test]
fn rt_race_a_cancel_against_a_due_entry() {
    let toy = Address::<Toy>::new(0);
    let seen = RefCell::new(BTreeSet::new());
    let manifest = prop("P-RT-01", "Cancel x Fire of one entry", "R5 Cancel, R9 Fire", 8, "rt_race_a_cancel_against_a_due_entry").explore(due_against_cancel, |det| {
        let fired = det.state(toy).unwrap().fired.clone();
        if fired.len() > 1 {
            return Err(format!("one entry fired twice: {fired:?}"));
        }
        if det.choices().is_empty() {
            seen.borrow_mut().insert(fired);
        }
        Ok(())
    });
    manifest.assert_complete();
    assert_eq!(*seen.borrow(), BTreeSet::from([vec![], vec![7]]), "a cancel before the Fire removes the entry, a cancel behind the Fire in the queue is too late");
}

#[test]
fn rt_prop_task_panic_each_read() {
    for rounds in 1..=4u64 {
        let mut det = DetScheduler::new(0);
        let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
        det.start_epoch(1);
        let mut expected = Vec::new();
        for round in 0..rounds {
            det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(100 + round));
            det.post(toy, EpochStamp::Epoch(1), ToyInput::Ask(BOOM));
            det.run_until_quiet(30).unwrap();
            if round == 0 { expected.extend([BOOM, 100]); } else { expected.extend([100 + round, BOOM]); }
            assert_eq!(det.state(toy).unwrap().failed, expected, "round {round} of {rounds}: each failing task gives its own failure, the first death lists its pending ids ascending, a later start fails at its own start");
        }
    }
}
