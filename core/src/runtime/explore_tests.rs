use std::cell::RefCell;
use std::collections::HashSet;

use super::testing::*;
use super::*;

#[test]
fn explorer_finds_the_planted_race() {
    let ledger = race(0).1;
    let report = explore(|| race(0).0, Limits { depth: 4, max_traces: 1000 }, |det| {
        if det.state(ledger).unwrap().lost == 0 { Ok(()) } else { Err("deposit lost after seal".to_string()) }
    });
    assert!(!report.violations.is_empty());
    let violation = &report.violations[0];
    assert_eq!(violation.message, "deposit lost after seal");
    let seal = violation.trace.iter().position(|entry| entry.owner == "ledger" && entry.input == "Seal").unwrap();
    let deposit = violation.trace.iter().position(|entry| entry.owner == "ledger" && entry.input == "Deposit(5)").unwrap();
    assert!(seal < deposit);
}

#[test]
fn explorer_passes_when_the_property_holds_in_every_order() {
    let ledger = race(0).1;
    let report = explore(|| race(0).0, Limits { depth: 4, max_traces: 1000 }, |det| {
        let state = det.state(ledger).unwrap();
        if state.total + state.lost <= 5 { Ok(()) } else { Err("money appeared".to_string()) }
    });
    assert!(report.violations.is_empty());
    assert!(report.traces >= 4);
}

#[test]
fn explorer_stops_at_the_trace_limit() {
    let report = explore(|| race(0).0, Limits { depth: 4, max_traces: 2 }, |_| Ok(()));
    assert_eq!(report.traces, 2);
}

fn async_reply() -> (DetScheduler, Address<Toy>) {
    let pending = ParkedBacks::default();
    let runner = || ToyRunner { pending: Some(pending.clone()), ..ToyRunner::default() };
    let mut det = DetScheduler::new(0);
    let pump = det.add::<Toy>("pump", ToyState::default(), runner());
    let ledger = det.add::<Toy>("ledger", ToyState { pump: Some(pump), ..ToyState::default() }, runner());
    let sender = det.add::<Toy>("sender", ToyState { on_go: Some((ledger, ToyInput::Note(9))), ..ToyState::default() }, ToyRunner::default());
    det.start_epoch(1);
    det.post(ledger, EpochStamp::Epoch(1), ToyInput::WorkAsync(3));
    det.post(sender, EpochStamp::Epoch(1), ToyInput::Go);
    (det, ledger)
}

#[test]
fn explorer_finds_both_orders_of_an_asynchronous_effect_result_and_another_input() {
    let ledger = async_reply().1;
    let seen = RefCell::new(HashSet::new());
    let report = explore(async_reply_scheduler, Limits { depth: 8, max_traces: 1000 }, |det| {
        let results = &det.state(ledger).unwrap().results;
        if results.len() == 2 {
            seen.borrow_mut().insert(results.clone());
        }
        Ok(())
    });
    assert!(report.violations.is_empty());
    assert_eq!(*seen.borrow(), HashSet::from([vec![6, 9], vec![9, 6]]));
}

fn async_reply_scheduler() -> DetScheduler { async_reply().0 }
