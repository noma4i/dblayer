use super::testing::*;
use super::*;

#[test]
fn explorer_finds_the_planted_race() {
    let ledger = race(0).1;
    let report = explore(|| race(0).0, Limits { depth: 4, max_traces: 1000 }, |det| {
        if det.state(ledger).lost == 0 { Ok(()) } else { Err("deposit lost after seal".to_string()) }
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
        let state = det.state(ledger);
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
