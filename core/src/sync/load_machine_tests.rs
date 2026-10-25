use std::collections::BTreeSet;

use super::load_machine::*;

pub(super) const WINDOW: &str = "w";
pub(super) const MODEL: &str = "moments";

pub(super) fn stored(epoch: u64, applied: i64, has_rows: bool) -> Stored {
    let data = Data::Landed { has_more: false, total: None, loaded_at: applied };
    Stored { epoch, order: Order { applied, pages: 1 }, data, has_rows, revision: 0 }
}

pub(super) fn not_loaded(epoch: u64) -> Stored {
    Stored { epoch, order: Order::NONE, data: Data::NotLoaded, has_rows: false, revision: 0 }
}

pub(super) fn network() -> LoadError {
    LoadError::new("network", "offline")
}

pub(super) fn observe(machine: &mut Machine, seed: Stored) {
    step(machine, Event::Observe { window: WINDOW.to_string(), model: MODEL.to_string(), seed: Some(seed) });
}

pub(super) fn start(machine: &mut Machine, flight: FlightId, cursor: bool) {
    step(machine, Event::Start { window: WINDOW.to_string(), model: MODEL.to_string(), flight, cursor, seed: None });
}

pub(super) fn view(machine: &Machine) -> RelationState {
    machine.view(WINDOW).unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Before {
    NotLoaded,
    Loading,
    Loaded,
    Empty,
    Failed,
    Closed,
}

fn machine_in(before: Before) -> Machine {
    let mut machine = Machine::default();
    match before {
        Before::NotLoaded => observe(&mut machine, not_loaded(0)),
        Before::Loading => {
            observe(&mut machine, stored(0, 5, true));
            start(&mut machine, 1, false);
        }
        Before::Loaded => observe(&mut machine, stored(0, 5, true)),
        Before::Empty => observe(&mut machine, stored(0, 5, false)),
        Before::Failed => {
            observe(&mut machine, not_loaded(0));
            start(&mut machine, 1, false);
            step(&mut machine, Event::Failed { window: WINDOW.to_string(), flight: 1, error: network() });
        }
        Before::Closed => {
            observe(&mut machine, stored(0, 5, true));
            step(&mut machine, Event::Close);
        }
    }
    assert_eq!(view(&machine).phase, expected_phase(before), "setup {before:?}");
    machine
}

fn expected_phase(before: Before) -> LoadPhase {
    match before {
        Before::NotLoaded => LoadPhase::NotLoaded,
        Before::Loading => LoadPhase::Loading,
        Before::Loaded => LoadPhase::Loaded,
        Before::Empty => LoadPhase::Empty,
        Before::Failed => LoadPhase::Failed,
        Before::Closed => LoadPhase::Closed,
    }
}

fn end(event: &str, flight: FlightId, rows: bool) -> Event {
    let window = WINDOW.to_string();
    match event {
        "landed" => Event::Landed { window, flight, stored: Some(stored(0, 9, rows)) },
        "discarded" => Event::Discarded { window, flight },
        "failed_network" => Event::Failed { window, flight, error: network() },
        "failed_database" => Event::Failed { window, flight, error: LoadError::new("database", "disk") },
        "abandoned" => Event::Abandoned { window, flight },
        "panicked" => Event::Panicked { window, flight },
        other => panic!("unknown event {other}"),
    }
}

#[test]
fn inv_loadm_04_flight_end_events_follow_the_transition_table() {
    let ends = [
        ("landed", false, LoadPhase::Empty),
        ("landed", true, LoadPhase::Loaded),
        ("discarded", true, LoadPhase::Loaded),
        ("failed_network", true, LoadPhase::Loaded),
        ("failed_database", true, LoadPhase::Loaded),
        ("abandoned", true, LoadPhase::Loaded),
        ("panicked", true, LoadPhase::Loaded),
    ];
    for (event, rows, expected) in ends {
        let mut machine = machine_in(Before::Loading);
        step(&mut machine, end(event, 1, rows));
        assert_eq!(view(&machine).phase, expected, "{event} rows={rows} from Loading with rows");
        assert_eq!(machine.flights(WINDOW), 0, "{event}");
        assert_eq!(machine.orphan_ends, 0, "{event}");
    }
    let without_rows = [
        ("landed", false, LoadPhase::Empty),
        ("discarded", false, LoadPhase::NotLoaded),
        ("failed_network", false, LoadPhase::Failed),
        ("failed_database", false, LoadPhase::Failed),
        ("abandoned", false, LoadPhase::NotLoaded),
        ("panicked", false, LoadPhase::Failed),
    ];
    for (event, rows, expected) in without_rows {
        let mut machine = machine_in(Before::NotLoaded);
        start(&mut machine, 1, false);
        step(&mut machine, end(event, 1, rows));
        assert_eq!(view(&machine).phase, expected, "{event} from Loading without rows");
        assert_eq!(machine.orphan_ends, 0, "{event}");
    }
}

#[test]
fn inv_loadm_04_start_sets_the_phase_and_kind_by_what_the_window_holds() {
    let cells = [
        (Before::NotLoaded, false, LoadPhase::Loading, (true, false, false)),
        (Before::Loaded, false, LoadPhase::Loading, (false, true, false)),
        (Before::Empty, false, LoadPhase::Loading, (false, true, false)),
        (Before::Failed, false, LoadPhase::Loading, (true, false, false)),
        (Before::Loaded, true, LoadPhase::Loading, (false, false, true)),
    ];
    for (before, cursor, phase, (initial, refreshing, more)) in cells {
        let mut machine = machine_in(before);
        start(&mut machine, 7, cursor);
        let state = view(&machine);
        assert_eq!((state.phase, state.initial, state.refreshing, state.fetching_more), (phase, initial, refreshing, more), "{before:?} cursor={cursor}");
        assert_eq!(state.error, None, "start clears the error of {before:?}");
    }
    let mut closed = machine_in(Before::Closed);
    start(&mut closed, 7, false);
    assert_eq!((view(&closed).phase, closed.flights(WINDOW), closed.after_close), (LoadPhase::Closed, 0, 1));
}

#[test]
fn inv_loadm_04_an_end_without_a_start_changes_nothing_and_is_counted() {
    for before in [Before::NotLoaded, Before::Loaded, Before::Empty, Before::Failed, Before::Closed] {
        for event in ["landed", "discarded", "failed_network", "abandoned", "panicked"] {
            let mut machine = machine_in(before);
            let seen = view(&machine);
            step(&mut machine, end(event, 99, true));
            assert_eq!(view(&machine), seen, "{event} from {before:?}");
            assert_eq!(machine.orphan_ends + machine.after_close, 1, "{event} from {before:?}");
        }
    }
}

#[test]
fn inv_loadm_04_reset_online_offline_and_close_follow_the_transition_table() {
    let reset = |models: Option<&[&str]>| Event::Reset { epoch: 1, models: models.map(|models| models.iter().map(|model| model.to_string()).collect::<BTreeSet<_>>()) };
    for before in [Before::NotLoaded, Before::Loaded, Before::Empty, Before::Failed] {
        let mut machine = machine_in(before);
        step(&mut machine, reset(None));
        assert_eq!(view(&machine).phase, LoadPhase::NotLoaded, "reset all from {before:?}");
        assert_eq!(view(&machine).error, None);
    }
    let mut other_model = machine_in(Before::Loaded);
    step(&mut other_model, reset(Some(&["users"])));
    assert_eq!(view(&other_model).phase, LoadPhase::Loaded);
    let mut same_model = machine_in(Before::Loaded);
    step(&mut same_model, reset(Some(&[MODEL])));
    assert_eq!(view(&same_model).phase, LoadPhase::NotLoaded);
    let mut loading = machine_in(Before::Loading);
    step(&mut loading, reset(None));
    assert_eq!((view(&loading).phase, loading.flights(WINDOW)), (LoadPhase::Loading, 1));
    step(&mut loading, Event::Discarded { window: WINDOW.to_string(), flight: 1 });
    assert_eq!(view(&loading).phase, LoadPhase::NotLoaded);
    for before in [Before::NotLoaded, Before::Loading, Before::Loaded, Before::Empty, Before::Failed] {
        let mut machine = machine_in(before);
        let seen = view(&machine);
        let effects = step(&mut machine, Event::Offline);
        assert_eq!((view(&machine), effects), (seen, vec![]), "offline from {before:?}");
        step(&mut machine, Event::Close);
        assert_eq!(view(&machine).phase, LoadPhase::Closed, "close from {before:?}");
    }
}

#[test]
fn inv_loadm_08_online_retries_each_observed_window_that_failed_by_network_once() {
    let observed = || BTreeSet::from([WINDOW.to_string()]);
    let mut machine = machine_in(Before::Failed);
    let effects = step(&mut machine, Event::Online { observed: observed() });
    assert_eq!(effects, vec![Effect::Retry { window: WINDOW.to_string(), kind: LoadKind::Initial }]);
    assert!(step(&mut machine, Event::Online { observed: observed() }).is_empty(), "the retry is consumed");

    let mut unobserved = machine_in(Before::Failed);
    assert!(step(&mut unobserved, Event::Online { observed: BTreeSet::new() }).is_empty());

    let mut with_rows = machine_in(Before::Loaded);
    start(&mut with_rows, 2, false);
    step(&mut with_rows, end("failed_network", 2, true));
    assert_eq!(view(&with_rows).phase, LoadPhase::Loaded);
    assert_eq!(step(&mut with_rows, Event::Online { observed: observed() }), vec![Effect::Retry { window: WINDOW.to_string(), kind: LoadKind::Refresh }]);

    let mut server_error = machine_in(Before::NotLoaded);
    start(&mut server_error, 3, false);
    step(&mut server_error, Event::Failed { window: WINDOW.to_string(), flight: 3, error: LoadError::new("server", "no") });
    assert!(step(&mut server_error, Event::Online { observed: observed() }).is_empty());

    let mut running = machine_in(Before::Failed);
    start(&mut running, 4, false);
    assert!(step(&mut running, Event::Online { observed: observed() }).is_empty());
}

#[test]
fn inv_loadm_09_inv_fresh_03_a_reset_discards_a_read_taken_before_it() {
    let mut machine = Machine::default();
    step(&mut machine, Event::Reset { epoch: 1, models: None });
    observe(&mut machine, stored(0, 5, true));
    assert_eq!(view(&machine).phase, LoadPhase::NotLoaded);
    start(&mut machine, 1, false);
    step(&mut machine, Event::Landed { window: WINDOW.to_string(), flight: 1, stored: Some(stored(1, 7, true)) });
    assert_eq!(view(&machine).phase, LoadPhase::Loaded);
}

#[test]
fn inv_loadm_02_an_older_stored_read_never_replaces_a_newer_one() {
    let mut machine = machine_in(Before::NotLoaded);
    start(&mut machine, 1, false);
    start(&mut machine, 2, false);
    step(&mut machine, Event::Landed { window: WINDOW.to_string(), flight: 2, stored: Some(Stored { data: Data::Landed { has_more: false, total: Some(8), loaded_at: 20 }, ..stored(0, 20, true) }) });
    step(&mut machine, Event::Landed { window: WINDOW.to_string(), flight: 1, stored: Some(Stored { data: Data::Landed { has_more: true, total: Some(7), loaded_at: 10 }, ..stored(0, 10, true) }) });
    let state = view(&machine);
    assert_eq!((state.has_more, state.total, state.loaded_at, state.phase), (false, Some(8), Some(20), LoadPhase::Loaded));
}

#[test]
fn inv_loadm_03_a_failure_stays_in_the_snapshot_until_the_next_start() {
    let mut machine = machine_in(Before::Loaded);
    start(&mut machine, 1, false);
    start(&mut machine, 2, true);
    step(&mut machine, end("failed_network", 1, true));
    let state = view(&machine);
    assert_eq!((state.phase, state.error.map(|error| error.kind)), (LoadPhase::Loading, Some("network")));
    step(&mut machine, end("landed", 2, true));
    assert_eq!(view(&machine).error, None);
}
