use std::collections::{BTreeMap, BTreeSet};

use super::load_machine::*;

const WINDOWS: [(&str, &str); 2] = [("a", "m1"), ("b", "m2")];
const SEQUENCES: u64 = 10_000;
const LENGTH: usize = 40;

pub(super) struct Random(pub(super) u64);

impl Random {
    pub(super) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub(super) fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    pub(super) fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }
}

struct Script {
    random: Random,
    next_flight: FlightId,
    open: Vec<(String, FlightId)>,
    epoch: u64,
    history: Vec<Event>,
}

impl Script {
    fn stored(&mut self) -> Option<Stored> {
        if self.random.chance(8) {
            return None;
        }
        let epoch = if self.random.chance(4) { self.epoch.saturating_sub(1) } else { self.epoch };
        let has_rows = self.random.chance(2);
        let applied = self.random.below(20) as i64;
        let revision = self.random.below(30) as i64;
        Some(match self.random.below(3) {
            0 => Stored { epoch, order: Order::NONE, data: Data::NotLoaded, has_rows: false, revision },
            1 => Stored { epoch, order: Order::NONE, data: Data::Local, has_rows: true, revision },
            _ => Stored { epoch, order: Order { applied, pages: 1 + self.random.below(3) as i64 }, data: Data::Landed { has_more: has_rows, total: Some(applied), loaded_at: applied }, has_rows, revision },
        })
    }

    fn error(&mut self) -> LoadError {
        let kind = ["network", "server", "database", "unrecognized"][self.random.below(4)];
        LoadError::new(kind, kind)
    }

    fn next(&mut self) -> Event {
        let (window, model) = WINDOWS[self.random.below(WINDOWS.len())];
        let (window, model) = (window.to_string(), model.to_string());
        let ending = if self.open.is_empty() { None } else { Some(self.random.below(self.open.len())) };
        match (self.random.below(14), ending) {
            (0, _) => Event::Observe { window, model, seed: self.stored() },
            (1..=3, _) => {
                let flight = self.next_flight;
                self.next_flight += 1;
                self.open.push((window.clone(), flight));
                Event::Start { window, model, flight, cursor: self.random.chance(3), seed: self.stored() }
            }
            (4..=9, Some(at)) => {
                let (window, flight) = self.open.remove(at);
                match self.random.below(6) {
                    0 | 1 => Event::Landed { window, flight, stored: self.stored() },
                    2 => Event::Discarded { window, flight },
                    3 => Event::Failed { window, flight, error: self.error() },
                    4 => Event::Abandoned { window, flight },
                    _ => Event::Panicked { window, flight },
                }
            }
            (10, _) => {
                self.epoch += 1;
                let models = self.random.chance(2).then(|| BTreeSet::from([model]));
                Event::Reset { epoch: self.epoch, models }
            }
            (11, _) => if self.random.chance(2) { Event::Offline } else { Event::Online { observed: WINDOWS.iter().filter(|_| self.random.chance(2)).map(|(window, _)| window.to_string()).collect() } },
            (12, _) => match self.random.below(3) {
                0 => Event::ReadOk { window, has_rows: self.random.chance(2) },
                1 => Event::ReadFailed { window, error: self.error() },
                _ => Event::Landed { window, flight: 9_000 + self.random.below(3) as u64, stored: None },
            },
            (_, _) => if self.random.chance(12) { Event::Close } else { Event::Abandoned { window, flight: 9_100 } },
        }
    }
}

#[derive(Clone)]
enum Outcome {
    Nothing,
    Landed,
    Failed(LoadError),
}

struct Seen {
    model: String,
    open: Vec<(FlightId, LoadKind)>,
    accepted: Option<(Order, Data)>,
    has_rows: bool,
    needs: i64,
    outcome: Outcome,
    retry: Option<LoadKind>,
    read_error: Option<LoadError>,
}

pub(super) struct Replay {
    windows: BTreeMap<String, Seen>,
    closed: bool,
    ignored: u32,
    retried: BTreeSet<(String, LoadKind)>,
}

fn seen<'a>(run: &'a mut Replay, window: &str, model: &str) -> &'a mut Seen {
    run.windows.entry(window.to_string()).or_insert_with(|| Seen { model: model.to_string(), open: vec![], accepted: None, has_rows: false, needs: 0, outcome: Outcome::Nothing, retry: None, read_error: None })
}

pub(super) fn replay(history: &[Event]) -> Replay {
    let mut run = Replay { windows: BTreeMap::new(), closed: false, ignored: 0, retried: BTreeSet::new() };
    let mut all_epoch = 0;
    let mut model_epochs: BTreeMap<String, u64> = BTreeMap::new();
    for event in history {
        run.retried.clear();
        if run.closed {
            run.ignored += 1;
            continue;
        }
        let epoch_of = |model: &str, all_epoch: u64, model_epochs: &BTreeMap<String, u64>| all_epoch.max(model_epochs.get(model).copied().unwrap_or(0));
        let take = |seen: &mut Seen, stored: &Option<Stored>, only_unread: bool, current: u64| {
            let Some(stored) = stored else { return };
            if stored.epoch < current || (only_unread && seen.accepted.is_some()) {
                return;
            }
            if seen.accepted.as_ref().is_some_and(|(order, _)| stored.order < *order) {
                return;
            }
            seen.accepted = Some((stored.order, stored.data.clone()));
            seen.has_rows = stored.has_rows;
        };
        match event {
            Event::Observe { window, model, seed } => {
                let current = epoch_of(model, all_epoch, &model_epochs);
                take(seen(&mut run, window, model), seed, true, current);
            }
            Event::Start { window, model, flight, cursor, seed } => {
                let current = epoch_of(model, all_epoch, &model_epochs);
                let seen = seen(&mut run, window, model);
                take(seen, seed, true, current);
                let kind = if *cursor { LoadKind::More } else if matches!(seen.accepted, Some((_, Data::Landed { .. } | Data::Local))) { LoadKind::Refresh } else { LoadKind::Initial };
                seen.open.push((*flight, kind));
                seen.outcome = Outcome::Nothing;
                seen.retry = None;
            }
            Event::Landed { window, flight, stored } => {
                let current = run.windows.get(window).map_or(0, |seen| epoch_of(&seen.model, all_epoch, &model_epochs));
                let valid = run.windows.get_mut(window).is_some_and(|seen| match seen.open.iter().position(|(id, _)| id == flight) {
                    Some(at) => {
                        seen.open.remove(at);
                        seen.outcome = Outcome::Landed;
                        seen.retry = None;
                        if let Some(stored) = stored.as_ref().filter(|stored| stored.epoch >= current) {
                            seen.needs = seen.needs.max(stored.revision);
                        }
                        take(seen, stored, false, current);
                        true
                    }
                    None => false,
                });
                if !valid {
                    run.ignored += 1;
                }
            }
            Event::Discarded { window, flight } | Event::Abandoned { window, flight } => {
                let valid = run.windows.get_mut(window).is_some_and(|seen| seen.open.iter().position(|(id, _)| id == flight).map(|at| seen.open.remove(at)).is_some());
                if !valid {
                    run.ignored += 1;
                }
            }
            Event::Failed { window, flight, error } => {
                let valid = run.windows.get_mut(window).is_some_and(|seen| match seen.open.iter().position(|(id, _)| id == flight) {
                    Some(at) => {
                        let (_, kind) = seen.open.remove(at);
                        seen.retry = (error.kind == "network").then_some(kind);
                        seen.outcome = Outcome::Failed(error.clone());
                        true
                    }
                    None => false,
                });
                if !valid {
                    run.ignored += 1;
                }
            }
            Event::Panicked { window, flight } => {
                let valid = run.windows.get_mut(window).is_some_and(|seen| match seen.open.iter().position(|(id, _)| id == flight) {
                    Some(at) => {
                        seen.open.remove(at);
                        seen.retry = None;
                        seen.outcome = Outcome::Failed(LoadError::internal());
                        true
                    }
                    None => false,
                });
                if !valid {
                    run.ignored += 1;
                }
            }
            Event::Reset { epoch, models } => {
                match models {
                    Some(models) => models.iter().for_each(|model| {
                        let known = model_epochs.entry(model.clone()).or_insert(0);
                        *known = (*known).max(*epoch);
                    }),
                    None => all_epoch = all_epoch.max(*epoch),
                }
                for seen in run.windows.values_mut().filter(|seen| models.as_ref().is_none_or(|models| models.contains(&seen.model))) {
                    seen.accepted = Some((Order::NONE, Data::NotLoaded));
                    seen.has_rows = false;
                    seen.outcome = Outcome::Nothing;
                    seen.read_error = None;
                    seen.retry = None;
                }
            }
            Event::Offline => {}
            Event::Online { observed } => {
                let mut retried = BTreeSet::new();
                for (window, seen) in run.windows.iter_mut().filter(|(window, seen)| seen.open.is_empty() && observed.contains(*window)) {
                    if let Some(kind) = seen.retry.take() {
                        retried.insert((window.clone(), kind));
                    }
                }
                run.retried = retried;
            }
            Event::Close => run.closed = true,
            Event::ReadOk { window, has_rows } => {
                if let Some(seen) = run.windows.get_mut(window) {
                    seen.has_rows = *has_rows;
                    seen.read_error = None;
                }
            }
            Event::ReadFailed { window, error } => {
                if let Some(seen) = run.windows.get_mut(window) {
                    seen.read_error = Some(error.clone());
                }
            }
        }
    }
    run
}

pub(super) fn expected(run: &Replay, window: &str) -> Option<RelationState> {
    let seen = run.windows.get(window)?;
    let last_failure = match &seen.outcome {
        Outcome::Failed(error) => Some(error.clone()),
        _ => None,
    };
    let (has_more, total, loaded_at) = match &seen.accepted {
        Some((_, Data::Landed { has_more, total, loaded_at })) => (*has_more, *total, Some(*loaded_at)),
        _ => (false, None, None),
    };
    let landed_or_local = matches!(&seen.accepted, Some((_, Data::Landed { .. } | Data::Local)));
    let ladder = [
        (run.closed, LoadPhase::Closed),
        (!seen.open.is_empty(), LoadPhase::Loading),
        (seen.read_error.is_some(), LoadPhase::Failed),
        (last_failure.is_some() && !seen.has_rows, LoadPhase::Failed),
        (last_failure.is_some(), LoadPhase::Loaded),
        (!landed_or_local, LoadPhase::NotLoaded),
        (!seen.has_rows, LoadPhase::Empty),
    ];
    let phase = ladder.iter().find(|(holds, _)| *holds).map_or(LoadPhase::Loaded, |(_, phase)| *phase);
    let running = |kind| seen.open.iter().any(|(_, running)| *running == kind);
    Some(RelationState {
        complete: loaded_at.is_some() && !has_more && seen.open.is_empty() && seen.read_error.is_none() && last_failure.is_none(),
        phase,
        initial: running(LoadKind::Initial),
        refreshing: running(LoadKind::Refresh),
        fetching_more: running(LoadKind::More),
        has_more,
        total,
        error: seen.read_error.clone().or(last_failure),
        loaded_at,
        revision: seen.needs,
    })
}

#[test]
fn inv_loadm_01_the_phase_equals_the_oracle_over_random_histories() {
    for seed in 1..=SEQUENCES {
        let mut script = Script { random: Random(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1), next_flight: 1, open: vec![], epoch: 0, history: vec![] };
        let mut machine = Machine::default();
        for index in 0..LENGTH {
            let event = script.next();
            script.history.push(event.clone());
            let effects = step(&mut machine, event);
            let run = replay(&script.history);
            for (window, _) in WINDOWS {
                assert_eq!(machine.view(window), expected(&run, window), "seed {seed} step {index} window {window}: {:?}", script.history);
            }
            assert_eq!(machine.orphan_ends + machine.after_close, run.ignored, "seed {seed} step {index} ignored events");
            let retries: BTreeSet<(String, LoadKind)> = effects.iter().filter_map(|effect| if let Effect::Retry { window, kind } = effect { Some((window.clone(), *kind)) } else { None }).collect();
            assert_eq!(retries, run.retried, "seed {seed} step {index} retries");
        }
        let settled: usize = WINDOWS.iter().map(|(window, _)| machine.flights(window)).sum();
        let still_open = script.open.iter().filter(|(window, _)| machine.view(window).is_some()).count();
        if !script.history.iter().any(|event| matches!(event, Event::Close)) {
            assert_eq!(settled, still_open, "seed {seed}: the machine holds exactly the flights that did not end");
        }
    }
}
