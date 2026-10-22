use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::{Map, Value};
use tokio::sync::watch;

use super::load_machine::{step, Effect, Event, FlightId, LoadError, LoadKind, Machine, RelationState, Stored};
use super::locked;

#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub model: String,
    pub relation: String,
    pub params: Map<String, Value>,
}

enum Queued {
    Event(Event),
    Online,
}

#[derive(Default)]
struct OwnerState {
    machine: Machine,
    senders: HashMap<String, watch::Sender<RelationState>>,
    targets: HashMap<String, Target>,
    retries: Vec<(String, LoadKind)>,
}

#[derive(Default)]
pub(crate) struct LoadOwner {
    queue: Mutex<VecDeque<Queued>>,
    state: Mutex<OwnerState>,
    next_flight: AtomicU64,
    epoch: AtomicU64,
}

impl LoadOwner {
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub(crate) fn submit(&self, event: Event) {
        self.enqueue(Queued::Event(event));
    }

    fn enqueue(&self, queued: Queued) {
        locked(&self.queue).push_back(queued);
        self.drain();
    }

    fn drain(&self) {
        let mut state = locked(&self.state);
        loop {
            let Some(queued) = locked(&self.queue).pop_front() else { return };
            let event = match queued {
                Queued::Event(event) => event,
                Queued::Online => Event::Online { observed: state.senders.iter().filter(|(_, sender)| sender.receiver_count() > 0).map(|(window, _)| window.clone()).collect::<BTreeSet<_>>() },
            };
            for effect in step(&mut state.machine, event) {
                match effect {
                    Effect::Publish { window, state: published } => {
                        if let Some(sender) = state.senders.get(&window) {
                            sender.send_replace(published);
                        }
                    }
                    Effect::Retry { window, kind } => state.retries.push((window, kind)),
                }
            }
        }
    }

    pub(crate) fn observe(&self, window: &str, target: Target, seed: Option<Stored>) -> watch::Receiver<RelationState> {
        self.remember(window, &target);
        self.submit(Event::Observe { window: window.to_string(), model: target.model, seed });
        let mut state = locked(&self.state);
        let view = state.machine.view(window).unwrap_or_else(RelationState::closed);
        state.senders.entry(window.to_string()).or_insert_with(|| watch::channel(view).0).subscribe()
    }

    pub(crate) fn begin_flight(&self, window: &str, target: Target, cursor: bool, seed: Option<Stored>) -> FlightId {
        let flight = self.next_flight.fetch_add(1, Ordering::SeqCst);
        self.remember(window, &target);
        self.submit(Event::Start { window: window.to_string(), model: target.model, flight, cursor, seed });
        flight
    }

    fn remember(&self, window: &str, target: &Target) {
        locked(&self.state).targets.entry(window.to_string()).or_insert_with(|| target.clone());
    }

    pub(crate) fn reset(&self, models: Option<&[String]>) {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.submit(Event::Reset { epoch, models: models.map(|models| models.iter().cloned().collect()) });
    }

    pub(crate) fn online(&self) -> Vec<(Target, LoadKind)> {
        self.enqueue(Queued::Online);
        let mut state = locked(&self.state);
        let retries = std::mem::take(&mut state.retries);
        retries.into_iter().filter_map(|(window, kind)| state.targets.get(&window).cloned().map(|target| (target, kind))).collect()
    }

    #[cfg(test)]
    pub(crate) fn orphan_ends(&self) -> u32 {
        locked(&self.state).machine.orphan_ends
    }

    #[cfg(test)]
    pub(crate) fn flights(&self, window: &str) -> usize {
        locked(&self.state).machine.flights(window)
    }
}

pub(crate) struct FlightTicket<'a> {
    owner: &'a LoadOwner,
    window: String,
    flight: FlightId,
    ended: bool,
}

impl<'a> FlightTicket<'a> {
    pub(crate) fn new(owner: &'a LoadOwner, window: &str, target: Target, cursor: bool, seed: Option<Stored>) -> Self {
        let flight = owner.begin_flight(window, target, cursor, seed);
        Self { owner, window: window.to_string(), flight, ended: false }
    }

    fn end(mut self, event: impl FnOnce(String, FlightId) -> Event) {
        self.ended = true;
        self.owner.submit(event(self.window.clone(), self.flight));
    }

    pub(crate) fn landed(self, stored: Option<Stored>) {
        self.end(|window, flight| Event::Landed { window, flight, stored });
    }

    pub(crate) fn discarded(self) {
        self.end(|window, flight| Event::Discarded { window, flight });
    }

    pub(crate) fn failed(self, error: LoadError) {
        self.end(|window, flight| Event::Failed { window, flight, error });
    }
}

impl Drop for FlightTicket<'_> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let (window, flight) = (self.window.clone(), self.flight);
        self.owner.submit(if std::thread::panicking() { Event::Panicked { window, flight } } else { Event::Abandoned { window, flight } });
    }
}
