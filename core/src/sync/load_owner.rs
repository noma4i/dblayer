use std::collections::{BTreeSet, HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

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
    Target(Event, Target),
    Online,
}

#[derive(Clone)]
pub struct RelationWatch {
    // The receiver must drop before the lease checks remaining observers.
    receiver: watch::Receiver<RelationState>,
    _lease: RelationLease,
}

#[derive(Clone)]
struct RelationLease {
    owner: Weak<LoadOwner>,
    window: String,
}

impl RelationWatch {
    pub fn borrow(&self) -> watch::Ref<'_, RelationState> { self.receiver.borrow() }
    pub fn borrow_and_update(&mut self) -> watch::Ref<'_, RelationState> { self.receiver.borrow_and_update() }
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> { self.receiver.changed().await }
}

impl Drop for RelationLease {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            locked(&owner.state).forget_window(&self.window);
        }
    }
}

#[derive(Default)]
struct OwnerState {
    machine: Machine,
    senders: HashMap<String, watch::Sender<RelationState>>,
    targets: HashMap<String, Target>,
    retries: Vec<(String, LoadKind)>,
}

impl OwnerState {
    fn forget_window(&mut self, window: &str) {
        if self.senders.get(window).is_some_and(|sender| sender.receiver_count() > 0) {
            return;
        }
        if self.machine.forget_window(window) {
            self.senders.remove(window);
            self.targets.remove(window);
            self.retries.retain(|(key, _)| key != window);
        }
    }
}

#[derive(Default)]
pub(crate) struct LoadOwner {
    queue: Mutex<VecDeque<Queued>>,
    state: Mutex<OwnerState>,
    next_flight: AtomicU64,
    epoch: AtomicU64,
    #[cfg(test)]
    panic_window: Mutex<Option<String>>,
    #[cfg(test)]
    history: Mutex<Vec<Event>>,
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
        self.drain_locked(&mut locked(&self.state));
    }

    fn drain_locked(&self, state: &mut OwnerState) {
        loop {
            let Some(queued) = locked(&self.queue).pop_front() else { return };
            let event = match queued {
                Queued::Event(event) => event,
                Queued::Target(event, target) => {
                    if let Some(window) = event.window() {
                        state.targets.entry(window.to_string()).or_insert(target);
                    }
                    event
                }
                Queued::Online => Event::Online { observed: state.senders.iter().filter(|(_, sender)| sender.receiver_count() > 0).map(|(window, _)| window.clone()).collect::<BTreeSet<_>>() },
            };
            #[cfg(test)]
            locked(&self.history).push(event.clone());
            let window = event.window().map(str::to_string);
            let finished = event.ends_flight();
            let resets = matches!(event, Event::Reset { .. });
            let stepped = catch_unwind(AssertUnwindSafe(|| {
                #[cfg(test)]
                if window.as_deref().is_some_and(|window| locked(&self.panic_window).as_deref() == Some(window)) {
                    locked(&self.panic_window).take();
                    panic!("injected panic for a window event");
                }
                step(&mut state.machine, event)
            }));
            let effects = match (stepped, window.clone()) {
                (Ok(effects), _) => effects,
                (Err(_), Some(window)) => step(&mut state.machine, Event::ReadFailed { window, error: LoadError::internal() }),
                (Err(_), None) => Vec::new(),
            };
            for effect in effects {
                match effect {
                    Effect::Publish { window, state: published } => {
                        if let Some(sender) = state.senders.get(&window) {
                            sender.send_replace(published);
                        }
                    }
                    Effect::Retry { window, kind } => state.retries.push((window, kind)),
                }
            }
            if finished {
                if let Some(window) = &window { state.forget_window(window); }
            }
            if resets {
                let state = &mut *state;
                let observed = |window: &str| state.senders.get(window).is_some_and(|sender| sender.receiver_count() > 0);
                state.machine.forget(observed);
                let kept: BTreeSet<String> = state.senders.keys().filter(|window| state.machine.view(window).is_some()).cloned().collect();
                state.senders.retain(|window, _| kept.contains(window));
                state.targets.retain(|window, _| kept.contains(window));
            }
        }
    }

    pub(crate) fn observe(self: &Arc<Self>, window: &str, target: Target, seed: Option<Stored>) -> RelationWatch {
        let receiver = {
            let mut state = locked(&self.state);
            let view = state.machine.view(window).unwrap_or_else(RelationState::closed);
            let receiver = state.senders.entry(window.to_string()).or_insert_with(|| watch::channel(view).0).subscribe();
            locked(&self.queue).push_back(Queued::Target(Event::Observe { window: window.to_string(), model: target.model.clone(), seed }, target));
            self.drain_locked(&mut state);
            receiver
        };
        RelationWatch { receiver, _lease: RelationLease { owner: Arc::downgrade(self), window: window.to_string() } }
    }

    pub(crate) fn begin_flight(&self, window: &str, target: Target, cursor: bool, seed: Option<Stored>) -> FlightId {
        let flight = self.next_flight.fetch_add(1, Ordering::SeqCst);
        self.enqueue(Queued::Target(Event::Start { window: window.to_string(), model: target.model.clone(), flight, cursor, seed }, target));
        flight
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
    pub(crate) fn panic_on(&self, window: &str) {
        *locked(&self.panic_window) = Some(window.to_string());
    }

    #[cfg(test)]
    pub(crate) fn history(&self) -> Vec<Event> {
        locked(&self.history).clone()
    }

    #[cfg(test)]
    pub(crate) fn tracked_windows(&self) -> usize {
        locked(&self.state).machine.windows_len()
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
