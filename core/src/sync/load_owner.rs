use std::collections::{BTreeSet, HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::db::dispatch::{Effects, OrderedEffects};

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
            owner.with_state(|state, effects| state.forget_window(&self.window, effects));
        }
    }
}

#[derive(Default)]
struct OwnerState {
    machine: Machine,
    senders: HashMap<String, watch::Sender<RelationState>>,
    targets: HashMap<String, Target>,
    retries: Vec<(String, LoadKind)>,
    chains: HashMap<String, MembersChain>,
}

struct MembersChain {
    identity: Arc<()>,
    readers: usize,
    result: watch::Sender<Option<Result<(), super::SyncError>>>,
    task: tokio::task::AbortHandle,
}

pub(crate) struct MembersRead {
    identity: Arc<()>,
    owner: Weak<LoadOwner>,
    window: String,
    result: watch::Receiver<Option<Result<(), super::SyncError>>>,
}

impl MembersRead {
    pub(crate) async fn finished(&mut self) -> Result<(), super::SyncError> {
        self.result.wait_for(Option::is_some).await.map_err(|_| super::SyncError::Cancelled)?.as_ref().cloned().ok_or(super::SyncError::Cancelled)?
    }
}

impl Drop for MembersRead {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else { return };
        owner.with_state(|state, effects| {
            let Some(chain) = state.chains.get_mut(&self.window) else { return };
            if !Arc::ptr_eq(&chain.identity, &self.identity) { return; }
            chain.readers -= 1;
            if chain.readers > 0 { return; }
            if let Some(chain) = state.chains.remove(&self.window) {
                effects.push(move || {
                    crate::db::lock::effect(|| chain.task.abort());
                    drop(chain);
                });
            }
        });
    }
}

impl OwnerState {
    fn forget_window(&mut self, window: &str, effects: &mut Effects) {
        if self.senders.get(window).is_some_and(|sender| sender.receiver_count() > 0) {
            return;
        }
        if self.machine.forget_window(window) {
            if let Some(sender) = self.senders.remove(window) { effects.release(sender); }
            self.targets.remove(window);
            self.retries.retain(|(key, _)| key != window);
        }
    }
}

#[derive(Default)]
pub(crate) struct LoadOwner {
    queue: Mutex<VecDeque<Queued>>,
    state: Mutex<OwnerState>,
    outbox: OrderedEffects,
    next_flight: AtomicU64,
    epoch: AtomicU64,
    #[cfg(test)]
    panic_window: Mutex<Option<String>>,
    #[cfg(test)]
    history: Mutex<Vec<Event>>,
}

impl LoadOwner {
    fn with_state<R>(&self, apply: impl FnOnce(&mut OwnerState, &mut Effects) -> R) -> R {
        self.outbox.under(&self.state, apply)
    }

    pub(crate) fn members(self: &Arc<Self>, window: &str, start: impl FnOnce(watch::Sender<Option<Result<(), super::SyncError>>>) -> tokio::task::AbortHandle) -> MembersRead {
        self.with_state(|state, _| {
            let chain = state.chains.entry(window.to_string()).or_insert_with(|| {
                let result = watch::channel(None).0;
                let task = start(result.clone());
                MembersChain { identity: Arc::new(()), readers: 0, result, task }
            });
            chain.readers += 1;
            MembersRead { identity: chain.identity.clone(), owner: Arc::downgrade(self), window: window.to_string(), result: chain.result.subscribe() }
        })
    }

    pub(crate) async fn unobserved(&self, window: &str) {
        let sender = locked(&self.state).senders.get(window).cloned();
        if let Some(sender) = sender { sender.closed().await; }
    }

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
        self.with_state(|state, effects| self.drain_locked(state, effects));
    }

    fn drain_locked(&self, state: &mut OwnerState, effects: &mut Effects) {
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
            let produced = match (stepped, window.clone()) {
                (Ok(produced), _) => produced,
                (Err(_), Some(window)) => step(&mut state.machine, Event::ReadFailed { window, error: LoadError::internal() }),
                (Err(_), None) => Vec::new(),
            };
            for effect in produced {
                match effect {
                    Effect::Publish { window, state: published } => {
                        if let Some(sender) = state.senders.get(&window).cloned() {
                            effects.push(move || { crate::db::lock::effect(|| sender.send_replace(*published)); });
                        }
                    }
                    Effect::Retry { window, kind } => state.retries.push((window, kind)),
                }
            }
            if finished {
                if let Some(window) = &window {
                    if state.machine.flights(window) == 0 {
                        if let Some(chain) = state.chains.remove(window) { effects.release(chain); }
                    }
                    state.forget_window(window, effects);
                }
            }
            if resets {
                let state = &mut *state;
                let observed = |window: &str| state.senders.get(window).is_some_and(|sender| sender.receiver_count() > 0);
                state.machine.forget(observed);
                let kept: BTreeSet<String> = state.senders.keys().filter(|window| state.machine.view(window).is_some()).cloned().collect();
                let gone: Vec<String> = state.senders.keys().filter(|window| !kept.contains(*window)).cloned().collect();
                effects.release(gone.iter().filter_map(|window| state.senders.remove(window)).collect::<Vec<_>>());
                state.targets.retain(|window, _| kept.contains(window));
            }
        }
    }

    pub(crate) fn observe(self: &Arc<Self>, window: &str, target: Target, seed: Option<Stored>) -> RelationWatch {
        let receiver = self.with_state(|state, effects| {
            let view = state.machine.view(window).unwrap_or_else(RelationState::closed);
            let receiver = state.senders.entry(window.to_string()).or_insert_with(|| watch::channel(view).0).subscribe();
            locked(&self.queue).push_back(Queued::Target(Event::Observe { window: window.to_string(), model: target.model.clone(), seed }, target));
            self.drain_locked(state, effects);
            receiver
        });
        RelationWatch { receiver, _lease: RelationLease { owner: Arc::downgrade(self), window: window.to_string() } }
    }

    pub(crate) fn seed_observed(&self, window: &str, model: String, seed: Option<Stored>) {
        self.with_state(|state, effects| {
            if state.senders.get(window).is_some_and(|sender| sender.receiver_count() > 0) {
                locked(&self.queue).push_back(Queued::Event(Event::Observe { window: window.to_string(), model, seed }));
                self.drain_locked(state, effects);
            }
        });
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

    pub(crate) fn session_refused(&self, window: &str) -> bool {
        let error = locked(&self.state).machine.view(window).and_then(|state| state.error);
        error.is_some_and(|error| matches!(error.cause, Some(super::SyncError::Transport(crate::transport::TransportError::Session { .. }))))
    }

    pub(crate) fn online(&self) -> Vec<(Target, LoadKind)> {
        self.enqueue(Queued::Online);
        let mut state = locked(&self.state);
        let retries = std::mem::take(&mut state.retries);
        retries.into_iter().filter_map(|(window, kind)| state.targets.get(&window).cloned().map(|target| (target, kind))).collect()
    }

    #[cfg(test)]
    pub(crate) fn guards_free(&self) -> bool {
        use crate::test_wait::mutex_free;
        mutex_free(&self.state) && mutex_free(&self.queue) && self.outbox.is_free()
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

    #[cfg(test)]
    pub(crate) fn members_readers(&self, window: &str) -> usize {
        locked(&self.state).chains.get(window).map_or(0, |chain| chain.readers)
    }
}

pub(crate) struct FlightTicket<'a> {
    cancel_failed: bool,
    owner: &'a LoadOwner,
    window: String,
    flight: FlightId,
    ended: bool,
}

impl<'a> FlightTicket<'a> {
    pub(crate) fn new(owner: &'a LoadOwner, window: &str, target: Target, cursor: bool, seed: Option<Stored>) -> Self {
        let flight = owner.begin_flight(window, target, cursor, seed);
        Self { cancel_failed: false, owner, window: window.to_string(), flight, ended: false }
    }

    pub(crate) fn cancel_failed(mut self) -> Self { self.cancel_failed = true; self }

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
        self.owner.submit(if std::thread::panicking() { Event::Panicked { window, flight } } else if self.cancel_failed { Event::Failed { window, flight, error: LoadError::of(&super::SyncError::Cancelled) } } else { Event::Abandoned { window, flight } });
    }
}
