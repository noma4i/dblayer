//! Owner-level tests of the `window` machine: the owner `Windows` on `DetScheduler`, every row of the table and every property.
//! A row test feeds an input to the owner through its address and asserts what the owner does outward: the effects, the outcomes of the callers, the stage and the phase.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

const NOW: i64 = 1_000_000;

use super::ref_window::Phase;
use super::ref_window::{Effect, Failure, Goal, Input, Marker3, Read, Reject, Result3, PRELOAD};
use crate::owners::contract::{EpochStarted, ErrorClass, Online, RejectReason, Rejected, RequestError};
use crate::owners::contract::WindowPhase as OwnerPhase;
use crate::spec::support::{new_log, sent_batches, snapshot, take, Discard, Emitted, Log, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreStub, StoreStubState};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter};
use crate::owners::read::windows::{self as w, AnswerPage, StaleKeys, WindowEffect, WindowKey, WindowKind, WindowOutcome, WindowStage, Windows, WindowsEffect, WindowsInput, WindowsPeers};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, Effect as RuntimeEffect, EffectRunner, Effects, EpochStamp, Limits, Outbound, StateOwner, Time};
use crate::spec::support::committed_at;

const EPOCH: u64 = 1;
/// Slots of the peers and of the owner: the Store, the Deliveries, then the Windows.
const STORE: usize = 0;
const DELIVERIES: usize = 1;
const WINDOWS: usize = 2;
/// The id of the stream that watches the window in the tests: a call gets a smaller id.
const STREAM: u32 = 100;

type Spied = Log<Emitted<WindowsEffect, WindowsInput>>;

fn peers() -> WindowsPeers { WindowsPeers { store: Address::<StoreWriter>::new(STORE), deliveries: Address::<Deliveries>::new(DELIVERIES) } }

/// Adds the Store and the Deliveries as sinks, then the Windows watched by `log`.
struct PinStore;

#[derive(Clone, Default)]
struct PinState {
    revision: Arc<AtomicU64>,
    batches: Log<StoreBatch>,
    /// The Store holds a mark that the saved window of the key owes a reread.
    marked: Arc<std::sync::atomic::AtomicBool>,
}

impl StateOwner for PinStore {
    const EPOCHED: bool = false;
    type State = PinState;
    type Input = StoreInput;
    type Effect = crate::spec::support::NoEffect;

    fn step(pin: &mut Self::State, input: Self::Input, _now: crate::runtime::Time) -> Effects<Self> {
        match input {
            StoreInput::Pin { request_id, .. } => vec![RuntimeEffect::Send(Outbound::new(Address::<Windows>::new(WINDOWS), w::WindowsInput::Pinned { request_id, base_revision: pin.revision.load(Ordering::SeqCst) }))],
            StoreInput::ReadAfter { read_id, .. } => vec![RuntimeEffect::Send(Outbound::new(Address::<Windows>::new(WINDOWS), w::WindowsInput::ReadReady { read_id }))],
            StoreInput::Commit(batch) => {
                let marks = matches!(batch.ops.as_slice(), [StoreOp::MarkStale { .. }]);
                let id = batch.id;
                pin.batches.lock().unwrap_or_else(PoisonError::into_inner).push(batch);
                if !marks {
                    return Vec::new();
                }
                pin.marked.store(true, Ordering::SeqCst);
                vec![RuntimeEffect::Send(Outbound::new(Address::<Windows>::new(WINDOWS), w::WindowsInput::Committed(committed_at(id, i64::try_from(pin.revision.load(Ordering::SeqCst)).unwrap_or(0), &[]))))]
            }
            _ => Vec::new(),
        }
    }

    fn epoch_ended(epoch: u64, cause: Ended) -> Self::Input { StoreWriter::epoch_ended(epoch, cause) }
    fn closing() -> Self::Input { StoreInput::Idle }
}

fn add_windows(det: &mut DetScheduler, log: &Spied, state: w::WindowsState, runner: impl EffectRunner<Spy<Windows>>, pin: PinState) -> Address<Spy<Windows>> {
    det.add::<PinStore>("store", pin, Silent);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Windows>>("windows", SpyState::new(state, log), runner)
}

type RestedMap = Arc<std::sync::Mutex<std::collections::BTreeMap<WindowKey, (w::SavedWindow, crate::owners::contract::WindowState)>>>;

fn rested_sink(map: &RestedMap) -> w::Rested {
    let map = map.clone();
    Arc::new(move |key, rested| {
        let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
        match rested {
            Some(rested) => map.insert(key.clone(), rested),
            None => map.remove(key),
        };
    })
}

type HeldReads = Arc<std::sync::Mutex<Vec<(WindowKey, u64)>>>;

/// The read pool of the tests: the saved window of every key is none, or the read waits in `held` until the test answers it.
struct Reads {
    held: HeldReads,
    hold: Arc<std::sync::atomic::AtomicBool>,
    rested: RestedMap,
    marked: Arc<std::sync::atomic::AtomicBool>,
}

impl Reads {
    fn auto(rested: RestedMap) -> Self { Self { held: HeldReads::default(), hold: Arc::default(), rested, marked: Arc::default() } }
}

impl EffectRunner<Spy<Windows>> for Reads {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Spy<Windows>>) {
        let WindowsEffect::ReadWindow { key, read_id } = effect else { return };
        if self.hold.load(Ordering::SeqCst) {
            self.held.lock().unwrap_or_else(PoisonError::into_inner).push((key, read_id));
        } else {
            let marked = self.marked.load(Ordering::SeqCst);
            let saved = self.rested.lock().unwrap_or_else(PoisonError::into_inner).get(&key).map(|(saved, _)| w::SavedWindow { stale: saved.stale || marked, ..saved.clone() });
            back.reply(stamp, w::WindowsInput::WindowRead { key, read_id, result: Ok(saved) });
        }
    }
}

fn runs(emitted: Vec<Emitted<WindowsEffect, WindowsInput>>) -> Vec<WindowsEffect> { emitted.into_iter().filter_map(|entry| if let Emitted::Run(effect) = entry { Some(effect) } else { None }).filter(|effect| !matches!(effect, WindowsEffect::ReadWindow { .. })).collect() }

fn inner(det: &DetScheduler, address: Address<Spy<Windows>>) -> &w::WindowsState { &det.state(address).expect("the owner state").inner }

fn key() -> WindowKey { WindowKey { model: "Moment".into(), name: "feed".into() } }

fn phase_of(phase: OwnerPhase) -> Phase {
    match phase {
        OwnerPhase::Idle => Phase::Idle,
        OwnerPhase::Loading => Phase::Loading,
        OwnerPhase::Loaded => Phase::Loaded,
        OwnerPhase::Failed => Phase::Failed,
    }
}

fn class_of(failure: Failure) -> ErrorClass {
    match failure {
        Failure::Network => ErrorClass::Network,
        Failure::Server => ErrorClass::Server,
        Failure::Permanent => ErrorClass::Permanent,
        Failure::InvalidCursor => ErrorClass::InvalidCursor,
    }
}

fn result_of(outcome: &WindowOutcome) -> Result3 {
    match outcome {
        WindowOutcome::Loaded { .. } => Result3::Loaded,
        WindowOutcome::Failed { .. } | WindowOutcome::Offline { .. } => Result3::Failed,
        WindowOutcome::PageLimit => Result3::PageLimit,
        WindowOutcome::Ended { .. } => Result3::Interrupted,
    }
}

fn goal_of(goal: Goal) -> w::Goal {
    match goal {
        Goal::First => w::Goal::First,
        Goal::Next => w::Goal::Next,
        Goal::All => w::Goal::All,
    }
}

fn id64(id: u32) -> u64 { if id == PRELOAD { w::PRELOAD } else { u64::from(id) } }

fn cursor_text(cursor: u32) -> String { format!("c{cursor}") }

/// The owner on the deterministic scheduler; `step` feeds one input of the table and returns the effects the owner sent.
struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Windows>>,
    rested: RestedMap,
    /// The Store holds a mark for the saved window: a `Stale` of the Feed or a mark batch of the owner set it, a landing batch clears it.
    marked: Arc<std::sync::atomic::AtomicBool>,
    log: Spied,
    /// The request in flight: its id and attempt, as the owner sent them last.
    flight: Option<(u64, u32)>,
    /// The id of the last batch the owner sent to the Store.
    batch: Option<u64>,
    revision: i64,
    pin_revision: Arc<AtomicU64>,
    stored: Log<StoreBatch>,
    online_seq: u64,
    /// The time of the next step: it only grows, the owner never sees the clock go back.
    now: i64,
    held: HeldReads,
    hold: Arc<std::sync::atomic::AtomicBool>,
    /// The last batch the owner sent to the Store.
    commit: Option<StoreBatch>,
    /// How many of the stored batches `window_batches` gave out.
    given: std::sync::atomic::AtomicUsize,
    /// Every outcome the owner gave to a caller, in order.
    outcomes: Vec<(u32, Result3)>,
    /// The outcomes of the last step.
    last_outcomes: Vec<(u32, Result3)>,
}

impl Twin {
    fn new() -> Self { Self::build(Some(true), u32::MAX, Vec::new(), Vec::new(), false) }

    fn before_online() -> Self { Self::build(None, u32::MAX, Vec::new(), Vec::new(), false) }

    fn with_max_pages(max: u32) -> Self { Self::build(Some(true), max, Vec::new(), Vec::new(), false) }

    fn members_complete() -> Self { Self::build(Some(true), u32::MAX, Vec::new(), Vec::new(), true) }

    fn with_sources() -> Self { Self::build(Some(true), u32::MAX, Vec::new(), vec![key()], false) }

    fn with_preload() -> Self { Self::build(Some(true), u32::MAX, vec![key()], Vec::new(), false) }

    /// A window of a key with `media`: a marker and a tail.
    fn media() -> Self { Self::assemble(Some(true), u32::MAX, (Vec::new(), Vec::new()), (false, true)) }

    fn build(online: Option<bool>, max: u32, preload: Vec<WindowKey>, sources: Vec<WindowKey>, members_complete: bool) -> Self { Self::assemble(online, max, (preload, sources), (members_complete, false)) }

    fn assemble(online: Option<bool>, max: u32, (preload, sources): (Vec<WindowKey>, Vec<WindowKey>), (members_complete, media): (bool, bool)) -> Self {
        let state = w::WindowsState::new(EPOCH, online, peers()).with_local_request_ids().with_preload(preload).with_sources(sources).with_max_pages(Arc::new(move |_| max)).with_members_complete(Arc::new(move |_| members_complete)).with_media(Arc::new(move |_| media)).with_foreground(Arc::new(|_| true)).with_lifetimes(Arc::new(|_| (300_000, 60_000)));
        Self::launch(state)
    }

    /// A twin whose preload keys wait for `RegisterOperations`, as the owner is installed.
    fn awaiting_operations() -> Self {
        let state = w::WindowsState::new(EPOCH, None, peers()).with_local_request_ids().with_preload(vec![key()]).with_operations_awaited().with_lifetimes(Arc::new(|_| (300_000, 60_000)));
        Self::launch(state)
    }

    fn launch(state: w::WindowsState) -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let pin = PinState::default();
        let (pin_revision, stored) = (pin.revision.clone(), pin.batches.clone());
        let rested = RestedMap::default();
        let state = state.with_rested(rested_sink(&rested));
        let mut reads = Reads::auto(rested.clone());
        let marked = pin.marked.clone();
        reads.marked = marked.clone();
        let (held, hold) = (reads.held.clone(), reads.hold.clone());
        let addr = add_windows(&mut det, &log, state, reads, pin);
        det.start_epoch(EPOCH);
        Self { det, addr, rested, marked, log, flight: None, batch: None, revision: 10, pin_revision, stored, online_seq: 0, now: NOW, held, hold, commit: None, given: std::sync::atomic::AtomicUsize::new(0), outcomes: Vec::new(), last_outcomes: Vec::new() }
    }

    /// The batches of the Window owner the Store received since the last call.
    fn window_batches(&self) -> Vec<StoreBatch> {
        let all = snapshot(&self.stored);
        let from = self.given.swap(all.len(), Ordering::SeqCst);
        all[from..].iter().filter(|batch| matches!(batch.author, BatchAuthor::Window)).cloned().collect()
    }

    /// The ops of the last batch the owner sent to the Store.
    fn commit_ops(&self) -> &[StoreOp] { self.commit.as_ref().map_or(&[][..], |batch| batch.ops.as_slice()) }

    fn state(&self) -> &w::WindowsState { inner(&self.det, self.addr) }

    fn stage(&self) -> Option<WindowStage> { self.state().stage(&key()).or_else(|| self.rested.lock().unwrap_or_else(PoisonError::into_inner).contains_key(&key()).then_some(WindowStage::Idle)) }

    /// The Feed marks the saved window of a key a `Stale` names in the Store at once.
    fn mark_rested_stale(&self, in_keys: bool) {
        if in_keys { self.marked.store(true, Ordering::SeqCst); }
    }

    fn run(&mut self) { self.det.run_until_quiet(10_000).expect("the owner is quiet"); }

    fn post(&mut self, input: w::WindowsInput) {
        self.det.post(self.addr, EpochStamp::Epoch(EPOCH), input);
        self.run();
    }

    /// A call of the platform: it carries no epoch stamp, the Lifecycle checked its epoch.
    fn post_call(&mut self, input: w::WindowsInput) {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.run();
    }

    fn ref_request(&self) -> (u64, u32) { self.flight.expect("a request in flight") }

    fn ref_landing(&self) -> u64 { self.batch.expect("a batch in flight") }

    /// Feeds one input of the table to the owner. Returns the effects it sent.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        self.det.tick(crate::runtime::Time(self.now as u64));
        take(&self.log);
        let k = key();
        match &input {
            Input::InterestPlus { id: PRELOAD } => self.post(w::WindowsInput::PreloadKeys { keys: vec![k] }),
            Input::InterestPlus { id } => self.post_call(w::WindowsInput::InterestPlus { key: k, id: id64(*id) }),
            Input::InterestPassive { id } => self.post_call(w::WindowsInput::InterestPassive { key: k, id: id64(*id) }),
            Input::InterestMinus { id } => self.post_call(w::WindowsInput::InterestMinus { key: k, id: id64(*id) }),
            Input::Load { id, goal, force } => self.post_call(w::WindowsInput::Load { key: k, id: id64(*id), goal: goal_of(*goal), force: *force, store_revision: 0 }),
            Input::Response { request, attempt, cursor, rows } => {
                self.marked.store(false, Ordering::SeqCst);
                let page = AnswerPage::new(1, if *rows { vec!["r1".into()] } else { Vec::new() }, cursor.map(cursor_text));
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page });
            }
            Input::TransportError { request, attempt, failure } => {
                self.post(w::WindowsInput::Failure { request: *request, attempt: *attempt, error: RequestError { class: class_of(*failure), failure: crate::ffi::DbError::Network { detail: "e".into() } } });
            }
            Input::MarkerAnswer { request, attempt, marker } => {
                self.marked.store(false, Ordering::SeqCst);
                let page = AnswerPage::marked(w::Marker { sequence: marker.sequence, values: [("revision".to_string(), marker.revision), ("count".to_string(), marker.count)].into() });
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page });
            }
            Input::MoreWithoutCursor { request, attempt } => {
                let mut page = AnswerPage::new(1, Vec::new(), None);
                page.more = true;
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page });
            }
            Input::Dropped { request, attempt } => self.post(w::WindowsInput::Dropped { request: *request, attempt: *attempt }),
            Input::Retry { request } => self.post(w::WindowsInput::RetryDue { request: *request }),
            Input::Committed { request, revision } => self.post(w::WindowsInput::Committed(committed_at(*request, *revision, &[]))),
            Input::Rejected { request, reason } => {
                let reason = match reason {
                    Reject::NoSession => RejectReason::NoSession,
                    Reject::Closed => RejectReason::Closed,
                    Reject::Storage => RejectReason::Storage("disk".into()),
                    Reject::Invalid => RejectReason::Invalid(String::new()),
                };
                self.post(w::WindowsInput::Rejected(Rejected { batch: *request, reason }));
            }
            Input::Stale { in_keys } => {
                let keys = if *in_keys { StaleKeys::all() } else { StaleKeys::Set(BTreeSet::new()) };
                self.mark_rested_stale(*in_keys);
                self.post(w::WindowsInput::Stale { model: "Moment".into(), keys, cause: w::StaleCause::Change { revision: None } });
            }
            Input::StaleGap => {
                self.mark_rested_stale(true);
                self.post(w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::all(), cause: w::StaleCause::Gap { online_seq: self.online_seq } });
            }
            Input::Foreground { seq } => self.post(w::WindowsInput::Foreground { seq: *seq, online_seq: 0 }),
            Input::SourceChanged { revision } => self.post(w::WindowsInput::StoreCommitted { models: vec!["Moment".into()], revision: *revision }),
            Input::Online(on) => {
                self.online_seq += u64::from(*on);
                self.post(w::WindowsInput::Online { online: Online(*on), online_seq: self.online_seq });
            }
            Input::EpochEnded => {
                self.det.end_epoch(EPOCH, Ended::SessionReset);
                self.run();
            }
            Input::Closing => {
                self.det.close();
                self.run();
            }
            Input::PowerSaving(on) => self.post(w::WindowsInput::PowerSaving { saving: *on }),
            Input::OfflineRefused => self.post_call(w::WindowsInput::OfflineRefused { key: k, message: "no network".into() }),
            Input::Read(read) => {
                let (key, read_id) = {
                    let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
                    if *read == Read::Stray { held.last().cloned() } else { held.pop() }
                }.expect("a read waits");
                let saved = |landed_at: i64, fresh_until: i64, stale: bool, generation: u64| w::SavedWindow { landed_at, fresh_until, stale, owed: false, cursor: None, pages: 1, has_rows: true, generation, marker: None, members: 0 };
                let (read_id, result) = match read {
                    Read::Saved { landed_at, fresh_until, stale, generation_ok } => (read_id, Ok(Some(saved(*landed_at, *fresh_until, *stale, if *generation_ok { 0 } else { 7 })))),
                    Read::Media { marker, members } => (read_id, Ok(Some(w::SavedWindow { marker: Some(w::Marker { sequence: marker.sequence, values: [("revision".to_string(), marker.revision), ("count".to_string(), marker.count)].into() }), members: *members, ..saved(NOW - 1_000, NOW - 1_000, false, 0) }))),
                    Read::MediaMarked { marker, members } => (read_id, Ok(Some(w::SavedWindow { marker: Some(w::Marker { sequence: marker.sequence, values: [("revision".to_string(), marker.revision), ("count".to_string(), marker.count)].into() }), members: *members, ..saved(NOW - 1_000, NOW - 1_000, true, 0) }))),
                    Read::None => (read_id, Ok(None)),
                    Read::Failed => (read_id, Err("disk".to_string())),
                    Read::Stray => (read_id + 50, Ok(None)),
                };
                self.post(w::WindowsInput::WindowRead { key, read_id, result });
            }
        }
        self.capture()
    }

    /// Moves the clock by `ms`, fires what is due and returns the effects it sent like `step`.
    fn advance(&mut self, ms: i64) -> Vec<Effect> {
        take(&self.log);
        self.now += ms;
        self.det.tick(crate::runtime::Time(self.now as u64));
        self.run();
        self.capture()
    }

    fn capture(&mut self) -> Vec<Effect> {
        let emitted = take(&self.log);
        if let Some(batch) = sent_batches(&emitted, &self.stored).into_iter().rev().find(|batch| batch.author == BatchAuthor::Window && !marks_only(batch)) { self.commit = Some(batch); }
        let (got, outcomes) = self.split(emitted);
        for effect in &got {
            match effect {
                Effect::Request { request, attempt, .. } | Effect::Marker { request, attempt, .. } | Effect::Tail { request, attempt, .. } => self.flight = Some((*request, *attempt)),
                Effect::Commit { request, .. } => self.batch = Some(*request),
                _ => {}
            }
        }
        self.outcomes.extend(outcomes.iter().copied());
        self.last_outcomes = outcomes;
        let held = self.hold.load(Ordering::SeqCst);
        got.into_iter().filter(|effect| held || *effect != Effect::ReadWindow).collect()
    }

    /// One `WindowState` effect stands for the sends to every stream of the interest.
    fn split(&self, emitted: Vec<Emitted<WindowsEffect, WindowsInput>>) -> (Vec<Effect>, Vec<(u32, Result3)>) {
        let (mut modelled, mut outcomes) = (Vec::new(), Vec::new());
        let mut batches = sent_batches(&emitted, &self.stored).into_iter();
        for entry in emitted {
            match entry {
                Emitted::Run(WindowsEffect::Window { effect, .. }) => match effect {
                    WindowEffect::Request { request, attempt, kind: w::RequestKind::Marker { up_to }, .. } => modelled.push(Effect::Marker { request, attempt, up_to }),
                    WindowEffect::Request { request, attempt, kind: w::RequestKind::Tail { after }, .. } => modelled.push(Effect::Tail { request, attempt, after }),
                    WindowEffect::Request { request, attempt, page, .. } => modelled.push(Effect::Request { request, attempt, page }),
                    WindowEffect::Cancel { request, attempt } => modelled.push(Effect::Cancel { request, attempt }),
                    WindowEffect::DerivePreload { revision } => modelled.push(Effect::DerivePreload { revision }),
                    WindowEffect::Outcome { id, outcome } => outcomes.push((u32::try_from(id).expect("a test id"), result_of(&outcome))),
                },
                Emitted::Run(WindowsEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(WindowsEffect::SnapshotRequest { .. }) => {}
                Emitted::Run(WindowsEffect::ReadWindow { .. }) => modelled.push(Effect::ReadWindow),
                Emitted::Send { to: STORE, payload, .. } if payload.is::<crate::owners::store::StoreInput>(|input| matches!(input, crate::owners::store::StoreInput::Commit(_))) => {
                    let batch = batches.next().expect("a batch for every Commit");
                    if batch.author == BatchAuthor::Window && !marks_only(&batch) { modelled.push(Effect::Commit { request: batch.id, replace_all: batch.ops.iter().any(|op| matches!(op, StoreOp::Window { replace_all: true, .. })) }); }
                }
                Emitted::Send { to: DELIVERIES, .. } if modelled.last() == Some(&Effect::WindowState) => {}
                Emitted::Send { to: DELIVERIES, .. } => modelled.push(Effect::WindowState),
                Emitted::Schedule { id, input: WindowsInput::RetryDue { .. }, .. } => modelled.push(Effect::ScheduleRetry { request: id.0 }),
                Emitted::Send { .. } | Emitted::Schedule { .. } | Emitted::Cancel(_) | Emitted::Broadcast(_) => {}
            }
        }
        (modelled, outcomes)
    }

    fn load_state(&self) -> crate::owners::contract::WindowState { self.state().load_state(&key()).or_else(|| self.rested.lock().unwrap_or_else(PoisonError::into_inner).get(&key()).map(|(_, state)| state.clone())).expect("the window exists") }

    fn phase(&self) -> Phase { phase_of(self.load_state().phase) }

    /// The answer of the request in flight: a page with a row and the cursor of the next page.
    fn respond(&mut self, cursor: Option<u32>) -> Vec<Effect> {
        let (request, attempt) = self.ref_request();
        self.step(Input::Response { request, attempt, cursor, rows: true })
    }

    fn land(&mut self) -> Vec<Effect> {
        let request = self.ref_landing();
        self.revision += 1;
        let revision = self.revision;
        self.step(Input::Committed { request, revision })
    }

    /// The request in flight answers and lands: the next page is `cursor`.
    fn finish(&mut self, cursor: Option<u32>) -> Vec<Effect> {
        self.respond(cursor);
        self.land()
    }

    fn fail(&mut self, failure: Failure) -> Vec<Effect> {
        let (request, attempt) = self.ref_request();
        self.step(Input::TransportError { request, attempt, failure })
    }

    fn call(&mut self, id: u32, goal: Goal) -> Vec<Effect> { self.step(Input::Load { id, goal, force: false }) }

    fn watch(&mut self) -> Vec<Effect> { self.step(Input::InterestPlus { id: STREAM }) }
}

fn marks_only(batch: &StoreBatch) -> bool { matches!(batch.ops.as_slice(), [StoreOp::MarkStale { .. }]) }

fn none() -> Vec<Effect> { Vec::new() }

fn requests(effects: &[Effect]) -> usize { effects.iter().filter(|effect| matches!(effect, Effect::Request { .. })).count() }

/// `Idle{fresh}` with the first page landed and no cursor, no interest, no waiters.
fn fresh() -> Twin {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.finish(None);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    twin
}

/// The same with a cursor to a second page.
fn fresh_with_cursor() -> Twin {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.finish(Some(2));
    twin
}

/// `Idle{fresh}` watched by a stream.
fn fresh_watched() -> Twin {
    let mut twin = Twin::new();
    twin.watch();
    twin.finish(None);
    twin
}

fn loading() -> Twin {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin
}

fn loading_watched() -> Twin {
    let mut twin = Twin::new();
    twin.watch();
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin
}

fn landing() -> Twin {
    let mut twin = loading();
    twin.respond(None);
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
    twin
}

fn waiting() -> Twin {
    let mut twin = Twin::before_online();
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    twin
}

/// `Idle{not fresh}` after a permanent error, watched by a stream.
fn failed_watched() -> Twin {
    let mut twin = loading_watched();
    twin.fail(Failure::Permanent);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    twin
}

// Dictionary rows.

mod window_gap_cells_tests;
mod window_gap_loading_tests;
mod window_gap_landing_tests;
use window_gap_landing_tests::*;
mod window_gap_property_tests;
mod window_fresh_tests;
mod window_phase_tests;
mod window_saving_tests;
mod window_pages_tests;
mod window_media_tests;
mod window_orders_tests;
mod window_orders_more_tests;
mod window_stale_revision_tests;
