//! Owner-level tests of the `window` machine: the owner `Windows` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_window.rs`) and to the owner through its address, then compares the effects,
//! the outcomes of the callers, the stage and the phase. Names are the `Test` column, `_owner` marks the owner twin of a test that also runs on the API.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

const NOW: i64 = 1_000_000;

use super::ref_window::Phase;
use super::ref_window::{Effect, Failure, Goal, Input, Marker3, Read, Reject, Result3, State, Window, PRELOAD};
use crate::owners::contract::{Committed, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError};
use crate::owners::contract::WindowPhase as OwnerPhase;
use crate::spec::support::{new_log, number_after, snapshot, take, Discard, Emitted, Landing, Log, Reset, ResetState, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreGeneration, StoreStub, StoreStubState};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::store::{RowValue, StoreInput, StoreWriter};
use crate::owners::read::windows::{self as w, AnswerPage, StaleKeys, WindowEffect, WindowKey, WindowKind, WindowOutcome, WindowStage, Windows, WindowsEffect, WindowsPeers};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, Effect as RuntimeEffect, EffectRunner, Effects, EpochStamp, Limits, Outbound, StateOwner, Time};

const EPOCH: u64 = 1;
/// Slots of the peers and of the owner: the Store, the Deliveries, then the Windows.
const STORE: usize = 0;
const DELIVERIES: usize = 1;
const WINDOWS: usize = 2;
/// The id of the stream that watches the window in the tests: a call gets a smaller id.
const STREAM: u32 = 100;

type Spied = Log<Emitted<WindowsEffect>>;

fn peers() -> WindowsPeers { WindowsPeers { store: Address::<StoreWriter>::new(STORE), deliveries: Address::<Deliveries>::new(DELIVERIES) } }

/// Adds the Store and the Deliveries as sinks, then the Windows watched by `log`.
struct PinStore;

impl StateOwner for PinStore {
    const EPOCHED: bool = false;
    type State = Arc<AtomicU64>;
    type Input = StoreInput;
    type Effect = crate::spec::support::NoEffect;

    fn step(revision: &mut Self::State, input: Self::Input) -> Effects<Self> {
        match input {
            StoreInput::Pin { request_id, .. } => vec![RuntimeEffect::Send(Outbound::new(Address::<Windows>::new(WINDOWS), w::WindowsInput::Pinned { request_id, base_revision: revision.load(Ordering::SeqCst) }))],
            _ => Vec::new(),
        }
    }

    fn tick(_: Time) -> Self::Input { StoreInput::Idle }
    fn epoch_ended(epoch: u64, cause: Ended) -> Self::Input { StoreWriter::epoch_ended(epoch, cause) }
    fn closing() -> Self::Input { StoreInput::Idle }
}

fn add_windows(det: &mut DetScheduler, log: &Spied, state: w::WindowsState, runner: impl EffectRunner<Spy<Windows>>, revision: Arc<AtomicU64>) -> Address<Spy<Windows>> {
    det.add::<PinStore>("store", revision, Silent);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Windows>>("windows", SpyState::new(state, log), runner)
}

type HeldReads = Arc<std::sync::Mutex<Vec<(WindowKey, u64)>>>;

/// The read pool of the tests: the saved window of every key is none, or the read waits in `held` until the test answers it.
struct Reads {
    held: HeldReads,
    hold: Arc<std::sync::atomic::AtomicBool>,
}

impl Reads {
    fn auto() -> Self { Self { held: HeldReads::default(), hold: Arc::default() } }
}

impl EffectRunner<Spy<Windows>> for Reads {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Spy<Windows>>) {
        let WindowsEffect::ReadWindow { key, read_id } = effect else { return };
        if self.hold.load(Ordering::SeqCst) {
            self.held.lock().unwrap_or_else(PoisonError::into_inner).push((key, read_id));
        } else {
            back.reply(stamp, w::WindowsInput::WindowRead { key, read_id, result: Ok(None) });
        }
    }
}

fn runs(emitted: Vec<Emitted<WindowsEffect>>) -> Vec<WindowsEffect> { emitted.into_iter().filter_map(|entry| if let Emitted::Run(effect) = entry { Some(effect) } else { None }).filter(|effect| !matches!(effect, WindowsEffect::ReadWindow { .. })).collect() }

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
        WindowOutcome::Failed { .. } => Result3::Failed,
        WindowOutcome::PageLimit => Result3::PageLimit,
        WindowOutcome::Interrupted => Result3::Interrupted,
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

fn stage_of(state: &State) -> WindowStage {
    match state {
        State::Opening => WindowStage::Opening,
        State::Idle => WindowStage::Idle,
        State::Loading { .. } => WindowStage::Loading,
        State::Landing { .. } => WindowStage::Landing,
        State::Waiting { .. } => WindowStage::Waiting,
        State::Ended => WindowStage::Ended,
    }
}

/// The owner and the reference side by side.
struct Twin {
    det: DetScheduler,
    addr: Address<Spy<Windows>>,
    log: Spied,
    reference: Window,
    revision: i64,
    generation: u64,
    pin_revision: Arc<AtomicU64>,
    online_seq: u64,
    /// The `now` the inputs carry.
    now: i64,
    held: HeldReads,
    hold: Arc<std::sync::atomic::AtomicBool>,
    /// The text of the last batch the owner sent to the Store.
    commit: String,
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
        let state = w::WindowsState::new(EPOCH, online, peers()).with_preload(preload).with_sources(sources).with_max_pages(Arc::new(move |_| max)).with_members_complete(Arc::new(move |_| members_complete)).with_media(Arc::new(move |_| media)).with_foreground(Arc::new(|_| true)).with_lifetimes(Arc::new(|_| (300_000, 60_000)));
        Self::launch(state, online, max, (members_complete, media))
    }

    /// A twin whose preload keys wait for `RegisterOperations`, as the owner is installed.
    fn awaiting_operations() -> Self {
        let state = w::WindowsState::new(EPOCH, None, peers()).with_preload(vec![key()]).with_operations_awaited().with_lifetimes(Arc::new(|_| (300_000, 60_000)));
        Self::launch(state, None, u32::MAX, (false, false))
    }

    fn launch(state: w::WindowsState, online: Option<bool>, max: u32, (members_complete, media): (bool, bool)) -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let pin_revision = Arc::new(AtomicU64::new(0));
        let reads = Reads::auto();
        let (held, hold) = (reads.held.clone(), reads.hold.clone());
        let addr = add_windows(&mut det, &log, state, reads, pin_revision.clone());
        det.start_epoch(EPOCH);
        let mut reference = if online.is_some() { Window::new() } else { Window::before_online() };
        reference.online = online;
        reference.max_pages = max;
        reference.members_complete = members_complete;
        reference.media = media;
        Self { det, addr, log, reference, revision: 10, generation: 0, pin_revision, online_seq: 0, now: NOW, held, hold, commit: String::new() }
    }

    fn state(&self) -> &w::WindowsState { inner(&self.det, self.addr) }

    fn stage(&self) -> Option<WindowStage> { self.state().stage(&key()) }

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

    fn ref_request(&self) -> (u64, u32) {
        match self.reference.state {
            State::Loading { request, attempt, .. } => (request, attempt),
            other => panic!("no request in flight in {other:?}"),
        }
    }

    fn ref_landing(&self) -> u64 {
        match self.reference.state {
            State::Landing { request, .. } => request,
            other => panic!("no landing in {other:?}"),
        }
    }

    /// Feeds one input of the table to both sides. Returns the effects of the reference after the check against the owner.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        let k = key();
        match &input {
            Input::InterestPlus { id: PRELOAD } => self.post(w::WindowsInput::PreloadKeys { keys: vec![k], now: self.now }),
            Input::InterestPlus { id } => self.post_call(w::WindowsInput::InterestPlus { key: k, id: id64(*id), now: self.now }),
            Input::InterestPassive { id } => self.post_call(w::WindowsInput::InterestPassive { key: k, id: id64(*id) }),
            Input::InterestMinus { id } => self.post_call(w::WindowsInput::InterestMinus { key: k, id: id64(*id) }),
            Input::Load { id, goal, force } => self.post_call(w::WindowsInput::Load { key: k, id: id64(*id), goal: goal_of(*goal), force: *force, store_revision: 0, now: self.now }),
            Input::Response { request, attempt, cursor, rows } => {
                let page = AnswerPage::new(1, if *rows { vec!["r1".into()] } else { Vec::new() }, cursor.map(cursor_text));
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page, now: self.now });
            }
            Input::TransportError { request, attempt, failure } => {
                self.post(w::WindowsInput::Failure { request: *request, attempt: *attempt, error: RequestError { message: "e".to_string(), class: class_of(*failure), source: None } });
            }
            Input::MarkerAnswer { request, attempt, marker } => {
                let page = AnswerPage::marked(w::Marker { sequence: marker.sequence, values: [("revision".to_string(), marker.revision), ("count".to_string(), marker.count)].into() });
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page, now: self.now });
            }
            Input::MoreWithoutCursor { request, attempt } => {
                let mut page = AnswerPage::new(1, Vec::new(), None);
                page.more = true;
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page, now: self.now });
            }
            Input::Dropped { request, attempt } => self.post(w::WindowsInput::Dropped { request: *request, attempt: *attempt }),
            Input::Retry { request } => self.post(w::WindowsInput::RetryDue { request: *request }),
            Input::Committed { request, revision } => self.post(w::WindowsInput::Committed(Committed { batch: *request, revision: *revision, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0 })),
            Input::Rejected { request, reason } => {
                let reason = match reason {
                    Reject::StaleGeneration => RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: self.generation },
                    Reject::NoSession => RejectReason::NoSession,
                    Reject::Closed => RejectReason::Closed,
                    Reject::Storage => RejectReason::Storage("disk".into()),
                    Reject::Invalid => RejectReason::Storage("invalid".into()),
                };
                self.post(w::WindowsInput::Rejected(Rejected { batch: *request, reason }));
            }
            Input::Stale { in_keys } => {
                let keys = if *in_keys { StaleKeys::All } else { StaleKeys::Set(BTreeSet::new()) };
                self.post(w::WindowsInput::Stale { model: "Moment".into(), keys, cause: w::StaleCause::Change });
            }
            Input::StaleGap => self.post(w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: w::StaleCause::Gap { online_seq: self.online_seq } }),
            Input::Foreground { seq } => self.post(w::WindowsInput::Foreground(*seq)),
            Input::SourceChanged { revision } => self.post(w::WindowsInput::StoreCommitted { models: vec!["Moment".into()], revision: *revision }),
            Input::Online(on) => {
                self.online_seq += u64::from(*on);
                self.post(w::WindowsInput::Online { online: Online(*on), online_seq: self.online_seq, now: self.now });
            }
            Input::EpochEnded => {
                self.det.end_epoch(EPOCH, Ended::SessionReset);
                self.run();
            }
            Input::ModelsReset { own_model } => {
                self.generation += 1;
                let model = if *own_model { "Moment" } else { "Chat" };
                self.post(w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: self.generation, models: vec![model.into()] }));
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
                let saved = |landed_at: i64, fresh_until: i64, stale: bool, generation: u64| w::SavedWindow { landed_at, fresh_until, stale, cursor: None, pages: 1, has_rows: true, generation, marker: None, members: 0 };
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
        let emitted = take(&self.log);
        if let Some(Emitted::Send { input, .. }) = emitted.iter().rev().find(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.contains("author: Window"))) { self.commit.clone_from(input); }
        let pin_sent = emitted.iter().any(|entry| matches!(entry, Emitted::Send { to: STORE, input } if input.starts_with("Pin {")));
        self.reference.now = self.now;
        let mut want = self.reference.step(input);
        if !self.hold.load(Ordering::SeqCst) && self.reference.state == State::Opening {
            want.extend(self.reference.step(Input::Read(Read::None)));
        }
        want.dedup_by(|next, last| *next == Effect::WindowState && *last == Effect::WindowState);
        let (got, outcomes) = self.split(emitted);
        let watched = self.reference.interest.iter().any(|id| *id != PRELOAD);
        let expected: Vec<Effect> = want.iter().filter(|effect| watched || **effect != Effect::WindowState).cloned().collect();
        let mut ordered = expected.clone();
        if pin_sent {
            if let Some(index) = ordered.iter().position(|effect| matches!(effect, Effect::Request { .. } | Effect::Marker { .. } | Effect::Tail { .. })) {
                let request = ordered.remove(index);
                ordered.push(request);
            }
        }
        assert_eq!(got, ordered, "the owner and the reference give the same effects");
        let mut want_outcomes = self.reference.take_outcomes();
        let mut got_outcomes = outcomes;
        want_outcomes.sort_by_key(|(id, _)| *id);
        got_outcomes.sort_by_key(|(id, _)| *id);
        assert_eq!(got_outcomes, want_outcomes, "the owner and the reference answer the same callers");
        self.same_state();
        let held = self.hold.load(Ordering::SeqCst);
        expected.into_iter().filter(|effect| held || *effect != Effect::ReadWindow).collect()
    }

    fn same_state(&self) {
        let Some(stage) = self.stage() else { return };
        assert_eq!(stage, stage_of(&self.reference.state), "the stage");
        if stage != WindowStage::Ended {
            assert_eq!(self.phase(), self.reference.phase, "the phase");
        }
        let interest: Vec<u64> = self.reference.interest.iter().map(|id| id64(*id)).collect();
        let mut owner_interest = self.state().interest(&key());
        owner_interest.sort_unstable();
        let mut want_interest = interest;
        want_interest.sort_unstable();
        assert_eq!(owner_interest, want_interest, "the interest");
        let waiters: Vec<u64> = self.reference.waiters.keys().map(|id| id64(*id)).collect();
        assert_eq!(self.state().waiters(&key()), waiters, "the waiters");
        if stage != WindowStage::Ended {
            let state = self.load_state();
            assert_eq!((state.has_more, state.settled), (self.reference.has_more(), self.reference.settled), "has_more and settled");
        }
    }

    /// The reference models `WindowState` as one effect; the owner sends it to every stream of the interest, so a window with no stream sends none.
    fn split(&self, emitted: Vec<Emitted<WindowsEffect>>) -> (Vec<Effect>, Vec<(u32, Result3)>) {
        let (mut modelled, mut outcomes) = (Vec::new(), Vec::new());
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
                Emitted::Send { to: STORE, input } if input.contains("author: Window") => modelled.push(Effect::Commit { request: number_after(&input, "id: ").expect("the batch id"), replace_all: input.contains("replace_all: true") }),
                Emitted::Send { to: DELIVERIES, .. } if modelled.last() == Some(&Effect::WindowState) => {}
                Emitted::Send { to: DELIVERIES, .. } => modelled.push(Effect::WindowState),
                Emitted::Schedule { id, input, .. } if input.starts_with("RetryDue") => modelled.push(Effect::ScheduleRetry { request: id.0 }),
                Emitted::Send { .. } | Emitted::Schedule { .. } | Emitted::Cancel(_) | Emitted::Broadcast(_) => {}
            }
        }
        (modelled, outcomes)
    }

    fn load_state(&self) -> crate::owners::contract::WindowState { self.state().load_state(&key()).expect("the window exists") }

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
mod window_saving_tests;
mod window_pages_tests;
mod window_media_tests;
