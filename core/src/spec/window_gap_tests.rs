//! Owner-level tests of the `window` machine: the owner `Windows` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_window.rs`) and to the owner through its address, then compares the effects,
//! the outcomes of the callers, the stage and the phase. Names are the `Test` column, `_owner` marks the owner twin of a test that also runs on the API.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

use super::harness::Phase;
use super::ref_window::{Effect, Failure, Goal, Input, Reject, Result3, State, Window, PRELOAD};
use crate::owners::contract::{Committed, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError};
use crate::owners::contract::WindowPhase as OwnerPhase;
use crate::owners::support::{new_log, number_after, snapshot, take, Discard, Emitted, Landing, Log, Reset, ResetState, Silent, Sink, Source, SourceInput, SourceState, Spy, SpyState, StoreGeneration, StoreStub, StoreStubState};
use crate::owners::read::deliveries::Deliveries;
use crate::owners::store::{RowValue, StoreWriter};
use crate::owners::read::windows::{self as w, AnswerPage, StaleKeys, WindowEffect, WindowKey, WindowKind, WindowOutcome, WindowStage, Windows, WindowsEffect, WindowsPeers};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, EffectRunner, EpochStamp, Limits};

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
fn add_windows(det: &mut DetScheduler, log: &Spied, state: w::WindowsState, runner: impl EffectRunner<Spy<Windows>>) -> Address<Spy<Windows>> {
    det.add::<Sink<StoreWriter>>("store", new_log(), Discard);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    det.add::<Spy<Windows>>("windows", SpyState::new(state, log), runner)
}

fn runs(emitted: Vec<Emitted<WindowsEffect>>) -> Vec<WindowsEffect> { emitted.into_iter().filter_map(|entry| if let Emitted::Run(effect) = entry { Some(effect) } else { None }).collect() }

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
}

impl Twin {
    fn new() -> Self { Self::build(Some(true), u32::MAX, Vec::new(), Vec::new()) }

    fn before_online() -> Self { Self::build(None, u32::MAX, Vec::new(), Vec::new()) }

    fn with_max_pages(max: u32) -> Self { Self::build(Some(true), max, Vec::new(), Vec::new()) }

    fn with_sources() -> Self { Self::build(Some(true), u32::MAX, Vec::new(), vec![key()]) }

    fn with_preload() -> Self { Self::build(Some(true), u32::MAX, vec![key()], Vec::new()) }

    fn build(online: Option<bool>, max: u32, preload: Vec<WindowKey>, sources: Vec<WindowKey>) -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let state = w::WindowsState::new(EPOCH, online, peers()).with_preload(preload).with_sources(sources).with_max_pages(Arc::new(move |_| max)).with_foreground(Arc::new(|_| true));
        let addr = add_windows(&mut det, &log, state, Discard);
        det.start_epoch(EPOCH);
        let mut reference = if online.is_some() { Window::new() } else { Window::before_online() };
        reference.online = online;
        reference.max_pages = max;
        Self { det, addr, log, reference, revision: 10, generation: 0 }
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
            Input::InterestPlus { id } => self.post_call(w::WindowsInput::InterestPlus { key: k, id: id64(*id) }),
            Input::InterestPassive { id } => self.post_call(w::WindowsInput::InterestPassive { key: k, id: id64(*id) }),
            Input::InterestMinus { id } => self.post_call(w::WindowsInput::InterestMinus { key: k, id: id64(*id) }),
            Input::Load { id, goal, force } => self.post_call(w::WindowsInput::Load { key: k, id: id64(*id), goal: goal_of(*goal), force: *force, store_revision: 0 }),
            Input::Response { request, attempt, cursor, rows } => {
                let page = AnswerPage::new(1, if *rows { vec!["r1".into()] } else { Vec::new() }, cursor.map(cursor_text));
                self.post(w::WindowsInput::Response { request: *request, attempt: *attempt, page });
            }
            Input::TransportError { request, attempt, failure } => {
                self.post(w::WindowsInput::Failure { request: *request, attempt: *attempt, error: RequestError { message: "e".to_string(), class: class_of(*failure), source: None } });
            }
            Input::Dropped { request, attempt } => self.post(w::WindowsInput::Dropped { request: *request, attempt: *attempt }),
            Input::Retry { request } => self.post(w::WindowsInput::RetryDue { request: *request }),
            Input::Committed { request, revision } => self.post(w::WindowsInput::Committed(Committed { batch: *request, revision: *revision, tables: Vec::new(), models: Default::default() })),
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
                self.post(w::WindowsInput::Stale { model: "Moment".into(), keys });
            }
            Input::Foreground { seq } => self.post(w::WindowsInput::Foreground(*seq)),
            Input::SourceChanged { revision } => self.post(w::WindowsInput::StoreCommitted { models: vec!["Moment".into()], revision: *revision }),
            Input::Online(on) => self.post(w::WindowsInput::Online(Online(*on))),
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
        }
        let emitted = take(&self.log);
        let want = self.reference.step(input);
        let (got, outcomes) = self.split(emitted);
        let watched = self.reference.interest.iter().any(|id| *id != PRELOAD);
        let expected: Vec<Effect> = want.iter().filter(|effect| watched || **effect != Effect::WindowState).cloned().collect();
        assert_eq!(got, expected, "the owner and the reference give the same effects");
        let mut want_outcomes = self.reference.take_outcomes();
        let mut got_outcomes = outcomes;
        want_outcomes.sort_by_key(|(id, _)| *id);
        got_outcomes.sort_by_key(|(id, _)| *id);
        assert_eq!(got_outcomes, want_outcomes, "the owner and the reference answer the same callers");
        self.same_state();
        expected
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
    }

    /// The reference models `WindowState` as one effect; the owner sends it to every stream of the interest, so a window with no stream sends none.
    fn split(&self, emitted: Vec<Emitted<WindowsEffect>>) -> (Vec<Effect>, Vec<(u32, Result3)>) {
        let (mut modelled, mut outcomes) = (Vec::new(), Vec::new());
        for entry in emitted {
            match entry {
                Emitted::Run(WindowsEffect::Window { effect, .. }) => match effect {
                    WindowEffect::Request { request, attempt, page, .. } => modelled.push(Effect::Request { request, attempt, page }),
                    WindowEffect::Cancel { request, attempt } => modelled.push(Effect::Cancel { request, attempt }),
                    WindowEffect::DerivePreload { revision } => modelled.push(Effect::DerivePreload { revision }),
                    WindowEffect::Outcome { id, outcome } => outcomes.push((u32::try_from(id).expect("a test id"), result_of(&outcome))),
                },
                Emitted::Run(WindowsEffect::Drained(_)) => modelled.push(Effect::Drained),
                Emitted::Run(WindowsEffect::SnapshotRequest { .. }) => {}
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

#[test]
fn win_preload_created() {
    let mut twin = Twin::with_preload();
    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(1), generation: 0 }));
    twin.post_call(w::WindowsInput::Online(Online(true)));
    let effects = twin.reference.step(Input::InterestPlus { id: PRELOAD });
    let (got, _) = twin.split(take(&twin.log));
    assert_eq!(got, effects.iter().filter(|effect| **effect != Effect::WindowState).cloned().collect::<Vec<_>>(), "the preload key loads without a stream to tell");
    assert_eq!(twin.state().interest(&key()), [w::PRELOAD]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_preload_derived() {
    let mut twin = Twin::new();
    twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()] });
    let effects = twin.reference.step(Input::InterestPlus { id: PRELOAD });
    let (got, _) = twin.split(take(&twin.log));
    assert_eq!(got, effects.iter().filter(|effect| **effect != Effect::WindowState).cloned().collect::<Vec<_>>());
    assert_eq!(twin.state().interest(&key()), [w::PRELOAD]);
    twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()] });
    assert!(take(&twin.log).is_empty(), "a key that has a machine is not created again");
}

#[test]
fn win_preload_source_committed() {
    for stage in [WindowStage::Loading, WindowStage::Landing, WindowStage::Waiting, WindowStage::Idle] {
        let mut twin = build_sources(Some(stage));
        assert_eq!(twin.step(Input::SourceChanged { revision: 7 }), [Effect::DerivePreload { revision: 7 }]);
        assert_eq!(twin.stage(), Some(stage), "the state does not change");
    }
}

/// A twin whose key is a source of a preload declaration, in the stage `stage`.
fn build_sources(stage: Option<WindowStage>) -> Twin {
    let mut twin = Twin::with_sources();
    match stage {
        Some(WindowStage::Waiting) => {
            twin.step(Input::Online(false));
            twin.call(1, Goal::First);
        }
        Some(WindowStage::Loading) => {
            twin.call(1, Goal::First);
        }
        Some(WindowStage::Landing) => {
            twin.call(1, Goal::First);
            twin.respond(None);
        }
        _ => {
            twin.call(1, Goal::First);
            twin.finish(None);
        }
    }
    assert_eq!(twin.stage(), stage);
    twin
}

#[test]
fn win_nodes_empty() {
    let mut twin = Twin::new();
    let nodes = WindowKey::nodes("Moment", &[]);
    twin.post_call(w::WindowsInput::Load { key: nodes, id: 1, goal: w::Goal::First, force: false, store_revision: 42 });
    let (_, outcomes) = twin.split(take(&twin.log));
    assert_eq!(outcomes, [(1, Result3::Loaded)]);
    assert_eq!(twin.state().windows(), 0, "no machine for a key without ids");
    assert_eq!(Window::empty_nodes_outcome(1), (1, Result3::Loaded));
}

#[test]
fn win_created() {
    let mut twin = Twin::new();
    assert_eq!(twin.state().windows(), 0);
    assert_eq!(twin.watch(), [Effect::Request { request: 1, attempt: 1, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.state().windows(), 1);
    let mut called = Twin::new();
    assert_eq!(called.call(1, Goal::First), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
    assert_eq!(called.state().windows(), 1);
}

// Idle.

#[test]
fn win_idle_interest() {
    let mut twin = Twin::new();
    assert_eq!(twin.watch(), [Effect::Request { request: 1, attempt: 1, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
    let mut offline = Twin::before_online();
    assert_eq!(offline.watch(), [Effect::WindowState], "before the first Online the window waits");
    assert_eq!(offline.stage(), Some(WindowStage::Waiting));
    let mut known = fresh();
    assert_eq!(known.watch(), [Effect::WindowState], "a fresh window only tells its phase");
    assert_eq!(known.stage(), Some(WindowStage::Idle));
    assert_eq!(known.phase(), Phase::Loaded);
}

#[test]
fn win_idle_interest_passive() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::InterestPassive { id: STREAM }), [Effect::WindowState], "the phase goes to the stream and no request starts");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
}

#[test]
fn win_idle_interest_fresh() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::First).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty(), "the call was answered at once");
}

#[test]
fn win_idle_interest_all_done() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_interest_all_more() {
    let mut twin = fresh_with_cursor();
    assert_eq!(twin.call(2, Goal::All), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.state().waiters(&key()), [2]);
}

#[test]
fn win_idle_interest_all_offline() {
    let mut twin = fresh_with_cursor();
    twin.step(Input::Online(false));
    assert!(twin.call(2, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert!(matches!(twin.reference.state, State::Waiting { force: false, page: 2 }));
}

#[test]
fn win_idle_interest_load() {
    let mut twin = Twin::new();
    assert_eq!(twin.call(1, Goal::First), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
    assert_eq!(twin.state().waiters(&key()), [1]);
    let mut forced = fresh();
    assert_eq!(forced.step(Input::Load { id: 2, goal: Goal::First, force: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }], "force loads a fresh window");
}

#[test]
fn win_idle_interest_offline() {
    let mut twin = Twin::before_online();
    assert!(twin.call(1, Goal::First).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    let mut off = Twin::new();
    off.step(Input::Online(false));
    off.call(1, Goal::First);
    assert_eq!(off.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_interest_minus() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::InterestMinus { id: STREAM }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().interest(&key()).is_empty());
}

#[test]
fn win_idle_load_more() {
    let mut twin = fresh_with_cursor();
    assert_eq!(twin.call(2, Goal::Next), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.phase(), Phase::Loading);
    twin.finish(None);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_load_more_offline() {
    let mut twin = fresh_with_cursor();
    twin.step(Input::Online(false));
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert!(matches!(twin.reference.state, State::Waiting { page: 2, .. }));
}

#[test]
fn win_idle_load_more_end() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::First);
    twin.finish(Some(2));
    assert!(twin.call(2, Goal::Next).is_empty());
    assert!(twin.call(3, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_stale_watched() {
    let mut twin = fresh_watched();
    assert_eq!(twin.step(Input::Stale { in_keys: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.phase(), Phase::Loading);
}

#[test]
fn win_idle_stale_watched_offline() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_stale_unwatched() {
    let mut twin = fresh();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
    assert!(twin.call(2, Goal::First).len() == 1, "the window is not fresh any more");
}

#[test]
fn win_idle_stale_other_key() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::Stale { in_keys: false }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.reference.fresh);
}

/// win_foreground_reload (INV-API-07): a window with a reader that is idle and online loads its first page again at every return to the front.
#[test]
fn win_foreground_reload() {
    let mut twin = Twin::new();
    twin.step(Input::InterestPlus { id: 1 });
    twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
    twin.step(Input::Committed { request: 1, revision: 5 });
    assert_eq!(twin.step(Input::Foreground { seq: 1 }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
}

/// win_foreground_other: no reader, the same sequence number, a window that is not idle and no network give no request.
#[test]
fn win_foreground_other() {
    let mut unread = Twin::new();
    unread.step(Input::Load { id: 1, goal: Goal::First, force: false });
    unread.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
    unread.step(Input::Committed { request: 1, revision: 5 });
    assert!(unread.step(Input::Foreground { seq: 1 }).is_empty(), "a waiter is no reader");
    let mut again = Twin::new();
    again.step(Input::InterestPlus { id: 1 });
    again.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
    again.step(Input::Committed { request: 1, revision: 5 });
    again.step(Input::Foreground { seq: 1 });
    assert!(again.step(Input::Foreground { seq: 1 }).is_empty(), "the same return counts once");
    let mut loading = Twin::new();
    loading.step(Input::InterestPlus { id: 1 });
    assert!(loading.step(Input::Foreground { seq: 1 }).is_empty(), "a window that loads already is not idle");
    let mut offline = Twin::new();
    offline.step(Input::InterestPlus { id: 1 });
    offline.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
    offline.step(Input::Committed { request: 1, revision: 5 });
    offline.step(Input::Online(false));
    assert!(offline.step(Input::Foreground { seq: 1 }).is_empty(), "no network, no request");
}

#[test]
fn win_idle_online_retry() {
    let mut twin = failed_watched();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_idle_online() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::Online(true)).is_empty(), "a fresh window is not reread");
    let mut unwatched = Twin::new();
    unwatched.step(Input::Online(false));
    assert!(unwatched.step(Input::Online(true)).is_empty());
    let mut failed = Twin::new();
    failed.call(1, Goal::First);
    failed.fail(Failure::Permanent);
    assert!(failed.step(Input::Online(true)).is_empty(), "a window with no interest is not reread");
}

#[test]
fn win_idle_reload_on_connect() {
    let mut twin = fresh_watched();
    twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: id64(STREAM) });
    twin.post(w::WindowsInput::Online(Online(false)));
    twin.post(w::WindowsInput::Online(Online(true)));
    assert_eq!(twin.stage(), Some(WindowStage::Loading), "a fresh window with a reload interest loads again when the network returns");
    let mut plain = fresh_watched();
    plain.post(w::WindowsInput::Online(Online(false)));
    plain.post(w::WindowsInput::Online(Online(true)));
    assert_eq!(plain.stage(), Some(WindowStage::Idle), "a fresh window without the interest stays");
}

#[test]
fn win_idle_models_reset() {
    let mut twin = fresh_watched();
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_idle_models_reset_offline() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_models_reset_unwatched() {
    let mut twin = fresh();
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_idle_models_other() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
    assert!(twin.reference.fresh);
}

#[test]
fn win_idle_stray() {
    let mut twin = fresh();
    for input in [
        Input::Response { request: 9, attempt: 9, cursor: None, rows: true },
        Input::TransportError { request: 9, attempt: 9, failure: Failure::Network },
        Input::Dropped { request: 9, attempt: 9 },
        Input::Retry { request: 9 },
        Input::Committed { request: 9, revision: 99 },
        Input::Rejected { request: 9, reason: Reject::Storage },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
}

// Loading.

#[test]
fn win_loading_join() {
    let mut twin = loading();
    assert_eq!(twin.watch(), [Effect::WindowState]);
    assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_join_waiter() {
    let mut twin = loading();
    assert!(twin.call(2, Goal::First).is_empty());
    assert!(twin.step(Input::Load { id: 3, goal: Goal::First, force: true }).is_empty());
    assert_eq!(twin.state().waiters(&key()), [1, 2, 3]);
    twin.finish(None);
    assert!(twin.state().waiters(&key()).is_empty(), "every waiter of the request was answered");
}

#[test]
fn win_loading_leave() {
    let mut twin = loading_watched();
    twin.call(2, Goal::First);
    assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin.finish(None);
}

#[test]
fn win_loading_last_leave() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_loading_load_more() {
    let mut twin = loading();
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.reference.waiters[&2], super::ref_window::Want::Next(2));
    twin.finish(Some(2));
    assert_eq!(twin.stage(), Some(WindowStage::Loading), "the page after the current one loads for the waiter");
}

#[test]
fn win_loading_response() {
    let mut twin = loading();
    assert_eq!(twin.respond(None), [Effect::Commit { request: 1, replace_all: false }]);
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_loading_stray() {
    let mut twin = loading();
    for input in [
        Input::Response { request: 9, attempt: 1, cursor: None, rows: true },
        Input::Response { request: 1, attempt: 9, cursor: None, rows: true },
        Input::TransportError { request: 1, attempt: 9, failure: Failure::Network },
        Input::Dropped { request: 9, attempt: 1 },
        Input::Retry { request: 9 },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
    }
}

#[test]
fn win_loading_temp_error() {
    let mut twin = loading();
    assert_eq!(twin.fail(Failure::Network), [Effect::ScheduleRetry { request: 1 }]);
    assert!(matches!(twin.reference.state, State::Loading { retry: true, attempts: 1, .. }));
    let mut dropped = loading();
    assert_eq!(dropped.step(Input::Dropped { request: 1, attempt: 1 }), [Effect::ScheduleRetry { request: 1 }]);
    let mut server = loading();
    assert_eq!(server.fail(Failure::Server), [Effect::ScheduleRetry { request: 1 }]);
}

#[test]
fn win_loading_attempts_exhausted() {
    let mut twin = loading_watched();
    for _ in 0..2 {
        assert_eq!(twin.fail(Failure::Network), [Effect::ScheduleRetry { request: 1 }]);
        twin.step(Input::Retry { request: 1 });
    }
    assert_eq!(twin.fail(Failure::Network), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
    let mut caller = loading();
    caller.fail(Failure::Server);
    caller.step(Input::Retry { request: 1 });
    caller.fail(Failure::Server);
    assert!(caller.state().waiters(&key()).is_empty(), "the waiter heard `Failed`");
}

#[test]
fn win_loading_invalid_cursor() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    assert_eq!(twin.fail(Failure::InvalidCursor), [Effect::Request { request: 3, attempt: 3, page: 1 }]);
    assert!(twin.reference.cursor_reset_used && twin.reference.replace_all);
    assert_eq!(twin.respond(Some(2)), [Effect::Commit { request: 3, replace_all: true }]);
    twin.land();
    let mut again = fresh_with_cursor();
    again.call(2, Goal::Next);
    again.fail(Failure::InvalidCursor);
    assert_eq!(again.fail(Failure::InvalidCursor), none(), "the second INVALID_CURSOR is permanent");
    assert_eq!(again.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_loading_error() {
    let mut twin = loading_watched();
    assert_eq!(twin.fail(Failure::Permanent), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
    let mut rows = fresh_watched();
    rows.step(Input::Stale { in_keys: true });
    rows.fail(Failure::Permanent);
    assert_eq!(rows.phase(), Phase::Loaded, "rows stay when a later load fails");
}

#[test]
fn win_loading_retry() {
    let mut twin = loading();
    twin.fail(Failure::Network);
    assert_eq!(twin.step(Input::Retry { request: 1 }), [Effect::Request { request: 1, attempt: 2, page: 1 }]);
    assert!(matches!(twin.reference.state, State::Loading { retry: false, attempt: 2, .. }));
}

#[test]
fn win_loading_retry_stray() {
    let mut twin = loading();
    assert!(twin.step(Input::Retry { request: 1 }).is_empty(), "no retry is due while the attempt is in flight");
    twin.fail(Failure::Network);
    assert!(twin.step(Input::Retry { request: 9 }).is_empty());
}

#[test]
fn win_loading_offline() {
    let mut twin = loading_watched();
    assert_eq!(twin.step(Input::Online(false)), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.phase(), Phase::Loading);
    let mut waiting_retry = loading();
    waiting_retry.fail(Failure::Network);
    assert!(waiting_retry.step(Input::Online(false)).is_empty(), "nothing is in flight to cancel");
}

#[test]
fn win_loading_online() {
    let mut twin = loading();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_stale() {
    let mut twin = loading();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.reference.reread_due);
}

#[test]
fn win_loading_models_reset() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Cancel { request: 1, attempt: 1 }, Effect::Request { request: 2, attempt: 2, page: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_models_other() {
    let mut twin = loading();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
}

#[test]
fn win_loading_stray_store() {
    let mut twin = loading();
    assert!(twin.step(Input::Committed { request: 1, revision: 50 }).is_empty());
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::Storage }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

// Landing.

#[test]
fn win_landing_committed() {
    let mut twin = Twin::new();
    twin.watch();
    twin.call(1, Goal::First);
    twin.respond(None);
    assert_eq!(twin.land(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Loaded);
    assert!(twin.reference.fresh && twin.load_state().loaded == Some(twin.revision));
    assert!(twin.state().waiters(&key()).is_empty());
}

#[test]
fn win_landing_committed_again() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.step(Input::Stale { in_keys: true });
    twin.watch();
    twin.respond(None);
    let effects = twin.land();
    assert_eq!(effects, [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_committed_again_offline() {
    let mut twin = landing();
    twin.watch();
    twin.step(Input::Stale { in_keys: true });
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_landing_committed_again_unwatched() {
    let mut twin = landing();
    twin.step(Input::Stale { in_keys: true });
    assert_eq!(twin.land(), none(), "the only waiter was answered, nobody asks for a reread");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_landing_committed_all_next() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    assert_eq!(twin.land(), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.state().waiters(&key()), [1], "the caller of `all` waits, the other heard `Loaded`");
    twin.finish(None);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_committed_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    assert_eq!(twin.land(), none(), "page limit reached: all heard an outcome");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_committed_all_offline() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.state().waiters(&key()), [1]);
}

#[test]
fn win_landing_committed_next_page() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    twin.call(3, Goal::Next);
    twin.respond(Some(3));
    assert_eq!(twin.land(), [Effect::Request { request: 3, attempt: 3, page: 3 }], "the waiters of page 2 heard `Loaded`");
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_committed_next_page_offline() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(Some(2));
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.state().waiters(&key()), [2]);
}

#[test]
fn win_landing_committed_next_page_end() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(None);
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty(), "no more pages: every waiter heard `Loaded`");
}

#[test]
fn win_landing_committed_next_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(Some(2));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_stale_generation() {
    let mut twin = landing();
    twin.watch();
    assert_eq!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }), [Effect::Request { request: 2, attempt: 2, page: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_stale_generation_offline() {
    let mut twin = landing();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_landing_stale_generation_unwatched() {
    let mut twin = landing();
    twin.step(Input::InterestMinus { id: 1 });
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_landing_no_session() {
    for reason in [Reject::NoSession, Reject::Closed] {
        let mut twin = landing();
        assert!(twin.step(Input::Rejected { request: 1, reason }).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Ended));
    }
}

#[test]
fn win_landing_storage() {
    for reason in [Reject::Storage, Reject::Invalid] {
        let mut twin = landing();
        twin.watch();
        assert_eq!(twin.step(Input::Rejected { request: 1, reason }), [Effect::WindowState]);
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert_eq!(twin.phase(), Phase::Failed);
    }
}

#[test]
fn win_landing_join() {
    let mut twin = landing();
    assert_eq!(twin.watch(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_landing_load() {
    let mut twin = landing();
    assert!(twin.call(2, Goal::First).is_empty());
    assert!(twin.call(3, Goal::Next).is_empty());
    twin.land();
    assert!(twin.state().waiters(&key()).is_empty(), "the landing answered the waiters that joined it");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_leave() {
    let mut twin = landing();
    twin.call(2, Goal::First);
    assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
    twin.land();
    assert_eq!(twin.stage(), Some(WindowStage::Idle), "the landing ends, a cancelled waiter hears nothing");
}

#[test]
fn win_landing_stale() {
    let mut twin = landing();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.reference.reread_due);
}

#[test]
fn win_landing_online() {
    let mut twin = landing();
    assert!(twin.step(Input::Online(false)).is_empty());
    assert_eq!(twin.reference.online, Some(false));
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_landing_wait() {
    let mut twin = landing();
    for input in [
        Input::ModelsReset { own_model: true },
        Input::Response { request: 1, attempt: 1, cursor: None, rows: true },
        Input::TransportError { request: 1, attempt: 1, failure: Failure::Permanent },
        Input::Dropped { request: 1, attempt: 1 },
        Input::Retry { request: 1 },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Landing));
    }
}

// Waiting.

#[test]
fn win_waiting_resume() {
    let mut twin = waiting();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_waiting_offline() {
    let mut twin = waiting();
    assert!(twin.step(Input::Online(false)).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_waiting_join() {
    let mut twin = waiting();
    assert_eq!(twin.watch(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_waiting_load() {
    let mut twin = waiting();
    assert!(twin.call(2, Goal::First).is_empty());
    assert!(twin.step(Input::Load { id: 3, goal: Goal::All, force: true }).is_empty());
    assert!(matches!(twin.reference.state, State::Waiting { force: true, .. }));
    assert_eq!(twin.state().waiters(&key()), [1, 2, 3]);
}

#[test]
fn win_waiting_leave() {
    let mut twin = waiting();
    twin.call(2, Goal::First);
    assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_waiting_last_leave() {
    let mut twin = waiting();
    assert!(twin.step(Input::InterestMinus { id: 1 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_waiting_stale() {
    let mut twin = waiting();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_waiting_stray() {
    let mut twin = waiting();
    for input in [
        Input::Response { request: 1, attempt: 1, cursor: None, rows: true },
        Input::TransportError { request: 1, attempt: 1, failure: Failure::Network },
        Input::Dropped { request: 1, attempt: 1 },
        Input::Retry { request: 1 },
        Input::Committed { request: 1, revision: 5 },
        Input::Rejected { request: 1, reason: Reject::Storage },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    }
}

// Ended.

#[test]
fn win_epoch_ended() {
    for build in [loading as fn() -> Twin, landing, waiting, fresh] {
        let mut twin = build();
        let in_flight = twin.stage() == Some(WindowStage::Loading);
        let effects = twin.step(Input::EpochEnded);
        assert_eq!(effects.iter().filter(|effect| matches!(effect, Effect::Cancel { .. })).count(), usize::from(in_flight));
        assert_eq!(twin.stage(), Some(WindowStage::Ended));
    }
    let mut retry = loading();
    retry.fail(Failure::Network);
    assert!(retry.step(Input::EpochEnded).is_empty(), "a window that waits for a retry has nothing in flight");
}

#[test]
fn win_closing() {
    for build in [loading as fn() -> Twin, landing, waiting, fresh] {
        let mut twin = build();
        let effects = twin.step(Input::Closing);
        assert_eq!(effects.last(), Some(&Effect::Drained));
        assert_eq!(twin.stage(), Some(WindowStage::Ended));
    }
}

#[test]
fn win_ended_closing() {
    let mut twin = loading();
    twin.step(Input::EpochEnded);
    assert_eq!(twin.step(Input::Closing), [Effect::Drained]);
    let mut empty = Twin::new();
    empty.post_call(w::WindowsInput::Closing);
    let (got, _) = empty.split(take(&empty.log));
    assert_eq!(got, [Effect::Drained], "also without a machine");
}

#[test]
fn win_ended() {
    let mut twin = loading();
    twin.step(Input::EpochEnded);
    assert!(twin.step(Input::InterestPlus { id: 5 }).is_empty());
    assert!(twin.call(6, Goal::First).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Ended));
}

// Waits: every call of the platform ends with one outcome on every terminal cell.

#[test]
fn win_wait_load() {
    let mut loaded = loading();
    loaded.finish(None);
    let mut failed = loading();
    failed.fail(Failure::Permanent);
    let mut ended = loading();
    ended.step(Input::EpochEnded);
    let mut closed = loading();
    closed.step(Input::Closing);
    let mut fresh_call = fresh();
    fresh_call.call(2, Goal::First);
}

#[test]
fn win_wait_load_more() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    twin.finish(None);
    let mut end = fresh();
    end.call(3, Goal::Next);
    let mut limit = Twin::with_max_pages(1);
    limit.call(1, Goal::First);
    limit.finish(Some(2));
    limit.call(2, Goal::Next);
}

#[test]
fn win_wait_load_all() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::All);
    twin.finish(Some(3));
    twin.finish(None);
    let mut failed = fresh_with_cursor();
    failed.call(2, Goal::All);
    failed.fail(Failure::Permanent);
    let mut done = fresh();
    done.call(3, Goal::All);
}

#[test]
fn win_wait_nodes() {
    let mut twin = Twin::new();
    let nodes = WindowKey::nodes("Moment", &["m1", "m2"]);
    take(&twin.log);
    twin.post_call(w::WindowsInput::Load { key: nodes.clone(), id: 1, goal: w::Goal::First, force: false, store_revision: 0 });
    assert!(matches!(runs(take(&twin.log)).as_slice(), [WindowsEffect::Window { effect: WindowEffect::Request { page: 1, .. }, .. }]));
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["m1".into()], None) });
    twin.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 11, tables: Vec::new(), models: Default::default() }));
    assert!(runs(take(&twin.log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 1, outcome: WindowOutcome::Loaded { revision: 11, .. } }, .. })));
    twin.post_call(w::WindowsInput::Load { key: nodes, id: 2, goal: w::Goal::First, force: false, store_revision: 0 });
    assert!(runs(take(&twin.log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 2, outcome: WindowOutcome::Loaded { revision: 11, .. } }, .. })), "a fresh nodes key answers at once");
}

#[test]
fn win_wait_cancel() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    let mut shared = loading();
    shared.call(2, Goal::First);
    assert!(shared.step(Input::InterestMinus { id: 1 }).is_empty(), "another waiter keeps the request");
}

#[test]
fn win_wait_offline() {
    let mut twin = waiting();
    twin.step(Input::Online(true));
    twin.finish(None);
    let mut failed = waiting();
    failed.step(Input::Online(true));
    failed.fail(Failure::Permanent);
}

// Properties. An executor stub answers requests and commits in the same scheduler, the explorer orders everything it sends.

#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    Requested(u64),
    Cancelled(u64),
    Landed { request: u64, basis: u64, revision: i64 },
    Rejected(u64),
    Outcome(u32, WindowOutcome),
}

struct Executor {
    log: Log<Seen>,
}

impl EffectRunner<Spy<Windows>> for Executor {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Spy<Windows>>) {
        let WindowsEffect::Window { effect, .. } = effect else { return };
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        match effect {
            WindowEffect::Request { request, attempt, page, .. } => {
                log.push(Seen::Requested(request));
                back.reply(stamp, w::WindowsInput::Response { request, attempt, page: AnswerPage::new(page, vec!["r1".into()], None) });
            }
            WindowEffect::Cancel { request, .. } => log.push(Seen::Cancelled(request)),
            WindowEffect::Outcome { id, outcome } => log.push(Seen::Outcome(u32::try_from(id).expect("a test id"), outcome)),
            WindowEffect::DerivePreload { .. } => {}
        }
    }
}

type Shared = Rc<(Log<Seen>, Arc<AtomicU64>)>;

thread_local! {
    static SEEN: RefCell<Option<Shared>> = const { RefCell::new(None) };
}

fn seen() -> (Log<Seen>, Arc<AtomicU64>) {
    let shared = SEEN.with(|slot| slot.borrow().clone()).expect("a rig was built");
    (shared.0.clone(), shared.1.clone())
}

/// Two callers, a stale mark, the network going down and up, a reset of the model: each is a source on its own.
fn explore_rig() -> DetScheduler {
    let mut det = DetScheduler::new(1);
    let log = new_log();
    let generation = Arc::new(AtomicU64::new(0));
    let windows_slot = Address::<Windows>::new(WINDOWS);
    let noted = log.clone();
    det.add::<StoreStub<Windows>>("store", StoreStubState {
        to: windows_slot,
        generation: generation.clone(),
        revision: 0,
        only: None,
        committed: w::WindowsInput::Committed,
        rejected: w::WindowsInput::Rejected,
        note: Box::new(move |landing| noted.lock().unwrap_or_else(PoisonError::into_inner).push(match landing {
            Landing::Landed { batch, basis, revision, .. } => Seen::Landed { request: batch, basis, revision },
            Landing::Rejected { batch, .. } => Seen::Rejected(batch),
        })),
    }, Silent);
    det.add::<Sink<Deliveries>>("deliveries", new_log(), Discard);
    let windows = det.add::<Spy<Windows>>("windows", SpyState::new(w::WindowsState::new(EPOCH, Some(true), peers()), &new_log()), Executor { log: log.clone() });
    let windows = Address::<Windows>::new(windows.index());
    det.start_epoch(EPOCH);
    let load = |id: u64| w::WindowsInput::Load { key: key(), id, goal: w::Goal::First, force: false, store_revision: 0 };
    let callers = det.add::<Source<Windows>>("callers", SourceState { to: windows, script: vec![load(1), load(2)] }, Silent);
    let marks = det.add::<Source<Windows>>("feed", SourceState { to: windows, script: vec![w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All }] }, Silent);
    let network = det.add::<Source<Windows>>("network", SourceState { to: windows, script: vec![w::WindowsInput::Online(Online(false)), w::WindowsInput::Online(Online(true))] }, Silent);
    let lifecycle = det.add::<Reset<Windows>>("lifecycle", ResetState { to: windows, input: w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] }), generation: 1 }, StoreGeneration(generation.clone()));
    for source in [callers, marks, network] {
        det.post(source, EpochStamp::Control, SourceInput::Go);
    }
    det.post(lifecycle, EpochStamp::Control, SourceInput::Go);
    SEEN.with(|slot| *slot.borrow_mut() = Some(Rc::new((log, generation))));
    det
}

const LIMITS: Limits = Limits { depth: 4, max_traces: 30_000 };

fn open_requests(events: &[Seen]) -> usize {
    let mut open: BTreeSet<u64> = BTreeSet::new();
    let mut worst = 0;
    for event in events {
        match event {
            Seen::Requested(request) => {
                open.insert(*request);
            }
            Seen::Cancelled(request) | Seen::Rejected(request) => {
                open.remove(request);
            }
            Seen::Landed { request, .. } => {
                open.remove(request);
            }
            Seen::Outcome(..) => {}
        }
        worst = worst.max(open.len());
    }
    worst
}

/// P-WIN-01: a caller gets at most one outcome on any trace and exactly one once the window is not waiting for the network.
#[test]
fn win_prop_outcome_once() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let events = snapshot(&log);
        for id in [1u32, 2] {
            let count = events.iter().filter(|event| matches!(event, Seen::Outcome(found, _) if *found == id)).count();
            if count > 1 {
                return Err(format!("caller {id} got {count} outcomes"));
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 10);
    let report = explore(explore_rig, Limits { depth: 40, max_traces: 3_000 }, |det| {
        if !det.ready().is_empty() {
            return Ok(());
        }
        let (log, _) = seen();
        let events = snapshot(&log);
        let windows = det.state(Address::<Spy<Windows>>::new(WINDOWS)).map(|spy| &spy.inner).ok_or("no state")?;
        if windows.stage(&key()) == Some(WindowStage::Waiting) {
            return Ok(());
        }
        for id in [1u32, 2] {
            let count = events.iter().filter(|event| matches!(event, Seen::Outcome(found, _) if *found == id)).count();
            if count != 1 && windows.windows() > 0 {
                return Err(format!("caller {id} got {count} outcomes"));
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-02: `Loaded{R}` is given only for a revision at which the rows of the answer landed.
#[test]
fn win_prop_loaded_rows() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let events = snapshot(&log);
        for (at, event) in events.iter().enumerate() {
            if let Seen::Outcome(_, WindowOutcome::Loaded { revision, .. }) = event {
                let landed = events[..at].iter().any(|before| matches!(before, Seen::Landed { revision: found, .. } if found == revision));
                if !landed {
                    return Err(format!("Loaded at {revision} without a landing"));
                }
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-03: at most one request of the key is in the network.
#[test]
fn win_prop_single_flight() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let open = open_requests(&snapshot(&log));
        if open > 1 {
            return Err(format!("{open} requests in the network"));
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-04: an answer issued at generation g does not land after a reset of its model; the store refuses it and the window asks again.
#[test]
fn win_prop_generation() {
    let report = explore(explore_rig, Limits { depth: 40, max_traces: 3_000 }, |det| {
        let (log, generation) = seen();
        let events = snapshot(&log);
        let current = generation.load(Ordering::SeqCst);
        if det.ready().is_empty() && events.iter().any(|event| matches!(event, Seen::Rejected(_))) {
            let landed_new = events.iter().any(|event| matches!(event, Seen::Landed { basis, .. } if *basis >= current));
            let state = det.state(Address::<Spy<Windows>>::new(WINDOWS)).map(|spy| &spy.inner);
            let waiting = state.is_some_and(|windows| windows.stage(&key()) == Some(WindowStage::Waiting));
            let idle = state.is_some_and(|windows| windows.stage(&key()) == Some(WindowStage::Idle) && windows.interest(&key()).is_empty());
            if !landed_new && !waiting && !idle {
                return Err("a refused answer was not asked again".into());
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-05: going online reads once every window that is not fresh and has interest or waiters, with no earlier network error needed.
#[test]
fn win_prop_online_once() {
    let mut twin = failed_watched();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert!(twin.step(Input::Online(true)).is_empty(), "a second signal does not read again");
    let mut quiet = fresh_watched();
    assert!(quiet.step(Input::Online(true)).is_empty(), "a fresh window is not reread");
    let mut stale = fresh_watched();
    stale.step(Input::Online(false));
    stale.step(Input::Stale { in_keys: true });
    assert_eq!(stale.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState], "a stale window reads without an earlier network error");
    let mut waiting_call = waiting();
    assert_eq!(requests(&waiting_call.step(Input::Online(true))), 1);
}

/// P-WIN-06: no input, no request and no schedule; a retry is scheduled only while online.
#[test]
fn win_prop_idle() {
    let twin = fresh();
    assert_eq!(twin.det.ready().len(), 0);
    assert!(take(&twin.log).is_empty());
    let mut offline = Twin::new();
    offline.call(1, Goal::First);
    offline.step(Input::Online(false));
    let effects = offline.step(Input::TransportError { request: 1, attempt: 1, failure: Failure::Network });
    assert!(effects.iter().all(|effect| !matches!(effect, Effect::ScheduleRetry { .. })), "no retry is scheduled offline");
}

/// P-WIN-07: the last `Interest-` cancels the request and the answer of the cancelled request is not committed; after the point of acceptance the landing ends.
#[test]
fn win_prop_cancel() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert!(twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true }).is_empty(), "no Commit for the cancelled request");
    let mut landed = landing();
    landed.step(Input::InterestMinus { id: 1 });
    assert!(landed.land().is_empty(), "the landing ends without a new load");
    assert_eq!(landed.stage(), Some(WindowStage::Idle));
}

/// P-WIN-08: `Stale` during Loading or Landing gives exactly one more load after the landing while interest or waiters exist.
#[test]
fn win_prop_stale_reload_once() {
    let mut twin = loading_watched();
    for _ in 0..3 {
        twin.step(Input::Stale { in_keys: true });
    }
    twin.respond(None);
    let again = twin.land();
    assert_eq!(requests(&again), 1, "three marks make one more load");
    twin.respond(None);
    let last = twin.land();
    assert_eq!(requests(&last), 0);
}

/// P-WIN-09: the conditions read only the fields `online` and `generation[m]` of the dictionary.
#[test]
fn win_prop_fields_only() {
    let mut twin = Twin::new();
    twin.step(Input::Online(false));
    twin.post(w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 5, models: vec!["Moment".into()] }));
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting), "a machine created after Online(false) reads the field of the dictionary");
    take(&twin.log);
    twin.post(w::WindowsInput::Online(Online(true)));
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None) });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { Some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Moment\": 5"), "the basis is the generation of the dictionary: {sent}");
}

#[test]
fn win_prop_landing_settles_first() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::All);
    twin.respond(Some(2));
    let effects = twin.land();
    assert_eq!(requests(&effects), 1, "the request for the next page is chosen by the AllPages waiter alone");
    assert_eq!(twin.state().waiters(&key()).len(), 1, "the waiter of the first page left with its outcome");
}

#[test]
fn win_base_revision() {
    let sent_batch = |twin: &mut Twin| take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { Some(input) } else { None }).expect("a batch");
    let mut twin = Twin::new();
    twin.post_call(w::WindowsInput::Load { key: key(), id: 1, goal: w::Goal::First, force: false, store_revision: 7 });
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None) });
    assert!(sent_batch(&mut twin).contains("base_revision: 7"), "the batch carries the revision of the Store at the entry of the call");
    twin.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 9, tables: Vec::new(), models: Default::default() }));
    twin.post_call(w::WindowsInput::Load { key: key(), id: 2, goal: w::Goal::First, force: true, store_revision: 5 });
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None) });
    assert!(sent_batch(&mut twin).contains("base_revision: 9"), "the revision of the last landing is newer than the entry of the call");
}

/// P-WIN-10: the outcome `Loaded` of `AllPages` comes only at the end of the pages: one request per page after the first.
#[test]
fn win_prop_load_all_complete() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    let mut requests_after_first = 0;
    for page in 1..=3u32 {
        let cursor = (page < 3).then_some(page + 1);
        twin.respond(cursor);
        requests_after_first += requests(&twin.land());
    }
    assert_eq!(requests_after_first, 2, "one request per page after the first");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty());
}

/// P-WIN-11: a finished waiter does not stay: `Stale` and `Online(true)` give no request when interest and waiters are empty; the preload stays.
#[test]
fn win_prop_waiters_leave() {
    let mut twin = loading();
    twin.finish(None);
    assert!(twin.state().waiters(&key()).is_empty());
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty());
    let mut preload = Twin::with_preload();
    preload.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(1), generation: 0 }));
    preload.post_call(w::WindowsInput::Online(Online(true)));
    preload.reference.step(Input::InterestPlus { id: PRELOAD });
    preload.reference.take_outcomes();
    preload.step(Input::Stale { in_keys: true });
    assert_eq!(preload.state().interest(&key()), [w::PRELOAD], "the preload interest stays");
}

/// P-WIN-12: before the first addressed `Online` a machine asks for nothing.
#[test]
fn win_prop_no_request_before_online() {
    let mut twin = Twin::before_online();
    assert!(requests(&twin.watch()) == 0);
    assert!(requests(&twin.call(1, Goal::First)) == 0);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(requests(&twin.step(Input::Online(true))), 1);
}

/// The dictionary: windows of two keys are independent, the answer goes to the key that owns the request, `Closing` gives one `Drained`.
#[test]
fn win_dictionary_owner() {
    let mut det = DetScheduler::new(5);
    let log = new_log();
    let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()), Discard);
    det.start_epoch(EPOCH);
    let other = WindowKey { model: "Chat".into(), name: "room".into() };
    for (key, id) in [(key(), 1), (other.clone(), 2)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: 0 });
    }
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["c1".into()], None) });
    det.run_until_quiet(100).expect("quiet");
    let state = inner(&det, windows);
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Loading), Some(WindowStage::Landing)), "the answer of request 2 reaches the key that owns it");
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Chat".into()] }));
    det.close();
    det.run_until_quiet(100).expect("quiet");
    let effects = runs(take(&log));
    let drained = effects.iter().filter(|effect| matches!(effect, WindowsEffect::Drained(_))).count();
    assert_eq!(drained, 1, "one Drained for all windows");
    assert!(effects.iter().any(|effect| matches!(effect, WindowsEffect::Window { key, effect: WindowEffect::Cancel { request: 1, .. } } if *key == self::key())), "the loading window is cancelled at Closing");
    let state = inner(&det, windows);
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Ended), Some(WindowStage::Ended)));
}

/// `landed_revision` belongs to the machine: a key answers a fresh call with its own last landing, not the last landing of any key.
#[test]
fn win_landed_revision_per_machine() {
    let mut det = DetScheduler::new(5);
    let log = new_log();
    let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()), Discard);
    det.start_epoch(EPOCH);
    let other = WindowKey { model: "Chat".into(), name: "room".into() };
    let load = |key: WindowKey, id: u64, revision: i64| w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: revision };
    for (key, id) in [(key(), 1), (other.clone(), 2)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), load(key, id, 0));
    }
    for (request, row, revision) in [(1, "r1", 5), (2, "c1", 9)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request, attempt: u32::try_from(request).expect("a small id"), page: AnswerPage::new(1, vec![row.into()], None) });
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Committed(Committed { batch: request, revision, tables: Vec::new(), models: Default::default() }));
    }
    det.run_until_quiet(100).expect("quiet");
    take(&log);
    det.post(windows, EpochStamp::Epoch(EPOCH), load(key(), 3, 99));
    det.run_until_quiet(100).expect("quiet");
    assert!(runs(take(&log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 3, outcome: WindowOutcome::Loaded { revision: 5, .. } }, .. })), "the fresh key answers with its own landing");
}

/// win_loading_response (P-WIN-04): the basis of a batch names every model of the answer with the generation it had when the request left, also a model the window did not know before.
#[test]
fn win_basis_names_every_model_of_the_answer_at_issue() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    take(&twin.log);
    twin.post(w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Chat".into()] }));
    let mut page = AnswerPage::new(1, vec!["r1".into()], None);
    page.dependency_models = BTreeSet::from(["Moment".to_string(), "Chat".to_string()]);
    page.payload.related = vec![("Chat".into(), RowValue { row_id: "c1".into(), values: "{}".into() })];
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { Some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Chat\": 0") && sent.contains("\"Moment\": 0"), "the generations of the issue, not the reset that came after: {sent}");
}

/// win_landing_stale_generation: the new request carries the generations of the refusal.
#[test]
fn win_stale_generation_reissues_with_the_generations_of_the_refusal() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None) });
    twin.post(w::WindowsInput::Rejected(Rejected { batch: 1, reason: RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 3 } }));
    take(&twin.log);
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None) });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { Some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Moment\": 3"), "the basis of the new request is the generation of the refusal: {sent}");
}

/// win_wait_nodes: an id that holds a comma stays one id and the key keeps the order and the boundaries of the ids.
#[test]
fn win_nodes_key_keeps_the_boundaries_of_the_ids() {
    let joined = WindowKey::nodes("Moment", &["a,b"]);
    let split = WindowKey::nodes("Moment", &["a", "b"]);
    assert_ne!(joined, split);
    assert_eq!(joined.kind(), WindowKind::Nodes { ids: vec!["a,b".to_string()] });
    assert_eq!(split.kind(), WindowKind::Nodes { ids: vec!["a".to_string(), "b".to_string()] });
}

/// win_created, P-WIN-12: the dictionary lives for one epoch. The key of the old epoch is gone and the first request of the new epoch waits for the addressed `Online`.
#[test]
fn win_dictionary_and_online_belong_to_the_epoch() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH + 1, identity: Some(1), generation: 0 }));
    assert_eq!(twin.state().windows(), 0, "the machines of the old epoch are gone");
    take(&twin.log);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 7, goal: w::Goal::First, force: false, store_revision: 0 });
    let (requests_before_online, _) = twin.split(take(&twin.log));
    assert!(requests_before_online.is_empty(), "no request before the addressed Online: {requests_before_online:?}");
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    twin.post_call(w::WindowsInput::Online(Online(true)));
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}
