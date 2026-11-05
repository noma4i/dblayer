//! Owner-level tests of `specs/machines/window.md`: the owner `Windows` on `DetScheduler`, every row of the table and every property.
//! A row test feeds the same input to the reference (`ref_window.rs`) and to the owner through its address, then compares the effects,
//! the outcomes of the callers and the phase. Names are the `Test` column, `_owner` marks the owner twin of a test that also runs on the API.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

use super::harness::Phase;
use super::ref_window::{Effect, Failure, Input, Reject, Result3, State, Window};
use crate::owners::contract::{Committed, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, Transport, RequestError, TransportOutcome};
use crate::owners::deliveries::{LoadState, Phase as OwnerPhase};
use crate::owners::support::{new_log, snapshot, take, Log, Recorder, Reset, ResetState, Silent, Source, SourceInput, SourceState, StoreGeneration};
use crate::owners::windows::{self as w, WindowOutcome, AnswerPage, WindowStage, WindowEffect, WindowKey, Windows, WindowsEffect};
use crate::runtime::{explore, Address, Back, Cause as Ended, DetScheduler, EffectRunner, EpochStamp, Limits};

const EPOCH: u64 = 1;

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
    }
}

fn result_of(outcome: &WindowOutcome) -> Result3 {
    match outcome {
        WindowOutcome::Loaded { .. } => Result3::Loaded,
        WindowOutcome::Failed { .. } => Result3::Failed,
        WindowOutcome::Interrupted => Result3::Interrupted,
    }
}

/// The owner and the reference side by side.
struct Twin {
    det: DetScheduler,
    addr: Address<Windows>,
    log: Log<WindowsEffect>,
    reference: Window,
    /// The server has a next page: a response carries a cursor.
    cursor: bool,
    /// Interest ids join as streams: they stay in the interest after the outcome.
    streams: bool,
    revision: i64,
    stale_mark: i64,
    generation: u64,
}

impl Twin {
    fn new() -> Self { Self::with(true, true) }

    fn with(online: bool, session: bool) -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let addr = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, online, session), Recorder::<Windows>::new(&log));
        det.start_epoch(EPOCH);
        let mut reference = Window::new();
        reference.online = online;
        reference.session = session;
        Self { det, addr, log, reference, cursor: false, streams: false, revision: 10, stale_mark: 0, generation: 0 }
    }

    fn state(&self) -> &w::WindowsState { self.det.state(self.addr).expect("the owner state") }

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

    fn ref_request(&self) -> u64 {
        match &self.reference.state {
            State::Loading { request, .. } | State::Landing { request, .. } => *request,
            other => panic!("no request in {other:?}"),
        }
    }

    /// Feeds one input of the table to both sides. Returns the effects of the reference after the check against the owner.
    fn step(&mut self, input: Input) -> Vec<Effect> {
        take(&self.log);
        let k = key();
        match &input {
            Input::InterestPlus { id, force } => {
                if self.streams {
                    self.reference.watched = true;
                }
                self.post_call(w::WindowsInput::InterestPlus { key: k, id: *id, force: *force, stream: self.streams });
            }
            Input::InterestMinus { id } => self.post_call(w::WindowsInput::InterestMinus { key: k, id: *id }),
            Input::LoadMore { id } => self.post_call(w::WindowsInput::LoadMore { key: k, id: *id }),
            Input::Response { request } => {
                let cursor = self.cursor.then(|| "next".to_string());
                self.reference.cursor = self.cursor;
                self.post(w::WindowsInput::Transport(Transport { req_id: *request, outcome: TransportOutcome::Response(AnswerPage { page: 1, rows: vec!["r1".into()], cursor }) }));
            }
            Input::TransportError { request, failure } => {
                let error = RequestError { message: "e".into(), class: class_of(*failure) };
                self.post(w::WindowsInput::Transport(Transport { req_id: *request, outcome: TransportOutcome::Error(error) }));
            }
            Input::Committed { request } => {
                self.revision += 1;
                self.post(w::WindowsInput::Committed(Committed { batch: *request, revision: self.revision, tables: Vec::new() }));
            }
            Input::Rejected { request, reason } => {
                let reason = match reason {
                    Reject::StaleGeneration => RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: self.generation },
                    Reject::NoSession => RejectReason::NoSession,
                    Reject::Storage => RejectReason::Storage("disk".into()),
                };
                self.post(w::WindowsInput::Rejected(Rejected { batch: *request, reason }));
            }
            Input::Stale { newer_than_issue } => {
                let revision = if *newer_than_issue {
                    self.stale_mark += 1000;
                    self.stale_mark
                } else {
                    0
                };
                self.post(w::WindowsInput::Stale { key: k, revision });
            }
            Input::Online(on) => self.post(w::WindowsInput::Online(Online(*on))),
            Input::EpochStarted => self.post(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: None })),
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
            Input::RetryDue => {
                let request = self.ref_request();
                self.post(w::WindowsInput::RetryDue { request });
            }
        }
        let (got, outcomes) = self.split(take(&self.log));
        let ended_interest = matches!((&self.reference.state, &input), (State::Ended, Input::InterestPlus { .. }));
        let want = if let (true, Input::InterestPlus { id, .. }) = (ended_interest, &input) {
            self.reference.ended_interest(*id);
            Vec::new()
        } else {
            self.reference.step(input)
        };
        assert_eq!(got, want, "the owner and the reference give the same effects");
        assert_eq!(outcomes, self.reference.take_outcomes(), "the owner and the reference answer the same callers");
        want
    }

    fn split(&self, effects: Vec<WindowsEffect>) -> (Vec<Effect>, Vec<(u32, Result3)>) {
        let (mut modelled, mut outcomes) = (Vec::new(), Vec::new());
        for effect in effects {
            match effect {
                WindowsEffect::Window { effect, .. } => match effect {
                    WindowEffect::Request { request, page, .. } => modelled.push(Effect::Request { request, page }),
                    WindowEffect::Cancel { request } => modelled.push(Effect::Cancel { request }),
                    WindowEffect::Commit { request, .. } => modelled.push(Effect::Commit { request }),
                    WindowEffect::ScheduleRetry { .. } => modelled.push(Effect::ScheduleRetry),
                    WindowEffect::Announce(_) => modelled.push(Effect::WindowState),
                    WindowEffect::Outcome { id, outcome } => outcomes.push((id, result_of(&outcome))),
                },
                WindowsEffect::Drained(_) => modelled.push(Effect::Drained),
            }
        }
        (modelled, outcomes)
    }

    fn load_state(&self) -> LoadState { self.state().load_state(&key()).expect("the window exists") }

    fn phase(&self) -> Phase { phase_of(self.load_state().phase) }
}

fn run_load(twin: &mut Twin, id: u32) -> u64 {
    let request = twin.reference.state_request();
    twin.step(Input::Response { request });
    twin.step(Input::Committed { request });
    let _ = id;
    request
}

trait RefRequest {
    fn state_request(&self) -> u64;
}

impl RefRequest for Window {
    fn state_request(&self) -> u64 {
        match &self.state {
            State::Loading { request, .. } | State::Landing { request, .. } => *request,
            other => panic!("no request in {other:?}"),
        }
    }
}

/// A window that loaded its first page for caller 1: `Idle{fresh}`, rows in the base. The reference outcome of the call is read.
fn fresh() -> Twin {
    let mut twin = Twin::new();
    twin.step(Input::InterestPlus { id: 1, force: false });
    run_load(&mut twin, 1);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    twin
}

/// The same with a stream in the interest: the window is watched.
fn fresh_watched() -> Twin {
    let mut twin = Twin::new();
    twin.streams = true;
    twin.step(Input::InterestPlus { id: 1, force: false });
    run_load(&mut twin, 1);
    twin
}

fn loading() -> Twin {
    let mut twin = Twin::new();
    twin.step(Input::InterestPlus { id: 1, force: false });
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin
}

fn landing() -> Twin {
    let mut twin = loading();
    twin.step(Input::Response { request: 1 });
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
    twin
}

fn waiting() -> Twin {
    let mut twin = Twin::with(false, true);
    twin.step(Input::InterestPlus { id: 1, force: false });
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    twin
}

#[test]
fn win_idle_interest_fresh_owner() {
    let mut twin = fresh();
    let effects = twin.step(Input::InterestPlus { id: 2, force: false });
    assert_eq!(effects, [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Loaded);
}

#[test]
fn win_idle_interest_load_owner() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::InterestPlus { id: 1, force: false }), [Effect::Request { request: 1, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.phase(), Phase::Loading);
    let mut forced = fresh();
    assert_eq!(forced.step(Input::InterestPlus { id: 2, force: true }), [Effect::Request { request: 2, page: 1 }, Effect::WindowState], "force loads a fresh window");
}

#[test]
fn win_idle_interest_offline_owner() {
    let mut twin = Twin::with(false, true);
    assert_eq!(twin.step(Input::InterestPlus { id: 1, force: false }), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    let mut no_session = Twin::with(true, false);
    assert_eq!(no_session.step(Input::InterestPlus { id: 1, force: false }), [Effect::WindowState]);
    assert_eq!(no_session.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_interest_minus_owner() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::InterestMinus { id: 1 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().interest(&key()).is_empty());
}

#[test]
fn win_idle_load_more_owner() {
    let mut twin = Twin::new();
    twin.cursor = true;
    twin.step(Input::InterestPlus { id: 1, force: false });
    run_load(&mut twin, 1);
    assert_eq!(twin.step(Input::LoadMore { id: 2 }), [Effect::Request { request: 2, page: 2 }]);
    assert_eq!(twin.phase(), Phase::Loading);
}

#[test]
fn win_idle_load_more_end_owner() {
    let mut twin = fresh();
    assert!(twin.step(Input::LoadMore { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_stale_watched() {
    let mut twin = fresh_watched();
    assert_eq!(twin.step(Input::Stale { newer_than_issue: true }), [Effect::Request { request: 2, page: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.phase(), Phase::Loading);
}

#[test]
fn win_idle_stale_unwatched() {
    let mut twin = fresh();
    assert!(Window::idle_fresh().stale_idle(false).is_empty(), "the reference gives no effect for an unwatched window");
    assert!(twin.step(Input::Stale { newer_than_issue: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.step(Input::InterestPlus { id: 2, force: false }), [Effect::Request { request: 2, page: 1 }, Effect::WindowState], "the next interest loads because the window is stale");
}

/// Drives a load to `Idle{last_error = network}`: three temporary network errors.
fn network_failed() -> Twin {
    let mut twin = Twin::new();
    twin.streams = true;
    twin.step(Input::InterestPlus { id: 1, force: false });
    for _ in 0..2 {
        assert_eq!(twin.step(Input::TransportError { request: 1, failure: Failure::Network }), [Effect::ScheduleRetry]);
        assert_eq!(twin.step(Input::RetryDue), [Effect::Request { request: 1, page: 1 }]);
    }
    assert_eq!(twin.step(Input::TransportError { request: 1, failure: Failure::Network }), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    twin
}

#[test]
fn win_idle_online_retry_owner() {
    let mut twin = network_failed();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, page: 1 }]);
}

#[test]
fn win_idle_online_owner() {
    let mut twin = fresh();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert!(twin.step(Input::Online(false)).is_empty());
    let mut failed = Twin::new();
    failed.step(Input::InterestPlus { id: 1, force: false });
    failed.step(Input::TransportError { request: 1, failure: Failure::Permanent });
    assert!(failed.step(Input::Online(true)).is_empty(), "a permanent error is not retried when the network returns");
}

#[test]
fn win_idle_models_reset() {
    let mut watched = fresh_watched();
    assert_eq!(watched.step(Input::ModelsReset { own_model: true }), [Effect::Request { request: 2, page: 1 }]);
    let mut unwatched = fresh();
    assert!(unwatched.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(unwatched.step(Input::InterestPlus { id: 2, force: false }), [Effect::Request { request: 2, page: 1 }, Effect::WindowState], "a reset window is no longer fresh");
}

#[test]
fn win_idle_models_other() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
    assert_eq!(twin.step(Input::InterestPlus { id: 2, force: false }), [Effect::WindowState], "the window of another model stays fresh");
}

#[test]
fn win_idle_stray_owner() {
    let mut twin = fresh();
    for input in [Input::Response { request: 99 }, Input::TransportError { request: 99, failure: Failure::Network }, Input::Committed { request: 99 }, Input::Rejected { request: 99, reason: Reject::Storage }] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_loading_join_owner() {
    let mut twin = loading();
    assert!(twin.step(Input::InterestPlus { id: 2, force: true }).is_empty());
    twin.step(Input::Response { request: 1 });
    let effects = twin.step(Input::Committed { request: 1 });
    assert_eq!(effects, [Effect::WindowState]);
}

#[test]
fn win_loading_leave_owner() {
    let mut twin = loading();
    twin.step(Input::InterestPlus { id: 2, force: false });
    assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_last_leave_owner() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_loading_response_owner() {
    let mut twin = loading();
    assert!(twin.step(Input::Response { request: 7 }).is_empty(), "an answer of another request is not taken");
    assert_eq!(twin.step(Input::Response { request: 1 }), [Effect::Commit { request: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_loading_temp_error_owner() {
    for (failure, retries) in [(Failure::Network, 2), (Failure::Server, 1)] {
        let mut twin = Twin::new();
        twin.step(Input::InterestPlus { id: 1, force: false });
        for _ in 0..retries {
            assert_eq!(twin.step(Input::TransportError { request: 1, failure }), [Effect::ScheduleRetry]);
            assert_eq!(twin.step(Input::RetryDue), [Effect::Request { request: 1, page: 1 }]);
        }
        assert_eq!(twin.step(Input::TransportError { request: 1, failure }), [Effect::WindowState], "attempts are used up");
    }
}

#[test]
fn win_loading_attempts_exhausted_owner() {
    let twin = network_failed();
    assert_eq!(twin.phase(), Phase::Failed, "no rows: Failed");
    let mut with_rows = fresh();
    with_rows.step(Input::InterestPlus { id: 2, force: true });
    for _ in 0..2 {
        with_rows.step(Input::TransportError { request: 2, failure: Failure::Network });
        with_rows.step(Input::RetryDue);
    }
    with_rows.step(Input::TransportError { request: 2, failure: Failure::Network });
    assert_eq!(with_rows.phase(), Phase::Loaded, "rows with an error: Loaded");
    assert!(with_rows.load_state().error.is_some());
}

#[test]
fn win_loading_error_owner() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::TransportError { request: 1, failure: Failure::Permanent }), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
}

#[test]
fn win_loading_offline_owner() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::Online(false)), [Effect::Cancel { request: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.phase(), Phase::Loading);
}

#[test]
fn win_loading_stale() {
    let mut twin = loading();
    assert!(twin.step(Input::Stale { newer_than_issue: true }).is_empty());
    twin.step(Input::Response { request: 1 });
    assert_eq!(twin.step(Input::Committed { request: 1 }), [Effect::Request { request: 2, page: 1 }, Effect::WindowState], "a mark made during the load loads once more after the answer");
}

#[test]
fn win_loading_stale_covered() {
    let mut twin = loading();
    assert!(twin.step(Input::Stale { newer_than_issue: false }).is_empty());
    twin.step(Input::Response { request: 1 });
    assert_eq!(twin.step(Input::Committed { request: 1 }), [Effect::WindowState], "a mark the request already covers does not load again");
}

#[test]
fn win_loading_models_reset() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Cancel { request: 1 }, Effect::Request { request: 2, page: 1 }]);
    assert!(twin.step(Input::Response { request: 1 }).is_empty(), "the answer of the cancelled request is not taken");
}

#[test]
fn win_loading_models_other() {
    let mut twin = loading();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_committed_owner() {
    let mut twin = landing();
    assert_eq!(twin.step(Input::Committed { request: 1 }), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Loaded);
    assert!(twin.load_state().loaded.is_some());
}

#[test]
fn win_landing_committed_again() {
    let mut twin = landing();
    assert!(twin.step(Input::Stale { newer_than_issue: true }).is_empty());
    assert_eq!(twin.step(Input::Committed { request: 1 }), [Effect::Request { request: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_stale_generation() {
    let mut twin = landing();
    twin.generation = 1;
    assert_eq!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }), [Effect::Request { request: 2, page: 1 }]);
}

#[test]
fn win_landing_no_session() {
    let mut twin = landing();
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::NoSession }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Ended));
}

#[test]
fn win_landing_storage() {
    let mut twin = landing();
    assert_eq!(twin.step(Input::Rejected { request: 1, reason: Reject::Storage }), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
}

#[test]
fn win_landing_join() {
    let mut twin = landing();
    assert!(twin.step(Input::InterestPlus { id: 2, force: false }).is_empty());
    assert_eq!(twin.step(Input::Committed { request: 1 }), [Effect::WindowState], "both callers get the outcome of the landing");
}

#[test]
fn win_landing_stale() {
    let mut twin = landing();
    assert!(twin.step(Input::Stale { newer_than_issue: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_landing_wait() {
    let mut twin = landing();
    for input in [Input::InterestMinus { id: 9 }, Input::Stale { newer_than_issue: false }, Input::Online(true), Input::ModelsReset { own_model: true }] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Landing), "the landing ends by the verdict of the store alone");
    }
}

#[test]
fn win_waiting_resume_owner() {
    let mut by_network = waiting();
    assert_eq!(by_network.step(Input::Online(true)), [Effect::Request { request: 1, page: 1 }]);
    let mut by_session = Twin::with(true, false);
    by_session.step(Input::InterestPlus { id: 1, force: false });
    assert_eq!(by_session.step(Input::EpochStarted), [Effect::Request { request: 1, page: 1 }]);
    assert_eq!(by_session.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_waiting_join_owner() {
    let mut twin = waiting();
    assert!(twin.step(Input::InterestPlus { id: 2, force: false }).is_empty());
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 1, page: 1 }]);
    assert_eq!(twin.state().interest(&key()), [1, 2]);
}

#[test]
fn win_waiting_leave_owner() {
    let mut twin = waiting();
    assert!(twin.step(Input::InterestMinus { id: 1 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_epoch_ended() {
    for mut twin in [loading(), landing(), waiting()] {
        let want_cancel = twin.stage() == Some(WindowStage::Loading);
        let effects = twin.step(Input::EpochEnded);
        assert_eq!(effects.is_empty(), !want_cancel);
        assert_eq!(twin.stage(), Some(WindowStage::Ended));
    }
    let mut idle = fresh();
    assert!(idle.step(Input::EpochEnded).is_empty());
    assert_eq!(idle.stage(), Some(WindowStage::Ended));
}

#[test]
fn win_closing_owner() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::Closing), [Effect::Cancel { request: 1 }, Effect::Drained]);
    assert_eq!(twin.stage(), Some(WindowStage::Ended));
    let mut idle = fresh();
    assert_eq!(idle.step(Input::Closing), [Effect::Drained]);
}

#[test]
fn win_ended_owner() {
    let mut twin = loading();
    twin.step(Input::EpochEnded);
    assert!(twin.step(Input::InterestPlus { id: 5, force: false }).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Ended));
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
    store_generation: Arc<AtomicU64>,
    revision: i64,
}

impl EffectRunner<Windows> for Executor {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Windows>) {
        let WindowsEffect::Window { effect, .. } = effect else { return };
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        match effect {
            WindowEffect::Request { request, page, .. } => {
                log.push(Seen::Requested(request));
                let answer = AnswerPage { page, rows: vec!["r1".into()], cursor: None };
                back.reply(stamp, w::WindowsInput::Transport(Transport { req_id: request, outcome: TransportOutcome::Response(answer) }));
            }
            WindowEffect::Cancel { request } => log.push(Seen::Cancelled(request)),
            WindowEffect::Commit { request, basis, .. } => {
                let current = self.store_generation.load(Ordering::SeqCst);
                if basis < current {
                    log.push(Seen::Rejected(request));
                    back.reply(stamp, w::WindowsInput::Rejected(Rejected { batch: request, reason: RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: current } }));
                } else {
                    self.revision += 1;
                    log.push(Seen::Landed { request, basis, revision: self.revision });
                    back.reply(stamp, w::WindowsInput::Committed(Committed { batch: request, revision: self.revision, tables: Vec::new() }));
                }
            }
            WindowEffect::Outcome { id, outcome } => log.push(Seen::Outcome(id, outcome)),
            WindowEffect::ScheduleRetry { .. } | WindowEffect::Announce(_) => {}
        }
    }
}

thread_local! {
    static SEEN: RefCell<Option<Rc<(Log<Seen>, Arc<AtomicU64>)>>> = const { RefCell::new(None) };
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
    let windows = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, true, true), Executor { log: log.clone(), store_generation: generation.clone(), revision: 0 });
    det.start_epoch(EPOCH);
    let callers = det.add::<Source<Windows>>("callers", SourceState { to: windows, script: vec![
        w::WindowsInput::InterestPlus { key: key(), id: 1, force: false, stream: false },
        w::WindowsInput::InterestPlus { key: key(), id: 2, force: false, stream: false },
    ] }, Silent);
    let marks = det.add::<Source<Windows>>("feed", SourceState { to: windows, script: vec![w::WindowsInput::Stale { key: key(), revision: 500 }] }, Silent);
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
fn win_prop_outcome_once_owner() {
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
}

/// P-WIN-01 at the end of a trace: nothing is left unanswered unless the window waits for the network.
#[test]
fn win_prop_outcome_complete_owner() {
    let report = explore(explore_rig, Limits { depth: 40, max_traces: 3_000 }, |det| {
        if !det.ready().is_empty() {
            return Ok(());
        }
        let (log, _) = seen();
        let events = snapshot(&log);
        let windows = det.state(Address::<Windows>::new(0)).ok_or("no state")?;
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
fn win_prop_loaded_rows_owner() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let events = snapshot(&log);
        for (at, event) in events.iter().enumerate() {
            if let Seen::Outcome(_, WindowOutcome::Loaded { revision }) = event {
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
fn win_prop_single_flight_owner() {
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
        for event in &events {
            if let Seen::Landed { basis, .. } = event {
                if *basis < current && events.iter().filter(|other| matches!(other, Seen::Landed { basis: later, .. } if *later >= current)).count() == 0 && !det.ready().is_empty() {
                    continue;
                }
            }
        }
        if det.ready().is_empty() && events.iter().any(|event| matches!(event, Seen::Rejected(_))) {
            let landed_new = events.iter().any(|event| matches!(event, Seen::Landed { basis, .. } if *basis >= current));
            let waiting = det.state(Address::<Windows>::new(0)).is_some_and(|windows| windows.stage(&key()) == Some(WindowStage::Waiting));
            if !landed_new && !waiting {
                return Err("a refused answer was not asked again".into());
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-05: going online rereads once, and only windows with interest and a network error.
#[test]
fn win_prop_online_once_owner() {
    let mut twin = network_failed();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, page: 1 }]);
    assert!(twin.step(Input::Online(true)).is_empty(), "a second signal does not read again");
    let mut quiet = fresh();
    assert!(quiet.step(Input::Online(true)).is_empty(), "a window without an error is not reread");
}

/// P-WIN-06: no input, no request and no schedule; a retry is scheduled only while online.
#[test]
fn win_prop_idle_owner() {
    let twin = fresh();
    assert_eq!(twin.det.ready().len(), 0);
    assert!(take(&twin.log).is_empty());
    let mut offline = Twin::new();
    offline.step(Input::InterestPlus { id: 1, force: false });
    offline.step(Input::Online(false));
    let effects = offline.step(Input::TransportError { request: 1, failure: Failure::Network });
    assert!(!effects.contains(&Effect::ScheduleRetry), "no retry is scheduled offline");
}

/// P-WIN-07: the last `Interest-` cancels the request and the answer of the cancelled request is not committed.
#[test]
fn win_prop_cancel_owner() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1 }]);
    assert!(twin.step(Input::Response { request: 1 }).is_empty(), "no Commit for the cancelled request");
}

/// The dictionary: windows of two keys are independent, the answer goes to the key that owns the request, `Closing` gives one `Drained`.
#[test]
fn win_dictionary_owner() {
    let mut det = DetScheduler::new(5);
    let log = new_log();
    let windows = det.add::<Windows>("windows", w::WindowsState::new(EPOCH, true, true), Recorder::<Windows>::new(&log));
    det.start_epoch(EPOCH);
    let other = WindowKey { model: "Chat".into(), name: "room".into() };
    for (key, id) in [(key(), 1), (other.clone(), 2)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::InterestPlus { key, id, force: false, stream: false });
    }
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Transport(Transport { req_id: 2, outcome: TransportOutcome::Response(AnswerPage { page: 1, rows: vec!["c1".into()], cursor: None }) }));
    det.run_until_quiet(100).expect("quiet");
    let state = det.state(windows).expect("state");
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Loading), Some(WindowStage::Landing)), "the answer of request 2 reaches the key that owns it");
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Chat".into()] }));
    det.close();
    det.run_until_quiet(100).expect("quiet");
    let effects = take(&log);
    let drained = effects.iter().filter(|effect| matches!(effect, WindowsEffect::Drained(_))).count();
    assert_eq!(drained, 1, "one Drained for all windows");
    assert!(effects.iter().any(|effect| matches!(effect, WindowsEffect::Window { key, effect: WindowEffect::Cancel { request: 1 } } if *key == self::key())), "the loading window is cancelled at Closing");
    let state = det.state(windows).expect("state");
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Ended), Some(WindowStage::Ended)));
}
