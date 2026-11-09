use super::*;


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
fn win_landing_stale_gap_covered() {
    let mut twin = landing();
    twin.step(Input::Online(true));
    assert!(twin.step(Input::StaleGap).is_empty());
    assert!(twin.reference.reread_due, "the request began before the return of the network");
    let mut covered = Twin::new();
    covered.step(Input::Online(true));
    covered.call(1, Goal::First);
    covered.respond(None);
    assert_eq!(covered.stage(), Some(WindowStage::Landing));
    assert!(covered.step(Input::StaleGap).is_empty());
    assert!(!covered.reference.reread_due, "the landing began after the return of the network");
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
fn win_wait_refresh_result() {
    let mut success = fresh();
    let effects = success.step(Input::Load { id: 9, goal: Goal::First, force: true });
    assert_eq!(requests(&effects), 1);
    assert!(!success.outcomes.iter().any(|(id, _)| *id == 9));
    success.finish(None);
    assert!(success.outcomes.contains(&(9, Result3::Loaded)));

    let mut failed = fresh();
    failed.step(Input::Load { id: 9, goal: Goal::First, force: true });
    failed.fail(Failure::Permanent);
    assert!(failed.outcomes.contains(&(9, Result3::Failed)));

    let mut offline = fresh();
    offline.step(Input::Online(false));
    take(&offline.log);
    offline.post(w::WindowsInput::LoadNoWait { key: key(), id: 9, goal: goal_of(Goal::First), force: true, store_revision: 0, now: offline.now, verb: "refresh" });
    let (_, answered) = offline.split(take(&offline.log));
    assert!(answered.contains(&(9, Result3::Failed)), "refresh goes as LoadNoWait and the owner answers Offline at once (D-252)");

    for closing in [false, true] {
        let mut twin = fresh();
        twin.step(Input::Load { id: 9, goal: Goal::First, force: true });
        take(&twin.log);
        if closing { twin.det.close(); } else { twin.det.end_epoch(EPOCH, Ended::SessionReset); }
        twin.run();
        let effects = take(&twin.log);
        let answers: Vec<_> = effects.iter().filter_map(|effect| match effect { Emitted::Run(WindowsEffect::Window { effect: WindowEffect::Outcome { id: 9, outcome }, .. }) => Some(format!("{outcome:?}")), _ => None }).collect();
        let cause = if closing { "Closed" } else { "SessionEnded" };
        assert_eq!(answers.len(), 1);
        assert!(answers[0].contains(cause), "{:?}", answers);
    }
}

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
    twin.post_call(w::WindowsInput::Load { key: nodes.clone(), id: 1, goal: w::Goal::First, force: false, store_revision: 0, now: NOW });
    assert!(matches!(runs(take(&twin.log)).as_slice(), [WindowsEffect::Window { effect: WindowEffect::Request { page: 1, .. }, .. }]));
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["m1".into()], None), now: NOW });
    twin.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 11, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert!(runs(take(&twin.log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 1, outcome: WindowOutcome::Loaded { revision: 11, .. } }, .. })));
    twin.post_call(w::WindowsInput::Load { key: nodes, id: 2, goal: w::Goal::First, force: false, store_revision: 0, now: NOW });
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
pub(super) enum Seen {
    Requested(u64),
    Cancelled(u64),
    Landed { request: u64, basis: u64, revision: i64 },
    Rejected(u64),
    Outcome(u32, WindowOutcome),
}

pub(super) struct Executor {
    pub(super) log: Log<Seen>,
}

impl EffectRunner<Spy<Windows>> for Executor {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Spy<Windows>>) {
        let effect = match effect {
            WindowsEffect::Window { effect, .. } => effect,
            WindowsEffect::ReadWindow { key, read_id } => return back.reply(stamp, w::WindowsInput::WindowRead { key, read_id, result: Ok(None) }),
            _ => return,
        };
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        match effect {
            WindowEffect::Request { request, attempt, page, .. } => {
                log.push(Seen::Requested(request));
                back.reply(stamp, w::WindowsInput::Response { request, attempt, page: AnswerPage::new(page, vec!["r1".into()], None), now: NOW });
            }
            WindowEffect::Cancel { request, .. } => log.push(Seen::Cancelled(request)),
            WindowEffect::Outcome { id, outcome } => log.push(Seen::Outcome(u32::try_from(id).expect("a test id"), outcome)),
            WindowEffect::DerivePreload { .. } => {}
        }
    }
}

pub(super) type Shared = Rc<(Log<Seen>, Arc<AtomicU64>)>;

thread_local! {
    static SEEN: RefCell<Option<Shared>> = const { RefCell::new(None) };
}

pub(super) fn seen() -> (Log<Seen>, Arc<AtomicU64>) {
    let shared = SEEN.with(|slot| slot.borrow().clone()).expect("a rig was built");
    (shared.0.clone(), shared.1.clone())
}

/// Two callers, a stale mark, the network going down and up, a reset of the model: each is a source on its own.
pub(super) struct WindowStore;

impl StateOwner for WindowStore {
    const EPOCHED: bool = false;
    type State = StoreStubState<Windows>;
    type Input = StoreInput;
    type Effect = crate::spec::support::NoEffect;

    fn step(state: &mut Self::State, input: Self::Input) -> Effects<Self> {
        if let StoreInput::Pin { request_id, .. } = input {
            return vec![RuntimeEffect::Send(Outbound::new(state.to, w::WindowsInput::Pinned { request_id, base_revision: u64::try_from(state.revision).unwrap() }))];
        }
        StoreStub::<Windows>::step(state, input).into_iter().map(|effect| match effect {
            RuntimeEffect::Send(outbound) => RuntimeEffect::Send(outbound),
            RuntimeEffect::Run(effect) => match effect {},
            _ => unreachable!(),
        }).collect()
    }

    fn tick(_: Time) -> Self::Input { StoreInput::Idle }
    fn epoch_ended(epoch: u64, cause: Ended) -> Self::Input { StoreWriter::epoch_ended(epoch, cause) }
    fn closing() -> Self::Input { StoreInput::Idle }
}

pub(super) fn explore_rig() -> DetScheduler {
    let mut det = DetScheduler::new(1);
    let log = new_log();
    let generation = Arc::new(AtomicU64::new(0));
    let windows_slot = Address::<Windows>::new(WINDOWS);
    let noted = log.clone();
    det.add::<WindowStore>("store", StoreStubState {
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
    let load = |id: u64| w::WindowsInput::Load { key: key(), id, goal: w::Goal::First, force: false, store_revision: 0, now: NOW };
    let callers = det.add::<Source<Windows>>("callers", SourceState { to: windows, script: vec![load(1), load(2)] }, Silent);
    let marks = det.add::<Source<Windows>>("feed", SourceState { to: windows, script: vec![w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: w::StaleCause::Change }] }, Silent);
    let network = det.add::<Source<Windows>>("network", SourceState { to: windows, script: vec![w::WindowsInput::Online { online: Online(false), online_seq: 0, now: NOW }, w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW }] }, Silent);
    let lifecycle = det.add::<Reset<Windows>>("lifecycle", ResetState { to: windows, input: w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Moment".into()] }), generation: 1 }, StoreGeneration(generation.clone()));
    for source in [callers, marks, network] {
        det.post(source, EpochStamp::Control, SourceInput::Go);
    }
    det.post(lifecycle, EpochStamp::Control, SourceInput::Go);
    SEEN.with(|slot| *slot.borrow_mut() = Some(Rc::new((log, generation))));
    det
}

pub(super) const LIMITS: Limits = Limits { depth: 4, max_traces: 30_000 };

pub(super) fn open_requests(events: &[Seen]) -> usize {
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
