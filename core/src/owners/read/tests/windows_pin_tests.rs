use crate::owners::read::deliveries::{Deliveries, EndCause};
use crate::owners::contract::{EpochEnded, EpochStarted, Online};
use crate::runtime::Cause;
use crate::owners::read::windows::{WindowOutcome, AnswerPage, Goal, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::{StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Address, Effect, StateOwner};

fn state() -> WindowsState {
    WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) }).with_local_request_ids()
}

fn load(state: &mut WindowsState) -> Vec<Effect<Windows>> {
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    let _ = Windows::step(state, WindowsInput::Load { key: key.clone(), id: 1, goal: Goal::First, force: false, store_revision: 0 }, crate::runtime::Time(0));
    Windows::step(state, WindowsInput::WindowRead { key, read_id: 1, result: Ok(None) }, crate::runtime::Time(0))
}

fn store_inputs(effects: Vec<Effect<Windows>>) -> Vec<StoreInput> {
    effects.into_iter().filter_map(|effect| match effect {
        Effect::Send(outbound) if outbound.to == 0 => outbound.payload.into_any().downcast::<StoreInput>().ok().map(|input| *input),
        _ => None,
    }).collect()
}

#[test]
fn win_loading_pinned() {
    let mut state = state();
    let initial = load(&mut state);
    assert!(!initial.iter().any(|effect| matches!(effect, Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { .. }, .. }))));
    assert!(matches!(store_inputs(initial).as_slice(), [StoreInput::Pin { epoch: 1, request_id: 1 }]));
    let pinned = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 12 }, crate::runtime::Time(0));
    assert!(pinned.iter().any(|effect| matches!(effect, Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { request: 1, .. }, .. }))));
    let landed = Windows::step(&mut state, WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec![], None) }, crate::runtime::Time(0));
    let inputs = store_inputs(landed);
    assert!(matches!(inputs.as_slice(), [StoreInput::Commit(batch)] if matches!(batch.ops.as_slice(), [StoreOp::Window { base_revision: 12, .. }])));
}

#[test]
fn win_pinned_stray() {
    let mut state = state();
    let stray = Windows::step(&mut state, WindowsInput::Pinned { request_id: 7, base_revision: 2 }, crate::runtime::Time(0));
    assert!(matches!(store_inputs(stray).as_slice(), [StoreInput::Unpin { request_id: 7 }]));
    let _ = load(&mut state);
    let _ = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 2 }, crate::runtime::Time(0));
    let duplicate = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 3 }, crate::runtime::Time(0));
    assert!(duplicate.is_empty());
}

fn no_wait(state: &mut WindowsState) -> Vec<Effect<Windows>> {
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    Windows::step(state, WindowsInput::LoadNoWait { key, id: 7, goal: Goal::First, force: true, store_revision: 0, verb: "refresh" }, crate::runtime::Time(0))
}

fn outcome_of(effects: &[Effect<Windows>]) -> Option<WindowOutcome> {
    effects.iter().find_map(|effect| match effect {
        Effect::Run(WindowsEffect::Window { effect: WindowEffect::Outcome { id: 7, outcome }, .. }) => Some(outcome.clone()),
        _ => None,
    })
}

#[test]
fn win_load_nowait_offline() {
    let mut state = WindowsState::new(1, Some(false), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    let effects = no_wait(&mut state);
    assert_eq!(outcome_of(&effects), Some(WindowOutcome::Offline { message: "Item.items refresh needs the network".into() }), "the owner answers Offline by its own online");
}

#[test]
fn win_load_nowait_before_snapshot_waits() {
    let online = |value: bool| WindowsInput::Online { online: Online(value), online_seq: 1 };
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    assert!(no_wait(&mut state).is_empty(), "no answer and no request before the snapshot");
    assert_eq!(outcome_of(&Windows::step(&mut state, online(false), crate::runtime::Time(0))), Some(WindowOutcome::Offline { message: "Item.items refresh needs the network".into() }), "Online(false) answers the waiting call");
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    no_wait(&mut state);
    let effects = Windows::step(&mut state, online(true), crate::runtime::Time(0));
    assert_eq!(outcome_of(&effects), None, "Online(true) releases the call as a load");
    assert!(!effects.is_empty(), "the load of the window starts");
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    no_wait(&mut state);
    assert_eq!(outcome_of(&Windows::step(&mut state, WindowsInput::Closing, crate::runtime::Time(0))), Some(WindowOutcome::Ended { cause: EndCause::Closed }), "a waiting call ends with the owner");
}

/// win_load_nowait_before_snapshot_waits over every order of the second epoch: EpochStarted(0), the addressed snapshot (1), the broadcast (2), the call (3).
/// The snapshot follows its request and a call stamped with the epoch follows its start.
#[test]
fn win_prop_nowait_second_epoch_orders() {
    let valid = crate::test_wait::orders_of(&[0usize, 1, 2, 3], &[(0, 1), (0, 3)]);
    assert_eq!(valid.len(), 8);
    for network in [true, false] {
        for order in &valid {
            let mut state = WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
            let mut effects = Windows::step(&mut state, WindowsInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }), crate::runtime::Time(0));
            let online = WindowsInput::Online { online: Online(network), online_seq: 1 };
            for event in order.iter() {
                effects.extend(match event {
                    0 => Windows::step(&mut state, WindowsInput::EpochStarted(EpochStarted { epoch: 2, identity: Some(1), generation: 0 }), crate::runtime::Time(0)),
                    1 | 2 => Windows::step(&mut state, online.clone(), crate::runtime::Time(0)),
                    _ => no_wait(&mut state),
                });
            }
            assert_eq!(state.held(), 0, "order {order:?} network {network}: no held call is left once the snapshot came");
            let outcomes = effects.iter().filter(|effect| outcome_of(std::slice::from_ref(effect)).is_some()).count();
            assert_eq!(outcomes, usize::from(!network), "order {order:?} network {network}: only a call without network is answered before the close");
            effects.extend(Windows::step(&mut state, WindowsInput::Closing, crate::runtime::Time(0)));
            let total = effects.iter().filter(|effect| outcome_of(std::slice::from_ref(effect)).is_some()).count();
            assert_eq!(total, 1, "order {order:?} network {network}: exactly one outcome for the call");
        }
    }
}

/// win_loading_error (P-BND-04, D-195): the window ends with the typed failure of the request as it came: a source-less failure is no `Storage`.
#[test]
fn win_loading_error_keeps_the_type_of_the_failure() {
    use crate::ffi::DbError;
    use crate::owners::contract::{ErrorClass, RequestError};
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    for failure in [DbError::Invalid { detail: "bad".into() }, DbError::Storage { detail: "disk".into() }, DbError::Dropped { detail: "gone".into() }, DbError::Runtime { detail: "task".into() }, DbError::Interrupted { detail: "epoch".into() }] {
        let mut state = state();
        no_wait(&mut state);
        let _ = Windows::step(&mut state, WindowsInput::WindowRead { key: key.clone(), read_id: 1, result: Ok(None) }, crate::runtime::Time(0));
        let _ = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 12 }, crate::runtime::Time(0));
        let effects = Windows::step(&mut state, WindowsInput::Failure { request: 1, attempt: 1, error: RequestError { class: ErrorClass::Permanent, failure: failure.clone() } }, crate::runtime::Time(0));
        assert_eq!(outcome_of(&effects), Some(WindowOutcome::Failed { failure: failure.clone() }), "{failure:?}");
    }
}

/// P-WIN-34: a change that arrives while the request is on its way marks the next load, the batch of the load in flight keeps the source of its own load.
#[test]
fn win_change_during_the_request_marks_the_next_load() {
    use crate::owners::read::windows::{StaleCause, StaleKeys};
    use crate::schema::RowSource::{NodeReread, Page};
    let key = WindowKey::relation("Moment", "node", &[(crate::schema::NODE_PARAM.to_string(), serde_json::json!("n1"))].into_iter().collect());
    let now = crate::runtime::Time(0);
    let mut state = state();
    let _ = Windows::step(&mut state, WindowsInput::InterestPassive { key: key.clone(), id: 9 }, now);
    let _ = Windows::step(&mut state, WindowsInput::WindowRead { key: key.clone(), read_id: 1, result: Ok(None) }, now);
    let mut pending = store_inputs(Windows::step(&mut state, WindowsInput::Load { key: key.clone(), id: 1, goal: Goal::First, force: true, store_revision: 0 }, now));
    let mut sources = Vec::new();
    for change_in_flight in [true, false] {
        let request = pending.iter().find_map(|input| match input { StoreInput::Pin { request_id, .. } => Some(*request_id), _ => None }).expect("the load pins its request");
        let asked = Windows::step(&mut state, WindowsInput::Pinned { request_id: request, base_revision: 1 }, now);
        let attempt = asked.iter().find_map(|effect| match effect { Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { attempt, .. }, .. }) => Some(*attempt), _ => None }).expect("the pinned load asks the network");
        if change_in_flight {
            let _ = Windows::step(&mut state, WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: StaleCause::Change }, now);
        }
        let landed = store_inputs(Windows::step(&mut state, WindowsInput::Response { request, attempt, page: AnswerPage::new(1, vec![], None) }, now));
        let batch = landed.iter().find_map(|input| match input { StoreInput::Commit(batch) => Some(batch.clone()), _ => None }).expect("the answer lands");
        sources.extend(batch.ops.iter().filter_map(|op| match op { StoreOp::Window { source, .. } => Some(*source), _ => None }));
        let committed = crate::owners::contract::Committed { batch: batch.id, revision: 5, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() };
        pending = store_inputs(Windows::step(&mut state, WindowsInput::Committed(committed), now));
    }
    assert_eq!(sources, vec![Page, NodeReread], "the load asked before the change lands as a page, the reread asked after it carries the mark");
}

/// P-WIN-34: a marked load keeps its mark through a network retry, an offline pause and a last-reader close, until a batch lands.
#[test]
fn win_marked_load_keeps_its_mark_until_it_lands() {
    use crate::ffi::DbError;
    use crate::owners::contract::{ErrorClass, RequestError};
    use crate::owners::read::windows::{StaleCause, StaleKeys};
    use crate::schema::RowSource::NodeReread;
    struct Run {
        state: WindowsState,
        key: WindowKey,
        pin: Option<u64>,
        loading: u64,
        request: Option<(u64, u32)>,
        sources: Vec<crate::schema::RowSource>,
        reads: u64,
    }
    impl Run {
        fn new() -> Run {
            let key = WindowKey::relation("Moment", "node", &[(crate::schema::NODE_PARAM.to_string(), serde_json::json!("n1"))].into_iter().collect());
            let mut run = Run { state: state(), key, pin: None, loading: 0, request: None, sources: Vec::new(), reads: 0 };
            run.interest();
            run
        }
        fn step(&mut self, input: WindowsInput) {
            let effects = Windows::step(&mut self.state, input, crate::runtime::Time(0));
            for effect in &effects {
                if let Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { request, attempt, .. }, .. }) = effect {
                    self.request = Some((*request, *attempt));
                }
            }
            for input in store_inputs(effects) {
                match input {
                    StoreInput::Pin { request_id, .. } => self.pin = Some(request_id),
                    StoreInput::Commit(batch) => self.sources.extend(batch.ops.iter().filter_map(|op| match op { StoreOp::Window { source, .. } => Some(*source), _ => None })),
                    _ => {}
                }
            }
        }
        fn interest(&mut self) {
            self.reads += 1;
            self.step(WindowsInput::InterestPassive { key: self.key.clone(), id: 8 + self.reads });
            self.step(WindowsInput::WindowRead { key: self.key.clone(), read_id: self.reads, result: Ok(None) });
        }
        fn load(&mut self) {
            self.step(WindowsInput::Load { key: self.key.clone(), id: self.reads, goal: Goal::First, force: true, store_revision: 0 });
            self.loading = self.pin.take().expect("the load pins its request");
            self.step(WindowsInput::Pinned { request_id: self.loading, base_revision: 1 });
            assert!(self.request.is_some(), "the pinned load asks the network");
        }
        fn change(&mut self) { self.step(WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: StaleCause::Change }); }
        fn network_down_once(&mut self) {
            let (request, attempt) = self.request.take().expect("a request is out");
            self.step(WindowsInput::Failure { request, attempt, error: RequestError { class: ErrorClass::Network, failure: DbError::Dropped { detail: "down".into() } } });
            assert!(self.request.is_none(), "the failed request waits for its retry");
        }
        fn retry(&mut self) {
            self.step(WindowsInput::RetryDue { request: self.loading });
            assert!(self.request.is_some(), "the retry asks the network again");
        }
        fn answer(&mut self) {
            let (request, attempt) = self.request.take().expect("a request is out");
            self.step(WindowsInput::Response { request, attempt, page: AnswerPage::new(1, vec![], None) });
        }
    }
    type Order = fn(&mut Run);
    let cases: Vec<(&str, Order)> = vec![
        ("a: Stale(Change), load, a temporary refusal, the retry, the answer", |run| {
            run.change();
            run.load();
            run.network_down_once();
            run.retry();
            run.answer();
        }),
        ("b: load, a temporary refusal, Stale(Change) in the pause, the retry, the answer", |run| {
            run.load();
            run.network_down_once();
            run.change();
            run.retry();
            run.answer();
        }),
        ("c: Stale(Change), load, request out, Online(false), Online(true), the answer of the new load", |run| {
            run.change();
            run.load();
            run.step(WindowsInput::Online { online: Online(false), online_seq: 1 });
            run.step(WindowsInput::Online { online: Online(true), online_seq: 2 });
            let request_id = run.pin.take().expect("the reconnect load pins its request");
            run.step(WindowsInput::Pinned { request_id, base_revision: 1 });
            run.answer();
        }),
        ("d: Stale(Change), load, request out, the last reader leaves, a new interest and load, the answer", |run| {
            run.change();
            run.load();
            run.step(WindowsInput::InterestMinus { key: run.key.clone(), id: 9 });
            run.interest();
            run.step(WindowsInput::Load { key: run.key.clone(), id: run.reads, goal: Goal::First, force: true, store_revision: 0 });
            run.answer();
        }),
    ];
    let mut wrong = Vec::new();
    for (case, order) in cases {
        let mut run = Run::new();
        order(&mut run);
        if run.sources != vec![NodeReread] {
            wrong.push(format!("{case}: landing sources {:?}, the spec asks [NodeReread]", run.sources));
        }
    }
    assert!(wrong.is_empty(), "{} wrong cases:\n{}", wrong.len(), wrong.join("\n"));
}
