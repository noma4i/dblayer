use crate::owners::read::deliveries::Deliveries;
use crate::owners::contract::{EpochEnded, EpochStarted, Online};
use crate::runtime::Cause;
use crate::owners::read::windows::{WindowOutcome, AnswerPage, Goal, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::{StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Address, Effect, StateOwner};

const NOW: i64 = 1_000_000;

fn state() -> WindowsState {
    WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) })
}

fn load(state: &mut WindowsState) -> Vec<Effect<Windows>> {
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    let _ = Windows::step(state, WindowsInput::Load { key: key.clone(), id: 1, goal: Goal::First, force: false, store_revision: 0, now: NOW });
    Windows::step(state, WindowsInput::WindowRead { key, read_id: 1, result: Ok(None) })
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
    let pinned = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 12 });
    assert!(pinned.iter().any(|effect| matches!(effect, Effect::Run(WindowsEffect::Window { effect: WindowEffect::Request { request: 1, .. }, .. }))));
    let landed = Windows::step(&mut state, WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec![], None), now: NOW });
    let inputs = store_inputs(landed);
    assert!(matches!(inputs.as_slice(), [StoreInput::Commit(batch)] if matches!(batch.ops.as_slice(), [StoreOp::Window { base_revision: 12, .. }])));
}

#[test]
fn win_pinned_stray() {
    let mut state = state();
    let stray = Windows::step(&mut state, WindowsInput::Pinned { request_id: 7, base_revision: 2 });
    assert!(matches!(store_inputs(stray).as_slice(), [StoreInput::Unpin { request_id: 7 }]));
    let _ = load(&mut state);
    let _ = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 2 });
    let duplicate = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 3 });
    assert!(duplicate.is_empty());
}

fn no_wait(state: &mut WindowsState) -> Vec<Effect<Windows>> {
    let key = WindowKey { model: "Item".into(), name: "items".into() };
    Windows::step(state, WindowsInput::LoadNoWait { key, id: 7, goal: Goal::First, force: true, store_revision: 0, now: NOW, verb: "refresh" })
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
    let online = |value: bool| WindowsInput::Online { online: Online(value), online_seq: 1, now: NOW };
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    assert!(no_wait(&mut state).is_empty(), "no answer and no request before the snapshot");
    assert_eq!(outcome_of(&Windows::step(&mut state, online(false))), Some(WindowOutcome::Offline { message: "Item.items refresh needs the network".into() }), "Online(false) answers the waiting call");
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    no_wait(&mut state);
    let effects = Windows::step(&mut state, online(true));
    assert_eq!(outcome_of(&effects), None, "Online(true) releases the call as a load");
    assert!(!effects.is_empty(), "the load of the window starts");
    let mut state = WindowsState::new(1, None, WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) });
    no_wait(&mut state);
    assert_eq!(outcome_of(&Windows::step(&mut state, WindowsInput::Closing)), Some(WindowOutcome::Interrupted), "a waiting call ends with the owner");
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
            let mut effects = Windows::step(&mut state, WindowsInput::EpochEnded(EpochEnded { epoch: 1, cause: Cause::SessionReset }));
            let online = WindowsInput::Online { online: Online(network), online_seq: 1, now: NOW };
            for event in order.iter() {
                effects.extend(match event {
                    0 => Windows::step(&mut state, WindowsInput::EpochStarted(EpochStarted { epoch: 2, identity: Some(1), generation: 0 })),
                    1 | 2 => Windows::step(&mut state, online.clone()),
                    _ => no_wait(&mut state),
                });
            }
            assert_eq!(state.held(), 0, "order {order:?} network {network}: no held call is left once the snapshot came");
            let outcomes = effects.iter().filter(|effect| outcome_of(std::slice::from_ref(effect)).is_some()).count();
            assert_eq!(outcomes, usize::from(!network), "order {order:?} network {network}: only a call without network is answered before the close");
            effects.extend(Windows::step(&mut state, WindowsInput::Closing));
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
        let mut state = state().with_local_request_ids();
        no_wait(&mut state);
        let _ = Windows::step(&mut state, WindowsInput::WindowRead { key: key.clone(), read_id: 1, result: Ok(None) });
        let _ = Windows::step(&mut state, WindowsInput::Pinned { request_id: 1, base_revision: 12 });
        let effects = Windows::step(&mut state, WindowsInput::Failure { request: 1, attempt: 1, error: RequestError { class: ErrorClass::Permanent, failure: failure.clone() } });
        assert_eq!(outcome_of(&effects), Some(WindowOutcome::Failed { failure: failure.clone() }), "{failure:?}");
    }
}
