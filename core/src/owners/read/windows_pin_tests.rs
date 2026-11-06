use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{AnswerPage, Goal, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::{StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Address, Effect, StateOwner};

fn state() -> WindowsState {
    WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) })
}

fn load(state: &mut WindowsState) -> Vec<Effect<Windows>> {
    Windows::step(state, WindowsInput::Load { key: WindowKey { model: "Item".into(), name: "items".into() }, id: 1, goal: Goal::First, force: false, store_revision: 0 })
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
    let landed = Windows::step(&mut state, WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec![], None) });
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
