use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{Goal, WindowKey, Windows, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::StoreWriter;
use crate::runtime::{Address, StateOwner};

use crate::test_wait::permutations;

fn state() -> WindowsState {
    WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) })
}

fn key() -> WindowKey { WindowKey { model: "Item".into(), name: "items".into() } }

fn load(id: u64) -> WindowsInput { WindowsInput::Load { key: key(), id, goal: Goal::First, force: false, store_revision: 0 } }

/// boundary_load_cancel_orders (P-BND-01): whatever the order in which the loads of reserved calls arrive, the window registers their interests in the order of the ids.
#[test]
fn boundary_load_cancel_orders() {
    for order in permutations(&[1, 2, 3]) {
        let mut state = state();
        for id in [1, 2, 3] { let _ = Windows::step(&mut state, WindowsInput::Reserve { id }, crate::runtime::Time(0)); }
        let mut arrived = Vec::new();
        for id in order.clone() {
            arrived.push(id);
            let _ = Windows::step(&mut state, load(id), crate::runtime::Time(0));
            let run = (1..=3).take_while(|next| arrived.contains(next)).count() as u64;
            assert_eq!(state.waiters(&key()), (1..=run).collect::<Vec<_>>(), "loads arrived as {order:?}, after {arrived:?}");
        }
    }
}

/// boundary_load_cancel_orders (P-BND-01): a call that ends before its load came, by `Unreserve` or by `InterestMinus`, frees the loads held behind it and keeps the other interests.
#[test]
fn a_call_that_ends_before_its_load_frees_the_loads_behind_it() {
    for ends in [WindowsInput::Unreserve { id: 2 }, WindowsInput::InterestMinus { key: key(), id: 2 }] {
        for order in [[3, 1], [1, 3]] {
            let mut state = state();
            for id in [1, 2, 3] { let _ = Windows::step(&mut state, WindowsInput::Reserve { id }, crate::runtime::Time(0)); }
            for id in order { let _ = Windows::step(&mut state, load(id), crate::runtime::Time(0)); }
            assert_eq!(state.waiters(&key()), [1], "{ends:?} {order:?}");
            let _ = Windows::step(&mut state, ends.clone(), crate::runtime::Time(0));
            assert_eq!(state.waiters(&key()), [1, 3], "{ends:?} {order:?}");
        }
    }
}
