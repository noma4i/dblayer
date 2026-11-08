use crate::owners::read::deliveries::Deliveries;
use crate::owners::read::windows::{Goal, WindowKey, Windows, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::store::StoreWriter;
use crate::runtime::{Address, StateOwner};

const NOW: i64 = 1_000_000;

fn state() -> WindowsState {
    WindowsState::new(1, Some(true), WindowsPeers { store: Address::<StoreWriter>::new(0), deliveries: Address::<Deliveries>::new(1) })
}

fn key() -> WindowKey { WindowKey { model: "Item".into(), name: "items".into() } }

fn load(id: u64) -> WindowsInput { WindowsInput::Load { key: key(), id, goal: Goal::First, force: false, store_revision: 0, now: NOW } }

fn permutations(items: &[u64]) -> Vec<Vec<u64>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|index| {
        let mut rest = items.to_vec();
        let head = rest.remove(index);
        permutations(&rest).into_iter().map(move |mut tail| { tail.insert(0, head); tail })
    }).collect()
}

/// boundary_load_cancel_orders (P-BND-01): whatever the order in which the loads of reserved calls arrive, the window registers their interests in the order of the ids.
#[test]
fn loads_register_in_the_order_of_their_reserved_ids() {
    for order in permutations(&[1, 2, 3]) {
        let mut state = state();
        for id in [1, 2, 3] { let _ = Windows::step(&mut state, WindowsInput::Reserve { id }); }
        let mut arrived = Vec::new();
        for id in order.clone() {
            arrived.push(id);
            let _ = Windows::step(&mut state, load(id));
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
            for id in [1, 2, 3] { let _ = Windows::step(&mut state, WindowsInput::Reserve { id }); }
            for id in order { let _ = Windows::step(&mut state, load(id)); }
            assert_eq!(state.waiters(&key()), [1], "{ends:?} {order:?}");
            let _ = Windows::step(&mut state, ends.clone());
            assert_eq!(state.waiters(&key()), [1, 3], "{ends:?} {order:?}");
        }
    }
}
