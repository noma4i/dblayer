use crate::check::model::Owner;

use super::sets_reference_tests::red_of;
use super::snapshot_world_tests::{SnapshotOwner, CASES, LANDS};

const SEEDS: u64 = 3;

fn red() -> Vec<String> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = CASES
            .iter()
            .flat_map(|case| (1..=SEEDS).map(move |seed| scope.spawn(move || {
                let owner = SnapshotOwner::new(*case, seed);
                red_of(&owner, seed * 7919, owner.alphabet().len() * 2).map(|found| format!("{}: {found}", case.name))
            })))
            .collect();
        handles.into_iter().filter_map(crate::test_wait::join_within).collect()
    })
}

#[test]
fn store_snapshot_lands_to_one_state() {
    let found = red();
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}
