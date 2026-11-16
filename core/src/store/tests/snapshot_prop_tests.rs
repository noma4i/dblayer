use crate::check::model::Owner;

use super::sets_reference_tests::red_of;
use super::snapshot_world_tests::{SnapshotOwner, CASES};

const SEEDS: u64 = 3;

fn red(case: usize) -> Vec<String> {
    let case = CASES[case];
    std::thread::scope(|scope| {
        let handles: Vec<_> = (1..=SEEDS)
            .map(|seed| scope.spawn(move || {
                let owner = SnapshotOwner::new(case, seed);
                red_of(&owner, seed * 7919, owner.alphabet().len() * 2).map(|found| format!("{}: {found}", case.name))
            }))
            .collect();
        handles.into_iter().filter_map(crate::test_wait::join_within).collect()
    })
}

fn lands_to_one_state(case: usize) {
    let found = red(case);
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}

#[test]
fn store_snapshot_lands_to_one_state_sets_full_rows() { lands_to_one_state(0); }

#[test]
fn store_snapshot_lands_to_one_state_sets_partial_projection() { lands_to_one_state(1); }

#[test]
fn store_snapshot_lands_to_one_state_sets_related_parent() { lands_to_one_state(2); }

#[test]
fn store_snapshot_lands_to_one_state_window_members() { lands_to_one_state(3); }

#[test]
fn store_snapshot_lands_to_one_state_window_order() { lands_to_one_state(4); }

#[test]
fn store_snapshot_lands_to_one_state_tombstone() { lands_to_one_state(5); }
