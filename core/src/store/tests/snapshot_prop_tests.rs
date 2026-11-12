use crate::check::explore::{minimize, seeded};
use crate::check::model::{run, DivergenceKind, Owner};

use super::snapshot_world_tests::{Case, SnapshotOwner, CASES, LANDS, REPEAT};

const SEEDS: u64 = 3;

fn first_red(case: Case, which: &'static str) -> Option<String> {
    (1..=SEEDS).find_map(|seed| {
        let owner = SnapshotOwner::new(case, seed, which);
        let trace = seeded(&owner.alphabet(), seed * 7919, owner.alphabet().len() * 2);
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let Err(divergence) = run(&owner, owner.name(), &trace).await else { return None };
            let small = minimize(&owner, *divergence).await;
            let inputs: Vec<String> = small.inputs.iter().map(ToString::to_string).collect();
            let DivergenceKind::Property { message, .. } = &small.fault else { return Some(format!("{} seed {seed}: observations diverged", case.name)) };
            Some(format!("{} seed {seed}: {} deliveries [{}] {which}: {message}", case.name, inputs.len(), inputs.join(", ")))
        })
    })
}

fn red(which: &'static str) -> Vec<String> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = CASES.iter().filter(|case| which == REPEAT || !case.revisions).map(|case| scope.spawn(move || first_red(*case, which))).collect();
        handles.into_iter().filter_map(|handle| handle.join().unwrap()).collect()
    })
}

/// P-DATA-104: one consistent snapshot delivered by any paths and projections, in any order and number of repeats, gives one state.
#[test]
fn store_snapshot_lands_to_one_state() {
    let found = red(LANDS);
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}

/// P-DATA-104: a delivery of a landed snapshot again changes no row, no revision and no member.
#[test]
fn store_snapshot_repeat_changes_nothing() {
    let found = red(REPEAT);
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}

/// The generator is not blind: the pages of the windows put 9 members into the state and the delete event of a missing row puts 1 tombstone.
#[test]
fn store_snapshot_windows_and_tombstones_reach_the_state() {
    let count = |case: Case, kind: &str, table: &str, only: fn(&crate::check::value::Input) -> bool| {
        let owner = SnapshotOwner::new(case, 1, LANDS);
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let mut landing = owner.real().await;
            let mut last = Vec::new();
            for input in owner.alphabet().iter().filter(|input| only(input)) {
                last = landing.apply(input).await;
            }
            let state = &last.iter().find(|observation| observation.kind == kind).unwrap().value;
            state[table].as_array().unwrap().len()
        })
    };
    let pages = count(CASES[3], "state", "scope_member", |input| input.text("window").is_some());
    let tombstones = count(CASES[5], "state", "tombstone", |input| input.text("path") == Some("delete"));
    assert_eq!((pages, tombstones), (9, 1));
}
