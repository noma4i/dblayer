use crate::check::explore::{minimize, seeded};
use crate::check::model::{run, DivergenceKind, Owner, Sut};

use super::snapshot_world_tests::{Case, SnapshotOwner, CASES, LANDS, REPEAT};

const SEEDS: u64 = 6;

fn first_red(case: Case, which: &'static str) -> Option<String> {
    let found: Vec<Option<String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (1..=SEEDS)
            .map(|seed| {
                scope.spawn(move || {
                    let began = std::time::Instant::now();
                    let note = |text: &str| { use std::io::Write; let mut file = std::fs::OpenOptions::new().create(true).append(true).open("/private/tmp/claude-501/-Users-noma4i-orca-workspaces-dblayer-snapshot-prop/1eaa2e0c-37a6-49a6-9652-4bb25cf4bac4/scratchpad/timing.log").unwrap(); writeln!(file, "{} {seed} {which} {text} {:?}", case.name, began.elapsed()).unwrap(); }; // TEMPORARY:
                    let owner = SnapshotOwner::new(case, seed, which);
                    let trace = seeded(&owner.alphabet(), seed * 7919, owner.alphabet().len() * 4);
                    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
                    runtime.block_on(async {
                        let outcome = run(&owner, owner.name(), &trace).await;
                        note(&format!("run done len {} red {}", trace.len(), outcome.is_err())); // TEMPORARY:
                        let Err(divergence) = outcome else { return None };
                        note(&format!("red at step {}", divergence.step)); // TEMPORARY:
                        let small = minimize(&owner, *divergence).await;
                        note("minimized"); // TEMPORARY:
                        let inputs: Vec<String> = small.inputs.iter().map(ToString::to_string).collect();
                        let DivergenceKind::Property { message, .. } = &small.fault else { return Some(format!("{} seed {seed}: observations diverged", case.name)) };
                        Some(format!("{} seed {seed}: {} deliveries [{}] {which}: {message}", case.name, inputs.len(), inputs.join(", ")))
                    })
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    found.into_iter().flatten().min_by_key(String::len)
}

fn red(which: &'static str) -> Vec<String> {
    CASES.iter().filter_map(|case| first_red(*case, which)).collect()
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
