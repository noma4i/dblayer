use crate::check::model::Owner;
use crate::check::explore::seeded;
use serde_json::json;

use super::sets_reference_tests::red_of;
use super::snapshot_world_tests::{Case, SnapshotOwner, CASES, LANDS, REPEAT};

const SEEDS: u64 = 3;

fn red(which: &'static str) -> Vec<String> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = CASES
            .iter()
            .filter(|case| which == REPEAT || !case.revisions)
            .flat_map(|case| (1..=SEEDS).map(move |seed| scope.spawn(move || {
                let owner = SnapshotOwner::new(*case, seed, which);
                red_of(&owner, seed * 7919, owner.alphabet().len() * 2).map(|found| format!("{}: {found}", case.name))
            })))
            .collect();
        handles.into_iter().filter_map(crate::test_wait::join_within).collect()
    })
}

#[test]
fn store_snapshot_lands_to_one_state() {
    let found = red(LANDS);
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}

#[test]
fn store_snapshot_repeat_changes_nothing() {
    let found = red(REPEAT);
    assert!(found.is_empty(), "cells against P-DATA-104:\n{}", found.join("\n"));
}

fn case_named(name: &str) -> Case {
    *CASES.iter().find(|case| case.name == name).unwrap()
}

/// The generator is not blind: every case repeats an informative delivery, the pages put the members of the windows into the state and a delete of a missing row puts 1 tombstone.
#[test]
fn store_snapshot_generator_reaches_the_state() {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let run = |case: Case, seed: u64, which: &'static str, inputs: Option<fn(&crate::check::value::Input) -> bool>| {
        let owner = SnapshotOwner::new(case, seed, which);
        runtime.block_on(async {
            let mut landing = owner.real().await;
            let trace = match inputs {
                Some(only) => owner.alphabet().into_iter().filter(|input| only(input)).collect(),
                None => seeded(&owner.alphabet(), seed * 7919, owner.alphabet().len() * 2),
            };
            let mut seen = Vec::new();
            for input in &trace {
                seen.push(landing.apply(input).await);
            }
            seen
        })
    };
    for case in CASES {
        let repeats = (1..=SEEDS).any(|seed| run(case, seed, REPEAT, None).iter().any(|observed| observed.iter().any(|observation| observation.kind == "repeated" && observation.value == json!(true))));
        assert!(repeats, "{}: no informative repeat in the traces", case.name);
    }
    let members = run(case_named("window_members"), 1, LANDS, Some(|input| input.text("window").is_some()));
    let state = &members.last().unwrap().iter().find(|observation| observation.kind == "state").unwrap().value;
    let mut held: Vec<(String, String)> = state["scope_member"].as_array().unwrap().iter().map(|member| (member["scope_key"].to_string(), member["row_id"].as_str().unwrap().to_string())).collect();
    held.sort();
    assert_eq!(held.len(), 9, "members of the four pages: {held:?}");
    assert_eq!(held.iter().map(|(_, id)| id.as_str()).collect::<std::collections::BTreeSet<_>>().len(), 3, "{held:?}");
    let tombstones = run(case_named("tombstone"), 1, LANDS, Some(|input| input.text("path") == Some("delete")));
    let state = &tombstones.last().unwrap().iter().find(|observation| observation.kind == "state").unwrap().value;
    assert_eq!(state["tombstone"].as_array().unwrap().len(), 1);
}
