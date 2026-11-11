use std::collections::BTreeMap;
use std::sync::Arc;

use crate::check::explore::{all_traces, minimize, seeded};
use crate::check::library::alphabet::{alphabet, prelude};
use crate::check::library::reference::{first_hole, modeled};
use crate::check::library::LibraryOwner;
use crate::check::model::{run, Divergence};
use crate::check::value::{Input, Observation};

/// Exhaustive depth, number of long programs and their length of the comparison with the reference. Measured against the 20 s limit of a test.
const DEPTH: usize = 2;
const SEEDS: u64 = 16;
const LENGTH: usize = 20;
const PARALLEL: usize = 8;
/// Distinct divergences that are minimized and printed.
const GROUPS: usize = 6;

fn depth() -> usize {
    std::env::var("DBL_REFERENCE_DEPTH").ok().and_then(|text| text.parse().ok()).unwrap_or(DEPTH)
}

fn seeds() -> u64 {
    std::env::var("DBL_REFERENCE_SEEDS").ok().and_then(|text| text.parse().ok()).unwrap_or(SEEDS)
}

fn programs() -> Vec<Vec<Input>> {
    let letters: Vec<Input> = alphabet().into_iter().filter(modeled).collect();
    let mut out = Vec::new();
    for anonymous in [false, true] {
        let head = prelude(anonymous);
        out.extend(all_traces(&letters, depth()).into_iter().map(|body| [head.clone(), body].concat()));
        out.extend((1..=seeds()).map(|seed| [head.clone(), seeded(&letters, seed, LENGTH)].concat()));
    }
    out
}

fn kinds(items: &[Observation]) -> String {
    items.iter().map(|item| item.value["kind"].as_str().map_or_else(|| item.kind.clone(), |kind| format!("{}:{kind}", item.kind))).collect::<Vec<_>>().join(",")
}

fn signature(divergence: &Divergence) -> String {
    format!("{} | expected [{}] | got [{}]", divergence.input.name, kinds(&divergence.expected), kinds(&divergence.got))
}

fn describe(divergence: &Divergence) -> String {
    format!("{divergence}\nprogram: {}", divergence.inputs.iter().map(ToString::to_string).collect::<Vec<_>>().join(" ; "))
}

/// Runs the programs against the reference, groups the divergences by shape, minimizes the shortest of each group.
async fn compare(owner: &Arc<LibraryOwner>, programs: Vec<Vec<Input>>) -> (usize, BTreeMap<&'static str, usize>, Vec<String>) {
    let (mut compared, mut holes) = (0usize, BTreeMap::<&'static str, usize>::new());
    let permits = Arc::new(tokio::sync::Semaphore::new(PARALLEL));
    let mut tasks = Vec::new();
    for program in programs {
        if let Some(hole) = first_hole(&program) {
            *holes.entry(hole).or_default() += 1;
            continue;
        }
        compared += 1;
        let (permits, owner) = (permits.clone(), owner.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = permits.acquire_owned().await.unwrap();
            run(owner.as_ref(), "library", &program).await.err()
        }));
    }
    let mut groups: BTreeMap<String, (usize, Divergence)> = BTreeMap::new();
    for task in tasks {
        let Some(divergence) = task.await.expect("a program must not panic") else { continue };
        let entry = groups.entry(signature(&divergence)).or_insert_with(|| (0, (*divergence).clone()));
        entry.0 += 1;
        if divergence.step < entry.1.step {
            entry.1 = *divergence;
        }
    }
    let mut ranked: Vec<(String, usize, Divergence)> = groups.into_iter().map(|(signature, (count, divergence))| (signature, count, divergence)).collect();
    ranked.sort_by_key(|(_, _, divergence)| divergence.step);
    let mut messages = Vec::new();
    for (signature, count, divergence) in ranked.into_iter().take(GROUPS) {
        let smallest = minimize(owner.as_ref(), divergence).await;
        messages.push(format!("{count} program(s): {signature}\n{}", describe(&smallest)));
    }
    (compared, holes, messages)
}

/// Requirement: library.md sections 3 to 7 for the slice O1 (environment and loads): after every input the stand shows what the reference shows.
/// Observable: the requests (`sent`), the answers, the outcomes of the loads by name, the errors. Assert: no program of the modeled inputs diverges; a program that meets a spec hole is not compared and is counted.
#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads() {
    let owner = Arc::new(LibraryOwner { properties: Vec::new(), reference: true });
    let (compared, holes, found) = compare(&owner, programs()).await;
    assert!(found.is_empty(), "compared {compared} programs to depth {}, {} seeds of length {LENGTH}, not compared (spec holes) {holes:?}\n\n{}", depth(), seeds(), found.join("\n\n"));
}
