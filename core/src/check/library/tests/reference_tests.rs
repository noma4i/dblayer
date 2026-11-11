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
const SEEDS: u64 = 8;
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

/// A load refused again and again, each refusal followed by a tick past the retry pause: the number of retries of docs/server-sync.md "Retries" is reached by no short random program.
fn retry_programs() -> Vec<Vec<Input>> {
    let named = |kind: &str, name: &str| Input::new(kind).with("as", name).with("named", true).with("relation", "feed");
    let node = Input::new("node").with("as", "nf").with("named", true).with("relation", "transcode_status").with("row", "m1").with("force", true);
    let starts = [named("begin_load", "l1"), named("refresh", "rf"), named("begin_load_all", "la"), node];
    let mut out = Vec::new();
    for start in starts {
        for kind in ["temporary", "server"] {
            let mut body = vec![start.clone()];
            for _ in 0..5 {
                body.push(Input::new("answer_error").with("kind", kind));
                body.push(Input::new("tick").with("ms", 60_000u64));
            }
            out.push([prelude(false), body].concat());
        }
    }
    out
}

/// One family of programs: its prelude (with or without a session), its body inputs, its exhaustive depth and its extra programs.
fn family(anonymous: bool, keep: fn(&Input) -> bool, deep: usize, extra: Vec<Vec<Input>>) -> Vec<Vec<Input>> {
    let letters: Vec<Input> = alphabet().into_iter().filter(modeled).filter(keep).collect();
    let head = prelude(anonymous);
    let mut out: Vec<Vec<Input>> = all_traces(&letters, deep).into_iter().map(|trace| [head.clone(), trace].concat()).collect();
    out.extend((1..=seeds()).map(|seed| [head.clone(), seeded(&letters, seed, LENGTH)].concat()));
    out.extend(extra);
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
        let shrunk = minimize(owner.as_ref(), divergence.clone()).await;
        let smallest = if self::signature(&shrunk) == signature { shrunk } else { divergence };
        messages.push(format!("{count} program(s): {signature}\n{}", describe(&smallest)));
    }
    (compared, holes, messages)
}

async fn assert_matches(programs: Vec<Vec<Input>>) {
    let owner = Arc::new(LibraryOwner { properties: Vec::new(), reference: true });
    let (compared, holes, found) = compare(&owner, programs).await;
    assert!(found.is_empty() && holes.is_empty(), "compared {compared} programs to depth {}, {} seeds of length {LENGTH}, not compared because of spec holes (must be empty) {holes:?}\n\n{}", depth(), seeds(), found.join("\n\n"));
}

/// Requirement: library.md sections 3 to 7 for the slice O1 (environment and loads): after every input the stand shows what the reference shows.
/// Observable: the requests (`sent`), the answers, the outcomes of the loads by name, the errors. Assert: no program of the modeled inputs diverges and none meets a spec hole.
/// The 3 families run as 3 tests, so each stays inside the time limit of a test on a loaded machine.
#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads() {
    assert_matches(family(false, |_| true, depth(), retry_programs())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_second_login() {
    assert_matches(family(false, |input| input.name != "login", depth(), Vec::new())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_session() {
    assert_matches(family(true, |_| true, depth().min(1), Vec::new())).await;
}
