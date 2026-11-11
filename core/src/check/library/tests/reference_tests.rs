use std::collections::BTreeMap;
use std::sync::Arc;

use crate::check::explore::{all_traces, minimize, seeded};
use crate::check::library::alphabet::{alphabet, is_stream, prelude, streams};
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

/// The letters of the family with streams: the environment, the streams of the feed and the entity of the row `m1`, the loads of the feed, the answers.
/// The streams with an open hole (`u1`, `e2`, `e3`) go in a family of their own so the skipped programs are counted by hole.
fn stream_letters(holes: bool) -> Vec<Input> {
    let hole = |input: &Input| input.text("as").is_some_and(|name| matches!(name, "u1" | "e2" | "e3"));
    let skipped = ["node", "nodes", "clock_back"];
    alphabet().into_iter().chain(streams()).filter(modeled).filter(|input| !skipped.contains(&input.name.as_str()) && !(input.name == "cancel" && input.text("target") == Some("l1"))).filter(|input| holes || !hole(input)).collect()
}

fn stream_family(anonymous: bool, holes: bool, deep: usize) -> Vec<Vec<Input>> {
    let letters = stream_letters(holes);
    let head = prelude(anonymous);
    let mut out: Vec<Vec<Input>> = all_traces(&letters, deep).into_iter().map(|trace| [head.clone(), trace].concat()).collect();
    out.extend((1..=seeds()).map(|seed| [head.clone(), seeded(&letters, seed, LENGTH)].concat()));
    out
}

/// The spec holes the family with streams may meet: named, counted, skipped.
const STREAM_HOLES: [&str; 4] = ["H22_subscription_request_of_a_live_list", "H20_members_window_request", "H21_entity_stream_with_a_fetch_other_than_never", "H23_stream_opened_again_under_the_name_of_a_live_one"];

/// Like `assert_matches`, but a program that meets a named hole is skipped and counted.
async fn assert_streams(programs: Vec<Vec<Input>>) {
    let owner = Arc::new(LibraryOwner { properties: Vec::new(), reference: true });
    let (compared, holes, found) = compare(&owner, programs).await;
    let unknown: Vec<_> = holes.keys().filter(|name| !STREAM_HOLES.contains(name)).collect();
    assert!(found.is_empty() && unknown.is_empty(), "compared {compared} programs with streams to depth {}, {} seeds of length {LENGTH}, skipped because of the spec holes {holes:?}, unknown holes {unknown:?}\n\n{}", depth(), seeds(), found.join("\n\n"));
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

/// Part `part` of `parts` of a family: the programs whose index modulo `parts` is `part - 1`.
fn part_of(programs: Vec<Vec<Input>>, part: usize, parts: usize) -> Vec<Vec<Input>> {
    programs.into_iter().enumerate().filter(|(index, _)| index % parts == part - 1).map(|(_, program)| program).collect()
}

/// Part `part` of 4 of a family: the programs whose index modulo 4 is `part - 1`.
fn quarter(programs: Vec<Vec<Input>>, part: usize) -> Vec<Vec<Input>> {
    programs.into_iter().enumerate().filter(|(index, _)| index % 4 == part - 1).map(|(_, program)| program).collect()
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
/// The 2 heavy families run as 4 tests each and the family without a session as 1, so each test stays inside the time limit of a test on a loaded machine.
#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_part_1() {
    assert_matches(quarter(family(false, |input| !is_stream(input), depth(), retry_programs()), 1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_part_2() {
    assert_matches(quarter(family(false, |input| !is_stream(input), depth(), retry_programs()), 2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_part_3() {
    assert_matches(quarter(family(false, |input| !is_stream(input), depth(), retry_programs()), 3)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_part_4() {
    assert_matches(quarter(family(false, |input| !is_stream(input), depth(), retry_programs()), 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_second_login_part_1() {
    assert_matches(quarter(family(false, |input| input.name != "login" && !is_stream(input), depth(), Vec::new()), 1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_second_login_part_2() {
    assert_matches(quarter(family(false, |input| input.name != "login" && !is_stream(input), depth(), Vec::new()), 2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_second_login_part_3() {
    assert_matches(quarter(family(false, |input| input.name != "login" && !is_stream(input), depth(), Vec::new()), 3)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_second_login_part_4() {
    assert_matches(quarter(family(false, |input| input.name != "login" && !is_stream(input), depth(), Vec::new()), 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_loads_without_session() {
    assert_matches(family(true, |input| !is_stream(input), depth().min(1), Vec::new())).await;
}

/// Requirement: library.md sections 3, 4, 5 and 7 for the slice O2 (streams `List`, `Count`, `Entity(never)`): the value of a stream after `pull`, its end and its `cancel`, the loads that a stream opens or joins.
/// Observable: the values of the streams (rows, phase, kind of the error, `loaded` present), `ended`, `empty`, the requests. Assert: no program diverges.
#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_part_1() {
    assert_streams(part_of(stream_family(false, false, depth()), 1, 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_part_2() {
    assert_streams(part_of(stream_family(false, false, depth()), 2, 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_part_3() {
    assert_streams(part_of(stream_family(false, false, depth()), 3, 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_part_4() {
    assert_streams(part_of(stream_family(false, false, depth()), 4, 4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_without_session() {
    assert_streams(stream_family(true, false, depth().min(1))).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn library_matches_reference_streams_with_holes() {
    assert_streams(stream_family(false, true, 1)).await;
}
