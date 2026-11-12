use std::sync::Arc;

use crate::check::corpus;
use crate::check::explore::{all_traces, minimize, save, seeded};
use crate::check::library::alphabet::{alphabet, prelude, reopened};
use crate::check::library::properties::{outcome_rules, request_rules};
use crate::check::library::LibraryOwner;
use crate::check::model::{run, Divergence, DivergenceKind, Property};
use crate::check::value::Input;

/// Exhaustive depth, number of long programs and their length of the general run. Measured against the 20 s limit of a test.
const DEPTH: usize = 1;
const SEEDS: u64 = 16;
const LENGTH: usize = 20;
const PARALLEL: usize = 8;

struct Program {
    tag: String,
    inputs: Vec<Input>,
}

fn seeds() -> u64 {
    std::env::var("DBL_LIBRARY_SEEDS").ok().and_then(|text| text.parse().ok()).unwrap_or(SEEDS)
}

fn first_seed() -> u64 {
    std::env::var("DBL_LIBRARY_FIRST_SEED").ok().and_then(|text| text.parse().ok()).unwrap_or(1)
}

fn corpus_dir() -> std::path::PathBuf {
    corpus::shared_dir().join("library")
}

/// Every program to `depth` and `seeds` long ones, on a core with a session and on a core without one.
fn programs(depth: usize, seeds: u64, length: usize) -> Vec<Program> {
    family_programs(&[false, true], depth, seeds, length)
}

fn family_programs(families: &[bool], depth: usize, seeds: u64, length: usize) -> Vec<Program> {
    let letters = alphabet();
    let mut out = Vec::new();
    for &anonymous in families {
        let family = if anonymous { "a" } else { "s" };
        let head = prelude(anonymous);
        for (index, body) in all_traces(&letters, depth).into_iter().enumerate() {
            out.push(Program { tag: format!("{family}d{index}"), inputs: [head.clone(), body].concat() });
        }
        for seed in first_seed()..first_seed() + seeds {
            out.push(Program { tag: format!("{family}{seed}"), inputs: [head.clone(), seeded(&letters, seed, length)].concat() });
        }
    }
    out
}

fn rule_of(divergence: &Divergence) -> &'static str {
    match &divergence.fault {
        DivergenceKind::Property { name, .. } => name,
        DivergenceKind::Observations => "observations",
    }
}

fn describe(divergence: &Divergence) -> String {
    format!("{divergence}\nprogram: {}", divergence.inputs.iter().map(ToString::to_string).collect::<Vec<_>>().join(" ; "))
}

/// Runs the programs in parallel (join in program order), minimizes the first break of each rule, saves it into the corpus and returns the messages.
async fn sweep(owner: Arc<LibraryOwner>, programs: Vec<Program>) -> Vec<String> {
    let permits = Arc::new(tokio::sync::Semaphore::new(PARALLEL));
    let mut tasks = Vec::new();
    for program in programs {
        let (permits, owner) = (permits.clone(), owner.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = permits.acquire_owned().await.unwrap();
            run(owner.as_ref(), "library", &program.inputs).await.err().map(|failed| (program.tag, *failed))
        }));
    }
    let mut first: Vec<Divergence> = Vec::new();
    let mut counts: std::collections::BTreeMap<&'static str, usize> = Default::default();
    for task in tasks {
        let Some((_, divergence)) = task.await.expect("a program must not panic") else { continue };
        *counts.entry(rule_of(&divergence)).or_default() += 1;
        if !first.iter().any(|seen| rule_of(seen) == rule_of(&divergence)) {
            first.push(divergence);
        }
    }
    let mut messages = Vec::new();
    for divergence in first {
        let smallest = minimize(owner.as_ref(), divergence).await;
        messages.push(format!("{}\n{} program(s) broke {}, saved: {:?}", describe(&smallest), counts[rule_of(&smallest)], rule_of(&smallest), save(owner.as_ref(), &corpus_dir(), &smallest)));
    }
    messages
}

async fn assert_green(properties: Vec<Property>) {
    let found = sweep(Arc::new(LibraryOwner { properties, reference: false }), programs(DEPTH, seeds(), LENGTH)).await;
    assert!(found.is_empty(), "{}", found.join("\n\n"));
}

/// The programs `prelude ; close ; open ; <letter>` for every letter of the alphabet.
fn reopen_programs() -> Vec<Program> {
    all_traces(&alphabet(), 1).into_iter().enumerate().map(|(index, body)| Program { tag: format!("r{index}"), inputs: reopened(body) }).collect()
}

/// Requirement: every program of depth 1 and 2 of the alphabet runs on the stand without a panic and without an unsupported input.
/// Observable: the observations of each program. Assert: none is `unsupported`.
#[tokio::test(flavor = "multi_thread")]
async fn library_alphabet_runs() {
    let mut tasks = Vec::new();
    let permits = Arc::new(tokio::sync::Semaphore::new(PARALLEL));
    let owner = Arc::new(LibraryOwner { properties: Vec::new(), reference: false });
    for program in family_programs(&[false], 1, 0, 0).into_iter().chain(family_programs(&[true], 1, 0, 0)) {
        let (permits, owner) = (permits.clone(), owner.clone());
        tasks.push(tokio::spawn(async move {
            let _permit = permits.acquire_owned().await.unwrap();
            let trace = run(owner.as_ref(), "library", &program.inputs).await.expect("no property is given");
            trace.steps.iter().find(|step| step.observed.iter().any(|item| item.kind == "unsupported")).map(|step| format!("{} -> unsupported: {}", program.tag, step.input))
        }));
    }
    let mut unsupported = Vec::new();
    for task in tasks {
        unsupported.extend(task.await.expect("a program must not panic"));
    }
    assert!(unsupported.is_empty(), "{}", unsupported.join("\n"));
}

/// Requirement: I2, I9, I12 and I13 of invariants.md hold on every program (all to depth D, S long ones of length L).
/// Observable: the requests of the mock log (operation, variables), the clock ticks, the network flag, the session. Assert: no program breaks a rule.
#[tokio::test(flavor = "multi_thread")]
async fn library_seed_requests() {
    assert_green(request_rules()).await;
}

/// Requirement: I2, I9, I12 and I13 of invariants.md hold on the first input after `close` + `open`.
/// Observable: the requests of the mock log, the network flag, the session. Assert: no program `prelude ; close ; open ; <letter>` breaks a rule.
#[tokio::test(flavor = "multi_thread")]
async fn library_reopen_requests() {
    let found = sweep(Arc::new(LibraryOwner { properties: [request_rules(), outcome_rules()].concat(), reference: false }), reopen_programs()).await;
    assert!(found.is_empty(), "{}", found.join("\n\n"));
}

/// Requirement: I7 of invariants.md: every accepted load ends with exactly one outcome, none only while the mock holds its request.
/// Observable: the outcome observations and the requests the mock holds. Assert: no program breaks the rule.
#[tokio::test(flavor = "multi_thread")]
async fn library_seed_outcomes() {
    assert_green(outcome_rules()).await;
}

/// Requirement: the programs saved from earlier breaks stay green.
/// Observable: the same properties over the saved inputs. Assert: no program of `core/tests/corpus/library` breaks a rule; an empty directory is green.
#[tokio::test(flavor = "multi_thread")]
async fn library_corpus_is_green() {
    let traces = if corpus_dir().exists() { corpus::read_dir(&corpus_dir()).unwrap() } else { Vec::new() };
    let owner = LibraryOwner { properties: [request_rules(), outcome_rules()].concat(), reference: false };
    let mut red = Vec::new();
    for trace in traces {
        if let Err(divergence) = run(&owner, "library", &trace.inputs()).await {
            red.push(format!("{}: {}", trace.name, describe(&divergence)));
        }
    }
    assert!(red.is_empty(), "{}", red.join("\n"));
}
