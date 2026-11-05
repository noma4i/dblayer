use super::corpus;
use super::counter::*;
use super::explore::*;
use super::model::*;

/// Requirement: all traces to depth N are listed. Observable: the count. Assert: 3+9+27+81 = 120 for 3 inputs and depth 4, no duplicates.
#[test]
fn all_traces_to_a_depth_are_listed_once() {
    let traces = all_traces(&CounterOwner::new(None).alphabet(), 4);
    assert_eq!(traces.len(), 120);
    let mut keys: Vec<String> = traces.iter().map(|t| t.iter().map(ToString::to_string).collect::<Vec<_>>().join(";")).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 120);
}

/// Requirement: a seed names one long trace. Observable: two traces of one seed and one of another. Assert: equal, then different.
#[test]
fn a_seed_gives_the_same_long_trace() {
    let alphabet = CounterOwner::new(None).alphabet();
    assert_eq!(seeded(&alphabet, 7, 50), seeded(&alphabet, 7, 50));
    assert_ne!(seeded(&alphabet, 7, 50), seeded(&alphabet, 8, 50));
}

/// Requirement: an agreeing core has no divergence over all traces to depth 4 and the seeded long traces.
/// Observable: find. Assert: None.
#[tokio::test]
async fn an_agreeing_core_has_no_divergence_over_the_generated_traces() {
    assert!(find(&CounterOwner::new(None), 4, &[1, 2, 3], 40).await.is_none());
}

/// Requirement: a planted difference is found and minimized to the smallest trace, the trace is saved to the corpus and reproduces.
/// Observable: the minimized inputs, the saved file replayed on the buggy and on the good core. Assert: [inc1, inc1, inc2]; the buggy core diverges at step 3, the good core replays green.
#[tokio::test]
async fn a_planted_difference_is_found_minimized_saved_and_reproduced() {
    let buggy = CounterOwner::new(Some(Bug::CountsOtherEpochAtTwo));
    let found = find(&buggy, 4, &[], 0).await.expect("depth 4 reaches the bug");
    let small = minimize(&buggy, found).await;
    assert_eq!(small.inputs, [inc(1), inc(1), inc(2)]);
    assert_eq!(small.step, 3);

    let dir = tempfile::tempdir().unwrap();
    let path = save(&buggy, dir.path(), &small).unwrap();
    let trace = corpus::read(&path).unwrap();
    assert_eq!(trace.inputs(), small.inputs);
    let mut bad = buggy.real().await;
    assert_eq!(replay(bad.as_mut(), &trace).await.unwrap_err().step, 3);
    let mut good = CounterOwner::new(None).real().await;
    replay(good.as_mut(), &trace).await.unwrap();
}

/// Requirement: a long seeded trace finds a difference that needs more inputs than the depth. Observable: find with depth 2. Assert: a divergence exists.
#[tokio::test]
async fn a_seeded_long_trace_finds_what_the_depth_does_not_reach() {
    let buggy = CounterOwner::new(Some(Bug::CountsOtherEpochAtTwo));
    assert!(find(&buggy, 2, &[1, 2, 3, 4, 5], 60).await.is_some());
}
