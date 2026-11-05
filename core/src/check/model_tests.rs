use super::counter::*;
use super::model::*;
use super::value::*;
use serde_json::json;

fn no_count_of_three(steps: &[Step]) -> Result<(), String> {
    match steps.iter().position(|step| step.observed.iter().any(|o| o.kind == "count" && o.value["n"] == 3)) {
        Some(index) => Err(format!("count 3 at step {}", index + 1)),
        None => Ok(()),
    }
}

/// Requirement: an agreeing core passes and the run returns the trace with the expected observations.
/// Observable: the returned trace. Assert: 3 steps, last one carries `ended`.
#[tokio::test]
async fn an_agreeing_core_passes_and_the_trace_carries_the_expected_observations() {
    let trace = run(&CounterOwner::new(None), "t", &[inc(1), inc(1), epoch_ended()]).await.unwrap();
    assert_eq!(trace.steps.len(), 3);
    assert_eq!(trace.steps[2].observed, [Observation::new("ended", json!({"epoch": 1, "n": 2}))]);
}

/// Requirement: a divergence report is step number, input, expected, got.
/// Observable: the Divergence of the planted bug. Assert: step 3, input inc{epoch:2}, expected [], got count 3.
#[tokio::test]
async fn a_planted_difference_is_reported_with_step_input_expected_and_got() {
    let divergence = run(&CounterOwner::new(Some(Bug::CountsOtherEpochAtTwo)), "t", &[inc(1), inc(1), inc(2), inc(1)]).await.unwrap_err();
    assert_eq!(divergence.step, 3);
    assert_eq!(divergence.input, inc(2));
    assert!(divergence.expected.is_empty());
    assert_eq!(divergence.got, [Observation::new("count", json!({"epoch": 1, "n": 3}))]);
    assert_eq!(divergence.fault, DivergenceKind::Observations);
}

/// Requirement: properties are checked after every step. Observable: the fault of the run. Assert: the named property fails at the step that makes it false.
#[tokio::test]
async fn a_property_is_checked_after_every_step() {
    let owner = CounterOwner { bug: None, extra: Some(Property { name: "P-COUNTER-99", check: no_count_of_three }) };
    let divergence = run(&owner, "t", &[inc(1), inc(1), inc(1), inc(1)]).await.unwrap_err();
    assert_eq!(divergence.step, 3);
    assert!(matches!(divergence.fault, DivergenceKind::Property { name: "P-COUNTER-99", .. }), "{divergence}");
}

/// Requirement: the built-in properties hold on a good trace and fail on a trace that breaks them.
/// Observable: the check results. Assert: P-COUNTER-01 and 02 pass on a good trace, 01 fails on a count in an ended epoch, 02 on a skipped count.
#[test]
fn the_counter_properties_fail_on_a_trace_that_breaks_them() {
    let owner = CounterOwner::new(None);
    let properties = owner.properties();
    let step = |input: Input, observed: Vec<Observation>| Step { input, observed };
    let count = |epoch: u64, n: u64| Observation::new("count", json!({"epoch": epoch, "n": n}));
    let ended = |epoch: u64, n: u64| Observation::new("ended", json!({"epoch": epoch, "n": n}));
    let good = vec![step(inc(1), vec![count(1, 1)]), step(epoch_ended(), vec![ended(1, 1)]), step(inc(2), vec![count(2, 1)])];
    assert!(properties.iter().all(|p| (p.check)(&good).is_ok()));
    let after_end = vec![step(inc(1), vec![count(1, 1)]), step(epoch_ended(), vec![ended(1, 1)]), step(inc(1), vec![count(1, 2)])];
    assert!((properties[0].check)(&after_end).is_err());
    let skipped = vec![step(inc(1), vec![count(1, 2)])];
    assert!((properties[1].check)(&skipped).is_err());
}
