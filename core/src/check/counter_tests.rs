use super::counter::*;
use super::model::RefModel;
use super::value::Observation;
use serde_json::json;

/// Requirement R4: an input of another epoch leaves no observation, the end of an epoch reports its count and restarts at 0.
/// Observable: the observations of the model. Assert: exact lists.
#[test]
fn the_model_drops_other_epochs_and_restarts_after_the_end() {
    let mut model = CounterRef::new();
    assert_eq!(model.step(&inc(1)), [Observation::new("count", json!({"epoch": 1, "n": 1}))]);
    assert!(model.step(&inc(2)).is_empty());
    assert_eq!(model.step(&epoch_ended()), [Observation::new("ended", json!({"epoch": 1, "n": 1}))]);
    assert!(model.step(&inc(1)).is_empty());
    assert_eq!(model.step(&inc(2)), [Observation::new("count", json!({"epoch": 2, "n": 1}))]);
}
