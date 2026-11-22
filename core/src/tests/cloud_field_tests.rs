use crate::cloud::{Choice, Stamp, StampConflict, StampedValue, choose_field};
use crate::schema::MergeRule;
use serde_json::json;

#[test]
fn cloud_field_winner_is_stable_in_both_delivery_orders() {
    let older = StampedValue { value: json!(12), stamp: Stamp { millis: 1, counter: 0, replica: [1; 16] } };
    let newer = StampedValue { value: json!(4), stamp: Stamp { millis: 2, counter: 0, replica: [2; 16] } };
    for (rule, expected) in [(None, Choice::Incoming), (Some(MergeRule::Monotonic), Choice::Stored)] {
        let reverse = match expected {
            Choice::Stored => Choice::Incoming,
            Choice::Incoming => Choice::Stored,
        };
        assert_eq!(choose_field(&older, &newer, rule.as_ref()), Ok(expected));
        assert_eq!(choose_field(&newer, &older, rule.as_ref()), Ok(reverse));
    }
    let conflicting = StampedValue { value: json!(4), ..older.clone() };
    assert_eq!(choose_field(&older, &older, None), Ok(Choice::Stored));
    assert_eq!(choose_field(&older, &conflicting, Some(&MergeRule::Monotonic)), Err(StampConflict));
}
