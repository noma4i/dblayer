//! Orders of incoming values for the merge rules of `data.md`. Requirement -> trace -> stored value after the last arrival.

use serde_json::{json, Value};

use super::MergeRule;
use super::MergeRule::*;
use crate::test_wait::permutations;

fn fold(rule: &MergeRule, stored: Option<Value>, order: &[Value]) -> Vec<Value> {
    let mut seen = Vec::new();
    let mut current = stored;
    for incoming in order {
        let next = rule.apply(current.as_ref(), incoming.clone());
        seen.push(next.clone());
        current = Some(next);
    }
    seen
}

fn last_of(order: &[Value], wanted: impl Fn(&Value) -> bool) -> Value { order.iter().rev().find(|value| wanted(value)).cloned().unwrap() }

#[test]
fn data_orders_merge_present_and_non_empty() {
    let incoming = [json!("a"), json!("b"), Value::Null, json!("")];
    let orders = permutations(&incoming);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let present = fold(&Present, Some(json!("seed")), &order);
        assert_eq!(present.last().unwrap(), &last_of(&order, |value| !value.is_null()), "present {order:?}");
        let non_empty = fold(&NonEmpty, Some(json!("seed")), &order);
        assert_eq!(non_empty.last().unwrap(), &last_of(&order, |value| !value.is_null() && value != &json!("")), "non_empty {order:?}");
    }
}

#[test]
fn data_orders_merge_positive() {
    let incoming = [json!(5), json!(3), json!(0), json!(-1), Value::Null];
    let orders = permutations(&incoming);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let seen = fold(&Positive, Some(json!(9)), &order);
        assert_eq!(seen.last().unwrap(), &last_of(&order, |value| value.as_i64().is_some_and(|n| n > 0)), "positive {order:?}");
        assert!(seen.iter().all(|value| value.as_i64().is_some_and(|n| n > 0)), "a stored size stays positive: {seen:?}");
    }
}

#[test]
fn data_orders_merge_monotonic_and_tiers() {
    let numbers = [json!(3), json!(7), Value::Null, json!(5)];
    let orders = permutations(&numbers);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let seen = fold(&Monotonic, Some(json!(1)), &order);
        assert_eq!(seen.last().unwrap(), &json!(7), "monotonic {order:?}");
        assert!(seen.windows(2).all(|pair| pair[0].as_i64() <= pair[1].as_i64()), "the stored number never lowers: {seen:?}");
    }
    let ladder = Ladder { tiers: vec![vec!["PENDING".into(), "PROGRESSING".into()], vec!["COMPLETED".into(), "FAILED".into()]] };
    let states = [json!("PENDING"), json!("COMPLETED"), json!("FAILED"), Value::Null];
    let orders = permutations(&states);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let seen = fold(&ladder, Some(json!("PROGRESSING")), &order);
        assert_eq!(seen.last().unwrap(), &last_of(&order, |value| value == &json!("COMPLETED") || value == &json!("FAILED")), "tiers {order:?}: the top tier wins, inside it the last arrival");
        let top = seen.iter().position(|value| value == &json!("COMPLETED") || value == &json!("FAILED"));
        assert!(top.is_none_or(|at| seen[at..].iter().all(|value| value == &json!("COMPLETED") || value == &json!("FAILED"))), "never steps down a tier: {seen:?}");
    }
}

#[test]
fn data_orders_merge_stable_list() {
    let stored = Some(json!(["a", "b", "c"]));
    let first = json!(["a", "b", "c", "d"]);
    let second = json!(["d", "c", "e"]);
    let orders = permutations(&[first.clone(), second.clone()]);
    assert_eq!(orders.len(), 2);
    for order in orders {
        let seen = fold(&StableList, stored.clone(), &order);
        let expected = if order[0] == first { json!(["c", "d", "e"]) } else { json!(["c", "d", "a", "b"]) };
        assert_eq!(seen.last().unwrap(), &expected, "{order:?}: kept places first, new items in the order of arrival");
    }
    for order in permutations(&[first.clone(), Value::Null]) {
        let seen = fold(&StableList, stored.clone(), &order);
        assert_eq!(seen.last().unwrap(), &json!(["a", "b", "c", "d"]), "{order:?}: null keeps the stored list wherever it arrives");
    }
}

#[test]
fn data_orders_merge_keys() {
    let rule = Keys { keys: [("fileUrl".to_string(), NonEmpty), ("width".to_string(), Positive), ("transcodeProgress".to_string(), Monotonic)].into() };
    let parts = [json!({"fileUrl": "f"}), json!({"width": 640, "transcodeProgress": 40}), json!({"transcodeProgress": 100, "kind": "video"}), Value::Null];
    let orders = permutations(&parts);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let seen = fold(&rule, Some(json!({"width": 0, "transcodeProgress": 0})), &order);
        assert_eq!(seen.last().unwrap(), &json!({"fileUrl": "f", "width": 640, "transcodeProgress": 100, "kind": "video"}), "{order:?}: objects with their own keys merge to the same object in any order");
    }
}
