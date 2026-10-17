use rusqlite::types::Value;

use super::order_key::OrderKey;
use crate::query::place_of;

fn sample() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Integer(-5),
        Value::Integer(0),
        Value::Integer(7),
        Value::Integer(i64::MAX),
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX - 1),
        Value::Real(-0.5),
        Value::Real(0.0),
        Value::Real(-0.0),
        Value::Real(6.999),
        Value::Real(7.0),
        Value::Real(9.3e18),
        Value::Real(-9.3e18),
        Value::Real(9_223_372_036_854_775_807.0),
        Value::Text(String::new()),
        Value::Text("a".into()),
        Value::Text("ab".into()),
        Value::Text("B".into()),
        Value::Text("é".into()),
        Value::Text("a\u{0}b".into()),
        Value::Text("a\u{0}".into()),
        Value::Blob(vec![]),
        Value::Blob(vec![0, 1]),
        Value::Blob(vec![255]),
    ]
}

#[test]
fn the_place_of_a_row_orders_like_its_order_key_for_every_pair_and_direction() {
    let values = sample();
    for descending in [[false, false], [true, false], [false, true], [true, true]] {
        for (left_index, left) in values.iter().enumerate() {
            for (right_index, right) in values.iter().enumerate() {
                for (left_tail, right_tail) in [(Value::Null, Value::Null), (Value::Integer(1), Value::Integer(2)), (Value::Integer(2), Value::Integer(1))] {
                    let (left_cells, right_cells) = (vec![left.clone(), left_tail], vec![right.clone(), right_tail]);
                    let expected = OrderKey::new(left_cells.clone(), &descending, "x".into()).cmp(&OrderKey::new(right_cells.clone(), &descending, "x".into()));
                    let found = place_of(&left_cells, &descending).cmp(&place_of(&right_cells, &descending));
                    assert_eq!(found, expected, "{descending:?} {left_index}:{left:?} against {right_index}:{right:?}");
                }
            }
        }
    }
}
