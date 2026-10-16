use std::cmp::Ordering;

use rusqlite::types::Value;
use rusqlite::Connection;

use super::order_key::compare_cells;

fn sample() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Integer(-5),
        Value::Integer(0),
        Value::Integer(7),
        Value::Integer(i64::MAX),
        Value::Integer(i64::MIN),
        Value::Real(-0.5),
        Value::Real(0.0),
        Value::Real(6.999),
        Value::Real(7.0),
        Value::Real(9.3e18),
        Value::Real(-9.3e18),
        Value::Real(9_223_372_036_854_775_807.0),
        Value::Text(String::new()),
        Value::Text("a".into()),
        Value::Text("B".into()),
        Value::Text("é".into()),
        Value::Text("a\u{0}b".into()),
        Value::Blob(vec![]),
        Value::Blob(vec![0, 1]),
        Value::Blob(vec![255]),
    ]
}

fn first_by(connection: &Connection, tie: &str) -> i64 {
    connection.query_row(&format!("SELECT id FROM v ORDER BY a, id {tie} LIMIT 1"), [], |row| row.get(0)).unwrap()
}

#[test]
fn comparison_of_stored_values_matches_sqlite_for_every_pair() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE v (id INTEGER PRIMARY KEY, a)").unwrap();
    let values = sample();
    for (left_index, left) in values.iter().enumerate() {
        for (right_index, right) in values.iter().enumerate() {
            connection.execute("DELETE FROM v", []).unwrap();
            connection.execute("INSERT INTO v (id, a) VALUES (1, ?1), (2, ?2)", rusqlite::params![left, right]).unwrap();
            let (ascending, descending) = (first_by(&connection, "ASC"), first_by(&connection, "DESC"));
            let expected = if ascending != descending { Ordering::Equal } else if ascending == 1 { Ordering::Less } else { Ordering::Greater };
            assert_eq!(compare_cells(left, right), expected, "{left_index}:{left:?} against {right_index}:{right:?}");
        }
    }
}
