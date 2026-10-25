use std::cmp::Ordering;

use rusqlite::types::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderKey {
    terms: Vec<(Value, bool)>,
    id: String,
}

impl OrderKey {
    pub(crate) fn extended(&self, term: Value, id: &str) -> Self {
        let mut terms = self.terms.clone();
        terms.push((term, false));
        Self { terms, id: id.to_string() }
    }

    pub(crate) fn new(cells: Vec<Value>, descending: &[bool], id: String) -> Self {
        Self { terms: cells.into_iter().zip(descending.iter().copied()).collect(), id }
    }
}

impl Eq for OrderKey {}

impl PartialOrd for OrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((left, descending), (right, _)) in self.terms.iter().zip(&other.terms) {
            let order = compare_cells(left, right);
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
        let permanent = |id: &str| !crate::identity::is_temp_id(id);
        permanent(&self.id).cmp(&permanent(&other.id)).then_with(|| self.id.as_bytes().cmp(other.id.as_bytes()))
    }
}

fn storage_class(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

pub(crate) fn compare_cells(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Integer(a), Value::Real(b)) => compare_integer_real(*a, *b),
        (Value::Real(a), Value::Integer(b)) => compare_integer_real(*b, *a).reverse(),
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
        _ => storage_class(left).cmp(&storage_class(right)),
    }
}

fn compare_integer_real(integer: i64, real: f64) -> Ordering {
    if real < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    if real >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    let truncated = real as i64;
    integer.cmp(&truncated).then_with(|| (integer as f64).partial_cmp(&real).unwrap_or(Ordering::Equal))
}
