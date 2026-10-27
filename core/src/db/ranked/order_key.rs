use std::cmp::Ordering;

use rusqlite::types::Value;
use crate::value::Numeric;

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
    let numeric = |value: &Value| match value {
        Value::Integer(value) => Some(Numeric::Integer(i128::from(*value))),
        Value::Real(value) => Some(Numeric::Real(*value)),
        _ => None,
    };
    if let (Some(left), Some(right)) = (numeric(left), numeric(right)) {
        return left.compare(right).unwrap_or(Ordering::Equal);
    }
    match (left, right) {
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
        _ => storage_class(left).cmp(&storage_class(right)),
    }
}
