use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::AssociationSpec;

pub(crate) struct Order<'a> {
    pub id: Option<&'a str>,
    pub tuple: Vec<Option<&'a Value>>,
}

fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64().partial_cmp(&right.as_f64()).unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        _ => Ordering::Equal,
    }
}

fn compare_ids(left: &str, right: &str) -> Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn is_temp(id: &str) -> bool {
    id.starts_with("temp-")
}

pub(crate) fn is_newer(candidate: &Order<'_>, current: &Order<'_>) -> bool {
    if let (Some(candidate_id), Some(current_id)) = (candidate.id, current.id) {
        if candidate_id == current_id {
            return true;
        }
        if is_temp(candidate_id) != is_temp(current_id) {
            return is_temp(candidate_id);
        }
    }
    if current.id.is_none() && current.tuple.iter().all(Option::is_none) {
        return true;
    }
    for (candidate, current) in candidate.tuple.iter().zip(&current.tuple) {
        if let (Some(candidate), Some(current)) = (candidate, current) {
            match compare_values(candidate, current) {
                Ordering::Greater => return true,
                Ordering::Less => return false,
                Ordering::Equal => {}
            }
        }
    }
    matches!((candidate.id, current.id), (Some(candidate), Some(current)) if compare_ids(candidate, current) == Ordering::Greater)
}

impl AssociationSpec {
    pub fn pointer_field(&self) -> Option<&str> {
        self.sets.iter().find(|(_, child)| child.as_str() == "id").map(|(parent, _)| parent.as_str())
    }

    fn parent_order<'a>(&'a self, row: &'a Map<String, Value>) -> Order<'a> {
        Order {
            id: self.pointer_field().and_then(|field| present(row.get(field))).and_then(Value::as_str),
            tuple: self.newer.iter().map(|newer| present(row.get(&newer.parent))).collect(),
        }
    }

    fn child_order<'a>(&'a self, row: &'a Map<String, Value>) -> Order<'a> {
        Order { id: row.get("id").and_then(Value::as_str), tuple: self.newer.iter().map(|newer| present(row.get(&newer.child))).collect() }
    }

    pub fn parent_is_newer(&self, stored: &Map<String, Value>, incoming: &Map<String, Value>) -> bool {
        self.newer.is_empty() || is_newer(&self.parent_order(incoming), &self.parent_order(stored))
    }

    pub fn child_is_newer(&self, parent: &Map<String, Value>, child: &Map<String, Value>) -> bool {
        self.newer.is_empty() || is_newer(&self.child_order(child), &self.parent_order(parent))
    }
}
