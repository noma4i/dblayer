use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::AssociationSpec;
use crate::identity::is_temp_id;

pub(crate) struct Order<'a> {
    pub id: Option<&'a str>,
    pub tuple: Vec<Option<&'a Value>>,
}

fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => crate::value::compare_numbers(left, right).unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Number(_), Value::String(_)) | (Value::String(_), Value::Number(_)) => match (crate::value::read_date(left), crate::value::read_date(right)) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => Ordering::Equal,
        },
        _ => Ordering::Equal,
    }
}

fn differs(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (present(left), present(right)) {
        (Some(left @ Value::Number(_)), Some(right @ Value::String(_))) | (Some(left @ Value::String(_)), Some(right @ Value::Number(_))) => compare_values(left, right) != Ordering::Equal,
        (left, right) => left != right,
    }
}

fn compare_ids(left: &str, right: &str) -> Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

pub(crate) fn is_newer(candidate: &Order<'_>, current: &Order<'_>) -> bool {
    if let (Some(candidate_id), Some(current_id)) = (candidate.id, current.id) {
        if candidate_id == current_id {
            return true;
        }
        if is_temp_id(candidate_id) != is_temp_id(current_id) {
            return is_temp_id(candidate_id);
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

    /// P-DATA-57: the `sets` pairs a write of `child` puts into `parent`; `stored` is the child row before the write, `confirmed` a child that replaced its temporary id and has no stored row yet.
    pub fn written_by<'a>(&'a self, parent: &Map<String, Value>, child: &Map<String, Value>, stored: Option<&Map<String, Value>>, confirmed: bool) -> Vec<(&'a String, &'a String)> {
        if !self.child_is_newer(parent, child) {
            return Vec::new();
        }
        let id = child.get("id").and_then(Value::as_str);
        let pointed = !confirmed && id.is_some() && self.pointer_field().and_then(|field| present(parent.get(field))).and_then(Value::as_str) == id;
        let predicted = id.is_some_and(is_temp_id);
        self.sets
            .iter()
            .filter(|(parent_field, child_field)| {
                let Some(value) = child.get(*child_field) else { return false };
                let Some(held) = present(parent.get(*parent_field)) else { return true };
                if pointed {
                    return stored.is_some_and(|stored| differs(stored.get(*child_field), Some(value)));
                }
                if predicted && self.newer.iter().any(|newer| newer.child == **child_field) {
                    return present(Some(value)).is_some_and(|value| compare_values(value, held) == Ordering::Greater);
                }
                true
            })
            .collect()
    }
}
