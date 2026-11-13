use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::AssociationSpec;
use crate::identity::is_temp_id;

fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => crate::value::compare_numbers(left, right).unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        _ => Ordering::Equal,
    }
}

impl AssociationSpec {
    pub fn pointer_field(&self) -> Option<&str> {
        self.sets.iter().find(|(_, child)| child.as_str() == "id").map(|(parent, _)| parent.as_str())
    }

    fn compared_parents(&self) -> impl Iterator<Item = &str> {
        self.newer.iter().flat_map(|field| self.sets.iter().filter(move |(_, child)| *child == field).map(|(parent, _)| parent.as_str()))
    }

    /// The fields `sets:` from the newest child, a temporary winner keeps the newer values of the newest confirmed child.
    pub fn derived(&self, newest: Option<&Map<String, Value>>, confirmed: Option<&Map<String, Value>>) -> Map<String, Value> {
        let mut values = self.from_child(newest);
        if newest.and_then(|child| child.get("id")).and_then(Value::as_str).is_some_and(is_temp_id) {
            let behind = self.from_child(confirmed);
            for parent in self.compared_parents() {
                let Some(held) = behind.get(parent) else { continue };
                if values.get(parent).is_none_or(|value| compare_values(held, value) == Ordering::Greater) { values.insert(parent.to_string(), held.clone()); }
            }
        }
        values
    }

    fn from_child(&self, child: Option<&Map<String, Value>>) -> Map<String, Value> {
        let Some(child) = child else { return Map::new() };
        self.sets.iter().filter_map(|(parent, field)| present(child.get(field)).map(|value| (parent.clone(), value.clone()))).collect()
    }
}
