use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::{AssociationKind, AssociationSpec, FieldSpec, ModelSpec};
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
        _ => Ordering::Equal,
    }
}

fn numeric_id(id: &str) -> Option<u64> {
    (!id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()) && (id == "0" || !id.starts_with('0'))).then(|| id.parse().ok()).flatten()
}

fn compare_ids(left: &str, right: &str) -> Ordering {
    match (numeric_id(left), numeric_id(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.as_bytes().cmp(right.as_bytes()),
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

    pub fn child_is_newer(&self, parent: &Map<String, Value>, child: &Map<String, Value>) -> bool {
        self.newer.is_empty() || is_newer(&self.child_order(child), &self.parent_order(parent))
    }

    pub fn newest<'a>(&self, children: impl IntoIterator<Item = &'a Map<String, Value>>) -> Option<&'a Map<String, Value>> {
        children.into_iter().reduce(|best, child| if is_newer(&self.child_order(child), &self.child_order(best)) { child } else { best })
    }

    /// P-DATA-59: the fields `sets:` from the server value of the parent and its newest child, a temporary child keeps the newer values of the newest confirmed one.
    pub fn derived(&self, server: &Map<String, Value>, newest: Option<&Map<String, Value>>, confirmed: Option<&Map<String, Value>>) -> Map<String, Value> {
        let mut values = self.chosen(server, newest);
        if newest.and_then(|child| child.get("id")).and_then(Value::as_str).is_some_and(is_temp_id) {
            let behind = self.chosen(server, confirmed);
            for newer in &self.newer {
                let Some(held) = behind.get(&newer.parent) else { continue };
                if values.get(&newer.parent).is_none_or(|value| compare_values(held, value) == Ordering::Greater) { values.insert(newer.parent.clone(), held.clone()); }
            }
        }
        values
    }

    fn chosen(&self, server: &Map<String, Value>, child: Option<&Map<String, Value>>) -> Map<String, Value> {
        let server: Map<String, Value> = self.sets.keys().filter_map(|field| present(server.get(field)).map(|value| (field.clone(), value.clone()))).collect();
        let Some(child) = child else { return server };
        let from_child: Map<String, Value> = self.sets.iter().filter_map(|(parent, field)| present(child.get(field)).map(|value| (parent.clone(), value.clone()))).collect();
        let id = child.get("id").and_then(Value::as_str);
        let pointed = self.pointer_field().and_then(|field| server.get(field)).and_then(Value::as_str).is_some_and(|pointer| Some(pointer) == id);
        let beats = self.newer.iter().find_map(|newer| Some((from_child.get(&newer.parent)?, server.get(&newer.parent)?))).is_some_and(|(child, server)| compare_values(child, server) == Ordering::Greater);
        let server_wins = !server.is_empty() && (pointed || (!id.is_some_and(is_temp_id) && !beats));
        let (mut values, winner) = if server_wins { (from_child, server) } else { (server, from_child) };
        values.extend(winner);
        values
    }
}

const SERVER_VALUE: &str = "server__";

impl ModelSpec {
    /// P-DATA-58: every parent field a child `sets` gets a hidden field beside it that keeps the value the server sent.
    pub fn with_server_values(models: &[ModelSpec]) -> Vec<ModelSpec> {
        let set: std::collections::BTreeSet<(&str, &str)> = models
            .iter()
            .flat_map(|model| model.associations.iter())
            .filter(|association| association.kind == AssociationKind::BelongsTo)
            .flat_map(|association| association.sets.keys().map(move |field| (association.target.as_str(), field.as_str())))
            .collect();
        models
            .iter()
            .map(|model| {
                let mut model = model.clone();
                let shadows: Vec<FieldSpec> = model
                    .fields
                    .iter()
                    .filter(|field| set.contains(&(model.key.as_str(), field.name.as_str())))
                    .map(|field| FieldSpec { name: format!("{SERVER_VALUE}{}", field.name), nullable: true, default: None, indexed: false, from: None, merge: None, searchable: false, ..field.clone() })
                    .filter(|shadow| !model.fields.iter().any(|field| field.name == shadow.name))
                    .collect();
                model.fields.extend(shadows);
                model
            })
            .collect()
    }

    pub fn server_values(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields.iter().filter_map(|field| field.name.strip_prefix(SERVER_VALUE).map(|declared| (declared, field.name.as_str())))
    }
}
