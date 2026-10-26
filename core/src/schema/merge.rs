use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::FieldKind;
use crate::value::ScalarKind;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MergeRule {
    Present,
    NonEmpty,
    Positive,
    Monotonic,
    Local,
    StableList,
    Ladder { tiers: Vec<Vec<String>> },
    Keys { keys: BTreeMap<String, MergeRule> },
}

fn is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
        _ => false,
    }
}

fn is_positive(value: &Value) -> bool {
    value.as_f64().is_some_and(|number| number > 0.0)
}

fn order(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64()?.partial_cmp(&right.as_f64()?),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn monotonic_class(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Number(_) => 1,
        Value::String(_) => 2,
        _ => 3,
    }
}

fn tier(tiers: &[Vec<String>], value: &Value) -> Option<usize> {
    let text = value.as_str()?;
    tiers.iter().position(|tier| tier.iter().any(|name| name == text))
}

impl MergeRule {
    pub fn fits(&self, kind: &FieldKind) -> bool {
        let number = matches!(kind, FieldKind::Scalar { kind: ScalarKind::Num | ScalarKind::Int | ScalarKind::Date });
        match self {
            MergeRule::Present | MergeRule::NonEmpty | MergeRule::Local => !matches!(kind, FieldKind::File),
            MergeRule::Positive | MergeRule::Monotonic => number,
            MergeRule::Ladder { tiers } => match kind {
                FieldKind::Enum { values } => tiers.iter().flatten().all(|name| values.contains(name)),
                FieldKind::Scalar { kind: ScalarKind::Str } => true,
                _ => false,
            },
            MergeRule::StableList => matches!(kind, FieldKind::List { .. }),
            MergeRule::Keys { .. } => matches!(kind, FieldKind::Object | FieldKind::Raw),
        }
    }

    pub(crate) fn local_keys(&self) -> impl Iterator<Item = &str> {
        let keys = match self {
            MergeRule::Keys { keys } => Some(keys),
            _ => None,
        };
        keys.into_iter().flatten().filter(|(_, rule)| **rule == MergeRule::Local).map(|(key, _)| key.as_str())
    }

    pub fn rank(&self, left: &Value, right: &Value) -> Ordering {
        match self {
            MergeRule::Present => (!left.is_null()).cmp(&!right.is_null()),
            MergeRule::NonEmpty => (!is_empty(left)).cmp(&!is_empty(right)),
            MergeRule::Positive => is_positive(left).cmp(&is_positive(right)),
            MergeRule::Monotonic => monotonic_class(left).cmp(&monotonic_class(right)).then_with(|| order(left, right).unwrap_or(Ordering::Equal)),
            MergeRule::Ladder { tiers } if !left.is_null() && !right.is_null() => tier(tiers, left).cmp(&tier(tiers, right)),
            MergeRule::Ladder { .. } => (!left.is_null()).cmp(&!right.is_null()),
            MergeRule::Local | MergeRule::StableList | MergeRule::Keys { .. } => Ordering::Equal,
        }
    }

    pub fn apply(&self, stored: Option<&Value>, incoming: Value) -> Value {
        let Some(stored) = stored.filter(|stored| !stored.is_null()) else { return incoming };
        let keep = match self {
            MergeRule::Present => incoming.is_null(),
            MergeRule::Local => false,
            MergeRule::NonEmpty => is_empty(&incoming) && !is_empty(stored),
            MergeRule::Positive => is_positive(stored) && !is_positive(&incoming),
            MergeRule::Monotonic => incoming.is_null() || order(&incoming, stored) == Some(Ordering::Less),
            MergeRule::Ladder { tiers } => incoming.is_null() || matches!((tier(tiers, &incoming), tier(tiers, stored)), (Some(incoming), Some(stored)) if incoming < stored),
            MergeRule::StableList => return Self::keep_places(stored, incoming),
            MergeRule::Keys { keys } => return Self::merge_keys(keys, stored, incoming),
        };
        if keep { stored.clone() } else { incoming }
    }

    fn keep_places(stored: &Value, incoming: Value) -> Value {
        let (Value::Array(stored), Value::Array(incoming)) = (stored, &incoming) else { return if incoming.is_null() { stored.clone() } else { incoming } };
        let mut merged: Vec<Value> = Vec::with_capacity(incoming.len());
        for item in stored.iter().filter(|item| incoming.contains(item)).chain(incoming) {
            if !merged.contains(item) {
                merged.push(item.clone());
            }
        }
        Value::Array(merged)
    }

    fn merge_keys(keys: &BTreeMap<String, MergeRule>, stored: &Value, incoming: Value) -> Value {
        let (Value::Object(stored), Value::Object(incoming)) = (stored, &incoming) else { return if incoming.is_null() { stored.clone() } else { incoming } };
        let mut merged: Map<String, Value> = stored.clone();
        for (key, value) in incoming {
            let rule = keys.get(key).unwrap_or(&MergeRule::Present);
            merged.insert(key.clone(), rule.apply(stored.get(key), value.clone()));
        }
        Value::Object(merged)
    }
}
