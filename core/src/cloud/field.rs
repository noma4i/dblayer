use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Stamp;
use crate::schema::MergeRule;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StampedValue {
    pub(crate) value: Value,
    pub(crate) stamp: Stamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Choice {
    Stored,
    Incoming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StampConflict;

pub(crate) fn choose_field(stored: &StampedValue, incoming: &StampedValue, rule: Option<&MergeRule>) -> Result<Choice, StampConflict> {
    if stored.stamp == incoming.stamp {
        return if stored.value == incoming.value { Ok(Choice::Stored) } else { Err(StampConflict) };
    }

    let rank = rule.map_or(Ordering::Equal, |rule| rule.rank(&stored.value, &incoming.value));
    Ok(match rank.then_with(|| stored.stamp.cmp(&incoming.stamp)) {
        Ordering::Greater => Choice::Stored,
        Ordering::Less => Choice::Incoming,
        Ordering::Equal => unreachable!(),
    })
}
