use std::collections::{HashMap, HashSet};

use crate::identity::{RowId, RowRef};
use crate::db::Row;
use crate::syncstate::SyncInfo;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stamp {
    pub revision: i64,
    pub sync: SyncInfo,
    pub related: Vec<(RowRef, Stamp)>,
    pub ephemeral: u64,
}

impl Stamp {
    pub fn of(row: &Row, related: Vec<(RowRef, Stamp)>) -> Self {
        Self { revision: row.revision, sync: row.sync.clone(), related, ephemeral: row.ephemeral }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub id: RowId,
    pub stamp: Stamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Diff {
    pub inserted: Vec<RowId>,
    pub removed: Vec<RowId>,
    pub updated: Vec<RowId>,
    pub moved: Vec<RowId>,
    pub replaced: Vec<(RowId, RowId)>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.inserted.is_empty() && self.removed.is_empty() && self.updated.is_empty() && self.moved.is_empty() && self.replaced.is_empty()
    }

    pub fn inserting(entries: &[Entry]) -> Self {
        Self { inserted: entries.iter().map(|entry| entry.id.clone()).collect(), ..Self::default() }
    }

    pub fn between(old: &[Entry], new: &[Entry], replaced: &[(RowId, RowId)]) -> Self {
        let old_ids: HashSet<&RowId> = old.iter().map(|entry| &entry.id).collect();
        let new_position: HashMap<&RowId, usize> = new.iter().enumerate().map(|(index, entry)| (&entry.id, index)).collect();
        let aliases: HashMap<&RowId, &RowId> = replaced
            .iter()
            .filter(|(from, to)| old_ids.contains(from) && new_position.contains_key(to) && !new_position.contains_key(from) && !old_ids.contains(to))
            .map(|(from, to)| (from, to))
            .collect();
        let canonical = |id: &RowId| -> RowId { aliases.get(id).map_or_else(|| id.clone(), |to| (*to).clone()) };
        let old_canonical: HashSet<RowId> = old.iter().map(|entry| canonical(&entry.id)).collect();
        let mut diff = Diff {
            replaced: old.iter().filter_map(|entry| aliases.get(&entry.id).map(|to| (entry.id.clone(), (*to).clone()))).collect(),
            inserted: new.iter().filter(|entry| !old_canonical.contains(&entry.id)).map(|entry| entry.id.clone()).collect(),
            removed: old.iter().filter(|entry| !new_position.contains_key(&canonical(&entry.id))).map(|entry| entry.id.clone()).collect(),
            ..Self::default()
        };
        let old_stamp: HashMap<RowId, &Stamp> = old.iter().map(|entry| (canonical(&entry.id), &entry.stamp)).collect();
        diff.updated = new
            .iter()
            .filter(|entry| !aliases.values().any(|to| *to == &entry.id))
            .filter(|entry| old_stamp.get(&entry.id).is_some_and(|stamp| **stamp != entry.stamp))
            .map(|entry| entry.id.clone())
            .collect();
        let common: Vec<usize> = old.iter().filter_map(|entry| new_position.get(&canonical(&entry.id)).copied()).collect();
        let stable = longest_increasing(&common);
        let mut moved: Vec<usize> = common.iter().enumerate().filter(|(index, _)| !stable.contains(index)).map(|(_, position)| *position).collect();
        moved.sort_unstable();
        diff.moved = moved.into_iter().map(|position| new[position].id.clone()).collect();
        diff
    }
}

pub(crate) fn longest_increasing(sequence: &[usize]) -> HashSet<usize> {
    let mut tails: Vec<usize> = Vec::new();
    let mut previous: Vec<Option<usize>> = vec![None; sequence.len()];
    for (index, value) in sequence.iter().enumerate() {
        let slot = tails.partition_point(|tail| sequence[*tail] < *value);
        previous[index] = slot.checked_sub(1).map(|before| tails[before]);
        if slot == tails.len() {
            tails.push(index);
        } else {
            tails[slot] = index;
        }
    }
    let mut kept = HashSet::new();
    let mut cursor = tails.last().copied();
    while let Some(index) = cursor {
        kept.insert(index);
        cursor = previous[index];
    }
    kept
}
