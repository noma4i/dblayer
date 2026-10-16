use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, HashSet};

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
    pub renamed: Vec<(RowId, RowId)>,
    pub at: BTreeMap<RowId, usize>,
    pub was: BTreeMap<RowId, usize>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.inserted.is_empty() && self.removed.is_empty() && self.updated.is_empty() && self.moved.is_empty() && self.replaced.is_empty()
    }

    pub fn inserting<E: Borrow<Entry>>(entries: &[E], replaced: &[(RowId, RowId)]) -> Self {
        let inserted: Vec<RowId> = entries.iter().map(|entry| entry_of(entry).id.clone()).collect();
        let at: BTreeMap<RowId, usize> = inserted.iter().cloned().enumerate().map(|(index, id)| (id, index)).collect();
        let renamed = replaced.iter().filter(|(from, to)| at.contains_key(to) && !at.contains_key(from)).cloned().collect();
        Self { inserted, at, renamed, ..Self::default() }
    }

    pub fn placed(&self) -> Vec<(RowId, usize)> {
        let mut placed: Vec<(RowId, usize)> = self.inserted.iter().chain(&self.moved).map(|id| (id.clone(), self.at[id])).collect();
        placed.sort_by_key(|(_, index)| *index);
        placed
    }

    pub fn between<E: Borrow<Entry>>(old: &[E], new: &[E], replaced: &[(RowId, RowId)]) -> Self {
        if old.len() == new.len() && old.iter().zip(new).all(|(before, after)| entry_of(before).id == entry_of(after).id) {
            let changed = old.iter().zip(new).enumerate().filter(|(_, (before, after))| {
                entry_of(*before).stamp != entry_of(*after).stamp
            });
            let (updated, at): (Vec<RowId>, BTreeMap<RowId, usize>) = changed
                .map(|(index, (_, after))| {
                    let after = entry_of(after);
                    (after.id.clone(), (after.id.clone(), index))
                })
                .unzip();
            return Self { updated, at, ..Self::default() };
        }
        let old_ids: HashSet<&RowId> = old.iter().map(|entry| &entry_of(entry).id).collect();
        let new_position: HashMap<&RowId, usize> = new.iter().enumerate().map(|(index, entry)| (&entry_of(entry).id, index)).collect();
        let aliases: HashMap<&RowId, &RowId> = replaced
            .iter()
            .filter(|(from, to)| old_ids.contains(from) && new_position.contains_key(to) && !new_position.contains_key(from) && !old_ids.contains(to))
            .map(|(from, to)| (from, to))
            .collect();
        let canonical = |id: &RowId| -> RowId { aliases.get(id).map_or_else(|| id.clone(), |to| (*to).clone()) };
        let old_canonical: HashSet<RowId> = old.iter().map(|entry| canonical(&entry_of(entry).id)).collect();
        let renamed = replaced.iter().filter(|(from, to)| new_position.contains_key(to) && !old_ids.contains(to) && !new_position.contains_key(from)).cloned().collect();
        let mut diff = Diff {
            renamed,
            replaced: old.iter().filter_map(|entry| aliases.get(&entry_of(entry).id).map(|to| (entry_of(entry).id.clone(), (*to).clone()))).collect(),
            inserted: new.iter().filter(|&entry| !old_canonical.contains(&entry_of(entry).id)).map(|entry| entry_of(entry).id.clone()).collect(),
            removed: old.iter().filter(|&entry| !new_position.contains_key(&canonical(&entry_of(entry).id))).map(|entry| entry_of(entry).id.clone()).collect(),
            ..Self::default()
        };
        let old_stamp: HashMap<RowId, &Stamp> = old.iter().map(|entry| (canonical(&entry_of(entry).id), &entry_of(entry).stamp)).collect();
        diff.updated = new
            .iter()
            .filter(|&entry| !aliases.values().any(|to| *to == &entry_of(entry).id))
            .filter(|&entry| old_stamp.get(&entry_of(entry).id).is_some_and(|stamp| **stamp != entry_of(entry).stamp))
            .map(|entry| entry_of(entry).id.clone())
            .collect();
        let common: Vec<usize> = old.iter().filter_map(|entry| new_position.get(&canonical(&entry_of(entry).id)).copied()).collect();
        let stable = longest_increasing(&common);
        let mut moved: Vec<usize> = common.iter().enumerate().filter(|(index, _)| !stable.contains(index)).map(|(_, position)| *position).collect();
        moved.sort_unstable();
        diff.moved = moved.into_iter().map(|position| entry_of(&new[position]).id.clone()).collect();
        let leaving: HashSet<&RowId> = diff.removed.iter().chain(&diff.moved).chain(diff.replaced.iter().map(|(_, to)| to)).collect();
        diff.was = old.iter().enumerate().map(|(index, entry)| (canonical(&entry_of(entry).id), index)).filter(|(id, _)| leaving.contains(id)).collect();
        let replaced_to = diff.replaced.iter().map(|(_, to)| to);
        diff.at = diff.inserted.iter().chain(&diff.updated).chain(&diff.moved).chain(replaced_to).map(|id| (id.clone(), new_position[id])).collect();
        diff
    }
}

fn entry_of<E: Borrow<Entry>>(item: &E) -> &Entry {
    item.borrow()
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
