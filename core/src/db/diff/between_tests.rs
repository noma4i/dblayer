use super::*;
use crate::identity::{ModelKey, RowId, RowRef};

fn id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn entry(text: &str, revision: i64) -> Entry {
    Entry { id: id(text), stamp: Stamp { revision, ..Stamp::default() } }
}

fn entries(items: &[(&str, i64)]) -> Vec<Entry> {
    items.iter().map(|(text, revision)| entry(text, *revision)).collect()
}

fn ids(items: &[&str]) -> Vec<RowId> {
    items.iter().map(|text| id(text)).collect()
}

#[test]
fn identical_lists_give_an_empty_diff() {
    let list = entries(&[("a", 1), ("b", 2)]);
    assert!(Diff::between(&list, &list, &[]).is_empty());
}

#[test]
fn first_state_is_all_inserted() {
    let diff = Diff::inserting(&entries(&[("a", 1), ("b", 2)]));
    assert_eq!(diff.inserted, ids(&["a", "b"]));
    assert!(diff.removed.is_empty() && diff.updated.is_empty() && diff.moved.is_empty());
}

#[test]
fn inserted_removed_and_updated_are_separated() {
    let diff = Diff::between(&entries(&[("a", 1), ("b", 1), ("c", 1)]), &entries(&[("a", 1), ("c", 2), ("d", 3)]), &[]);
    assert_eq!(diff.inserted, ids(&["d"]));
    assert_eq!(diff.removed, ids(&["b"]));
    assert_eq!(diff.updated, ids(&["c"]));
    assert!(diff.moved.is_empty());
}

#[test]
fn an_insert_in_front_does_not_move_the_others() {
    let diff = Diff::between(&entries(&[("a", 1), ("b", 1)]), &entries(&[("x", 1), ("a", 1), ("b", 1)]), &[]);
    assert_eq!(diff.inserted, ids(&["x"]));
    assert!(diff.moved.is_empty());
}

#[test]
fn a_removal_in_front_does_not_move_the_others() {
    let diff = Diff::between(&entries(&[("x", 1), ("a", 1), ("b", 1)]), &entries(&[("a", 1), ("b", 1)]), &[]);
    assert_eq!(diff.removed, ids(&["x"]));
    assert!(diff.moved.is_empty());
}

#[test]
fn a_row_jumping_to_the_front_is_the_only_one_moved() {
    let diff = Diff::between(&entries(&[("a", 1), ("b", 1), ("c", 1)]), &entries(&[("c", 1), ("a", 1), ("b", 1)]), &[]);
    assert_eq!(diff.moved, ids(&["c"]));
}

#[test]
fn a_changed_and_moved_row_is_in_both_lists() {
    let diff = Diff::between(&entries(&[("a", 1), ("b", 1)]), &entries(&[("b", 2), ("a", 1)]), &[]);
    assert_eq!(diff.updated, ids(&["b"]));
    assert_eq!(diff.moved.len(), 1);
}

#[test]
fn a_changed_related_row_marks_the_parent_updated() {
    let author = RowRef::new(ModelKey::new("users"), id("u1"));
    let old = vec![Entry { id: id("m1"), stamp: Stamp { revision: 1, related: vec![(author.clone(), 5)], ..Stamp::default() } }];
    let new = vec![Entry { id: id("m1"), stamp: Stamp { revision: 1, related: vec![(author, 6)], ..Stamp::default() } }];
    assert_eq!(Diff::between(&old, &new, &[]).updated, ids(&["m1"]));
}

#[test]
fn a_replaced_pair_is_neither_removed_nor_inserted() {
    let diff = Diff::between(&entries(&[("temp-1", 1), ("b", 1)]), &entries(&[("r1", 2), ("b", 1)]), &[(id("temp-1"), id("r1"))]);
    assert_eq!(diff.replaced, vec![(id("temp-1"), id("r1"))]);
    assert!(diff.inserted.is_empty() && diff.removed.is_empty() && diff.updated.is_empty() && diff.moved.is_empty());
}

#[test]
fn a_replaced_pair_without_the_new_row_is_ignored() {
    let diff = Diff::between(&entries(&[("temp-1", 1)]), &entries(&[("z", 1)]), &[(id("temp-1"), id("r1"))]);
    assert!(diff.replaced.is_empty());
    assert_eq!(diff.removed, ids(&["temp-1"]));
    assert_eq!(diff.inserted, ids(&["z"]));
}

struct Seed(u64);

impl Seed {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

fn random_list(seed: &mut Seed, pool: u64) -> Vec<Entry> {
    let mut picked: Vec<Entry> = Vec::new();
    for number in 0..pool {
        if seed.below(2) == 0 {
            picked.push(entry(&format!("r{number}"), seed.below(3) as i64));
        }
    }
    for index in (1..picked.len()).rev() {
        let other = seed.below(index as u64 + 1) as usize;
        picked.swap(index, other);
    }
    picked
}

fn apply(old: &[Entry], diff: &Diff, new: &[Entry]) -> Vec<Entry> {
    let renamed = |row: &RowId| diff.replaced.iter().find(|(from, _)| from == row).map(|(_, to)| to.clone());
    let mut fixed: std::collections::VecDeque<Entry> = old
        .iter()
        .filter(|entry| !diff.removed.contains(&entry.id))
        .map(|entry| Entry { id: renamed(&entry.id).unwrap_or_else(|| entry.id.clone()), stamp: entry.stamp.clone() })
        .filter(|entry| !diff.moved.contains(&entry.id))
        .collect();
    let mut result = Vec::new();
    for target in new {
        if diff.inserted.contains(&target.id) || diff.moved.contains(&target.id) {
            result.push(target.clone());
            continue;
        }
        let mut next = fixed.pop_front().expect("fixed row for the position");
        assert_eq!(next.id, target.id, "order broken");
        if diff.updated.contains(&next.id) || diff.replaced.iter().any(|(_, to)| to == &next.id) {
            next.stamp = target.stamp.clone();
        }
        result.push(next);
    }
    assert!(fixed.is_empty());
    result
}

#[test]
fn applying_the_diff_to_the_old_list_gives_the_new_list() {
    let mut seed = Seed(0x9E3779B97F4A7C15);
    for _ in 0..2000 {
        let old = random_list(&mut seed, 12);
        let mut new = random_list(&mut seed, 12);
        let mut replaced = Vec::new();
        if let Some(candidate) = old.iter().find(|entry| !new.iter().any(|other| other.id == entry.id)) {
            let at = seed.below(new.len() as u64 + 1) as usize;
            new.insert(at, entry("fresh", seed.below(3) as i64 + 5));
            replaced.push((candidate.id.clone(), id("fresh")));
        }
        let diff = Diff::between(&old, &new, &replaced);
        assert_eq!(apply(&old, &diff, &new), new, "old {old:?} new {new:?} diff {diff:?}");
    }
}
