use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use rusqlite::types::Value;
use serde_json::Map;

use super::*;
use crate::db::ranked::{OrderKey, Ranked};
use crate::db::watch::{Change, Related};
use crate::db::{Record, Row};
use crate::identity::RowId;
use crate::syncstate::SyncInfo;

type Items = BTreeMap<String, (i64, i64)>;

struct Dice(u64);

impl Dice {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % below
    }
}

fn id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn entries_of(items: &Items) -> Vec<Arc<Entry>> {
    let mut list: Vec<(&String, &(i64, i64))> = items.iter().collect();
    list.sort_by_key(|(name, (value, _))| (*value, (*name).clone()));
    list.into_iter().map(|(name, (_, revision))| Arc::new(Entry { id: id(name), stamp: Stamp { revision: *revision, ..Stamp::default() } })).collect()
}

fn ranked_of(items: &Items) -> Ranked {
    let by_name: BTreeMap<&str, i64> = items.iter().map(|(name, (value, _))| (name.as_str(), *value)).collect();
    let list = entries_of(items).into_iter().map(|entry| (Arc::new(OrderKey::new(vec![Value::Integer(by_name[entry.id.as_str()])], &[false], entry.id.to_string())), entry)).collect();
    Ranked::from_sorted(list)
}

fn row_of(entry: &Entry) -> Row {
    Arc::new(Record { id: entry.id.clone(), revision: entry.stamp.revision, data: Map::new(), sync: SyncInfo::default(), ephemeral: 0 })
}

fn applied(old: &[Arc<Entry>], new: &[Arc<Entry>], diff: Diff) -> Vec<(RowId, i64)> {
    let supplied: HashSet<&RowId> = diff.inserted.iter().chain(&diff.updated).chain(diff.replaced.iter().map(|(_, to)| to)).collect();
    let rows = new.iter().filter(|entry| supplied.contains(&entry.id)).map(|entry| row_of(entry)).collect();
    let change = Change { revision: 2, rows, related: Related::new(), diff, full: false, size: 0, window: None, keys: Default::default() };
    let mut list: Vec<Row> = old.iter().map(|entry| row_of(entry)).collect();
    change.apply_to(&mut list);
    list.iter().map(|row| (row.id.clone(), row.revision)).collect()
}

fn expected(new: &[Arc<Entry>]) -> Vec<(RowId, i64)> {
    new.iter().map(|entry| (entry.id.clone(), entry.stamp.revision)).collect()
}

fn edit(dice: &mut Dice, items: &mut Items, touched: &mut HashSet<String>, replaced: &mut Vec<(RowId, RowId)>, fresh: &mut u64) {
    let names: Vec<String> = items.keys().cloned().collect();
    let pick = (!names.is_empty()).then(|| names[dice.next(names.len() as u64) as usize].clone());
    match (dice.next(5), pick) {
        (0, Some(name)) => {
            items.remove(&name);
            touched.insert(name);
        }
        (1, Some(name)) => {
            let entry = items.get_mut(&name).unwrap();
            *entry = (dice.next(30) as i64, entry.1 + 1);
            touched.insert(name);
        }
        (2, Some(name)) => {
            items.get_mut(&name).unwrap().1 += 1;
            touched.insert(name);
        }
        (3, Some(name)) if !touched.contains(&name) => {
            *fresh += 1;
            let to = format!("r{fresh:04}");
            let (value, revision) = items.remove(&name).unwrap();
            items.insert(to.clone(), (if dice.next(2) == 0 { value } else { dice.next(30) as i64 }, revision + 1));
            replaced.push((id(&name), id(&to)));
            touched.extend([name, to]);
        }
        _ => {
            *fresh += 1;
            let name = format!("n{fresh:04}");
            items.insert(name.clone(), (dice.next(30) as i64, 1));
            touched.insert(name);
        }
    }
}

#[test]
fn random_steps_apply_to_the_new_list_and_name_the_same_rows_as_between() {
    let mut dice = Dice(11);
    let mut fresh = 0;
    for trial in 0..4000 {
        let mut old: Items = BTreeMap::new();
        for _ in 0..dice.next(45) {
            fresh += 1;
            old.insert(format!("o{fresh:04}"), (dice.next(30) as i64, 1));
        }
        let mut new = old.clone();
        let (mut touched, mut replaced) = (HashSet::new(), Vec::new());
        for _ in 0..1 + dice.next(5) {
            edit(&mut dice, &mut new, &mut touched, &mut replaced, &mut fresh);
        }
        let (before, after) = (entries_of(&old), entries_of(&new));
        let stepped = Diff::stepping(&ranked_of(&old), &ranked_of(&new), &touched, &replaced);
        let full = Diff::between(&before, &after, &replaced);
        assert_eq!(applied(&before, &after, stepped.clone()), expected(&after), "trial {trial}: {stepped:?} for {replaced:?}");
        assert_eq!((&stepped.inserted, &stepped.removed, &stepped.updated), (&full.inserted, &full.removed, &full.updated), "trial {trial}");
        assert_eq!((&stepped.replaced, &stepped.renamed), (&full.replaced, &full.renamed), "trial {trial}");
        assert_eq!(stepped.is_empty(), full.is_empty(), "trial {trial}");
    }
}

#[test]
fn a_row_moved_far_is_the_only_moved_row() {
    let old: Items = (0..50).map(|index| (format!("r{index:02}"), (index as i64 * 2, 1))).collect();
    let mut new = old.clone();
    new.insert("r40".to_string(), (-1, 2));
    let diff = Diff::stepping(&ranked_of(&old), &ranked_of(&new), &HashSet::from(["r40".to_string()]), &[]);
    assert_eq!((diff.moved, diff.updated, diff.was[&id("r40")], diff.at[&id("r40")]), (vec![id("r40")], vec![id("r40")], 40, 0));
}

#[test]
fn two_touched_rows_that_swap_places_move_one_of_them() {
    let old: Items = (0..6).map(|index| (format!("r{index}"), (index as i64 * 2, 1))).collect();
    let mut new = old.clone();
    new.insert("r2".to_string(), (6, 2));
    new.insert("r3".to_string(), (4, 2));
    let (before, after) = (entries_of(&old), entries_of(&new));
    let diff = Diff::stepping(&ranked_of(&old), &ranked_of(&new), &HashSet::from(["r2".to_string(), "r3".to_string()]), &[]);
    assert_eq!(diff.moved.len(), 1);
    assert_eq!(applied(&before, &after, diff), expected(&after));
}
