use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{json, Map};

use super::diff::{Diff, Entry, Stamp};
use super::fixtures::*;
use super::watch::{Change, Related};
use super::{Record, Row};
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::touches::total;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::query::{Order, Query};
use crate::syncstate::SyncInfo;
use crate::value::SortDirection;

const SMALL: usize = 100;
const LARGE: usize = 10_000;

type Entries = Vec<Arc<Entry>>;
type Case = fn(usize) -> (Entries, Entries, Vec<(RowId, RowId)>);

fn id_at(index: usize) -> RowId {
    rid(&format!("u{index:06}"))
}

fn entry_of(id: RowId, revision: i64) -> Arc<Entry> {
    Arc::new(Entry { id, stamp: Stamp { revision, ..Stamp::default() } })
}

fn entries_of(count: usize) -> Entries {
    (0..count).map(|index| entry_of(id_at(index), 1)).collect()
}

fn row_of(entry: &Entry) -> Row {
    Arc::new(Record { id: entry.id.clone(), revision: entry.stamp.revision, data: Map::new(), sync: SyncInfo::default(), ephemeral: 0 })
}

fn ids_of(rows: &[Row]) -> Vec<RowId> {
    rows.iter().map(|row| row.id.clone()).collect()
}

fn update(size: usize) -> (Entries, Entries, Vec<(RowId, RowId)>) {
    let mut new = entries_of(size);
    new[size / 2] = entry_of(id_at(size / 2), 2);
    (entries_of(size), new, Vec::new())
}

fn remove(size: usize) -> (Entries, Entries, Vec<(RowId, RowId)>) {
    let mut new = entries_of(size);
    new.remove(size / 2);
    (entries_of(size), new, Vec::new())
}

fn insert(size: usize) -> (Entries, Entries, Vec<(RowId, RowId)>) {
    let mut new = entries_of(size);
    new.insert(size / 2, entry_of(rid("inserted"), 1));
    (entries_of(size), new, Vec::new())
}

fn relocate(size: usize) -> (Entries, Entries, Vec<(RowId, RowId)>) {
    let mut new = entries_of(size);
    let moved = new.remove(size / 2);
    new.insert(0, moved);
    (entries_of(size), new, Vec::new())
}

fn replace(size: usize) -> (Entries, Entries, Vec<(RowId, RowId)>) {
    let mut new = entries_of(size);
    new[size / 2] = entry_of(rid("renamed"), 1);
    (entries_of(size), new, vec![(id_at(size / 2), rid("renamed"))])
}

fn change_between(old: &[Arc<Entry>], new: &[Arc<Entry>], replaced: &[(RowId, RowId)]) -> Change {
    let diff = Diff::between(old, new, replaced);
    let supplied: HashSet<&RowId> = diff.inserted.iter().chain(&diff.updated).chain(diff.replaced.iter().map(|(_, to)| to)).collect();
    let rows = new.iter().filter(|entry| supplied.contains(&entry.id)).map(|entry| row_of(entry)).collect();
    Change { revision: 2, rows, related: Related::new(), diff, full: false, size: 0, window: None }
}

fn apply_cost(size: usize, case: Case) -> usize {
    let (old, new, replaced) = case(size);
    let change = change_between(&old, &new, &replaced);
    let mut rows: Vec<Row> = old.iter().map(|entry| row_of(entry)).collect();
    let before = total();
    change.apply_to(&mut rows);
    let cost = total() - before;
    assert_eq!(ids_of(&rows), new.iter().map(|entry| entry.id.clone()).collect::<Vec<_>>());
    cost
}

fn cannot_enter_cost(size: usize) -> usize {
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..size {
        let row = obj(json!({"id": id_at(index).to_string(), "name": format!("n{index:06}")}));
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: id_at(index), row });
    }
    db.land(envelope).unwrap();
    db.settle();
    let (_log, callback) = recorder();
    let _observer = db.observe(Query::new("users").ordered(vec![Order::new("id", SortDirection::Asc)]), callback).unwrap();
    db.settle();
    let users = ModelKey::new("users");
    let event = super::dispatch::Event { revision: i64::MAX / 2, models: [users.clone()].into(), rows: [RowRef::new(users, rid("absent"))].into(), ..super::dispatch::Event::default() };
    let before = total();
    db.shared.refresh_groups(&[event]);
    total() - before
}

#[test]
fn applying_one_change_touches_the_same_ids_at_every_list_size() {
    let cases: [(&str, Case); 5] = [("update", update), ("remove", remove), ("insert", insert), ("move", relocate), ("replace", replace)];
    for (name, case) in cases {
        assert_eq!(apply_cost(SMALL, case), apply_cost(LARGE, case), "id touches for one {name} at {SMALL} and {LARGE} rows");
    }
}

#[test]
fn a_row_outside_a_plain_list_touches_the_same_ids_at_every_list_size() {
    assert_eq!(cannot_enter_cost(SMALL), cannot_enter_cost(LARGE), "id touches at {SMALL} and {LARGE} rows");
}
