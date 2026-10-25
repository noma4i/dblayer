use std::collections::HashSet;
use std::sync::Arc;

use serde_json::json;

use super::fixtures::*;
use super::Db;
use super::listeners::{Listener, Mailbox, Subscriber, Taken};
use super::watch::{Body, Loaded, Subject, Version};
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::touches::total;
use crate::identity::ModelKey;
use crate::query::{Order, Query};
use crate::value::SortDirection;

const SMALL: usize = 100;
const LARGE: usize = 10_000;

fn name_at(index: usize) -> String {
    format!("n{index:06}")
}

fn upsert(db: &Db, rows: impl Iterator<Item = (String, String)>) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for (id, name) in rows {
        let row = obj(json!({"id": id.clone(), "name": name}));
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&id), row });
    }
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
}

fn query() -> Query {
    Query::new("users").ordered(vec![Order::new("name", SortDirection::Asc)])
}

fn seeded(size: usize) -> Db {
    let db = booted();
    upsert(&db, (0..size).map(|index| (format!("u{index:06}"), name_at(index))));
    db
}

fn first_version(db: &Db) -> Arc<Version> {
    Arc::new(db.shared.evaluate(&Subject::Rows(query())).unwrap().version)
}

fn follow(db: &Db, previous: &Arc<Version>, touched: &[&str]) -> Arc<Version> {
    let touched: HashSet<String> = touched.iter().map(|id| id.to_string()).collect();
    let epoch = db.shared.ephemeral.lock().epoch;
    let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
    let evaluated = db.shared.evaluate_step(&query(), previous, &touched, horizon).unwrap();
    Arc::new(evaluated.version.succeeding(previous, &[], evaluated.loaded).expect("the step changes the list"))
}

type Edit = fn(&Db, usize) -> Vec<String>;

fn update(db: &Db, size: usize) -> Vec<String> {
    let id = format!("u{:06}", size / 2);
    upsert(db, [(id.clone(), name_at(size / 2))].into_iter().map(|(id, name)| (id, format!("{name}!"))));
    vec![id]
}

fn insert(db: &Db, size: usize) -> Vec<String> {
    upsert(db, [("inserted".to_string(), format!("{}-", name_at(size / 2)))].into_iter());
    vec!["inserted".to_string()]
}

fn relocate(db: &Db, size: usize) -> Vec<String> {
    let id = format!("u{:06}", size / 2);
    upsert(db, [(id.clone(), "a".to_string())].into_iter());
    vec![id]
}

fn remove(db: &Db, size: usize) -> Vec<String> {
    let id = format!("u{:06}", size / 2);
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("users"), id: rid(&id), reason: crate::commit::DestroyReason::Server });
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    vec![id]
}

fn step_cost(size: usize, edit: Edit) -> usize {
    let db = seeded(size);
    let previous = first_version(&db);
    let touched = edit(&db, size);
    let touched: Vec<&str> = touched.iter().map(String::as_str).collect();
    let before = total();
    follow(&db, &previous, &touched);
    total() - before
}

fn skipped_cost(size: usize) -> usize {
    let db = seeded(size);
    let delivered = first_version(&db);
    let first_edit = update(&db, size);
    let one = follow(&db, &delivered, &first_edit.iter().map(String::as_str).collect::<Vec<_>>());
    let second_edit = insert(&db, size);
    let two = follow(&db, &one, &second_edit.iter().map(String::as_str).collect::<Vec<_>>());
    let mut mailbox = Mailbox::default();
    for version in [&one, &two] {
        let supply = match &version.body {
            Body::Rows(body) => body.step.as_ref().map(|step| step.supply()).unwrap_or_default(),
            Body::Count(_) => Loaded::default(),
        };
        mailbox.push(version.clone(), &[], supply);
    }
    let mut taken: Taken = mailbox.take().unwrap();
    taken.delivered = Some(delivered.clone());
    let subscriber = Subscriber::new(Listener::Rows(Box::new(|_| {})), Default::default());
    let Body::Rows(body) = &two.body else { unreachable!() };
    let before = total();
    let diff = subscriber.rows_diff(&delivered, &taken, body).expect("two steps change the list");
    let cost = total() - before;
    assert!(diff.inserted.iter().any(|id| id.as_str() == "inserted"));
    cost
}

#[test]
fn one_step_touches_the_same_ids_at_every_list_size() {
    let cases: [(&str, Edit); 4] = [("update", update), ("insert", insert), ("move", relocate), ("remove", remove)];
    for (name, edit) in cases {
        assert_eq!(step_cost(SMALL, edit), step_cost(LARGE, edit), "id touches for one {name} step at {SMALL} and {LARGE} rows");
    }
}

#[test]
fn joining_two_skipped_steps_touches_the_same_ids_at_every_list_size() {
    assert_eq!(skipped_cost(SMALL), skipped_cost(LARGE), "id touches at {SMALL} and {LARGE} rows");
}

#[test]
fn a_commit_missing_from_the_named_rows_makes_the_step_read_the_whole_list() {
    let db = seeded(SMALL);
    let previous = first_version(&db);
    let named = update(&db, SMALL);
    let epoch = db.shared.ephemeral.lock().epoch;
    let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
    upsert(&db, [("u000010".to_string(), "a".to_string())].into_iter());
    let touched: HashSet<String> = named.into_iter().collect();
    let evaluated = db.shared.evaluate_step(&query(), &previous, &touched, horizon).unwrap();
    let Body::Rows(body) = &evaluated.version.body else { unreachable!() };
    let order: Vec<String> = body.entries().iter().map(|entry| entry.id.to_string()).collect();
    assert_eq!(order, db.query(&query()).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>());
}

#[test]
fn a_live_ordered_list_reads_only_the_written_rows_after_a_write() {
    let db = seeded(2_000);
    let (log, callback) = recorder();
    let _observer = db.observe(query(), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let loaded = |db: &Db| crate::db::locked(&db.shared.counters).rows_loaded;
    let before = loaded(&db);
    for index in [500, 900, 1500] {
        upsert(&db, [(format!("u{index:06}"), format!("{}!", name_at(index)))].into_iter());
    }
    assert_eq!(loaded(&db) - before, 3);
    assert_eq!(changes(&log).len(), 4);
    assert_eq!(ids(changes(&log).last().unwrap()), db.query(&query()).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>());
}

fn scoped_query() -> Query {
    Query::new("users").in_scope(crate::identity::ListKey::from_raw("roster"))
}

fn scoped_step_cost(size: usize) -> usize {
    use crate::commit::{ScopeOp, ScopeRef};
    let db = booted();
    let scope = ScopeRef::new(ModelKey::new("users"), crate::identity::ListKey::from_raw("roster"));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..size {
        let id = format!("u{index:06}");
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&id), row: obj(json!({"id": id.clone(), "name": name_at(index)})) });
        envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: rid(&id), order_key: format!("k{index:06}") });
    }
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let previous = Arc::new(db.shared.evaluate(&Subject::Rows(scoped_query())).unwrap().version);
    let id = format!("u{:06}", size / 2);
    upsert(&db, [(id.clone(), "changed".to_string())].into_iter());
    let touched: HashSet<String> = HashSet::from([id]);
    let epoch = db.shared.ephemeral.lock().epoch;
    let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
    let before = total();
    db.shared.evaluate_step(&scoped_query(), &previous, &touched, horizon).unwrap();
    total() - before
}

#[test]
fn one_change_in_a_scoped_list_costs_the_same_work_at_every_list_size() {
    let (small, large) = (scoped_step_cost(SMALL), scoped_step_cost(LARGE));
    assert!(large <= small + small / 10, "{small} touches at {SMALL} rows, {large} at {LARGE}");
}
