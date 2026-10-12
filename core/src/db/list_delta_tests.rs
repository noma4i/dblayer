use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use crate::query::{Order, Query, Where};
use crate::value::SortDirection;

const WAIT: Duration = Duration::from_secs(10);

fn by_name() -> Query {
    Query::new("users").ordered(vec![Order::new("name", SortDirection::Asc), Order::new("id", SortDirection::Asc)])
}

fn list_of(count: usize, query: Query) -> (Db, ChangeLog, ObserverHandle) {
    let db = booted();
    for n in 0..count {
        put(&db, "users", json!({"id": format!("u{n:05}"), "name": format!("n{n:05}")}));
    }
    let (log, callback) = recorder();
    let observer = db.observe(query, callback).unwrap();
    db.settle();
    (db, log, observer)
}

fn one_commit_cost(count: usize) -> (usize, usize, usize) {
    let (db, _log, _observer) = list_of(count, by_name());
    let before = db.counters();
    put(&db, "users", json!({"id": format!("u{:05}", count / 2), "name": format!("n{:05}x", count / 2)}));
    let after = db.counters();
    (after.rows_delivered - before.rows_delivered, after.rows_loaded - before.rows_loaded, after.observation_reads - before.observation_reads)
}

#[test]
fn inv_observe_14_a_commit_of_one_row_delivers_and_loads_one_row_at_every_list_size() {
    let costs: Vec<_> = [20, 200, 2000].into_iter().map(one_commit_cost).collect();
    assert_eq!(costs, vec![(1, 1, 1); 3], "(rows_delivered, rows_loaded, list reads) per size 20, 200, 2000");
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

fn ids_of(rows: &[Row]) -> Vec<String> {
    rows.iter().map(|row| row.id.to_string()).collect()
}

fn operate(db: &Db, seed: &mut Seed, pool: u64) {
    let id = format!("u{:03}", seed.below(pool));
    match seed.below(4) {
        0 => {
            let _ = db.destroy("users", &rid(&id));
        }
        _ => put(db, "users", json!({"id": id, "name": format!("n{:02}", seed.below(30))})),
    }
}

#[test]
fn inv_observe_15_deliveries_rebuild_the_list_that_a_fresh_query_returns() {
    for query in [by_name(), by_name().limited(8), by_name().filtered(Where::in_list("name", (0..15).map(|n| json!(format!("n{n:02}"))).collect()))] {
        let db = booted();
        let mirror = Arc::new(Mutex::new(Vec::<Row>::new()));
        let sink = mirror.clone();
        let _observer = db.observe(query.clone(), move |change| change.apply_to(&mut sink.lock().unwrap())).unwrap();
        let mut seed = Seed(0x9E3779B97F4A7C15);
        for step in 0..300 {
            operate(&db, &mut seed, 40);
            db.settle();
            let fresh = ids_of(&db.query(&query).unwrap());
            assert_eq!(ids_of(&mirror.lock().unwrap()), fresh, "step {step} of {query:?}");
        }
    }
}

#[test]
fn inv_observe_15_a_listener_that_falls_behind_still_rebuilds_the_fresh_list() {
    let db = booted();
    for n in 0..15 {
        put(&db, "users", json!({"id": format!("u{n:03}"), "name": format!("n{:02}", n * 2)}));
    }
    let mirror = Arc::new(Mutex::new(Vec::<Row>::new()));
    let sink = mirror.clone();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let first = Mutex::new(true);
    let _observer = db
        .observe(by_name(), move |change| {
            change.apply_to(&mut sink.lock().unwrap());
            if std::mem::replace(&mut *first.lock().unwrap(), false) {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let mut seed = Seed(0xD1B54A32D192ED03);
    for _ in 0..60 {
        let (id, name) = (format!("u{:03}", seed.below(25)), format!("n{:02}", seed.below(30)));
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&id), row: obj(json!({"id": id, "name": name})) });
        db.land(envelope).unwrap();
    }
    release.send(()).unwrap();
    db.settle();
    assert_eq!(ids_of(&mirror.lock().unwrap()), ids_of(&db.query(&by_name()).unwrap()));
}

#[test]
fn inv_observe_16_a_row_that_cannot_enter_the_list_costs_no_read_and_no_delivery() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "keep"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users").filtered(Where::equal("name", json!("keep"))), callback).unwrap();
    db.settle();
    let cost = |run: &dyn Fn()| {
        let before = db.counters();
        run();
        let after = db.counters();
        (after.observation_reads - before.observation_reads, after.observer_calls - before.observer_calls)
    };
    assert_eq!(cost(&|| put(&db, "users", json!({"id": "u2", "name": "other"}))), (0, 0), "a new row that fails the filter");
    assert_eq!(cost(&|| put(&db, "users", json!({"id": "u2", "name": "another"}))), (0, 0), "a known row that still fails it");
    assert_eq!(cost(&|| put(&db, "users", json!({"id": "u2", "name": "keep"}))), (1, 1), "a row that now passes it enters");
    assert_eq!(cost(&|| put(&db, "users", json!({"id": "u2", "name": "gone"}))), (1, 1), "a row of the list that stops passing it leaves");
    assert_eq!(ids(changes(&log).last().unwrap()), vec!["u1"]);
}
