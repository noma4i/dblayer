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
    let mut seed = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for n in 0..count {
        let id = format!("u{n:05}");
        seed.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&id), row: obj(json!({"id": id, "name": format!("n{n:05}")})) });
    }
    db.land(&db.admit().unwrap(), seed).unwrap();
    let (log, callback) = recorder();
    let observer = db.observe(query, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
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
            let _ = db.destroy(&db.admit().unwrap(), "users", &rid(&id));
        }
        _ => put(db, "users", json!({"id": id, "name": format!("n{:02}", seed.below(30))})),
    }
}

#[test]
fn inv_observe_15_deliveries_rebuild_the_list_that_a_fresh_query_returns() {
    let queries = [by_name(), by_name().limited(8), by_name().filtered(Where::in_list("name", (0..15).map(|n| json!(format!("n{n:02}"))).collect()))];
    std::thread::scope(|scope| {
        let runs: Vec<_> = queries.into_iter().map(|query| scope.spawn(move || rebuild_matches_fresh(query))).collect();
        for run in runs {
            run.join().unwrap();
        }
    });
}

fn rebuild_matches_fresh(query: Query) {
    let db = booted();
    let mirror = Arc::new(Mutex::new(Vec::<Row>::new()));
    let sink = mirror.clone();
    let _observer = db.observe(query.clone(), move |change| change.apply_to(&mut sink.lock().unwrap())).unwrap();
    let mut seed = Seed(0x9E3779B97F4A7C15);
    for step in 0..300 {
        operate(&db, &mut seed, 40);
        db.await_revision(db.revision_seq().unwrap());
        let fresh = ids_of(&db.query(&query).unwrap());
        assert_eq!(ids_of(&mirror.lock().unwrap()), fresh, "step {step} of {query:?}");
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
        db.land(&db.admit().unwrap(), envelope).unwrap();
    }
    release.send(()).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids_of(&mirror.lock().unwrap()), ids_of(&db.query(&by_name()).unwrap()));
}

#[test]
fn inv_observe_16_a_row_that_cannot_enter_the_list_costs_no_read_and_no_delivery() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "keep"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users").filtered(Where::equal("name", json!("keep"))), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
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

#[test]
fn inv_observe_18_a_replaced_pair_reaches_a_reader_that_never_saw_the_temporary_row() {
    let db = booted();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let first = Mutex::new(true);
    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let _observer = db
        .observe(Query::new("moments"), move |change| {
            sink.lock().unwrap().push(change.clone());
            if std::mem::replace(&mut *first.lock().unwrap(), false) {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let temp = db.create(&db.admit().unwrap(), "moments", obj(json!({"text": "new"}))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: crate::commit::DestroyReason::Replaced { by: rid("m7") } });
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(json!({"id": "m7", "text": "new"})) });
    db.land(&db.admit().unwrap(), envelope).unwrap();
    release.send(()).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = log.lock().unwrap().clone();
    let last = seen.last().unwrap();
    let told: Vec<_> = seen.iter().map(|change| (ids(change), change.diff.replaced.clone(), change.diff.renamed.clone(), change.revision)).collect();
    assert_eq!((ids(last), last.diff.renamed.clone()), (vec!["m7".to_string()], vec![(temp.id.clone(), rid("m7"))]), "deliveries (ids, replaced, renamed, revision): {told:?}");
}

#[test]
fn inv_observe_18_a_pair_replaced_while_the_group_is_evaluated_still_reaches_the_reader() {
    let db = booted();
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("moments").limited(50), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let hold = crate::db::open::locked(&db.shared.observers);
    let writer = db.clone();
    let temp = crate::test_wait::join_within(std::thread::spawn(move || {
        let temp = writer.create(&writer.admit().unwrap(), "moments", obj(json!({"text": "new"}))).unwrap();
        crate::test_wait::spin_until("the dispatcher never took the create event", || writer.shared.dispatch.queued() == 0);
        let mut envelope = CommitEnvelope::new(WriteOrigin::Response, writer.generation());
        envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: crate::commit::DestroyReason::Replaced { by: rid("m7") } });
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(json!({"id": "m7", "text": "new"})) });
        writer.land(&writer.admit().unwrap(), envelope).unwrap();
        temp
    }));
    drop(hold);
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    let last = seen.last().unwrap();
    assert_eq!((ids(last), last.diff.renamed.clone()), (vec!["m7".to_string()], vec![(temp.id.clone(), rid("m7"))]));
}

#[test]
fn inv_observe_18_more_replaced_pairs_than_the_log_keeps_deliver_none_of_them() {
    let count = crate::db::dispatch::MAX_REPLACED_LOG + 4;
    let db = booted();
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("moments").limited(count as u32 + 10), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let mut first = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    let mut second = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    for n in 0..count {
        first.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("t{n}")), row: obj(json!({"id": format!("t{n}"), "text": "new"})) });
        second.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: rid(&format!("t{n}")), reason: crate::commit::DestroyReason::Replaced { by: rid(&format!("m{n}")) } });
        second.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{n}")), row: obj(json!({"id": format!("m{n}"), "text": "new"})) });
    }
    db.land(&db.admit().unwrap(), first).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.land(&db.admit().unwrap(), second).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    let last = seen.last().unwrap();
    assert_eq!((ids(last).len(), last.diff.renamed.len()), (count, 0));
}
