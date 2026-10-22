use serde_json::json;

use super::dispatch::Event;
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use super::fixtures::*;
use super::watch::Subject;
use super::window::WindowSpec;
use crate::identity::{ModelKey, RowRef};
use crate::query::Query;

fn hit(model: &str, id: &str) -> Event {
    let model = ModelKey::new(model);
    Event { revision: i64::MAX / 2, models: [model.clone()].into(), rows: [RowRef::new(model, rid(id))].into(), ..Event::default() }
}

#[test]
fn inv_observe_21_a_subscription_and_its_refreshes_read_outside_the_owner() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    db.settle();
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&log), [1, 2]);
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_21_a_second_subscription_to_a_group_joins_after_a_read_outside_the_owner() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (first, first_callback) = recorder();
    let (second, second_callback) = recorder();
    let _one = db.observe(Query::new("users"), first_callback).unwrap();
    let _two = db.observe(Query::new("users"), second_callback).unwrap();
    db.settle();
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!((calls(&first), calls(&second)), (vec![1, 2], vec![1, 2]));
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_21_a_window_grows_by_a_read_outside_the_owner() {
    let db = booted();
    for index in 0..6 {
        put(&db, "users", json!({"id": format!("u{index}"), "name": format!("n{index}")}));
    }
    let (log, callback) = recorder();
    let window = db.observe_window(Query::new("users").ordered(vec![crate::query::Order::new("name", crate::value::SortDirection::Asc)]), WindowSpec::Tail { size: 2 }, callback).unwrap();
    db.settle();
    window.expand_before(2);
    db.settle();
    assert_eq!(log.lock().unwrap().last().map(|change| change.size), Some(4));
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_21_a_retain_reads_outside_the_owner_and_delivers_the_row() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("u1")]).unwrap();
    db.settle();
    let rows: Vec<_> = log.lock().unwrap().iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["u1"]);
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_22_a_retain_whose_read_failed_keeps_the_id_and_reports_the_failure_by_event() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    observer.retain(&[rid("late")]).unwrap();
    db.settle();
    assert_eq!(db.counters().entity_reads_failed, 1);
    put(&db, "users", json!({"id": "late", "name": "arrived"}));
    let rows: Vec<_> = log.lock().unwrap().iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["late"]);
    assert_eq!(db.counters().sql_under_owner, 0);
}

#[test]
fn inv_observe_22_a_group_that_cannot_be_read_is_counted_and_read_again_by_the_next_commit() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _observer = db.observe(Query::new("users"), callback).unwrap();
    db.settle();
    *db.shared.store.lock().poison.lock().unwrap() = Some("the store cannot read".to_string());
    db.shared.refresh_groups(&[hit("users", "u1")]);
    db.settle();
    assert_eq!((db.counters().query_reads_failed, calls(&log)), (1, vec![1]));
    *db.shared.store.lock().poison.lock().unwrap() = None;
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert_eq!(db.counters().observation_reads, reads + 1);
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&log), [1, 2]);
    put(&db, "users", json!({"id": "u3", "name": "Cy"}));
    assert_eq!((db.counters().query_reads_failed, calls(&log)), (1, vec![1, 2, 3]));
}

#[test]
fn inv_observe_23_a_panic_while_applying_one_group_fails_that_group_and_leaves_the_others() {
    let db = booted();
    let (users, users_callback) = recorder();
    let (moments, moments_callback) = recorder();
    let _users = db.observe(Query::new("users"), users_callback).unwrap();
    let _moments = db.observe(Query::new("moments"), moments_callback).unwrap();
    db.settle();
    *db.shared.panic_key.lock().unwrap() = Some(Subject::Rows(Query::new("users")).key());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for (model, row) in [("users", json!({"id": "u1", "name": "Ann"})), ("moments", json!({"id": "m1", "userId": "u1", "text": "one"}))] {
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: rid(row["id"].as_str().unwrap()), row: obj(row) });
    }
    db.land(envelope).unwrap();
    db.settle();
    assert_eq!((calls(&users), calls(&moments)), (vec![0], vec![0, 1]));
    assert_eq!(db.counters().owner_panics, 1);
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert_eq!(calls(&users), [0, 2]);
}
