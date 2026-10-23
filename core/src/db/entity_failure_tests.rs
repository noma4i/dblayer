use serde_json::json;

use super::fixtures::*;

#[test]
fn inv_observe_24_a_retain_whose_read_failed_tells_its_subscriber_the_failure() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    observer.retain(&[rid("late")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "deliveries {seen:?}");
    assert!(seen[0].failure.is_some() && seen[0].rows.is_empty() && seen[0].removed.is_empty(), "deliveries {seen:?}");
}

#[test]
fn inv_observe_24_a_delivery_after_a_failed_read_carries_no_failure() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    observer.retain(&[rid("u1")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    observer.retain(&[rid("u1")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen.iter().map(|change| (change.failure.is_some(), change.rows.len())).collect::<Vec<_>>(), [(true, 0), (false, 1)], "deliveries {seen:?}");
}

#[test]
fn inv_observe_24_a_failed_read_tells_only_the_subscriber_whose_ids_it_read() {
    let db = booted();
    let (first, first_callback) = entity_recorder();
    let (second, second_callback) = entity_recorder();
    let reader = db.observe_entities("users", first_callback).unwrap();
    let _other = db.observe_entities("users", second_callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    reader.retain(&[rid("late")]).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!((first.lock().unwrap().len(), second.lock().unwrap().len()), (1, 0));
}
