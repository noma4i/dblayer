use serde_json::json;

use super::fixtures::*;

#[test]
fn a_row_inserted_after_its_id_was_retained_is_delivered() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("late")]).unwrap();
    db.settle();
    put(&db, "users", json!({"id": "late", "name": "arrived"}));
    let seen = log.lock().unwrap().clone();
    let rows: Vec<_> = seen.iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["late"], "deliveries {seen:?}");
}

#[test]
fn a_row_inserted_before_its_id_was_retained_is_delivered_by_the_retain() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    put(&db, "users", json!({"id": "early", "name": "arrived"}));
    observer.retain(&[rid("early")]).unwrap();
    db.settle();
    let seen = log.lock().unwrap().clone();
    let rows: Vec<_> = seen.iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["early"], "deliveries {seen:?}");
}

#[test]
fn a_retain_whose_read_failed_still_delivers_the_row_that_arrives_later() {
    let db = booted();
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    db.shared.store.fail_next_entity_reads(1);
    assert!(observer.retain(&[rid("late")]).is_err());
    db.settle();
    put(&db, "users", json!({"id": "late", "name": "arrived"}));
    let seen = log.lock().unwrap().clone();
    let rows: Vec<_> = seen.iter().flat_map(|change| change.rows.iter().map(|row| row.id.to_string())).collect();
    assert_eq!(rows, ["late"], "deliveries {seen:?}");
    assert_eq!(db.counters().entity_reads_failed, 1);
}

#[test]
fn a_reset_of_all_data_tells_every_retained_row_as_removed_and_the_next_row_of_the_id_is_delivered() {
    let db = booted();
    put(&db, "users", json!({"id": "kept", "name": "before"}));
    let (log, callback) = entity_recorder();
    let observer = db.observe_entities("users", callback).unwrap();
    observer.retain(&[rid("kept")]).unwrap();
    db.settle();
    db.reset_all().unwrap();
    db.settle();
    put(&db, "users", json!({"id": "kept", "name": "after"}));
    let seen = log.lock().unwrap().clone();
    let shape: Vec<(Vec<String>, Vec<String>)> = seen.iter().map(|change| (change.rows.iter().map(|row| row.id.to_string()).collect(), change.removed.iter().map(ToString::to_string).collect())).collect();
    assert_eq!(shape, [(vec!["kept".to_string()], vec![]), (vec![], vec!["kept".to_string()]), (vec!["kept".to_string()], vec![])], "deliveries {seen:?}");
}
