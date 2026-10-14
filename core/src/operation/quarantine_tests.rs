use super::*;
use crate::ddl::migrate_core;
use rusqlite::Connection;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

#[test]
fn add_returns_sequential_ids_and_counts_by_reason() {
    let connection = database();
    let first = quarantine_add(&connection, Some("moment"), &serde_json::json!({}), QuarantineReason::OrphanTempRow).unwrap();
    let second = quarantine_add(&connection, Some("moment"), &serde_json::json!({}), QuarantineReason::OrphanTempRow).unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(quarantine_counts(&connection).unwrap()["orphan-temp-row"], 2);
}
