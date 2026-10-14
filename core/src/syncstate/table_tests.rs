use std::collections::BTreeSet;

use rusqlite::Connection;

use super::*;
use crate::ddl::migrate_core;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn insert(connection: &Connection, action_key: &str, status: &str, started: i64, row_ids: &str, input: &str) -> i64 {
    connection
        .execute("INSERT INTO op (action_key, status, transport_started, created_at, since) VALUES (?1, ?2, ?3, 5, 5)", rusqlite::params![action_key, status, started])
        .unwrap();
    let seq = connection.last_insert_rowid();
    let ids: Vec<crate::identity::RowId> = serde_json::from_str(row_ids).unwrap();
    crate::operation::write_rows(&connection, seq, &ids).unwrap();
    crate::operation::write_value(&connection, seq, "input", Some(&serde_json::from_str(input).unwrap())).unwrap();
    seq
}

fn key(model: &str, id: &str) -> SyncKey {
    (model.to_string(), id.to_string())
}

fn keys(items: &[SyncKey]) -> BTreeSet<SyncKey> {
    items.iter().cloned().collect()
}

fn info(connection: &Connection, model: &str, id: &str) -> Option<SyncInfo> {
    read_sync(connection, model, &[id.to_string()]).unwrap().remove(id)
}

#[test]
fn recompute_writes_a_row_for_an_open_operation_and_reports_the_change_once() {
    let connection = database();
    insert(&connection, "moments:update", "pending", 0, r#"["m1"]"#, r#"{"id":"m1","text":"new","likes":2}"#);
    let wanted = keys(&[key("moments", "m1")]);
    assert_eq!(recompute_sync(&connection, &wanted).unwrap(), wanted);
    let stored = info(&connection, "moments", "m1").unwrap();
    assert_eq!((stored.state, stored.actions, stored.dirty_fields, stored.since), (SyncState::Pending, vec!["update".to_string()], vec!["likes".to_string(), "text".to_string()], 5));
    assert!(recompute_sync(&connection, &wanted).unwrap().is_empty());
}

#[test]
fn recompute_removes_the_row_when_no_operation_is_open() {
    let connection = database();
    let seq = insert(&connection, "moments:create", "pending", 0, r#"["temp-1"]"#, r#"{"id":"temp-1"}"#);
    let wanted = keys(&[key("moments", "temp-1")]);
    recompute_sync(&connection, &wanted).unwrap();
    connection.execute("DELETE FROM op WHERE seq = ?1", [seq]).unwrap();
    assert_eq!(recompute_sync(&connection, &wanted).unwrap(), wanted);
    assert_eq!(info(&connection, "moments", "temp-1"), None);
}

#[test]
fn recompute_only_touches_the_asked_keys() {
    let connection = database();
    insert(&connection, "moments:update", "pending", 0, r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    insert(&connection, "moments:update", "pending", 0, r#"["m2"]"#, r#"{"id":"m2","text":"b"}"#);
    recompute_sync(&connection, &keys(&[key("moments", "m1")])).unwrap();
    assert!(info(&connection, "moments", "m1").is_some());
    assert_eq!(info(&connection, "moments", "m2"), None);
}

#[test]
fn operation_with_a_temp_id_and_row_ids_gives_one_row_for_each_distinct_id() {
    assert_eq!(operation_keys("moments:create", &[crate::identity::RowId::new("temp-1").unwrap()], crate::identity::RowId::new("temp-1").ok().as_ref()), [key("moments", "temp-1")]);
    assert!(operation_keys("broken", &[crate::identity::RowId::new("a").unwrap()], None).is_empty());
}

#[test]
fn rebuild_repairs_missing_extra_and_stale_rows() {
    let connection = database();
    insert(&connection, "moments:update", "failed", 1, r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    insert(&connection, "moments:update", "pending", 0, r#"["m2"]"#, r#"{"id":"m2","text":"b"}"#);
    recompute_sync(&connection, &keys(&[key("moments", "m1")])).unwrap();
    connection.execute("UPDATE row_sync SET state = 'pending' WHERE row_id = 'm1'", []).unwrap();
    connection
        .execute_batch(
            "INSERT INTO row_sync (model, row_id, state, attempts, since) VALUES ('moments', 'ghost', 'pending', 0, 1);\
             INSERT INTO sync_action (model, row_id, pos, name) VALUES ('moments', 'ghost', 0, 'update');",
        )
        .unwrap();
    assert_eq!(rebuild_sync(&connection).unwrap(), 3);
    assert_eq!(info(&connection, "moments", "m1").unwrap().state, SyncState::Failed);
    assert_eq!(info(&connection, "moments", "m2").unwrap().state, SyncState::Pending);
    assert_eq!(info(&connection, "moments", "ghost"), None);
    assert_eq!(connection.query_row::<i64, _, _>("SELECT COUNT(*) FROM sync_action WHERE row_id = 'ghost'", [], |row| row.get(0)).unwrap(), 0);
    assert_eq!(rebuild_sync(&connection).unwrap(), 0);
}

#[test]
fn read_returns_only_rows_that_exist() {
    let connection = database();
    insert(&connection, "moments:update", "delivery_unknown", 1, r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    recompute_sync(&connection, &keys(&[key("moments", "m1")])).unwrap();
    let found = read_sync(&connection, "moments", &["m1".to_string(), "m9".to_string()]).unwrap();
    assert_eq!(found.keys().collect::<Vec<_>>(), ["m1"]);
    assert_eq!(found["m1"].state, SyncState::DeliveryUnknown);
    assert!(read_sync(&connection, "chats", &["m1".to_string()]).unwrap().is_empty());
}
