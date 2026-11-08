use std::collections::BTreeSet;

use rusqlite::Connection;

use super::*;
use crate::store::ddl::migrate_core;

fn database() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

#[test]
fn unknown_stored_sync_state_is_a_read_error() {
    let connection = database();
    connection.execute_batch("PRAGMA ignore_check_constraints = ON; INSERT INTO row_sync (model, row_id, state, attempts, since) VALUES ('moments', 'm1', 'broken', 0, 1)").unwrap();
    let error = read_sync(&connection, "moments", &["m1".into()]).unwrap_err();
    assert!(error.to_string().contains("unknown stored sync state broken"), "{error}");
}

#[test]
fn a_stored_upload_with_a_negative_size_or_transfer_is_a_read_error() {
    for (size, transfer, column) in [(-1, "7", "size"), (40, "-3", "transfer_id")] {
        let connection = database();
        connection.execute_batch(&format!("INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, size, transfer_id) VALUES ('moments', 'm1', 'photo', 'uploading', 'part', 2, 5, 1, 7, {size}, {transfer})")).unwrap();
        let error = read_sync(&connection, "moments", &["m1".into()]).unwrap_err();
        assert!(matches!(error, rusqlite::Error::IntegralValueOutOfRange(..) | rusqlite::Error::FromSqlConversionFailure(..)), "{column}: {error}");
    }
}

fn insert(connection: &Connection, action_key: &str, status: &str, row_ids: &str, input: &str) -> i64 {
    connection
        .execute("INSERT INTO op (action_key, status, created_at, since) VALUES (?1, ?2, 5, 5)", rusqlite::params![action_key, status])
        .unwrap();
    let seq = connection.last_insert_rowid();
    let ids: Vec<crate::identity::RowId> = serde_json::from_str(row_ids).unwrap();
    crate::store::operation::write_rows(connection, seq, &ids).unwrap();
    crate::store::operation::write_value(connection, seq, "input", Some(&serde_json::from_str(input).unwrap())).unwrap();
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
    insert(&connection, "moments:update", "pending", r#"["m1"]"#, r#"{"id":"m1","text":"new","likes":2}"#);
    let wanted = keys(&[key("moments", "m1")]);
    assert_eq!(recompute_sync(&connection, &wanted).unwrap(), wanted);
    let stored = info(&connection, "moments", "m1").unwrap();
    assert_eq!((stored.state, stored.actions, stored.dirty_fields, stored.since), (SyncState::Pending, vec!["update".to_string()], vec!["likes".to_string(), "text".to_string()], 5));
    assert!(recompute_sync(&connection, &wanted).unwrap().is_empty());
}

#[test]
fn recompute_removes_the_row_when_no_operation_is_open() {
    let connection = database();
    let seq = insert(&connection, "moments:create", "pending", r#"["temp-1"]"#, r#"{"id":"temp-1"}"#);
    let wanted = keys(&[key("moments", "temp-1")]);
    recompute_sync(&connection, &wanted).unwrap();
    connection.execute("DELETE FROM op WHERE seq = ?1", [seq]).unwrap();
    assert_eq!(recompute_sync(&connection, &wanted).unwrap(), wanted);
    assert_eq!(info(&connection, "moments", "temp-1"), None);
}

#[test]
fn recompute_only_touches_the_asked_keys() {
    let connection = database();
    insert(&connection, "moments:update", "pending", r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    insert(&connection, "moments:update", "pending", r#"["m2"]"#, r#"{"id":"m2","text":"b"}"#);
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
    insert(&connection, "moments:update", "failed", r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    insert(&connection, "moments:update", "pending", r#"["m2"]"#, r#"{"id":"m2","text":"b"}"#);
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
    insert(&connection, "moments:update", "failed", r#"["m1"]"#, r#"{"id":"m1","text":"a"}"#);
    recompute_sync(&connection, &keys(&[key("moments", "m1")])).unwrap();
    let found = read_sync(&connection, "moments", &["m1".to_string(), "m9".to_string()]).unwrap();
    assert_eq!(found.keys().collect::<Vec<_>>(), ["m1"]);
    assert_eq!(found["m1"].state, SyncState::Failed);
    assert!(read_sync(&connection, "chats", &["m1".to_string()]).unwrap().is_empty());
}
