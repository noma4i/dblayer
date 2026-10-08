use super::*;
use crate::ddl::migrate_core;
use rusqlite::Connection;

fn connection() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    migrate_core(&connection).unwrap();
    connection
}

fn record(id: &str, row: &str, field: &str, location: Location, data: &[u8]) -> FileRecord {
    FileRecord {
        id: id.into(),
        model: "moments".into(),
        row_id: row.into(),
        field: field.into(),
        descriptor: FileDescriptor { id: id.into(), size: data.len() as u64, name: Some("a.bin".into()), mime: Some("application/octet-stream".into()), sha256: Some("h".into()) },
        location,
        data: data.to_vec(),
        created_at: 7,
    }
}

#[test]
fn put_then_find_returns_the_record() {
    let connection = connection();
    let stored = record("f1", "m1", "media", Location::Table, b"abc");
    put(&connection, &stored).unwrap();
    assert_eq!(find(&connection, "moments", "m1", "media").unwrap(), Some(stored));
    assert_eq!(find(&connection, "moments", "m1", "other").unwrap(), None);
}

#[test]
fn directory_record_keeps_no_bytes_in_the_table() {
    let connection = connection();
    put(&connection, &record("f1", "m1", "media", Location::Directory, b"")).unwrap();
    let found = find(&connection, "moments", "m1", "media").unwrap().unwrap();
    assert_eq!((found.location, found.data), (Location::Directory, Vec::new()));
}

#[test]
fn put_replaces_the_file_of_the_same_pair() {
    let connection = connection();
    put(&connection, &record("f1", "m1", "media", Location::Table, b"one")).unwrap();
    put(&connection, &record("f2", "m1", "media", Location::Table, b"two")).unwrap();
    assert_eq!(find(&connection, "moments", "m1", "media").unwrap().unwrap().id, "f2");
    assert_eq!(all_references(&connection).unwrap().len(), 1);
}

#[test]
fn remove_pair_and_remove_row_delete_only_their_files() {
    let connection = connection();
    put(&connection, &record("f1", "m1", "media", Location::Table, b"1")).unwrap();
    put(&connection, &record("f2", "m1", "cover", Location::Table, b"2")).unwrap();
    put(&connection, &record("f3", "m2", "media", Location::Table, b"3")).unwrap();
    assert_eq!(remove_pair(&connection, "moments", "m1", "media").unwrap(), 1);
    assert_eq!(remove_row(&connection, "moments", "m1").unwrap(), 1);
    let left: Vec<String> = all_references(&connection).unwrap().into_iter().map(|reference| reference.id).collect();
    assert_eq!(left, vec!["f3"]);
}

#[test]
fn move_row_hands_files_to_the_new_id_and_replaces_a_clash() {
    let connection = connection();
    put(&connection, &record("f1", "temp-1", "media", Location::Table, b"1")).unwrap();
    put(&connection, &record("f2", "m9", "media", Location::Table, b"2")).unwrap();
    assert_eq!(move_row(&connection, "moments", "temp-1", "m9").unwrap(), 1);
    assert_eq!(find(&connection, "moments", "m9", "media").unwrap().unwrap().id, "f1");
    assert_eq!(find(&connection, "moments", "temp-1", "media").unwrap(), None);
}

#[test]
fn ids_of_rows_lists_files_of_the_given_rows() {
    let connection = connection();
    put(&connection, &record("f1", "m1", "media", Location::Table, b"1")).unwrap();
    put(&connection, &record("f2", "m2", "media", Location::Directory, b"")).unwrap();
    put(&connection, &record("f3", "m3", "media", Location::Table, b"3")).unwrap();
    let rows = [("moments".to_string(), "m1".to_string()), ("moments".to_string(), "m2".to_string())];
    let mut ids = ids_of_rows(&connection, &rows).unwrap();
    ids.sort();
    assert_eq!(ids, vec!["f1", "f2"]);
}

#[test]
fn clear_model_and_clear_all_empty_the_table() {
    let connection = connection();
    put(&connection, &record("f1", "m1", "media", Location::Table, b"1")).unwrap();
    let mut other = record("f2", "u1", "avatar", Location::Table, b"2");
    other.model = "users".into();
    put(&connection, &other).unwrap();
    clear_model(&connection, "moments").unwrap();
    assert_eq!(all_references(&connection).unwrap().len(), 1);
    clear_all(&connection).unwrap();
    assert!(all_references(&connection).unwrap().is_empty());
}

#[test]
fn placement_switches_to_a_directory_above_the_limit_only_when_one_exists() {
    assert_eq!(placement(TABLE_LIMIT, true), Location::Table);
    assert_eq!(placement(TABLE_LIMIT + 1, true), Location::Directory);
    assert_eq!(placement(TABLE_LIMIT + 1, false), Location::Table);
}
