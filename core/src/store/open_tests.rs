use super::fixtures::*;
use super::*;

#[test]
fn in_memory_store_has_empty_manifest_and_core_tables() {
    let store = Store::open_in_memory().unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest"), 0);
    for table in ["scope_member", "tombstone", "query_window", "op", "op_once", "quarantine"] {
        assert!(store.table_exists(table).unwrap(), "{table}");
    }
    assert_ne!(store.journal_mode().unwrap().to_lowercase(), "delete");
    assert!(!store.is_ready());
    assert!(store.path().is_none());
}

#[test]
fn open_creates_file_and_parent_directory_without_model_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nested").join("a.sqlite");
    let store = Store::open(&path).unwrap();
    assert!(path.exists());
    assert!(!store.table_exists("row_messages").unwrap());
    assert_eq!(store.path(), Some(path.as_path()));
    assert_eq!(store.journal_mode().unwrap().to_lowercase(), "wal");
}

#[test]
fn boot_twice_does_not_fail_on_existing_tables() {
    let (store, _) = booted_store();
    store.boot(&[messages_spec(), items_spec()], None).unwrap();
    assert!(store.table_exists("row_messages").unwrap());
}

#[test]
fn separate_user_files_do_not_share_rows() {
    let directory = tempfile::tempdir().unwrap();
    let first = Store::open(&directory.path().join("a.sqlite")).unwrap();
    let second = Store::open(&directory.path().join("b.sqlite")).unwrap();
    first.boot(&[messages_spec()], None).unwrap();
    second.boot(&[messages_spec()], None).unwrap();
    first.commit(envelope(&first, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    assert_eq!(first.row_count("messages").unwrap(), 1);
    assert_eq!(second.row_count("messages").unwrap(), 0);
}
