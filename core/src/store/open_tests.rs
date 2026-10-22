use super::fixtures::*;
use super::*;
use crate::ddl::RecreateReason;

#[test]
fn in_memory_store_has_empty_manifest_and_core_tables() {
    let store = Store::open_in_memory().unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest"), 0);
    for table in ["scope_member", "tombstone", "query_window", "op", "quarantine"] {
        assert!(store.table_exists(table).unwrap(), "{table}");
    }
    assert_ne!(store.journal_mode().unwrap().to_lowercase(), "delete");
    assert!(!store.is_ready());
}

#[test]
fn open_creates_file_and_parent_directory_without_model_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nested").join("a.sqlite");
    let store = Store::open(&path).unwrap();
    assert!(path.exists());
    assert!(!store.table_exists("row_messages").unwrap());
    assert_eq!(store.journal_mode().unwrap().to_lowercase(), "wal");
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

fn valid_file_with_marker(path: &std::path::Path, user_version: i64) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection.execute_batch("CREATE TABLE marker (value TEXT)").unwrap();
    connection.pragma_update(None, "user_version", user_version).unwrap();
}

#[test]
fn inv_open_01_a_file_that_does_not_fit_this_build_is_recreated_once() {
    type Plant = fn(&std::path::Path);
    let cases: [(&str, Plant, RecreateReason); 3] = [
        ("garbage bytes", |path| std::fs::write(path, "this is not a database ".repeat(400)).unwrap(), RecreateReason::UnreadableFile),
        ("newer schema version", |path| valid_file_with_marker(path, crate::ddl::SCHEMA_VERSION + 1), RecreateReason::SchemaVersion),
        (
            "changed core format",
            |path| {
                let store = Store::open(path).unwrap();
                store.boot(&[messages_spec()], None).unwrap();
                exec(&store, "CREATE TABLE marker (value TEXT); UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
            },
            RecreateReason::CoreFormat,
        ),
    ];
    for (case, plant, reason) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("a.sqlite");
        plant(&path);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.recreated(), Some(reason), "{case}");
        assert!(!store.table_exists("marker").unwrap(), "{case}: old content");
        store.boot(&[messages_spec()], None).unwrap();
        drop(store);
        assert_eq!(Store::open(&path).unwrap().recreated(), None, "{case}: second open");
    }
}

#[test]
fn inv_open_02_recreate_removes_every_file_of_the_old_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let beside = |suffix: &str| directory.path().join(format!("a.sqlite{suffix}"));
    let source = directory.path().join("source.sqlite");
    {
        let connection = rusqlite::Connection::open(&source).unwrap();
        connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(())).unwrap();
        connection.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        connection.execute_batch("CREATE TABLE marker (value TEXT)").unwrap();
        connection.pragma_update(None, "user_version", crate::ddl::SCHEMA_VERSION + 1).unwrap();
        for suffix in ["", "-wal", "-shm"] {
            std::fs::copy(directory.path().join(format!("source.sqlite{suffix}")), beside(suffix)).unwrap();
        }
    }
    std::fs::write(&path, "this is not a database ".repeat(400)).unwrap();
    for suffix in [".files", ".cache"] {
        std::fs::create_dir_all(beside(suffix)).unwrap();
        std::fs::write(beside(suffix).join("f1"), "bytes").unwrap();
    }

    let store = Store::open(&path).unwrap();

    assert_eq!(store.recreated(), Some(RecreateReason::SchemaVersion));
    assert!(!store.table_exists("marker").unwrap());
    assert!(!beside(".files").exists());
    assert!(!beside(".cache").exists());
}
