use super::fixtures::*;
use super::*;
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;
use serde_json::json;

fn presence_spec() -> ModelSpec {
    ModelSpec { storage: Storage::Memory, ..ModelSpec::new("presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("room", ScalarKind::Str).optional().indexed()]) }
}

fn models() -> [ModelSpec; 2] {
    [messages_spec(), presence_spec()]
}

fn reopen(path: &std::path::Path) -> Store {
    let store = Store::open(path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&models()).unwrap();
    store
}

fn seed(store: &Store) {
    let thread = scope("presence", "room");
    store
        .commit(envelope(
            vec![upsert("presence", json!({"id": "p1", "room": "r1"})), upsert("messages", message("m1", "c1"))],
            vec![append(&thread, "p1", "a")],
        ))
        .unwrap();
}

#[test]
fn store_prop_memory_model_tables() {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&models()).unwrap();
    seed(&store);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sqlite_temp_master WHERE name = 'row_presence'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'row_presence'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'fingerprint:presence'"), 0);
    assert_eq!(store.row_count("presence").unwrap(), 1);
    store.read(&["presence", "messages"], |reader| {
        assert_eq!(reader.point("presence", "p1")?.unwrap().get("room"), Some(&json!("r1")));
        assert!(reader.point("messages", "m1")?.is_some());
        assert_eq!(reader.revision_seq()?, 1);
        Ok(())
    }).unwrap();
    assert_eq!(store.scope_members("presence", scope("presence", "room").key.as_str()).unwrap().len(), 1);
    assert!(store.table_exists("row_presence").unwrap());
}

#[test]
fn memory_rows_and_their_cache_tables_do_not_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let store = reopen(&path);
        seed(&store);
        exec(&store, "INSERT INTO tombstone (model, id, seq, deleted_at) VALUES ('presence', 'gone', 1, 1);");
    }
    let store = reopen(&path);
    assert_eq!(store.row_count("presence").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member WHERE model = 'presence'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM tombstone WHERE model = 'presence'"), 0);
    assert_eq!(store.row_count("messages").unwrap(), 1);
}

#[test]
fn clearing_the_account_empties_memory_tables_too() {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    store.boot(&models()).unwrap();
    seed(&store);
    store.erase_account(store.generation().next(), false, false).unwrap();
    assert_eq!(store.row_count("presence").unwrap(), 0);
    assert_eq!(store.row_count("messages").unwrap(), 0);
}
