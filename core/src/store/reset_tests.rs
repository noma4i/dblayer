use super::fixtures::*;
use super::*;
use serde_json::json;

use crate::identity::ListKey;

#[test]
fn inv_reset_01_intent_never_persists_a_partial_model_list() {
    for rejected in 0..3 {
        let (store, _) = booted_store();
        let inner = store.lock();
        inner.connection.execute_batch(&format!(
            "CREATE TEMP TRIGGER reject_intent BEFORE INSERT ON reset_intent_model WHEN NEW.pos = {rejected} BEGIN SELECT RAISE(ABORT, 'injected intent failure'); END"
        )).unwrap();
        let models = vec!["messages".into(), "items".into(), "third".into()];
        assert!(super::reset::write_intent(&inner.connection, "cache_only", &models, 1).is_err());
        assert_eq!(super::reset::pending_intent(&inner.connection).unwrap(), None, "failure at model {rejected}");
        let count: i64 = inner.connection.query_row("SELECT COUNT(*) FROM reset_intent_model", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0, "failure at model {rejected}");
    }
}

fn seed_two_models(store: &Store) {
    store
        .commit(envelope(
            store,
            vec![upsert("messages", message("m1", "c1")), upsert("items", json!({"id": "i1"}))],
            vec![append(&scope("messages", "thread"), "m1", "a"), append(&scope("items", "list"), "i1", "a")],
        ))
        .unwrap();
    store.mark_tombstone("messages", "m9", 1, 1).unwrap();
    store.mark_tombstone("items", "i9", 2, 2).unwrap();
    let messages_window = ListKey::compose(&["messages", "thread"]).as_str().to_string() + ":forward";
    let items_window = ListKey::compose(&["items", "list"]).as_str().to_string() + ":forward";
    for window_key in [messages_window, items_window] {
        store
            .save_window(&WindowRecord {
                key: window_key,
                cursor: None,
                has_next: false,
                total: None,
                pages: 1,
                issued: 0,
                applied: 0,
                generation: 0,
            })
            .unwrap();
    }
}

fn user_tables(store: &Store) {
    exec(
        store,
        "INSERT INTO op (action_key, status, created_at, since) VALUES ('messages:send', 'pending', 1, 1);\
         INSERT INTO quarantine (model, reason) VALUES ('messages', 'test');",
    );
}

fn cache_counts(store: &Store) -> Vec<i64> {
    vec![
        count(store, "SELECT COUNT(*) FROM row_messages"),
        count(store, "SELECT COUNT(*) FROM row_items"),
        count(store, "SELECT COUNT(*) FROM scope_member WHERE model = 'messages'"),
        count(store, "SELECT COUNT(*) FROM scope_member WHERE model = 'items'"),
        count(store, "SELECT COUNT(*) FROM tombstone WHERE model = 'messages'"),
        count(store, "SELECT COUNT(*) FROM tombstone WHERE model = 'items'"),
        count(store, "SELECT COUNT(*) FROM query_window"),
    ]
}

#[test]
fn cache_only_clears_one_model_and_keeps_operations() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    user_tables(&store);
    store.erase_models(store.generation().next(), &["messages"].map(String::from)).unwrap();
    assert_eq!(cache_counts(&store), vec![0, 1, 0, 1, 0, 1, 1]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn mismatched_clears_all_cache_tables_and_the_outbox_and_keeps_the_other_user_tables() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    user_tables(&store);
    store.erase_account(store.generation().next(), false).unwrap();
    assert_eq!(cache_counts(&store), vec![0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn mismatched_restores_manifest_atomically() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    store.erase_account(store.generation().next(), false).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'format_version'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key LIKE 'fingerprint:%'"), 2);
}

#[test]
fn reset_advances_generation_and_the_revision_sequence() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    let before = store.generation();
    let revision = store.revision_seq().unwrap();
    store.erase_account(store.generation().next(), false).unwrap();
    assert!(store.generation() > before);
    assert_eq!(store.revision_seq().unwrap(), revision + 1);
    store.erase_models(store.generation().next(), &["items"].map(String::from)).unwrap();
    assert!(store.generation() > before.next());
}

#[test]
fn reset_unknown_model_fails_without_touching_data() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    assert!(matches!(store.erase_models(store.generation().next(), &["nope"].map(String::from)), Err(StoreError::UnknownModel(_))));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn writes_work_again_after_reset() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    store.erase_account(store.generation().next(), false).unwrap();
    store.commit(envelope(&store, vec![upsert("messages", message("m2", "c1"))], vec![])).unwrap();
    assert_eq!(store.row_count("messages").unwrap(), 1);
}

#[test]
fn resets_advance_the_revision_and_tell_the_hook() {
    use std::sync::{Arc, Mutex};
    let (store, _) = booted_store();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    type Seen = Vec<(i64, Option<Vec<String>>)>;
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let sink = seen.clone();
    store.set_hook(Box::new(move |event| {
        if let StoreEvent::Reset { revision, models, .. } = event {
            sink.lock().unwrap().push((revision, models));
        }
    }));
    store.erase_models(store.generation().next(), &["messages"].map(String::from)).unwrap();
    assert_eq!(store.revision_seq().unwrap(), 2);
    store.erase_account(store.generation().next(), false).unwrap();
    assert_eq!(store.revision_seq().unwrap(), 3);
    assert_eq!(*seen.lock().unwrap(), vec![(2, Some(vec!["messages".to_string()])), (3, None)]);
}
