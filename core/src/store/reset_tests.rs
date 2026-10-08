use super::fixtures::*;
use super::*;
use serde_json::json;

use crate::identity::ScopeKey;

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
    let messages_window = ScopeKey::compose(&["messages", "thread"]).as_str().to_string() + ":forward";
    let items_window = ScopeKey::compose(&["items", "list"]).as_str().to_string() + ":forward";
    for window_key in [messages_window, items_window] {
        store
            .save_window(&WindowRecord {
                key: window_key,
                cursor: None,
                has_next: false,
                ids: vec![],
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
        "INSERT INTO op (action_key, status, row_ids, input, created_at, since) VALUES ('messages:send', 'pending', x'5b5d', x'7b7d', 1, 1);\
         INSERT INTO op_once (namespace, dedupe_key) VALUES ('messages:send', 'd1');\
         INSERT INTO quarantine (model, payload, reason) VALUES ('messages', x'00', 'test');",
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
fn modes_define_intent_user_tables_and_blast_radius() {
    let cache = ResetMode::CacheOnly { models: vec!["zeta".into(), "alpha".into()] };
    assert_eq!(ResetPlan::intent_value(&cache), "cache_only");
    assert_eq!(ResetPlan::intent_value(&ResetMode::Incompatible), "incompatible");
    assert_eq!(ResetPlan::intent_value(&ResetMode::Logout), "logout");
    assert!(ResetPlan::keeps_user_tables(&cache));
    assert!(ResetPlan::keeps_user_tables(&ResetMode::Incompatible));
    assert!(!ResetPlan::keeps_user_tables(&ResetMode::Logout));
    assert_eq!(ResetPlan::blast_radius(&cache), vec!["alpha", "zeta"]);
    assert!(ResetPlan::blast_radius(&ResetMode::Incompatible).is_empty());
    assert!(ResetPlan::blast_radius(&ResetMode::Logout).is_empty());
}

#[test]
fn steps_per_mode_touch_only_what_the_mode_owns() {
    let cache = ResetMode::CacheOnly { models: vec!["zeta".into(), "alpha".into()] };
    let steps = ResetPlan::steps(&cache);
    assert_eq!(steps.len(), 10);
    assert_eq!(steps[0], ResetStep::WriteIntent("cache_only"));
    assert_eq!(steps[1], ResetStep::DeleteRows { model: "alpha".into() });
    assert_eq!(steps[5], ResetStep::DeleteRows { model: "zeta".into() });
    assert_eq!(steps[9], ResetStep::ClearIntent);
    assert_eq!(
        ResetPlan::steps(&ResetMode::Incompatible),
        vec![
            ResetStep::WriteIntent("incompatible"),
            ResetStep::DeleteScopeMembers { model: None },
            ResetStep::DeleteTombstones { model: None },
            ResetStep::DeleteQueryWindows { model: None },
            ResetStep::WriteManifest,
            ResetStep::ClearIntent,
        ]
    );
    assert_eq!(
        ResetPlan::steps(&ResetMode::Logout),
        vec![ResetStep::WriteIntent("logout"), ResetStep::DeleteDatabaseFile, ResetStep::ClearIntent]
    );
}

#[test]
fn resume_skips_completed_steps() {
    let mode = ResetMode::Incompatible;
    let steps = ResetPlan::steps(&mode);
    assert_eq!(ResetPlan::resume_steps(&mode, -1), steps);
    assert_eq!(
        ResetPlan::resume_steps(&mode, 3),
        vec![ResetStep::DeleteQueryWindows { model: None }, ResetStep::WriteManifest, ResetStep::ClearIntent]
    );
    assert!(ResetPlan::resume_steps(&mode, steps.len() as i64 + 1).is_empty());
}

#[test]
fn cache_only_clears_one_model_keeps_operations_and_clears_intent() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    user_tables(&store);
    store.reset_cache_only(&["messages"]).unwrap();
    assert_eq!(cache_counts(&store), vec![0, 1, 0, 1, 0, 1, 1]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn incompatible_clears_all_cache_tables_and_keeps_user_tables() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    user_tables(&store);
    store.reset_incompatible().unwrap();
    assert_eq!(cache_counts(&store), vec![0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op_once"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn incompatible_restores_manifest_atomically() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    store.reset_incompatible().unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'format_version'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key LIKE 'fingerprint:%'"), 2);
}

#[test]
fn reset_advances_generation_and_the_revision_sequence() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    let before = store.generation();
    let revision = store.revision_seq().unwrap();
    store.reset_incompatible().unwrap();
    assert!(store.generation() > before);
    assert_eq!(store.revision_seq().unwrap(), revision + 1);
    store.reset_cache_only(&["items"]).unwrap();
    assert!(store.generation() > before.next());
}

#[test]
fn reset_unknown_model_fails_without_touching_data() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    assert!(matches!(store.reset_cache_only(&["nope"]), Err(StoreError::UnknownModel(_))));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn writes_work_again_after_reset() {
    let (store, _) = booted_store();
    seed_two_models(&store);
    store.reset_incompatible().unwrap();
    store.commit(envelope(&store, vec![upsert("messages", message("m2", "c1"))], vec![])).unwrap();
    assert_eq!(store.row_count("messages").unwrap(), 1);
}

#[test]
fn logout_removes_database_files_and_marker() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("user-1.sqlite");
    let store = Store::open(&path).unwrap();
    store.boot(&[messages_spec()], None).unwrap();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    assert!(path.exists());
    store.logout().unwrap();
    assert!(!path.exists());
    assert!(!directory.path().join("user-1.sqlite-wal").exists());
    assert!(!directory.path().join("user-1.sqlite.reset").exists());
}

#[test]
fn open_completes_interrupted_logout() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("user-1.sqlite");
    {
        let store = Store::open(&path).unwrap();
        store.boot(&[messages_spec()], None).unwrap();
        store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    }
    std::fs::write(directory.path().join("user-1.sqlite.reset"), b"logout").unwrap();
    let reopened = Store::open(&path).unwrap();
    reopened.boot(&[messages_spec()], None).unwrap();
    assert_eq!(reopened.row_count("messages").unwrap(), 0);
    assert!(!directory.path().join("user-1.sqlite.reset").exists());
}

#[test]
fn logout_of_in_memory_store_is_a_noop() {
    let (store, _) = booted_store();
    store.logout().unwrap();
}

#[test]
fn resets_advance_the_revision_and_tell_the_hook() {
    use std::sync::{Arc, Mutex};
    let (store, _) = booted_store();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    let seen: Arc<Mutex<Vec<(i64, Option<Vec<String>>)>>> = Arc::default();
    let sink = seen.clone();
    store.set_hook(Box::new(move |event| {
        if let StoreEvent::Reset { revision, models } = event {
            sink.lock().unwrap().push((revision, models));
        }
    }));
    store.reset_cache_only(&["messages"]).unwrap();
    assert_eq!(store.revision_seq().unwrap(), 2);
    store.reset_incompatible().unwrap();
    assert_eq!(store.revision_seq().unwrap(), 3);
    assert_eq!(*seen.lock().unwrap(), vec![(2, Some(vec!["messages".to_string()])), (3, None)]);
}
