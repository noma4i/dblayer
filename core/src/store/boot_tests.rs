use super::fixtures::*;
use super::*;
use serde_json::json;

use crate::ddl::ManifestVerdict;
use crate::schema::{FieldSpec, ModelSpec, RelationSpec};
use crate::value::ScalarKind;

fn seed(store: &Store) {
    store
        .commit(envelope(
            store,
            vec![upsert("messages", message("m1", "c1")), upsert("items", json!({"id": "i1"}))],
            vec![append(&scope("messages", "thread"), "m1", "a"), append(&scope("items", "list"), "i1", "a")],
        ))
        .unwrap();
    exec(
        store,
        "INSERT INTO op (action_key, status, created_at, since) VALUES ('messages:send', 'pending', 1, 1);\
         INSERT INTO quarantine (model, reason) VALUES ('messages', 'test');",
    );
}

fn reopen(path: &std::path::Path, models: &[ModelSpec], data_version: Option<&str>) -> (Store, BootReport) {
    let store = Store::open(path).unwrap();
    let report = store.boot(models, data_version).unwrap();
    (store, report)
}

#[test]
fn boot_creates_model_tables_and_ready_follows_mark_ready() {
    let store = Store::open_in_memory().unwrap();
    assert!(!store.table_exists("row_messages").unwrap());
    let report = store.boot(&[messages_spec()], None).unwrap();
    assert_eq!(report.verdict, ManifestVerdict::Fresh);
    assert!(store.table_exists("row_messages").unwrap());
    assert!(!store.is_ready());
    store.mark_ready();
    assert!(store.is_ready());
    store.boot(&[messages_spec()], None).unwrap();
    assert!(!store.is_ready());
}

#[test]
fn second_boot_is_compatible_and_keeps_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, Some("v1"));
        seed(&store);
    }
    let (store, report) = reopen(&path, &models, Some("v1"));
    assert_eq!(report.verdict, ManifestVerdict::Compatible);
    assert_eq!(store.row_count("messages").unwrap(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 2);
}

#[test]
fn committed_work_survives_dropping_the_store_without_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    }
    let (store, _) = reopen(&path, &[messages_spec()], None);
    assert!(store.point("messages", "m1").unwrap().is_some());
}

#[test]
fn fingerprint_change_clears_every_server_model_and_quarantines_its_operations() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec(), items_spec()], None);
        seed(&store);
    }
    let changed = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str).optional(),
            FieldSpec::scalar("seen", ScalarKind::Bool).optional().indexed(),
            FieldSpec::scalar("extra", ScalarKind::Str).optional(),
        ],
    );
    let (store, report) = reopen(&path, &[changed, items_spec()], None);
    assert_eq!(report.verdict, ManifestVerdict::CacheOnly { models: vec!["messages".into()] });
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(store.row_count("items").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'schema-changed' AND model = 'messages'"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
    store.commit(envelope(&store, vec![upsert("messages", message("m2", "c1"))], vec![])).unwrap();
}

#[test]
fn renaming_a_relation_resets_every_server_model() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let mut first = messages_spec();
    first.relations.push(RelationSpec::new("thread", &["chatId"]));
    let mut renamed = messages_spec();
    renamed.relations.push(RelationSpec::new("timeline", &["chatId"]));
    {
        let (store, _) = reopen(&path, &[first, items_spec()], None);
        seed(&store);
    }
    let (store, report) = reopen(&path, &[renamed, items_spec()], None);
    assert_eq!(report.verdict, ManifestVerdict::CacheOnly { models: vec!["messages".into()] });
    assert_eq!(store.row_count("items").unwrap(), 0);
}

#[test]
fn added_model_clears_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    }
    let (store, report) = reopen(&path, &[messages_spec(), items_spec()], None);
    assert_eq!(report.verdict, ManifestVerdict::Compatible);
    assert_eq!(store.row_count("messages").unwrap(), 1);
    assert_eq!(store.row_count("items").unwrap(), 0);
}

#[test]
fn removed_model_clears_every_server_model_and_its_fingerprint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec(), items_spec()], None);
        seed(&store);
    }
    let (store, report) = reopen(&path, &[messages_spec()], None);
    assert_eq!(report.verdict, ManifestVerdict::CacheOnly { models: vec!["items".into()] });
    assert!(!store.table_exists("row_items").unwrap());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'fingerprint:items'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0);
    assert_eq!(store.row_count("messages").unwrap(), 0);
}

#[test]
fn incompatible_manifests_wipe_cache_and_quarantine_operations() {
    type Case = (&'static str, Option<&'static str>, Option<&'static str>, Option<&'static str>);
    let cases: [Case; 2] = [
        ("data version changed", Some("v1"), Some("v2"), None),
        ("manifest missing", None, None, Some("DELETE FROM manifest")),
    ];
    for (case, before, after, tamper) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("a.sqlite");
        let models = [messages_spec(), items_spec()];
        {
            let (store, _) = reopen(&path, &models, before);
            seed(&store);
            if let Some(sql) = tamper {
                exec(&store, sql);
            }
        }
        let (store, report) = reopen(&path, &models, after);
        assert_eq!(report.verdict, ManifestVerdict::Incompatible, "{case}");
        assert_eq!(store.row_count("messages").unwrap(), 0, "{case}: rows");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0, "{case}: membership");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0, "{case}: operations");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine"), 2, "{case}: quarantine");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'schema-changed'"), 1, "{case}: retired operation");
        if let Some(version) = after {
            assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM manifest WHERE key = 'data_version' AND value = '{version}'")), 1, "{case}: stamped version");
        }
    }
}

#[test]
fn interrupted_cache_only_reset_is_completed_before_manifest_check() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(
            &store,
            "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'cache_only', 1); INSERT INTO reset_intent_model (pos, model) VALUES (0, 'messages');",
        );
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(store.row_count("items").unwrap(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn an_interrupted_session_end_wipes_the_data_at_the_next_open_and_asks_for_the_credential_clear_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        store.write_session_end_intent(&store.lifecycle.begin_blocking(crate::lifecycle::Boundary::SessionEnd(crate::lifecycle::End::Local), None).unwrap()).unwrap();
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!((store.row_count("messages").unwrap(), store.row_count("items").unwrap()), (0, 0));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(store.reset_intent().unwrap(), None);
    assert!(store.take_session_end_replay(), "the open did not ask for the credential clear");
    assert!(!store.take_session_end_replay(), "the credential clear was asked twice");
}

#[test]
fn interrupted_incompatible_reset_is_completed_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'incompatible', 1)");
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!(store.row_count("items").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn corrupt_reset_intent_goes_to_quarantine_and_is_cleared() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        exec(&store, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'garbage', 1)");
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(!report.finished_interrupted_reset);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'corrupt-reset-intent'"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn boot_prunes_expired_tombstones() {
    let (store, clock) = booted_store();
    store.mark_tombstone("messages", "old", 1, 0).unwrap();
    clock.advance(std::time::Duration::from_millis(200_000_000));
    let report = store.boot(&[messages_spec(), items_spec()], None).unwrap();
    assert_eq!(report.tombstone_drops, 1);
}

#[test]
fn a_stale_core_table_shape_is_replaced_by_a_recreated_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        exec(
            &store,
            "DROP TABLE query_window; \
             CREATE TABLE query_window (key TEXT PRIMARY KEY NOT NULL, cursor TEXT, has_next INTEGER NOT NULL, ids BLOB NOT NULL, \
             issued INTEGER NOT NULL, applied INTEGER NOT NULL, generation INTEGER NOT NULL, count INTEGER NOT NULL, \
             landed_at INTEGER NOT NULL, stale_since INTEGER) STRICT; \
             UPDATE manifest SET value = '1' WHERE key = 'format_version'",
        );
    }
    let (store, report) = reopen(&path, &models, None);
    assert_eq!(store.recreated(), Some(crate::ddl::RecreateReason::CoreFormat));
    assert_eq!(report.verdict, ManifestVerdict::Fresh);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM pragma_table_info('query_window') WHERE name = 'total'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_sync"), 0);
}

#[test]
fn open_recreates_a_missing_core_table() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        exec(&store, "DROP TABLE row_sync");
    }
    let (store, _) = reopen(&path, &models, None);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_sync"), 0);
}
