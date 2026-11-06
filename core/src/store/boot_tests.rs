use super::fixtures::*;
use super::*;
use serde_json::json;

use crate::store::ddl::ManifestVerdict;
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
fn second_boot_is_matching_and_keeps_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, Some("v1"));
        seed(&store);
    }
    let (store, report) = reopen(&path, &models, Some("v1"));
    assert_eq!(report.verdict, ManifestVerdict::Match);
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
fn fingerprint_change_clears_cache_and_keeps_intents() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec(), items_spec()], None);
        seed(&store);
        exec(&store, "INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, source, transfer_id) VALUES ('messages', 'm1', 'upload', 'queued', 'prepare', 0, 1, 3, 7, 'source', 9)");
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
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE action_key = 'messages:send'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'schema-changed' AND model = 'messages'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_upload WHERE model = 'messages' AND row_id = 'm1' AND attempts = 3 AND seq = 7 AND transfer_id = 9"), 1);
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
    assert_eq!(report.verdict, ManifestVerdict::Match);
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
fn mismatched_manifests_wipe_cache_and_keep_operations() {
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
        assert_eq!(report.verdict, ManifestVerdict::Mismatch, "{case}");
        assert_eq!(store.row_count("messages").unwrap(), 0, "{case}: rows");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0, "{case}: membership");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1, "{case}: operations");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine"), 1, "{case}: quarantine");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'schema-changed'"), 0, "{case}: operations stay out of quarantine");
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
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
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
        store.execute_for_test("INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'session_end', 1)");
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
fn interrupted_mismatched_reset_is_completed_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'mismatched', 1)");
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!(store.row_count("items").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
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
    assert_eq!(report.recreated, Some(crate::store::ddl::RecreateReason::CoreFormat));
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

const SESSION_DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn store_prop_boot_any_layout() {
    let layouts: [(&str, &str, i64, i64); 4] = [
        (
            "before 6.8.0: pending without purpose and owner columns, no rows",
            "DROP TABLE encrypted_pending; CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, request_id TEXT NOT NULL, response_key TEXT NOT NULL, sequence INTEGER NOT NULL, path TEXT NOT NULL, body TEXT NOT NULL, owner_seq INTEGER, item_position INTEGER, stage TEXT) STRICT;",
            4,
            0,
        ),
        (
            "pending with an extra column, a row and no req_id: the row is dropped",
            "DROP TABLE encrypted_pending; CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, request_id TEXT NOT NULL, response_key TEXT NOT NULL, sequence INTEGER NOT NULL, path TEXT NOT NULL, body TEXT NOT NULL, owner_kind TEXT, owner_operation_id INTEGER, owner_item_id TEXT, owner_step TEXT, purpose INTEGER NOT NULL, upload INTEGER NOT NULL DEFAULT 0) STRICT; INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, purpose) VALUES (1, 'd', 'r', 'k', 5, '/p', '{}', 0);",
            4,
            0,
        ),
        (
            "session with an extra column",
            "DROP TABLE encrypted_session; CREATE TABLE encrypted_session (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, sequence INTEGER NOT NULL, extra TEXT) STRICT; INSERT INTO encrypted_session (id, credential_digest, sequence, extra) VALUES (1, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', 4, 'x');",
            4,
            0,
        ),
        (
            "pending with only its key columns",
            "DROP TABLE encrypted_pending; CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY, credential_digest TEXT NOT NULL) STRICT; INSERT INTO encrypted_pending VALUES (1, 'd');",
            4,
            0,
        ),
    ];
    for (case, plant, sequence, pending_rows) in layouts {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("a.sqlite");
        let models = [messages_spec()];
        {
            let (store, _) = reopen(&path, &models, None);
            exec(&store, &format!("INSERT OR REPLACE INTO encrypted_session (id, credential_digest, sequence) VALUES (1, '{SESSION_DIGEST}', {sequence});"));
            exec(&store, plant);
            exec(&store, "UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
        }
        let (store, report) = reopen(&path, &models, None);
        assert_eq!(report.recreated, Some(crate::store::ddl::RecreateReason::CoreFormat), "{case}");
        assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM encrypted_session WHERE credential_digest = '{SESSION_DIGEST}' AND sequence = {sequence}")), 1, "{case}: the session is kept");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_pending"), pending_rows, "{case}: pending rows");
        {
            let inner = store.lock();
            crate::store::session_state::pending(&inner.connection).unwrap_or_else(|error| panic!("{case}: reading pending: {error}"));
        }
        assert_eq!(store.saved().unwrap().2, crate::store::identity_of(SESSION_DIGEST), "{case}: the identity is published");
    }
}

fn plant_old_layout(path: &std::path::Path, plant: &str) {
    let (store, _) = reopen(path, &[messages_spec()], None);
    exec(&store, plant);
    exec(&store, "UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
}

#[test]
fn store_boot_layout_missing_column() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    plant_old_layout(&path, "DROP TABLE local_pending; CREATE TABLE local_pending (model TEXT NOT NULL, row_id TEXT NOT NULL, path TEXT NOT NULL, node INTEGER NOT NULL) STRICT; INSERT INTO local_pending VALUES ('messages', 'm1', 'body', 0);");
    let (store, report) = reopen(&path, &[messages_spec()], None);
    assert_eq!(report.dropped.len(), 1);
    assert_eq!((report.dropped[0].table.as_str(), report.dropped[0].reason), ("local_pending", "missing_column"));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM local_pending"), 0);
    let kept = text(&store, "SELECT columns FROM restore_failed WHERE source_table = 'local_pending'").unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&kept).unwrap()["row_id"], "m1");
    assert_eq!(store.restore_failed().unwrap().len(), 1);
}

#[test]
fn store_boot_layout_mismatch_row() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    plant_old_layout(
        &path,
        "DROP TABLE op; CREATE TABLE op (seq INTEGER PRIMARY KEY AUTOINCREMENT, action_key TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, since INTEGER NOT NULL);\
         INSERT INTO op VALUES (7, 'messages:send', 'pending', 1, 1);\
         INSERT INTO op VALUES (8, 'messages:send', 'weird', 1, 1);\
         DROP TABLE encrypted_session; CREATE TABLE encrypted_session (id INTEGER PRIMARY KEY, credential_digest TEXT NOT NULL, sequence TEXT NOT NULL);\
         INSERT INTO encrypted_session VALUES (1, 'd', 'not a number');",
    );
    let (store, report) = reopen(&path, &[messages_spec()], None);
    let mut dropped: Vec<_> = report.dropped.iter().map(|row| (row.table.as_str(), row.reason)).collect();
    dropped.sort_unstable();
    assert_eq!(dropped, vec![("encrypted_session", "type"), ("op", "constraint")]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE seq = 7"), 1, "the valid row is carried with its seq");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE seq = 8"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM restore_failed"), 2);
    exec(&store, "INSERT INTO op (action_key, status, created_at, since) VALUES ('messages:send', 'pending', 2, 2)");
    assert!(count(&store, "SELECT MAX(seq) FROM op") > 7, "new operations number above every kept seq");
}

#[test]
fn store_prop_boot_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    plant_old_layout(&path, "DROP TABLE local_pending; CREATE TABLE local_pending (model TEXT NOT NULL, row_id TEXT NOT NULL) STRICT; INSERT INTO local_pending VALUES ('messages', 'm1');");
    let (first, report) = reopen(&path, &[messages_spec()], None);
    assert_eq!(report.dropped.len(), 1);
    let before = first.restore_failed().unwrap();
    drop(first);
    let (second, report) = reopen(&path, &[messages_spec()], None);
    assert!(report.dropped.is_empty() && report.recreated.is_none());
    assert_eq!(second.restore_failed().unwrap(), before, "the kept data does not move");
    assert!(second.discard_restore_failed(&before[0].table, &before[0].key).unwrap());
    assert!(second.restore_failed().unwrap().is_empty());
}

#[test]
fn store_prop_boot_keys() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        exec(&store, "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, purpose, req_id) VALUES (1, 'd', 'req-1', 'k', 5, '/p', '{}', 0, 3);\
                      INSERT INTO manifest (key, value) VALUES ('saved_epoch', '9'), ('saved_generation', '4') ON CONFLICT(key) DO UPDATE SET value = excluded.value;\
                      UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
    }
    let (store, report) = reopen(&path, &[messages_spec()], None);
    assert!(report.recreated.is_some());
    assert_eq!(text(&store, "SELECT request_id FROM encrypted_pending").as_deref(), Some("req-1"));
    assert_eq!(count(&store, "SELECT sequence FROM encrypted_pending"), 5);
    assert_eq!((store.saved().unwrap().0, store.saved().unwrap().1), (9, 4), "the published epoch and generation survive the transfer");
}

#[test]
fn store_boot_corrupt_manifest_keeps_the_file_whole() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        exec(&store, "INSERT INTO manifest (key, value) VALUES ('saved_epoch', '-3') ON CONFLICT(key) DO UPDATE SET value = excluded.value;\
                      UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
    }
    let store = Store::open(&path).unwrap();
    assert!(matches!(store.boot(&[messages_spec()], None), Err(StoreError::CorruptManifest(_))));
    assert_eq!(text(&store, "SELECT value FROM manifest WHERE key = 'saved_epoch'").as_deref(), Some("-3"));
    assert_eq!(text(&store, "SELECT value FROM manifest WHERE key = 'format_version'").as_deref(), Some("old"));
}

#[test]
fn store_prop_boot_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        exec(&store, "INSERT INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, purpose, req_id) VALUES (1, 'd', 'req-1', 'k', 5, '/p', '{}', 0, 3);\
                      UPDATE manifest SET value = 'old' WHERE key = 'format_version';\
                      DROP TABLE row_messages;\
                      CREATE VIEW row_messages AS SELECT 1 AS id");
    }
    let store = Store::open(&path).unwrap();
    assert!(store.boot(&[messages_spec()], None).is_err(), "the model table cannot be created over the view");
    assert_eq!(text(&store, "SELECT value FROM manifest WHERE key = 'format_version'").as_deref(), Some("old"), "the format is not stamped: the old base is whole");
    assert_eq!(text(&store, "SELECT request_id FROM encrypted_pending").as_deref(), Some("req-1"), "the kept intent stays where it was");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM restore_failed"), 0, "nothing of the transfer is half done");
}
