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
    let store = Store::open(path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let report = store.boot(models, data_version).unwrap();
    (store, report)
}

#[test]
fn boot_creates_model_tables_and_ready_follows_mark_ready() {
    let store = Store::open_in_memory(std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
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
fn store_boot_journal_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unknown.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'unknown-end', 1)");
    }
    let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let result = store.boot(&models, None);
    assert!(result.is_err(), "unknown end journal must fail open: {result:?}");
    assert_eq!(store.reset_intent().unwrap().as_deref(), Some("unknown-end"));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE action_key = 'messages:send'"), 1);
}

#[test]
fn store_boot_replay_end_storage_error() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("failure.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, '0000000000000001', 1); INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'session_end', 1); CREATE TRIGGER refuse_session_delete BEFORE DELETE ON encrypted_session BEGIN SELECT RAISE(ABORT, 'session delete refused'); END");
    }
    let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(store.boot(&models, None).is_err(), "the failed replay must fail open");
    assert_eq!(store.reset_intent().unwrap().as_deref(), Some("session_end"));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_session"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE action_key = 'messages:send'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn store_boot_replay_end() {
    for (mode, session_count) in [("session_end", 0), ("account", 1)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("end.sqlite");
        let models = [messages_spec(), items_spec()];
        {
            let (store, _) = reopen(&path, &models, None);
            seed(&store);
            exec(&store, "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, '0000000000000001', 1)");
            exec(&store, &format!("INSERT INTO reset_intent (id, mode, started_at) VALUES (1, '{mode}', 1)"));
        }
        let (store, report) = reopen(&path, &models, None);
        assert!(report.finished_interrupted_reset, "{mode}");
        assert_eq!((store.row_count("messages").unwrap(), store.row_count("items").unwrap()), (0, 0), "{mode}");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_pending"), 0, "{mode}: pending");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0, "{mode}");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_session"), session_count, "{mode}");
        assert_eq!(store.reset_intent().unwrap(), None, "{mode}");
    }
}

#[test]
fn store_boot_replay_reset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reset.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, '0000000000000001', 1)");
        exec(&store, "INSERT INTO reset_intent (id, mode, started_at) VALUES (1, 'mismatched', 1)");
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(store.row_count("items").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_session"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn boot_prunes_expired_tombstones() {
    let (store, clock) = booted_store();
    store.mark_tombstone("messages", "old", 1, 0).unwrap();
    clock.advance(std::time::Duration::from_millis(85_400_001));
    let report = store.boot(&[messages_spec(), items_spec()], None).unwrap();
    assert_eq!(report.tombstone_drops, 1);
}

#[test]
fn boot_prunes_transfer_outcomes_older_than_seven_days() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::StoreOp;
    use crate::owners::write::outbox::OutboxOutcome;
    let (store, clock) = booted_store();
    let outcome = |transfer_id| StoreOp::TransferOutcome { transfer_id, cycle_id: 1, outcome: OutboxOutcome::Discarded };
    super::data_inv_tests::write(&store, BatchAuthor::Outbox, Vec::new(), vec![outcome(1)]);
    clock.advance(std::time::Duration::from_millis(6 * 86_400_000));
    super::data_inv_tests::write(&store, BatchAuthor::Outbox, Vec::new(), vec![outcome(2), StoreOp::TransferOutcome { transfer_id: 1, cycle_id: 2, outcome: OutboxOutcome::Discarded }]);
    clock.advance(std::time::Duration::from_millis(86_400_001));
    store.boot(&[messages_spec(), items_spec()], None).unwrap();
    let restored = store.read_restore().unwrap().transfer_outcomes;
    assert_eq!(restored.keys().copied().collect::<Vec<_>>(), vec![1, 2], "only the old cycle goes at open");
    assert_eq!(restored.get(&1), Some(&[(2, OutboxOutcome::Discarded)].into()));
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
    assert_eq!(failed_restore(&store).len(), 1);
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
    let before = failed_restore(&first);
    drop(first);
    let (second, report) = reopen(&path, &[messages_spec()], None);
    assert!(report.dropped.is_empty() && report.recreated.is_none());
    assert_eq!(failed_restore(&second), before, "the kept data does not move");
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
    let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
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
    let store = Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(matches!(store.boot(&[messages_spec()], None), Err(StoreError::Sqlite(_))), "the model table cannot be created over the view");
    assert_eq!(text(&store, "SELECT value FROM manifest WHERE key = 'format_version'").as_deref(), Some("old"), "the format is not stamped: the old base is whole");
    assert_eq!(text(&store, "SELECT request_id FROM encrypted_pending").as_deref(), Some("req-1"), "the kept intent stays where it was");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM restore_failed"), 0, "nothing of the transfer is half done");
}

const PREVIOUS_UPLOAD_TABLE: &str = "CREATE TABLE sync_upload (model TEXT NOT NULL, row_id TEXT NOT NULL, action TEXT NOT NULL, status TEXT NOT NULL, stage TEXT NOT NULL, sent INTEGER NOT NULL, total INTEGER NOT NULL, wait TEXT, attempts INTEGER NOT NULL, error_kind TEXT, error_message TEXT, seq INTEGER, source TEXT, batch INTEGER, upload_id TEXT, completed INTEGER, created_at INTEGER, inline INTEGER, size INTEGER, sha256 TEXT, mutation_path TEXT, mutation_body TEXT, on_failure TEXT, steps_path TEXT, steps_body TEXT, transfer_id INTEGER, cycle_id INTEGER, signed_id TEXT, aborted INTEGER, op_id INTEGER, fields TEXT, PRIMARY KEY (model, row_id, action)) STRICT, WITHOUT ROWID";

const PREVIOUS_UPLOAD_ERROR_TABLE: &str = "CREATE TABLE sync_upload_error (model TEXT NOT NULL, row_id TEXT NOT NULL, action TEXT NOT NULL, node INTEGER NOT NULL, parent INTEGER NOT NULL, pos INTEGER NOT NULL, name TEXT, kind INTEGER NOT NULL, whole INTEGER, fraction REAL, text TEXT, PRIMARY KEY (model, row_id, action, node)) STRICT, WITHOUT ROWID";

#[test]
fn a_format_change_carries_the_transfer_rows_of_sync_upload_and_not_the_folded_ones() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        exec(&store, &format!("DROP TABLE sync_upload; {PREVIOUS_UPLOAD_TABLE}; {PREVIOUS_UPLOAD_ERROR_TABLE}; \
            INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, transfer_id, size) VALUES ('messages', 'm1', 'photo', 'uploading', 'part', 2, 5, 1, 7, 7, 40); \
            INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts) VALUES ('messages', 'm2', 'photo', 'uploading', 'put', 10, 100, 0); \
            UPDATE manifest SET value = 'old' WHERE key = 'format_version'"));
    }
    let (store, report) = reopen(&path, &[messages_spec()], None);
    assert_eq!(report.recreated, Some(crate::store::ddl::RecreateReason::CoreFormat));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_upload WHERE row_id = 'm1' AND seq = 7 AND transfer_id = 7 AND sent = 2 AND total = 5 AND size = 40 AND status = 'uploading'"), 1, "the transfer row is carried with its values");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_upload WHERE row_id = 'm1' AND round_error IS NULL"), 1, "the carried row has the new column, empty");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_upload"), 1, "the folded row is not carried");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM restore_failed WHERE source_table = 'sync_upload' AND json_extract(columns, '$.row_id') = 'm2'"), 1, "the uncarrried row retains its original address");
    assert!(!store.table_exists("sync_upload_error").unwrap());
}

#[test]
fn a_format_change_carries_queued_operations_transfers_and_the_session_and_drops_the_old_upload_writer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let (store, _) = reopen(&path, &[messages_spec()], None);
        exec(&store, "INSERT INTO op (action_key, status, created_at, since) VALUES ('messages:send', 'pending', 1, 1); \
            INSERT INTO encrypted_session (id, credential_digest, sequence) VALUES (1, 'digest', 41); \
            INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, transfer_id) VALUES ('messages', 'm1', 'photo', 'uploading', 'part', 2, 5, 1, 7, 7); \
            CREATE TABLE blob_upload (id TEXT PRIMARY KEY NOT NULL, seq INTEGER); \
            CREATE TABLE blob_use (consumer_seq INTEGER NOT NULL, blob_id TEXT NOT NULL); \
            CREATE TABLE op_item (seq INTEGER NOT NULL, position INTEGER NOT NULL); \
            INSERT INTO blob_upload (id, seq) VALUES ('b1', 1); \
            INSERT INTO op_item (seq, position) VALUES (1, 0); \
            DROP TABLE op_value; DROP TABLE op_error; \
            CREATE TABLE op_value (seq INTEGER NOT NULL, item_position INTEGER NOT NULL DEFAULT -1, part TEXT NOT NULL, node INTEGER NOT NULL, parent INTEGER NOT NULL, pos INTEGER NOT NULL, name TEXT, kind INTEGER NOT NULL, whole INTEGER, fraction REAL, text TEXT, PRIMARY KEY (seq, item_position, part, node)) STRICT, WITHOUT ROWID; \
            CREATE TABLE op_error (seq INTEGER NOT NULL, item_position INTEGER NOT NULL, node INTEGER NOT NULL, parent INTEGER NOT NULL, pos INTEGER NOT NULL, name TEXT, kind INTEGER NOT NULL, whole INTEGER, fraction REAL, text TEXT, PRIMARY KEY (seq, item_position, node)) STRICT, WITHOUT ROWID; \
            INSERT INTO op_value (seq, item_position, part, node, parent, pos, name, kind, text) VALUES (1, -1, 'input', 0, 0, 0, 'text', 4, 'hello'); \
            INSERT INTO op_error (seq, item_position, node, parent, pos, name, kind, text) VALUES (1, -1, 0, 0, 0, 'detail', 4, 'refused'); \
            UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
    }
    let (store, report) = reopen(&path, &[messages_spec()], None);
    assert_eq!(report.recreated, Some(crate::store::ddl::RecreateReason::CoreFormat));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE action_key = 'messages:send' AND status = 'pending'"), 1, "the queued operation is carried");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op_value WHERE seq = 1 AND part = 'input' AND name = 'text' AND text = 'hello'"), 1, "the operation input is carried without the old item column");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op_error WHERE seq = 1 AND name = 'detail' AND text = 'refused'"), 1, "the operation error is carried without the old item column");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM pragma_table_info('op_value') WHERE name = 'item_position'") + count(&store, "SELECT COUNT(*) FROM pragma_table_info('op_error') WHERE name = 'item_position'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM sync_upload WHERE row_id = 'm1' AND transfer_id = 7 AND sent = 2 AND total = 5"), 1, "the transfer row is carried");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM encrypted_session WHERE credential_digest = 'digest' AND sequence = 41"), 1, "the saved session is carried");
    for table in ["blob_upload", "blob_use", "op_item"] {
        assert!(!store.table_exists(table).unwrap(), "{table}");
    }
    assert_eq!(count(&store, "SELECT COUNT(*) FROM restore_failed"), 0);
}

/// D-429, the Boot row of a format mismatch in `store.md`: the rows of a `storage: local` model with its old fingerprint and the `local: true` values of a server row stay after a format change; the server row itself is cache and goes.
#[test]
fn a_format_change_keeps_the_data_of_the_app() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let mut notes = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    notes.storage = crate::schema::Storage::Local;
    let mut messages = messages_spec();
    messages.fields.push(FieldSpec { merge: Some(crate::schema::MergeRule::Local), ..FieldSpec::scalar("draft", ScalarKind::Str).optional() });
    let models = vec![notes, messages];
    let open = || Store::open_models(Some(&path), models.clone(), None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    {
        let store = open();
        let batch = |ops| crate::owners::store::StoreBatch { author: crate::owners::contract::BatchAuthor::LocalWrites, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
        store.write_batch(&batch(vec![crate::owners::store::StoreOp::Rows { model: "messages".into(), rows: vec![crate::owners::store::RowValue { row_id: "m1".into(), values: json!({"id": "m1", "chatId": "c1", "body": "server"}).to_string() }] }]), store.files(), None).unwrap();
        store.write_batch(&batch(vec![crate::owners::store::StoreOp::LocalRows { model: "notes".into(), rows: vec![crate::owners::store::RowValue { row_id: "n1".into(), values: json!({"id": "n1", "text": "keep"}).to_string() }] }]), store.files(), None).unwrap();
        store.write_batch(&batch(vec![crate::owners::store::StoreOp::LocalFields { model: "messages".into(), row_id: "m1".into(), values: json!({"draft": "mine"}).to_string() }]), store.files(), None).unwrap();
        exec(&store, "UPDATE manifest SET value = 'old' WHERE key = 'format_version'");
    }
    let store = open();
    assert_eq!((store.open_report().recreated, store.open_report().reset_models.clone()), (Some(crate::store::ddl::RecreateReason::CoreFormat), vec!["messages".to_string()]), "the kept local model is not reported reset");
    let (notes, messages) = (crate::store::ddl::table_name("notes"), crate::store::ddl::table_name("messages"));
    let kept = (count(&store, &format!("SELECT COUNT(*) FROM {notes} WHERE id = 'n1' AND text = 'keep'")), count(&store, "SELECT COUNT(*) FROM local_pending WHERE model = 'messages' AND row_id = 'm1' AND path = 'draft'"), count(&store, &format!("SELECT COUNT(*) FROM {messages}")));
    assert_eq!(kept, (1, 1, 0), "the local row and the local value stay, the server row goes");
}
