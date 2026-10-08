use super::support::*;
use super::*;
use serde_json::json;

use crate::ddl::ManifestVerdict;
use crate::identity::ModelKey;
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
        "INSERT INTO op (action_key, action_mode, status, row_ids, input, created_at) VALUES ('messages:send', 'request', 'pending', x'5b5d', x'7b7d', 1);\
         INSERT INTO quarantine (model, payload, reason) VALUES ('messages', x'00', 'test');",
    );
}

fn reopen(path: &std::path::Path, models: &[ModelSpec], data_version: Option<&str>) -> (Store, BootReport) {
    let store = Store::open(path).unwrap();
    let report = store.boot(models, data_version).unwrap();
    (store, report)
}

#[test]
fn default_steps_have_six_entries_and_end_ready() {
    let steps = boot_steps(&BootConditions::default());
    assert_eq!(steps.len(), 6);
    assert_eq!(steps.first(), Some(&BootStep::OpenDatabase));
    assert_eq!(steps.last(), Some(&BootStep::MarkReady));
    assert!(!steps.contains(&BootStep::FinishInterruptedReset));
    assert!(!steps.contains(&BootStep::SweepStaleVersions));
}

#[test]
fn steps_follow_reset_intent_and_stale_version_models() {
    let intent = BootConditions { has_reset_intent: true, ..BootConditions::default() };
    assert_eq!(
        boot_steps(&intent),
        vec![
            BootStep::OpenDatabase,
            BootStep::Migrate,
            BootStep::FinishInterruptedReset,
            BootStep::ReconcileManifest,
            BootStep::CheckOperations,
            BootStep::Maintain,
            BootStep::MarkReady,
        ]
    );
    let stale = BootConditions { stale_version_models: vec![ModelKey::new("Message"), ModelKey::new("Media")], ..BootConditions::default() };
    assert!(boot_steps(&stale).contains(&BootStep::SweepStaleVersions));
}

#[test]
fn transaction_count_and_reader_visibility_follow_conditions() {
    let stale = BootConditions { stale_version_models: vec![ModelKey::new("Message"), ModelKey::new("Media")], ..BootConditions::default() };
    let intent = BootConditions { has_reset_intent: true, ..BootConditions::default() };
    assert_eq!(BootStep::transaction_count(&BootConditions::default()), 4);
    assert_eq!(BootStep::transaction_count(&stale), 6);
    assert_eq!(BootStep::transaction_count(&intent), 5);
    assert!(BootStep::MarkReady.reader_sees_rows_after());
    assert!(!BootStep::CheckOperations.reader_sees_rows_after());
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
        assert_eq!(store.journal_mode().unwrap().to_lowercase(), "wal");
        assert_eq!(store.synchronous_level().unwrap(), 1);
    }
    let (store, _) = reopen(&path, &[messages_spec()], None);
    assert!(store.point("messages", "m1").unwrap().is_some());
}

#[test]
fn sqlite_supports_strict_tables() {
    let store = Store::open_in_memory().unwrap();
    let version = store.sqlite_version().unwrap();
    let parts: Vec<u32> = version.split('.').map(|part| part.parse().unwrap()).collect();
    assert!(parts[0] > 3 || (parts[0] == 3 && parts[1] >= 37), "sqlite {version}");
}

#[test]
fn fingerprint_change_clears_only_that_model_and_keeps_operations() {
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
    assert_eq!(store.row_count("items").unwrap(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member WHERE model = 'messages'"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member WHERE model = 'items'"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
    store.commit(envelope(&store, vec![upsert("messages", message("m2", "c1"))], vec![])).unwrap();
}

#[test]
fn renaming_a_relation_resets_only_its_model() {
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
    assert_eq!(store.row_count("items").unwrap(), 1);
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
fn removed_model_loses_its_cache_and_fingerprint() {
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
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member WHERE model = 'items'"), 0);
    assert_eq!(store.row_count("messages").unwrap(), 1);
}

#[test]
fn data_version_mismatch_wipes_cache_and_keeps_user_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, Some("v1"));
        seed(&store);
    }
    let (store, report) = reopen(&path, &models, Some("v2"));
    assert_eq!(report.verdict, ManifestVerdict::Incompatible);
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'data_version' AND value = 'v2'"), 1);
}

#[test]
fn old_format_version_triggers_incompatible_reset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "UPDATE manifest SET value = '0' WHERE key = 'format_version'");
    }
    let (store, report) = reopen(&path, &models, None);
    assert_eq!(report.verdict, ManifestVerdict::Incompatible);
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
}

#[test]
fn missing_manifest_in_non_empty_database_is_incompatible() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "DELETE FROM manifest");
    }
    let (store, report) = reopen(&path, &models, None);
    assert_eq!(report.verdict, ManifestVerdict::Incompatible);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
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
            "INSERT INTO manifest (key, value) VALUES ('reset_intent', '{\"mode\":\"cache_only\",\"models\":[\"messages\"],\"started_at\":1}')",
        );
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(report.finished_interrupted_reset);
    assert_eq!(store.row_count("messages").unwrap(), 0);
    assert_eq!(store.row_count("items").unwrap(), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn interrupted_incompatible_reset_is_completed_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec(), items_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        seed(&store);
        exec(&store, "INSERT INTO manifest (key, value) VALUES ('reset_intent', '{\"mode\":\"incompatible\",\"models\":[],\"started_at\":1}')");
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
        exec(&store, "INSERT INTO manifest (key, value) VALUES ('reset_intent', 'garbage')");
    }
    let (store, report) = reopen(&path, &models, None);
    assert!(!report.finished_interrupted_reset);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM quarantine WHERE reason = 'corrupt-reset-intent'"), 1);
    assert_eq!(store.reset_intent().unwrap(), None);
}

#[test]
fn stale_record_versions_are_swept_with_membership_when_manifest_version_differs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    let models = [messages_spec()];
    {
        let (store, _) = reopen(&path, &models, None);
        store
            .commit(envelope(
                &store,
                vec![upsert("messages", message("m1", "c1")), upsert("messages", message("m2", "c1"))],
                vec![append(&scope("messages", "thread"), "m1", "a"), append(&scope("messages", "thread"), "m2", "b")],
            ))
            .unwrap();
        exec(&store, "UPDATE row_messages SET record_version = 0 WHERE id = 'm2'; UPDATE manifest SET value = '0' WHERE key = 'record_version'");
    }
    let (store, report) = reopen(&path, &models, None);
    assert_eq!(report.stale_version_drops, 1);
    assert_eq!(store.row_count("messages").unwrap(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 1);
    assert_eq!(store.counters().stale_version_drops, 1);
    assert_eq!(store.counters().rejected_records, 0);
}

#[test]
fn matching_record_version_skips_the_sweep() {
    let (store, _) = booted_store();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    exec(&store, "UPDATE row_messages SET record_version = 0");
    let report = store.boot(&[messages_spec(), items_spec()], None).unwrap();
    assert_eq!(report.stale_version_drops, 0);
    assert_eq!(store.row_count("messages").unwrap(), 0 + count(&store, "SELECT COUNT(*) FROM row_messages"));
}

#[test]
fn drop_stale_versions_removes_only_old_versions() {
    let (store, _) = booted_store();
    store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap();
    assert_eq!(store.drop_stale_versions("messages").unwrap(), 0);
    assert!(store.point("messages", "m1").unwrap().is_some());
    exec(
        &store,
        "INSERT INTO row_messages (id, revision, born_rev, field_rev, record_version, payload) VALUES ('m2', 0, 0, x'7b7d', 0, x'7b7d')",
    );
    assert_eq!(store.drop_stale_versions("messages").unwrap(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
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
fn schema_ahead_database_refuses_to_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("PRAGMA user_version = 99").unwrap();
    }
    assert!(matches!(Store::open(&path), Err(StoreError::Ddl(_))));
}
