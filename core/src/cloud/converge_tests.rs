use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::identity::RowId;

use super::fixtures::*;
use super::{CloudError, CloudState};

#[test]
fn inv_cloud_13_values_received_before_the_server_row_survive_its_arrival() {
    use crate::commit::{CommitEnvelope, EntityOp, ScopeRef, WriteOrigin};
    use crate::identity::{ModelKey, ScopeKey};
    for origin in [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Replace] {
        for pending in [None, Some(json!(null)), Some(json!("local"))] {
            let fake = FakeCloud::new();
            let a = device(&fake, 1_000);
            let id = RowId::new("p1").unwrap();
            let change = super::CloudChange { model: "profiles".into(), row_id: "p1".into(), field: "draft".into(), clock_ms: 100, clock_count: 0, replica: "remote".into(), value: json!("cloud") };
            a.db.apply_cloud_page(a.db.generation(), &a.db.cloud_replica(a.db.generation()).unwrap(), &[change], &[1]).unwrap();
            if let Some(value) = &pending {
                a.db.set_local("profiles", &id, "draft", value.clone()).unwrap();
            }
            let row = obj(json!({"id": "p1", "name": "server"}));
            if origin == WriteOrigin::Replace {
                let scope = ScopeRef::new(ModelKey::new("profiles"), ScopeKey::from_raw("all"));
                a.db.replace(&crate::db::ReplaceRequest::new(scope, crate::schema::Coverage::Complete), vec![row]).unwrap();
            } else {
                let mut envelope = CommitEnvelope::new(origin, a.db.generation());
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("profiles"), id: id.clone(), row });
                a.db.land(envelope).unwrap();
            }
            let row = a.db.find("profiles", &id).unwrap().unwrap();
            assert_eq!(row.text("name"), Some("server"));
            assert_eq!(row.text("draft"), pending.as_ref().map_or(Some("cloud"), serde_json::Value::as_str), "{origin:?} {pending:?}");
            let outgoing = a.db.cloud_batch(a.db.generation(), 10).unwrap();
            assert_eq!(outgoing.len(), usize::from(pending.is_some()));
            if let Some(value) = pending {
                assert_eq!(outgoing[0].1.value, value);
            }
        }
    }
}

#[tokio::test]
async fn inv_reset_03_cloud_reply_cannot_restore_rows_after_reset() {
    let fake = FakeCloud::new();
    let remote = device(&fake, 1_000);
    let id = note(&remote, json!({"title": "old account"}));
    remote.cloud.sync_now().await.unwrap();
    let local = device(&fake, 1_000);
    let db = local.db.clone();
    fake.before_pull_reply(move || db.reset_all().unwrap());
    let result = local.cloud.sync_now().await;
    assert_eq!(title_of(&local, &id), None);
    assert_eq!(local.db.cloud_cursor(local.db.generation()).unwrap(), None);
    assert!(result.is_err());
}

#[tokio::test]
async fn inv_reset_02_reset_removes_cloud_data_before_the_next_sync() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    let deleted = note(&a, json!({"title": "deleted"}));
    a.db.destroy("notes", &RowId::new(&deleted).unwrap()).unwrap();
    note(&a, json!({"title": "private"}));
    let own = a.db.cloud_replica(a.db.generation()).unwrap();
    a.db.apply_cloud_page(a.db.generation(), &own, &[], &[9]).unwrap();
    a.db.reset_all().unwrap();
    assert_eq!(a.db.cloud_pending().unwrap(), 0);
    assert_eq!(a.db.cloud_cursor(a.db.generation()).unwrap(), None);
    {
        let inner = a.db.shared.store.lock();
        for table in ["cloud_field", "cloud_field_node", "cloud_row", "cloud_log", "cloud_log_node"] {
            let count: i64 = inner.connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0)).unwrap();
            assert_eq!(count, 0, "{table} retains account data");
        }
    }
    a.cloud.sync_now().await.unwrap();
    assert!(fake.log().is_empty());
}

#[test]
fn inv_reset_04_old_cloud_pass_cannot_read_the_new_accounts_data() {
    let fake = FakeCloud::new();
    let device = device(&fake, 1_000);
    let old = device.db.generation();
    device.db.reset_all().unwrap();
    note(&device, json!({"title": "new account"}));
    let results = [
        device.db.cloud_batch(old, 100).map(|_| ()),
        device.db.cloud_cursor(old).map(|_| ()),
        device.db.cloud_replica(old).map(|_| ()),
        device.db.cloud_unstamped(old).map(|_| ()),
    ];
    assert!(results.iter().all(Result::is_err), "old cloud pass read the new generation: {results:?}");
}

#[tokio::test]
async fn inv_cloud_02_create_update_delete_reach_the_other_device() {
    let fake = FakeCloud::new();
    let (a, b) = (device(&fake, 1_000), device(&fake, 1_000));
    let id = note(&a, json!({"title": "first", "rank": 3, "tags": ["x", "y"]}));
    a.cloud.sync_now().await.unwrap();
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id).as_deref(), Some("first"));
    assert_eq!(field_of(&b, &id, "rank"), json!(3));
    assert_eq!(field_of(&b, &id, "tags"), json!(["x", "y"]));
    a.clock.advance(Duration::from_secs(5));
    edit(&a, &id, json!({"title": "second", "body": "text"}));
    a.cloud.sync_now().await.unwrap();
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id).as_deref(), Some("second"));
    assert_eq!(field_of(&b, &id, "body"), json!("text"));
    a.clock.advance(Duration::from_secs(5));
    a.db.destroy("notes", &RowId::new(&id).unwrap()).unwrap();
    a.cloud.sync_now().await.unwrap();
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id), None);
    assert_eq!(b.db.cloud_pending().unwrap(), 0);
}

#[tokio::test]
async fn inv_cloud_07_only_listed_attributes_and_local_models_travel() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    let id = note(&a, json!({"title": "t", "secret": "hidden"}));
    crate::db::fixtures::put(&a.db, "profiles", json!({"id": "p1", "name": "server name"}));
    a.db.set_local("profiles", &RowId::new("p1").unwrap(), "draft", json!("typing")).unwrap();
    a.cloud.sync_now().await.unwrap();
    let mut sent: Vec<(String, String)> = fake.log().into_iter().map(|change| (change.model, change.field)).collect();
    sent.sort();
    sent.dedup();
    assert!(sent.contains(&("profiles".to_string(), "draft".to_string())));
    assert!(sent.contains(&("notes".to_string(), "title".to_string())));
    assert!(!sent.iter().any(|(_, field)| field == "secret" || field == "name"), "sent {sent:?}");
    assert_eq!(title_of(&a, &id).as_deref(), Some("t"));
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_06_idle_makes_no_cloud_calls() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    a.cloud.start();
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let started = fake.calls();
    note(&a, json!({"title": "t"}));
    until(|| fake.pushes() > 0).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let busy = fake.calls();
    assert!(busy > started);
    a.clock.advance(Duration::from_secs(86_400));
    tokio::time::advance(Duration::from_secs(86_400)).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fake.calls(), busy);
    assert_eq!(a.cloud.state(), CloudState::Idle);
    a.cloud.stop();
}

#[tokio::test]
async fn inv_cloud_01_a_crash_between_write_and_push_keeps_the_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.db");
    let fake = FakeCloud::new();
    let id = {
        let a = device_with(Db::open(&path, specs(), None).unwrap(), &fake, 1_000);
        note(&a, json!({"title": "kept"}))
    };
    let a = device_with(Db::open(&path, specs(), None).unwrap(), &fake, 2_000);
    assert!(a.db.cloud_pending().unwrap() > 0);
    assert_eq!(fake.calls(), 0);
    a.cloud.sync_now().await.unwrap();
    let b = device(&fake, 3_000);
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id).as_deref(), Some("kept"));
}

#[tokio::test]
async fn inv_cloud_08_ack_covers_only_the_sent_versions() {
    let fake = FakeCloud::new();
    let a = Arc::new(device(&fake, 1_000));
    let b = device(&fake, 1_000);
    let id = note(&a, json!({"title": "first"}));
    let (db, edited) = (a.db.clone(), id.clone());
    let once = std::sync::atomic::AtomicBool::new(false);
    fake.before_push(move || {
        if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            db.update("notes", &RowId::new(&edited).unwrap(), obj(json!({"title": "second", "body": "extra"}))).unwrap();
        }
    });
    a.cloud.sync_now().await.unwrap();
    assert_eq!(a.db.cloud_pending().unwrap(), 0);
    let titles: Vec<_> = fake.log().into_iter().filter(|change| change.field == "title").map(|change| change.value).collect();
    assert_eq!(titles.len(), 2, "both versions reach the cloud: {titles:?}");
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id).as_deref(), Some("second"));
    assert_eq!(field_of(&b, &id, "body"), json!("extra"));
}

#[tokio::test]
async fn inv_cloud_09_pages_are_idempotent_and_a_lost_push_response_repeats_cleanly() {
    let fake = FakeCloud::new();
    let (a, b) = (device(&fake, 1_000), device(&fake, 1_000));
    let id = note(&a, json!({"title": "once"}));
    fake.fail_pushes(vec![(CloudError::Unavailable { detail: "lost answer".into() }, true)]);
    assert!(a.cloud.sync_now().await.is_err());
    assert!(a.db.cloud_pending().unwrap() > 0);
    a.cloud.sync_now().await.unwrap();
    fake.set_page(2);
    b.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&b, &id).as_deref(), Some("once"));
    let changes = fake.log();
    let c = device(&fake, 4_000);
    let own = c.db.cloud_replica(c.db.generation()).unwrap();
    c.db.apply_cloud_page(c.db.generation(), &own, &changes, &[9; 8]).unwrap();
    let revision = c.db.revision_seq().unwrap();
    c.db.apply_cloud_page(c.db.generation(), &own, &changes, &[9; 8]).unwrap();
    assert_eq!(c.db.cloud_cursor(c.db.generation()).unwrap(), Some(vec![9; 8]));
    assert_eq!(c.db.cloud_pending().unwrap(), 0, "remote changes are never journaled");
    assert_eq!(title_of(&c, &id).as_deref(), Some("once"));
    assert_eq!(c.db.revision_seq().unwrap(), revision + 1, "a repeated page moves only the cursor");
}

#[tokio::test]
async fn inv_cloud_05_delete_wins_over_a_later_edit_and_the_tombstone_never_expires() {
    let fake = FakeCloud::new();
    let (a, b) = (device(&fake, 1_000), device(&fake, 1_000));
    let id = note(&a, json!({"title": "doomed"}));
    a.cloud.sync_now().await.unwrap();
    b.cloud.sync_now().await.unwrap();
    a.clock.advance(Duration::from_secs(1));
    a.db.destroy("notes", &RowId::new(&id).unwrap()).unwrap();
    b.clock.advance(Duration::from_secs(60));
    edit(&b, &id, json!({"title": "edited later"}));
    a.cloud.sync_now().await.unwrap();
    b.cloud.sync_now().await.unwrap();
    a.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&a, &id), None);
    assert_eq!(title_of(&b, &id), None);
    let c = device(&fake, 1_000);
    c.clock.advance(Duration::from_secs(10 * 365 * 86_400));
    c.cloud.sync_now().await.unwrap();
    assert_eq!(title_of(&c, &id), None);
}
