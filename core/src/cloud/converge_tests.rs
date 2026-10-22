use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::identity::RowId;

use super::fixtures::*;
use super::{CloudError, CloudState};

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
    let own = c.db.cloud_replica().unwrap();
    c.db.apply_cloud_page(&own, &changes, &[9; 8]).unwrap();
    let revision = c.db.revision_seq().unwrap();
    c.db.apply_cloud_page(&own, &changes, &[9; 8]).unwrap();
    assert_eq!(c.db.cloud_cursor().unwrap(), Some(vec![9; 8]));
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
