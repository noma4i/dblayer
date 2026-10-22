use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::identity::RowId;
use crate::syncstate::SyncState;

use super::fixtures::*;
use super::{CloudAccount, CloudError, CloudState, MergeSource};

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

async fn tick(device: &Device, by: Duration) {
    device.clock.advance(by);
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_10_errors_decide_between_backoff_retry_and_stop() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    a.cloud.start();
    settle().await;
    fake.fail_pushes(vec![
        (CloudError::Unavailable { detail: "down".into() }, false),
        (CloudError::Throttled { retry_after_ms: 60_000 }, false),
    ]);
    note(&a, json!({"title": "t"}));
    settle().await;
    assert_eq!(fake.pushes(), 1);
    tick(&a, Duration::from_millis(900)).await;
    assert_eq!(fake.pushes(), 1, "the first retry waits the backoff delay");
    tick(&a, Duration::from_millis(200)).await;
    assert_eq!(fake.pushes(), 2);
    tick(&a, Duration::from_secs(59)).await;
    assert_eq!(fake.pushes(), 2, "the server retry-after is a floor");
    tick(&a, Duration::from_secs(2)).await;
    assert_eq!(fake.pushes(), 3);
    assert_eq!(a.db.cloud_pending().unwrap(), 0);
    assert_eq!(a.cloud.state(), CloudState::Idle);
    a.cloud.stop();
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_10_authorization_and_quota_stop_without_retries_and_keep_the_journal() {
    for (error, state) in [
        (CloudError::NeedsAuthorization { detail: "consent".into() }, CloudState::NeedsAuthorization),
        (CloudError::QuotaExceeded { detail: "full".into() }, CloudState::QuotaExceeded),
    ] {
        let fake = FakeCloud::new();
        let a = device(&fake, 1_000);
        a.cloud.start();
        settle().await;
        fake.fail_pushes(vec![(error, false)]);
        let id = note(&a, json!({"title": "t"}));
        settle().await;
        assert_eq!(a.cloud.state(), state);
        let calls = fake.calls();
        tick(&a, Duration::from_secs(86_400)).await;
        tokio::time::advance(Duration::from_secs(86_400)).await;
        settle().await;
        assert_eq!(fake.calls(), calls, "{state:?} does not retry on a timer");
        assert!(a.db.cloud_pending().unwrap() > 0);
        let row = a.db.sync_info("notes", &RowId::new(&id).unwrap()).unwrap();
        assert_eq!(row.state, SyncState::Synced, "cloudState is separate from record.sync");
        a.cloud.stop();
    }
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_10_offline_does_not_spend_attempts_and_a_signed_out_account_asks_for_consent() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    a.online.set_online(false);
    a.cloud.start();
    note(&a, json!({"title": "t"}));
    settle().await;
    assert_eq!(fake.calls(), 0, "offline makes no cloud calls");
    a.online.set_online(true);
    settle().await;
    assert_eq!(fake.pushes(), 1);
    assert_eq!(a.db.cloud_pending().unwrap(), 0);
    fake.set_account(CloudAccount::SignedOut);
    a.cloud.remote_changed();
    settle().await;
    assert_eq!(a.cloud.state(), CloudState::NeedsAuthorization);
    a.cloud.stop();
}

#[tokio::test]
async fn inv_cloud_11_rows_without_stamps_wait_for_a_source_choice() {
    for (source, local_wins) in [(MergeSource::UseLocal, true), (MergeSource::UseCloud, false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.db");
        let fake = FakeCloud::new();
        let remote = device(&fake, 5_000);
        remote.db.create("notes", obj(json!({"id": "n1", "title": "remote"}))).unwrap();
        remote.cloud.sync_now().await.unwrap();
        {
            let plain = Db::open(&path, vec![notes_spec(&[]), profiles_spec()], None).unwrap();
            plain.create("notes", obj(json!({"id": "n1", "title": "local"}))).unwrap();
        }
        let a = device_with(Db::open(&path, specs(), None).unwrap(), &fake, 1_000);
        let calls = fake.calls();
        a.cloud.sync_now().await.unwrap();
        assert_eq!(a.cloud.state(), CloudState::NeedsInitialMerge { rows: 1 });
        assert_eq!(fake.calls(), calls, "no cloud call before the choice");
        a.cloud.resolve_initial_merge(source).await.unwrap();
        a.cloud.sync_now().await.unwrap();
        remote.cloud.sync_now().await.unwrap();
        let want = if local_wins { "local" } else { "remote" };
        assert_eq!(title_of(&a, "n1").as_deref(), Some(want), "{source:?} on the first device");
        assert_eq!(title_of(&remote, "n1").as_deref(), Some(want), "{source:?} on the second device");
        assert_eq!(a.cloud.state(), CloudState::Idle);
    }
}
