use std::time::Duration;

use serde_json::json;

use crate::db::Db;
use crate::identity::RowId;
use crate::syncstate::SyncState;

use super::fixtures::*;
use super::{CloudAccount, CloudError, CloudState, MergeSource};

async fn state_is(cloud: &super::Cloud, expected: CloudState) {
    let mut state = cloud.state_changes();
    tokio::time::timeout(Duration::from_secs(60), state.wait_for(|state| *state == expected)).await.expect("the cloud never reached the awaited state").unwrap();
}

async fn tick_without_wake(device: &Device, by: Duration) {
    let polls = device.clock.polls();
    device.clock.advance(by);
    device.clock.await_polls(polls + 1).await;
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_10_errors_decide_between_backoff_retry_and_stop() {
    let fake = FakeCloud::new();
    let a = device(&fake, 1_000);
    a.cloud.start();
    until(|| fake.calls() >= 2).await;
    fake.fail_pushes(vec![
        (CloudError::Unavailable { detail: "down".into() }, false),
        (CloudError::Throttled { retry_after_ms: 60_000 }, false),
    ]);
    note(&a, json!({"title": "t"}));
    a.clock.await_started(1).await;
    assert_eq!(fake.pushes(), 1);
    tick_without_wake(&a, Duration::from_millis(900)).await;
    assert_eq!(fake.pushes(), 1, "the first retry waits the backoff delay");
    a.clock.advance(Duration::from_millis(200));
    a.clock.await_started(2).await;
    assert_eq!(fake.pushes(), 2);
    tick_without_wake(&a, Duration::from_secs(59)).await;
    assert_eq!(fake.pushes(), 2, "the server retry-after is a floor");
    a.clock.advance(Duration::from_secs(2));
    state_is(&a.cloud, CloudState::Idle).await;
    assert_eq!(fake.pushes(), 3);
    assert_eq!(a.db.cloud_pending().unwrap(), 0);
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
        until(|| fake.calls() >= 2).await;
        fake.fail_pushes(vec![(error, false)]);
        let id = note(&a, json!({"title": "t"}));
        state_is(&a.cloud, state.clone()).await;
        let calls = fake.calls();
        assert_eq!(a.clock.active(), 0, "{state:?} armed a retry timer");
        a.clock.advance(Duration::from_secs(86_400));
        tokio::time::advance(Duration::from_secs(86_400)).await;
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
    assert_eq!(fake.calls(), 0, "offline makes no cloud calls");
    a.online.set_online(true);
    until(|| fake.pushes() == 1 && a.db.cloud_pending().unwrap() == 0).await;
    fake.set_account(CloudAccount::SignedOut);
    a.cloud.remote_changed();
    state_is(&a.cloud, CloudState::NeedsAuthorization).await;
    a.cloud.stop();
}

#[tokio::test]
async fn inv_cloud_11_rows_without_stamps_wait_for_a_source_choice() {
    for (source, local_wins, edit_during_pull) in [(MergeSource::UseLocal, true, false), (MergeSource::UseLocal, true, true), (MergeSource::UseCloud, false, false)] {
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
        if edit_during_pull {
            let db = a.db.clone();
            let edited = std::sync::atomic::AtomicBool::new(false);
            fake.before_pull_reply(move || {
                if !edited.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    db.update("notes", &RowId::new("n1").unwrap(), obj(json!({"title": "edited"}))).unwrap();
                }
            });
        }
        a.cloud.resolve_initial_merge(source).await.unwrap();
        a.cloud.sync_now().await.unwrap();
        remote.cloud.sync_now().await.unwrap();
        let want = if !local_wins { "remote" } else if edit_during_pull { "edited" } else { "local" };
        assert_eq!(title_of(&a, "n1").as_deref(), Some(want), "{source:?} on the first device");
        assert_eq!(title_of(&remote, "n1").as_deref(), Some(want), "{source:?} on the second device");
        assert_eq!(a.cloud.state(), CloudState::Idle);
    }
}

#[tokio::test(start_paused = true)]
async fn inv_cloud_10_manual_sync_returns_the_original_error_without_retries_and_keeps_the_journal() {
    let cases = [
        (CloudError::Offline { detail: "no route".into() }, CloudState::Offline),
        (CloudError::Unavailable { detail: "down".into() }, CloudState::Offline),
        (CloudError::Throttled { retry_after_ms: 5_000 }, CloudState::Offline),
        (CloudError::NeedsAuthorization { detail: "consent".into() }, CloudState::NeedsAuthorization),
        (CloudError::QuotaExceeded { detail: "full".into() }, CloudState::QuotaExceeded),
        (CloudError::Rejected { detail: "refused".into() }, CloudState::Failed { detail: "refused".into() }),
    ];
    for (error, state) in cases {
        let fake = FakeCloud::new();
        let a = device(&fake, 1_000);
        note(&a, json!({"title": "t"}));
        fake.fail_pushes(vec![(error.clone(), false)]);
        let outcome = a.cloud.sync_now().await;
        assert_eq!(outcome, Err(error.clone()), "{error:?} comes back unchanged");
        assert_eq!(a.cloud.state(), state, "{error:?}");
        assert_eq!(fake.pushes(), 1, "{error:?} made one transport call");
        assert_eq!(a.clock.active(), 0, "{error:?} armed a retry timer");
        assert!(a.db.cloud_pending().unwrap() > 0, "{error:?} lost the journal");
    }
}
