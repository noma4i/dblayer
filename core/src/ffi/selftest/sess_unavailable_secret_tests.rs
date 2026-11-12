use super::crash_image_support::{advancing, timed_stand};
use super::rig_tests::bound;
use super::upload_stand::{plain, CREATE_MOMENT};
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{CredentialStore, DbError, OperationQueueStatus, ReadRequest, ReadValue};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const LANDED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;

/// The store of the app: the secret cannot be read while `down` is set.
struct Locked {
    inner: Arc<WatchedHeld>,
    down: AtomicBool,
}

impl CredentialStore for Locked {
    fn get(&self) -> Result<Option<String>, DbError> {
        if self.down.load(Ordering::SeqCst) { return Err(DbError::Storage { detail: "the keystore is locked".into() }); }
        CredentialStore::get(&*self.inner)
    }
    fn set(&self, credential: String) -> Result<(), DbError> { CredentialStore::set(&*self.inner, credential) }
    fn clear(&self) -> Result<(), DbError> { CredentialStore::clear(&*self.inner) }
}

/// sess_unavailable_mutate: a write accepted while the secret cannot be read stays `pending` without an error, and goes once when the secret can be read at the next `Foreground`.
#[tokio::test(flavor = "multi_thread")]
async fn sess_unavailable_write_waits_for_the_secret() {
    let (first, clock) = timed_stand();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let database = first.open_with(Some(&live), &[CREATE_MOMENT], Some(clock)).await;
    let secret = first.rig.credentials.inner.value().unwrap();
    database.close_now();

    let (mut stand, clock) = timed_stand();
    CredentialStore::set(&*stand.rig.credentials, secret).unwrap();
    let locked = Arc::new(Locked { inner: stand.rig.credentials.clone(), down: AtomicBool::new(true) });
    stand.store = Some(locked.clone());
    let database = stand.open_with(Some(&live), &[CREATE_MOMENT], Some(clock.clone())).await;
    let epoch = database.session_epoch();
    stand.script.data("CreateMoment", LANDED);
    bound(database.send(plain("Moment", r#"{"text":"A"}"#, epoch))).await.unwrap();
    let queue = || async {
        let stream = database.subscribe_values(epoch, ReadRequest::Operations);
        match bound(stream.pull_next()).await {
            Some(ReadValue::Operations { items, .. }) => items,
            other => panic!("the operation queue ended: {other:?}"),
        }
    };
    let waiting = queue().await;
    assert_eq!((waiting.len(), waiting.iter().all(|item| item.status != OperationQueueStatus::Failed && item.error.is_none()), stand.script.count("CreateMoment")), (1, true, 0), "the write waits without an error: {waiting:?}");
    locked.down.store(false, Ordering::SeqCst);
    let _foreground = database.life.foreground();
    advancing(&clock, crate::test_wait::settled(&database, "the write landed", || database.peek_row(epoch, "Moment".into(), "m77".into()).unwrap().is_some())).await;
    assert_eq!(stand.script.count("CreateMoment"), 1, "the write went once");
}
