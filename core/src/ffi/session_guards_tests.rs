use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::encrypted_tests::{code, failure, SubscriptionsOnly, ThroughServer};
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, SessionCode};
use crate::pacing::SystemClock;
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64, short_key_pin};
use crate::transport::{CredentialSlot as CoreCredentials};

pub(super) const SOCIAL_BODY: &str = r#"{"provider":"google","idToken":"t"}"#;

#[derive(Default)]
pub(super) struct WatchedHeld {
    pub(super) inner: MemoryCredentials,
    sets: AtomicUsize,
    clears: AtomicUsize,
    fail_next_clear: AtomicBool,
}

impl CredentialStore for WatchedHeld {
    fn get(&self) -> Result<Option<String>, DbError> { self.inner.get().map_err(failure) }

    fn set(&self, credential: String) -> Result<(), DbError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.inner.set(&credential).map_err(failure)
    }

    fn clear(&self) -> Result<(), DbError> {
        self.clears.fetch_add(1, Ordering::SeqCst);
        match self.fail_next_clear.swap(false, Ordering::SeqCst) {
            true => Err(DbError::Network { detail: "the app could not clear the credential".into() }),
            false => self.inner.clear().map_err(failure),
        }
    }
}

pub(super) struct Rig {
    pub(super) server: Arc<FakeServer>,
    pub(super) credentials: Arc<WatchedHeld>,
}

impl Rig {
    pub(super) fn new() -> Self {
        Self { server: FakeServer::new(Arc::new(SystemClock)), credentials: Arc::default() }
    }

    pub(super) fn open(&self, path: Option<&Path>, pin: &str) -> Result<Arc<Database>, DbError> {
        Database::open(Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 }, "[]".to_string(), Some(Arc::new(SubscriptionsOnly)), Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() }), Some(self.credentials.clone()), Some(Arc::new(ThroughServer(self.server.clone()))))
    }

    pub(super) fn touched(&self) -> (usize, usize) {
        (self.credentials.sets.load(Ordering::SeqCst), self.credentials.clears.load(Ordering::SeqCst))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pin_mismatch_at_open_and_in_the_config_never_touches_the_credential_store() {
    let rig = Rig::new();
    assert!(matches!(rig.open(None, short_key_pin()), Err(DbError::Invalid { .. })));
    assert_eq!(rig.touched(), (0, 0));
    let database = rig.open(None, pin()).unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    let (credential, before) = (rig.credentials.inner.value(), rig.touched());


    assert_eq!((rig.touched(), rig.credentials.inner.value()), (before, credential));
    database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_code_is_typed_and_keeps_the_credential() {
    let table = [
        ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable),
        ("UNAUTHENTICATED", SessionCode::Unauthenticated),
        ("PROVIDER_REJECTED", SessionCode::ProviderRejected),
        ("BLOCKED", SessionCode::Other),
    ];
    for (wire, expected) in table {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let database = rig.open(None, pin()).unwrap();
            database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
            let credential = rig.credentials.inner.value();
            rig.server.refuse_next(wire, confirmed);

            let refused = database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();

            assert_eq!(code(refused), expected, "{wire} confirmed={confirmed}");
            assert_eq!((rig.credentials.inner.value(), rig.touched()), (credential, (1, 0)), "{wire} confirmed={confirmed}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn resume_after_a_restart_sends_the_pending_request_and_the_server_runs_it_once() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let first = rig.open(Some(&path), pin()).unwrap();
    rig.server.drop_next_request();
    super::encrypted_tests::cancel_on_retry(&first, first.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into())).await;
    assert_eq!(rig.server.seen().len(), 0);
    drop(first);

    let restarted = rig.open(Some(&path), pin()).unwrap();
    let resumed = restarted.session_resume().await.unwrap();

    assert_eq!(resumed.as_deref(), Some(r#"{"nextStep":"authenticated"}"#));
    assert_eq!(restarted.session_resume().await.unwrap(), None);
    assert_eq!(rig.server.executed().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_lost_logout_answer_is_resumed_through_the_bridge_and_clears_the_credential() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let first = rig.open(Some(&path), pin()).unwrap();
    first.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.lose_next_answer();
    super::encrypted_tests::cancel_on_retry(&first, first.session_call("logout".into(), "{}".into())).await;
    assert!(rig.credentials.inner.value().is_some());
    drop(first);

    let restarted = rig.open(Some(&path), pin()).unwrap();
    let resumed = restarted.session_resume().await.unwrap();

    assert_eq!(resumed.as_deref(), Some(r#"{"revoked":true}"#));
    assert_eq!((rig.credentials.inner.value(), rig.server.executed().len()), (None, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_logout_whose_credential_could_not_be_cleared_is_finished_by_resume_through_the_bridge() {
    let rig = Rig::new();
    let database = rig.open(None, pin()).unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.credentials.fail_next_clear.store(true, Ordering::SeqCst);

    database.session_call("logout".into(), "{}".into()).await.unwrap_err();
    let resumed = database.session_resume().await.unwrap();

    assert_eq!(resumed.as_deref(), Some(r#"{"revoked":true}"#));
    assert_eq!((rig.credentials.inner.value(), rig.server.executed().len()), (None, 2));
}
