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
    pub(super) retries: Arc<super::RetryProgress>,
}

impl Rig {
    pub(super) fn new() -> Self {
        Self { server: FakeServer::new(Arc::new(SystemClock)), credentials: Arc::default(), retries: super::RetryProgress::new() }
    }

    pub(super) async fn open(&self, path: Option<&Path>, pin: &str) -> Result<Arc<Database>, DbError> {
        Database::open(Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 }, "[]".to_string(), Some(Arc::new(SubscriptionsOnly)), Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() }), Some(self.credentials.clone()), Some(Arc::new(ThroughServer(self.server.clone()))), None, Some(self.retries.clone())).await
    }

    pub(super) fn touched(&self) -> (usize, usize) {
        (self.credentials.sets.load(Ordering::SeqCst), self.credentials.clears.load(Ordering::SeqCst))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pin_mismatch_at_open_and_in_the_config_never_touches_the_credential_store() {
    let rig = Rig::new();
    assert!(matches!(rig.open(None, short_key_pin()).await, Err(DbError::Invalid { .. })));
    assert_eq!(rig.touched(), (0, 0));
    let database = rig.open(None, pin()).await.unwrap();
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
        ("INVALID_CODE", SessionCode::InvalidCode),
        ("BLOCKED", SessionCode::Other),
    ];
    for (wire, expected) in table {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let database = rig.open(None, pin()).await.unwrap();
            database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
            let credential = rig.credentials.inner.value();
            rig.server.refuse_next(wire, confirmed);

            let refused = database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();

            assert_eq!(code(refused), expected, "{wire} confirmed={confirmed}");
            assert_eq!((rig.credentials.inner.value(), rig.touched()), (credential, (1, 0)), "{wire} confirmed={confirmed}");
        }
    }
}

