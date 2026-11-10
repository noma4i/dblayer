use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::encrypted_tests::{code, failure, SubscriptionsOnly, ThroughServer};
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, SessionCode};
use crate::runtime::SystemClock;
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64, short_key_pin};
use crate::transport::{CredentialSlot as CoreCredentials};

pub(in super::super) const SOCIAL_BODY: &str = r#"{"provider":"google","idToken":"t"}"#;

#[test]
fn life_session_status_published_table() {
    use crate::ffi::{read::status_of_published, SessionStanding as Standing};
    use crate::owners::contract::{Published, PublishedState as State};
    for (state, closed, present, absent) in [
        (State::NoSession, false, Some((Standing::None, None)), Some((Standing::None, None))),
        (State::Session, false, Some((Standing::Active, Some(19))), Some((Standing::None, None))),
        (State::Resetting, false, Some((Standing::Active, Some(19))), Some((Standing::None, None))),
        (State::StorageFailed, false, Some((Standing::Active, Some(19))), Some((Standing::None, None))),
        (State::Ending, false, Some((Standing::Ending, Some(19))), Some((Standing::Ending, None))),
        (State::EndingFailed, false, Some((Standing::Ending, Some(19))), Some((Standing::Ending, None))),
        (State::Closing, false, None, None),
        (State::NoSession, true, None, None),
    ] {
        for (identity, expected) in [(Some(19), present), (None, absent)] {
            let published = Published { epoch: 23, identity, generation: 0, state, closed };
            let result = status_of_published(&published);
            match expected {
                Some((phase, identity)) => {
                    let status = result.unwrap();
                    assert_eq!((status.phase, status.identity, status.epoch), (phase, identity, 23), "{state:?}");
                    assert!(status.phase != Standing::Active || status.identity.is_some());
                }
                None => assert!(matches!(result, Err(DbError::Closed { .. })), "{state:?}: {result:?}"),
            }
        }
    }
}

#[derive(Default)]
pub(in super::super) struct WatchedHeld {
    pub(in super::super) inner: MemoryCredentials,
    sets: AtomicUsize,
    clears: AtomicUsize,
    fail_next_clear: AtomicBool,
    fail_next_set: AtomicBool,
}

impl CredentialStore for WatchedHeld {
    fn get(&self) -> Result<Option<String>, DbError> { self.inner.get().map_err(failure) }

    fn set(&self, credential: String) -> Result<(), DbError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_set.swap(false, Ordering::SeqCst) { return Err(DbError::Storage { detail: "credential save refused".into() }); }
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

#[tokio::test(flavor = "multi_thread")]
async fn life_session_state_read() {
    use crate::ffi::SessionStanding;
    let rig = Rig::new();
    assert!(matches!(crate::ffi::OpenDatabase::new().opened(), Err(DbError::NotOpen { .. })));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let database = rig.open(Some(&path), pin()).await.unwrap();
    let before = rig.server.seen().len();
    let status = database.session_status().unwrap();
    assert_eq!((status.phase, status.identity, status.epoch), (SessionStanding::None, None, database.session_epoch()));
    assert_eq!(rig.server.seen().len(), before);
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    let before = rig.server.seen().len();
    let active = database.session_status().unwrap();
    assert_eq!(active.phase, SessionStanding::Active);
    assert!(active.identity.is_some());
    assert!(active.epoch > status.epoch);
    assert_eq!(rig.server.seen().len(), before);
    database.close_database().await.unwrap();
    assert!(matches!(database.session_status(), Err(DbError::Closed { .. })));
    drop(database);
    let offline = Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: None }, crate::ffi::EMPTY_SPECS.to_string(), crate::ffi::OpenPorts { transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    let before = rig.server.seen().len();
    let restored = offline.session_status().unwrap();
    assert_eq!((restored.phase, restored.identity, restored.epoch), (SessionStanding::Active, active.identity, offline.session_epoch()));
    assert_eq!(rig.server.seen().len(), before);
    offline.session_reset().await.unwrap();
    let before = rig.server.seen().len();
    let reset = offline.session_status().unwrap();
    assert_eq!((reset.phase, reset.identity), (SessionStanding::None, None));
    assert!(reset.epoch > restored.epoch);
    assert_eq!(rig.server.seen().len(), before);
    offline.close_database().await.unwrap();
    let failed = rig.open(None, pin()).await.unwrap();
    let sets = rig.touched().0;
    rig.credentials.fail_next_set.store(true, Ordering::SeqCst);
    assert!(matches!(failed.session_call("social".into(), SOCIAL_BODY.into()).await, Err(DbError::Storage { .. })));
    assert_eq!(rig.touched().0, sets + 1);
    let before = rig.server.seen().len();
    let status = failed.session_status().unwrap();
    assert_eq!((status.phase, status.identity), (SessionStanding::None, None));
    assert_eq!(status.epoch, failed.session_epoch());
    assert_eq!(rig.server.seen().len(), before);
    failed.close_database().await.unwrap();
}

pub(in super::super) struct Rig {
    pub(in super::super) server: Arc<FakeServer>,
    pub(in super::super) credentials: Arc<WatchedHeld>,
}

impl Rig {
    pub(in super::super) fn new() -> Self {
        Self { server: FakeServer::new(Arc::new(SystemClock)), credentials: Arc::default() }
    }

    pub(in super::super) async fn open(&self, path: Option<&Path>, pin: &str) -> Result<Arc<Database>, DbError> { self.open_models(path, pin, crate::ffi::EMPTY_SPECS.to_string()).await }

    async fn open_models(&self, path: Option<&Path>, pin: &str, models: String) -> Result<Arc<Database>, DbError> {
        let database = Database::open(Config { path: path.map(|path| path.to_string_lossy().into_owned()), data_version: None }, models, crate::ffi::OpenPorts { transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(self.credentials.clone()), http: Some(Arc::new(ThroughServer(self.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await?;
        database.set_online(true)?;
        Ok(database)
    }

    pub(in super::super) fn touched(&self) -> (usize, usize) {
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

            if expected == SessionCode::Unauthenticated {
                assert!(matches!(refused, DbError::Unauthorized { .. }), "{wire} confirmed={confirmed}: {refused:?}");
            } else if expected == SessionCode::Other {
                assert!(matches!(&refused, DbError::Graphql { messages, .. } if messages == &[wire.to_string()]), "{wire} confirmed={confirmed}: an unknown name is a GraphQL error, got {refused:?}");
            } else {
                assert_eq!(code(refused), expected, "{wire} confirmed={confirmed}");
            }
            assert_eq!((rig.credentials.inner.value(), rig.touched()), (credential, (1, 0)), "{wire} confirmed={confirmed}");
        }
    }
}

/// P-LIFE-07, executor `ClearAccount`. Observable: the `CredentialStore` of the app (value and `clear` calls) and `encrypted_session`. `SessionReset` and `ServerEnded` clear the secret once, `IdentityChanged`, `Close` and `ResetModels` clear nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_credential_secret_is_cleared_only_by_session_reset_and_server_end() {
    use crate::test_wait::{within, settled};
    for end in ["identity_changed", "close", "reset_models", "session_reset", "server_ended"] {
        let rig = Rig::new();
        let database = rig.open_models(None, pin(), crate::ffi::specs(super::lifecycle_tests::models())).await.unwrap();
        within(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        let credential = rig.credentials.inner.value();
        assert!(credential.is_some() && database.signed_in_digest().is_some() && rig.touched() == (1, 0), "{end}: signed in");
        let life = database.life_handle();
        let epoch = life.published().epoch;
        match end {
            "identity_changed" => {
                life.credential_committed(life.published().identity.expect("an identity") + 1);
                settled(&database, "the next epoch was published", || life.published().epoch > epoch).await;
            }
            "close" => within(database.close_database()).await.unwrap(),
            "reset_models" => {
                let outcome = within(life.reset_models(["User".to_string()].into()).wait()).await;
                assert!(matches!(outcome, crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Life(Ok(())))), "{outcome:?}");
            }
            "session_reset" => within(database.session_reset()).await.unwrap(),
            _ => { within(database.session_call("logout".into(), "{}".into())).await.unwrap(); }
        }
        if matches!(end, "session_reset" | "server_ended") {
            assert_eq!((rig.credentials.inner.value(), rig.touched(), database.signed_in_digest()), (None, (1, 1), None), "{end}: the secret and the session are erased");
        } else {
            let session_kept = end == "close" || database.signed_in_digest().is_some();
            assert_eq!((rig.credentials.inner.value(), rig.touched(), session_kept), (credential, (1, 0), true), "{end}: the secret and the session stay");
        }
        database.close_now();
    }
}

