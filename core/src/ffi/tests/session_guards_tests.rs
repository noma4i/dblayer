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
    pub(in super::super) fail_next_set: AtomicBool,
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
    let offline = Database::open(Config { path: Some(path.to_string_lossy().into_owned()) }, crate::ffi::EMPTY_SPECS.to_string(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
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
        let database = Database::open(Config { path: path.map(|path| path.to_string_lossy().into_owned()) }, models, crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin.to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(self.credentials.clone()), http: Some(Arc::new(ThroughServer(self.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await?;
        database.set_online(true).await?;
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

/// P-LIFE-07, executor `ClearAccount`. Observable: the `CredentialStore` of the app (value and `clear` calls) and `encrypted_session`. `SessionReset` and `ServerEnded` clear the secret once, `IdentityChanged` and `Close` clear nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_credential_secret_is_cleared_only_by_session_reset_and_server_end() {
    use crate::test_wait::{within, settled};
    for end in ["identity_changed", "close", "session_reset", "server_ended"] {
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


struct ReleaseSql(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl Drop for ReleaseSql {
    fn drop(&mut self) {
        let (lock, ready) = &*self.0;
        *lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        ready.notify_one();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_login_beside_a_session_end_after_a_race_a_logout_and_a_close() {
    use crate::test_wait::within;
    use crate::transport::{EncryptedOperationClass, EncryptedSessionOperation};
    for order in ["concurrent", "after", "closed", "revoked"] {
        let rig = Rig::new();
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("first_login".into(), EncryptedSessionOperation { path: "/api/auth/social".into(), class: EncryptedOperationClass::Login });
        let database = within(Database::open(Config { path: None }, crate::ffi::EMPTY_SPECS.into(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(encrypted), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() })).await.unwrap();
        database.set_online(true).await.unwrap();
        within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await.unwrap();
        let original = rig.credentials.inner.value().unwrap();
        let identity = database.session_status().unwrap().identity;
        match order {
            "concurrent" => {
                let (login, reset) = within(async { tokio::join!(database.session_call("first_login".into(), SOCIAL_BODY.into()), database.session_reset()) }).await;
                reset.unwrap();
                let secret = rig.credentials.inner.value();
                let row = crate::store::session_state::session(&database.db.lock().connection).unwrap();
                match login {
                    Ok(_) => {
                        let secret = secret.expect("successful login retained credential");
                        let row = row.expect("successful login retained digest");
                        assert_ne!(secret, original);
                        assert_eq!(row.credential_digest, crate::transport::encrypted::exchange::digest_of(&secret));
                    }
                    Err(_) => assert!(secret.is_none() && row.is_none(), "refused login retained a credential"),
                }
            }
            "after" => {
                within(database.session_reset()).await.unwrap();
                within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await.unwrap();
                let secret = rig.credentials.inner.value().unwrap();
                let row = crate::store::session_state::session(&database.db.lock().connection).unwrap().unwrap();
                assert_ne!(secret, original);
                assert_ne!(database.session_status().unwrap().identity, identity);
                assert_eq!(row.credential_digest, crate::transport::encrypted::exchange::digest_of(&secret));
            }
            "closed" => {
                within(database.close_database()).await.unwrap();
                assert!(matches!(within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await, Err(DbError::Closed { .. })));
                continue;
            }
            "revoked" => {
                within(database.session_call("logout".into(), "{}".into())).await.unwrap();
                within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await.unwrap();
                let secret = rig.credentials.inner.value().unwrap();
                let row = crate::store::session_state::session(&database.db.lock().connection).unwrap().unwrap();
                assert_eq!(row.credential_digest, crate::transport::encrypted::exchange::digest_of(&secret));
            }
            _ => unreachable!(),
        }
        within(database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_login_during_session_end_orders() {
    use crate::ffi::SessionStanding;
    use crate::test_wait::within;
    for order in ["active", "ending", "failed"] {
        let rig = Rig::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.sqlite");
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("first_login".into(), crate::transport::EncryptedSessionOperation { path: "/api/auth/social".into(), class: crate::transport::EncryptedOperationClass::Login });
        let database = Database::open(Config { path: Some(path.to_string_lossy().into_owned()) }, crate::ffi::specs(super::lifecycle_tests::models()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(encrypted), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
        database.set_online(true).await.unwrap();
        within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await.unwrap();
        crate::store::fixtures::put(&database.db, "User", serde_json::json!({"id":"a", "name":"A"}));
        assert_eq!(database.db.row_count("User").unwrap(), 1);
        let original = rig.credentials.inner.value().unwrap();
        let identity = database.session_status().unwrap().identity;
        let before = rig.server.seen().len();
        for counter in &crate::owners::lifecycle::LOGIN_ENDING_INPUTS { counter.store(0, Ordering::SeqCst); }
        if order == "active" {
            let answer = within(database.session_call("first_login".into(), SOCIAL_BODY.into())).await;
            assert!(matches!(answer, Err(DbError::Session { code: SessionCode::AlreadyAuthenticated, .. })), "{answer:?}");
            assert_eq!(rig.server.seen().len(), before);
            assert_eq!(rig.credentials.inner.value().as_deref(), Some(original.as_str()));
            assert_eq!(database.session_status().unwrap().identity, identity);
            assert_eq!(database.db.row_count("User").unwrap(), 1);
        } else {
            if order == "failed" {
                database.db.lock().connection.execute_batch("CREATE TRIGGER refuse_session_erase BEFORE DELETE ON encrypted_session BEGIN SELECT RAISE(ABORT, 'refuse erase'); END;").unwrap();
            }
            let entered = Arc::new(tokio::sync::Notify::new());
            let latch = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let _release = ReleaseSql(latch.clone());
            let once = Arc::new(AtomicBool::new(false));
            let (seen, wait, first, life) = (entered.clone(), latch.clone(), once, database.life.clone());
            database.db.lock().connection.progress_handler(1, Some(move || {
                if life.published().state == crate::owners::contract::PublishedState::Ending && !first.swap(true, Ordering::SeqCst) {
                    seen.notify_one();
                    let (lock, ready) = &*wait;
                    let mut released = lock.lock().unwrap();
                    while !*released { released = ready.wait(released).unwrap(); }
                }
                false
            })).unwrap();
            let reset_db = database.clone();
            let reset = tokio::spawn(async move { reset_db.session_reset().await });
            within(entered.notified()).await;
            assert_eq!(database.session_status().unwrap().phase, SessionStanding::Ending);
            let login_db = database.clone();
            let (started, accepted) = tokio::sync::oneshot::channel();
            let login = tokio::spawn(async move { let _ = started.send(()); login_db.session_call("first_login".into(), SOCIAL_BODY.into()).await });
            within(accepted).await.unwrap();
            assert!(!login.is_finished());
            assert_eq!(rig.server.seen().len(), before);
            let (lock, ready) = &*latch;
            *lock.lock().unwrap() = true;
            ready.notify_one();
            let reset = within(reset).await.unwrap();
            let answer = within(login).await.unwrap();
            database.db.lock().connection.progress_handler(0, None::<fn() -> bool>).unwrap();
            if order == "failed" {
                assert!(matches!(reset, Err(DbError::Storage { .. })));
                assert!(matches!(answer, Err(DbError::Storage { .. })), "{answer:?}");
                assert_eq!(rig.server.seen().len(), before);
                assert_eq!(rig.credentials.inner.value().as_deref(), Some(original.as_str()));
                assert_eq!(database.db.row_count("User").unwrap(), 1);
                assert_eq!(database.signed_in_digest(), Some(crate::transport::encrypted::exchange::digest_of(&original)));
                assert_eq!(database.session_status().unwrap().phase, SessionStanding::Ending);
                database.db.lock().connection.execute_batch("DROP TRIGGER refuse_session_erase").unwrap();
            } else {
                reset.unwrap();
                let secret = rig.credentials.inner.value();
                let digest = database.signed_in_digest();
                let status = database.session_status().unwrap();
                match answer {
                    Ok(_) => {
                        let secret = secret.unwrap();
                        assert_ne!(secret, original);
                        assert_eq!(digest, Some(crate::transport::encrypted::exchange::digest_of(&secret)));
                        assert_eq!(status.phase, SessionStanding::Active);
                        assert!(crate::store::session_state::session(&database.db.lock().connection).unwrap().unwrap().sequence >= 1);
                        assert_eq!(database.db.row_count("User").unwrap(), 0);
                    }
                    Err(_) => {
                        assert_eq!(status.phase, SessionStanding::None);
                        assert_eq!(digest, None);
                        assert_eq!(database.db.row_count("User").unwrap(), 0);
                    }
                }
            }
        }
        let counts: Vec<_> = crate::owners::lifecycle::LOGIN_ENDING_INPUTS.iter().map(|counter| counter.load(Ordering::SeqCst)).collect();
        assert_eq!(counts, [0, 0], "{order}: Lifecycle received a different identity");
        within(database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_login_after_orphan_secret() {
    use crate::test_wait::within;
    for next in ["login", "reopen"] {
        let rig = Rig::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.sqlite");
        let database = rig.open(Some(&path), pin()).await.unwrap();
        within(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        let orphan = rig.credentials.inner.value().unwrap();
        rig.credentials.fail_next_clear.store(true, Ordering::SeqCst);
        within(database.session_reset()).await.unwrap();
        assert_eq!(database.signed_in_digest(), None);
        assert_eq!(rig.credentials.inner.value().as_deref(), Some(orphan.as_str()));
        if next == "login" {
            within(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
            let secret = rig.credentials.inner.value().unwrap();
            assert_ne!(secret, orphan);
            assert_eq!(database.signed_in_digest(), Some(crate::transport::encrypted::exchange::digest_of(&secret)));
            within(database.close_database()).await.unwrap();
        } else {
            within(database.close_database()).await.unwrap();
            drop(database);
            let reopened = rig.open(Some(&path), pin()).await.unwrap();
            assert_eq!(rig.credentials.inner.value(), None);
            assert_eq!(reopened.signed_in_digest(), None);
            within(reopened.close_database()).await.unwrap();
        }
    }
}
