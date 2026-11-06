use std::sync::Arc;

use async_trait::async_trait;

use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, EncryptedReply, SessionCode, Transport};
use crate::pacing::SystemClock;
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{CredentialSlot as CoreCredentials, HttpExchange, TransportError};
use crate::test_wait::recv_within;

pub(super) struct ThroughServer(pub(super) Arc<FakeServer>);

pub(super) fn failure(error: TransportError) -> DbError {
    match error {
        TransportError::Interrupted(detail) => DbError::Interrupted { detail },
        error => DbError::Network { detail: error.to_string() },
    }
}

#[async_trait]
impl EncryptedHttp for ThroughServer {
    async fn post(&self, path: String, body: String, purpose: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let reply = self.0.post(&path, body, purpose).await.map_err(failure)?;
        Ok(EncryptedReply { status: reply.status, body: reply.body })
    }
}

pub(super) struct Held(pub(super) Arc<MemoryCredentials>);

impl CredentialStore for Held {
    fn get(&self) -> Result<Option<String>, DbError> { self.0.get().map_err(failure) }
    fn set(&self, credential: String) -> Result<(), DbError> { self.0.set(&credential).map_err(failure) }
    fn clear(&self) -> Result<(), DbError> { self.0.clear().map_err(failure) }
}

pub(super) struct SubscriptionsOnly;

#[async_trait]
impl Transport for SubscriptionsOnly {
    async fn open_cable(&self, _: Arc<super::CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

pub(super) async fn rig(server_key: &str, pin: &str) -> (Arc<Database>, Arc<FakeServer>, Arc<MemoryCredentials>) {
    let server = FakeServer::new(Arc::new(SystemClock));
    let credentials = Arc::new(MemoryCredentials::default());
    let encrypted = EncryptedConfig { origin: origin().to_string(), server_key: server_key.to_string(), pin: pin.to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".to_string(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(ThroughServer(server.clone()))), None).await
    .unwrap();
    database.set_online(true).unwrap();
    (database, server, credentials)
}

pub(super) fn code(error: DbError) -> SessionCode {
    let DbError::Session { code, .. } = error else { panic!("expected a session error, got {error:?}") };
    code
}

pub(super) async fn cancel_on_retry(retries: &super::RetryProgress, call: impl std::future::Future) {
    let mut waiting = retries.inner.watch();
    {
        let mut call = Box::pin(call);
        let deadline = super::database::executor().unwrap().spawn(async { crate::test_wait::pass(std::time::Duration::from_secs(10)).await });
        tokio::select! { _ = waiting.wait_for(|snapshot| snapshot.active_calls > 0) => {}, _ = &mut call => {}, _ = deadline => panic!("the retry was not observed") }
        assert!(waiting.borrow().active_calls > 0, "call completed before retry");
    }
}

#[test]
fn cancel_on_retry_waits_for_a_retry_that_the_executor_reaches_late() { crate::test_wait::paused(async {
    use crate::transport::retry::Retrying;
    let retries = super::RetryProgress::new();
    let progress = retries.inner.clone();
    let clock = crate::pacing::ManualClock::new(std::time::Duration::ZERO);
    let online = crate::transport::Connectivity::new(true);
    let call = async {
        super::database::executor().unwrap().spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(300))).await.unwrap();
        let _ = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http).failed(TransportError::Network("down".into())).await;
    };
    cancel_on_retry(&retries, call).await;
}) }

#[tokio::test]
async fn dropping_the_bootstrap_call_cancels_the_inflight_http_future() {
    struct Waiting {
        started: tokio::sync::Notify,
        stopped: Arc<tokio::sync::Notify>,
    }
    struct Stopped(Arc<tokio::sync::Notify>);
    impl Drop for Stopped { fn drop(&mut self) { self.0.notify_one(); } }
    #[async_trait]
    impl EncryptedHttp for Waiting {
        async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
            let _stopped = Stopped(self.stopped.clone());
            self.started.notify_one();
            std::future::pending().await
        }
    }
    let stopped = Arc::new(tokio::sync::Notify::new());
    let http = Arc::new(Waiting { started: tokio::sync::Notify::new(), stopped: stopped.clone() });
    let config = crate::transport::EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::protocol_tests::config() };
    let mut call = Box::pin(super::fetch_server_config(config, crate::transport::AppPlatform::Ios, "1.2".into(), None, http.clone(), None, crate::transport::Connectivity::new(true)));
    crate::test_wait::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! { _ = http.started.notified() => {}, result = &mut call => panic!("unexpected completion {:?}", result.map(|_| ())) }
    }).await.unwrap();
    drop(call);
    crate::test_wait::timeout(std::time::Duration::from_secs(2), stopped.notified()).await.expect("dropping the foreign call drops HTTP");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sessionless_operation_posts_all_fields_without_a_session() {
    struct TicketPost;

    let body = r#"{"name":"Alex","contact":"alex@example.com","message":"Need assistance","step":"phone","device":"iPhone","os":"iOS","appVersion":"3.0","country":"AU"}"#;

    #[async_trait]
    impl EncryptedHttp for TicketPost {
        async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
            use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response};

            assert_eq!(path, "/api/ticket");
            let request = decrypt_request(&body).unwrap();
            assert_eq!(request["body"], serde_json::json!({"name":"Alex","contact":"alex@example.com","message":"Need assistance","step":"phone","device":"iPhone","os":"iOS","appVersion":"3.0","country":"AU"}));
            Ok(EncryptedReply { status: 200, body: seal_response(&request, None, serde_json::json!({})) })
        }
    }

    let mut encrypted = EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() };
    encrypted.endpoints.session_operations = [("ticket".into(), super::EncryptedSessionOperation { path: "/api/ticket".into(), class: crate::transport::EncryptedOperationClass::Sessionless })].into();
    let credentials = Arc::new(MemoryCredentials::default());
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".into(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(TicketPost)), None).await.unwrap();
    database.set_online(true).unwrap();
    assert!(credentials.value().is_none());

    database.session_call("ticket".into(), body.into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sequence_conflict_is_a_typed_code_and_the_credential_stays() {
    let (database, server, credentials) = rig(public_key_der_b64(), pin()).await;
    database.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into()).await.unwrap();
    let credential = credentials.value().unwrap();
    server.rewind_sequence(&credential, 5);

    let conflict = database.session_call("confirm".into(), r#"{"code":"1"}"#.into()).await.unwrap_err();

    assert_eq!(code(conflict), SessionCode::SequenceConflict);
    assert_eq!(credentials.value(), Some(credential));
}

#[tokio::test(flavor = "multi_thread")]
async fn sessionless_calls_leave_no_session_state_even_when_refused() {
    for status in [200, 400] {
        struct Reply(u16);
        #[async_trait]
        impl EncryptedHttp for Reply {
            async fn post(&self, path: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
                let request = crate::transport::encrypted::fake_server_tests::decrypt_request(&body).unwrap();
                let status = if path == "/api/ticket" { self.0 } else { 200 };
                Ok(EncryptedReply { status, body: if status == 200 {
                    crate::transport::encrypted::fake_server_tests::seal_response(&request, request["sequence"].as_i64(), serde_json::json!({}))
                } else { String::new() } })
            }
        }
        let mut encrypted = crate::transport::encrypted::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("ticket".into(), super::EncryptedSessionOperation { path: "/api/ticket".into(), class: crate::transport::EncryptedOperationClass::Sessionless });
        let credentials = Arc::new(MemoryCredentials::default());
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, "[]".into(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(credentials.clone()))), Some(Arc::new(Reply(status))), None).await.unwrap();
        database.set_online(true).unwrap();
        let result = database.session_call("ticket".into(), "{}".into()).await;
        if status == 200 { assert_eq!(result.unwrap(), "{}"); }
        else { assert!(matches!(result, Err(DbError::Network { .. })), "{result:?}"); }
        let second = database.session_call("ticket".into(), "{}".into()).await;
        if status == 200 { assert_eq!(second.unwrap(), "{}"); }
        else { assert!(matches!(second, Err(DbError::Network { .. })), "{second:?}"); }
        {
            let inner = database.db.shared.store.lock();
            let counts: (i64, i64) = inner.connection.query_row("SELECT (SELECT count(*) FROM encrypted_session), (SELECT count(*) FROM encrypted_pending)", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
            assert_eq!(counts, (0, 0), "status {status}");
            assert_eq!(credentials.value(), None);
        }
        assert_eq!(database.session_call("social".into(), "{}".into()).await.unwrap(), "{}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn retry_progress_delivers_the_current_snapshot_to_each_reader_and_a_slow_reader_delays_no_request() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::watch;
    use crate::transport::RetrySnapshot;

    struct Failing(watch::Sender<usize>);
    #[async_trait]
    impl EncryptedHttp for Failing {
        async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
            self.0.send_modify(|posts| *posts += 1);
            Ok(EncryptedReply { status: 503, body: String::new() })
        }
    }
    struct Tap {
        seen: watch::Sender<Option<RetrySnapshot>>,
        deliveries: AtomicUsize,
        gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }
    impl super::RetryProgressListener for Tap {
        fn changed(&self, snapshot: RetrySnapshot) {
            self.deliveries.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate { recv_within(&gate); }
            self.seen.send_replace(Some(snapshot));
        }
    }
    fn tap(gate: Option<std::sync::mpsc::Receiver<()>>) -> (Arc<Tap>, watch::Receiver<Option<RetrySnapshot>>) {
        let (seen, receiver) = watch::channel(None);
        (Arc::new(Tap { seen, deliveries: AtomicUsize::new(0), gate: Mutex::new(gate) }), receiver)
    }
    async fn reaches(seen: &mut watch::Receiver<Option<RetrySnapshot>>, expected: impl Fn(&RetrySnapshot) -> bool) {
        crate::test_wait::timeout(std::time::Duration::from_secs(5), seen.wait_for(|snapshot| snapshot.as_ref().is_some_and(&expected))).await.expect("snapshot delivered").unwrap();
    }

    let (posted, mut posts) = watch::channel(0);
    let http = Arc::new(Failing(posted));
    let config = crate::transport::EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::protocol_tests::config() };
    let retries = super::RetryProgress::new();
    let (release, gate) = std::sync::mpsc::channel();
    let (slow, mut slow_seen) = tap(Some(gate));
    let slow_subscription = retries.subscribe(slow.clone()).unwrap();
    let call = tokio::spawn({
        let retries = retries.clone();
        async move { super::fetch_server_config(config, crate::transport::AppPlatform::Ios, "1.2".into(), None, http, Some(retries), crate::transport::Connectivity::new(true)).await }
    });

    crate::test_wait::timeout(std::time::Duration::from_secs(5), posts.wait_for(|posts| *posts >= 2)).await.expect("the request retried while the first delivery blocked").unwrap();
    assert!(slow_seen.borrow().is_none(), "the blocked reader has not seen anything yet");
    assert_eq!(retries.current().active_calls, 1);

    let (late, mut late_seen) = tap(None);
    let late_subscription = retries.subscribe(late.clone()).unwrap();
    reaches(&mut late_seen, |snapshot| snapshot.active_calls == 1 && snapshot.last_attempt.as_ref().is_some_and(|attempt| attempt.reason == "HTTP 503")).await;

    release.send(()).unwrap();
    reaches(&mut slow_seen, |snapshot| snapshot.active_calls == 1).await;
    slow_subscription.cancel();
    let delivered = slow.deliveries.load(Ordering::SeqCst);

    call.abort();
    reaches(&mut late_seen, |snapshot| snapshot.active_calls == 0 && snapshot.last_attempt.is_none()).await;
    assert_eq!(slow.deliveries.load(Ordering::SeqCst), delivered, "a cancelled reader hears nothing more, the other reader kept going");
    drop(late_subscription);
}

const HOLD_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

fn hold_before_the_call(call_state: &crate::db::CallState) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (reached, at_the_call) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    *call_state.before_enter.lock().unwrap() = Some(Box::new(move || {
        reached.send(()).unwrap();
        released.recv_timeout(HOLD_WAIT).unwrap();
    }));
    (at_the_call, release)
}

#[tokio::test(flavor = "multi_thread")]
async fn inv_observe_08_a_retry_progress_call_that_passed_the_cancel_check_does_not_start_after_the_cancel_returns() {
    use crate::transport::retry::Retrying;
    struct Tap(std::sync::Mutex<std::sync::mpsc::Sender<()>>);
    impl super::RetryProgressListener for Tap {
        fn changed(&self, _: crate::transport::RetrySnapshot) {
            self.0.lock().unwrap().send(()).unwrap();
        }
    }
    let (heard, deliveries) = std::sync::mpsc::channel();
    let tap = Arc::new(Tap(std::sync::Mutex::new(heard)));
    let retries = super::RetryProgress::new();
    let subscription = retries.subscribe(tap.clone()).unwrap();
    deliveries.recv_timeout(HOLD_WAIT).unwrap();
    let (at_the_call, release) = hold_before_the_call(&subscription.calls);
    let progress = retries.inner.clone();
    let retrying = tokio::spawn(async move {
        let clock = crate::pacing::ManualClock::new(std::time::Duration::ZERO);
        let online = crate::transport::Connectivity::new(true);
        let _ = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http).failed(TransportError::Network("down".into())).await;
    });
    at_the_call.recv_timeout(HOLD_WAIT).unwrap();
    subscription.cancel();
    release.send(()).unwrap();
    let deadline = std::time::Instant::now() + HOLD_WAIT;
    while Arc::strong_count(&tap) > 1 {
        assert!(std::time::Instant::now() < deadline, "the blocked delivery did not end");
        tokio::task::yield_now().await;
    }
    assert!(deliveries.try_recv().is_err());
    retrying.abort();
}

/// Requirement: a call that the close ended answers `Closed`, a refused sign-in over a session answers `AlreadyAuthenticated`, a 401 stays `Http 401`.
/// Observable: the `DbError` of the session failure. Assert: the variant of each failure.
#[test]
fn a_session_failure_keeps_its_own_error() {
    use crate::owners::session::SessionError as Failed;
    assert!(matches!(super::encrypted::session_failure(Failed::Closed), DbError::Closed { .. }));
    assert!(matches!(super::encrypted::session_failure(Failed::SessionExists), DbError::Session { code: SessionCode::AlreadyAuthenticated, .. }));
    assert!(matches!(super::encrypted::session_failure(Failed::Unauthorized), DbError::Http { status: 401, .. }));
}
