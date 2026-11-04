use std::sync::{Arc, Mutex};
use std::time::Duration;
use serde_json::{json, Value};
use super::*;
use super::fake_server_tests::{decrypt_request, seal_response, FakeServer, MemoryCredentials};
use crate::{db::Db, pacing::TestClock};

struct Fault {
    error: Mutex<Option<Result<HttpReply, TransportError>>>,
    code: Option<(&'static str, bool)>,
    seen: Mutex<Vec<Value>>,
    server: Arc<FakeServer>,
}

#[async_trait::async_trait]
impl HttpExchange for Fault {
    async fn post(&self, path: &str, token: String, _: bool) -> Result<HttpReply, TransportError> {
        let request = decrypt_request(&token).unwrap();
        let first = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(request.clone());
            seen.len() == 1
        };
        if first {
            if let Some((code, confirmed)) = self.code {
                if confirmed {
                    let credential = request["credential"].as_str().unwrap();
                    self.server.clone().with_sequence(credential, request["sequence"].as_i64().unwrap());
                }
                return Ok(HttpReply { status: 200, body: seal_response(&request, if confirmed { request["sequence"].as_i64() } else { None }, json!({"error": code})) });
            }
            if let Some(error) = self.error.lock().unwrap().take() { return error; }
        }
        if request.get("credential").is_none() { return Ok(HttpReply { status: 200, body: seal_response(&request, None, json!({"ok": true})) }); }
        self.server.post(path, token, false).await
    }
}

fn rig(error: Option<Result<HttpReply, TransportError>>, code: Option<(&'static str, bool)>) -> (EncryptedClient, Arc<Fault>, Db) {
    rig_at(error, code, Arc::new(TestClock::new(Duration::from_secs(1_800_000_000))))
}

fn rig_at(error: Option<Result<HttpReply, TransportError>>, code: Option<(&'static str, bool)>, clock: Arc<dyn crate::pacing::Clock>) -> (EncryptedClient, Arc<Fault>, Db) {
    let http = Arc::new(Fault { error: Mutex::new(error), code, seen: Mutex::new(Vec::new()), server: FakeServer::new(clock.clone()) });
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let mut config = EncryptedConfig { origin: super::vectors_tests::origin().into(), server_key: super::vectors_tests::public_key_der_b64().into(), pin: super::vectors_tests::pin().into(), ..super::protocol_tests::config() };
    config.endpoints.session_operations.insert("ticket".into(), EncryptedSessionOperation { path: "/api/ticket".into(), sessionless: true });
    let client = EncryptedClient::new(&config, http.clone(), Arc::new(MemoryCredentials::default()), db.clone(), clock).unwrap();
    (client, http, db)
}

#[tokio::test]
async fn audit_offline_encrypted_http_makes_zero_attempts() {
    use std::{future::Future, task::{Context, Waker}};
    for failed_first in [false, true] {
        let clock = Arc::new(crate::pacing::ManualClock::new(Duration::from_secs(1_800_000_000)));
        let error = failed_first.then(|| Err(TransportError::Network("unavailable".into())));
        let (client, http, db) = rig_at(error, None, clock.clone());
        let client = Arc::new(client);
        let mut call = Box::pin(client.session_call("social", json!({})));
        if failed_first { assert!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending()); }
        db.connectivity().set_online(false);
        for _ in 0..3 {
            assert!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            clock.advance(Duration::from_secs(60));
            assert_eq!(http.seen.lock().unwrap().len(), usize::from(failed_first), "offline must not send HTTP");
        }
        assert!(super::state::pending(&db.shared.store.lock().connection).unwrap().is_some());
        db.connectivity().set_online(true);
        assert!(call.await.is_ok());
        let seen = http.seen.lock().unwrap();
        assert_eq!(seen.len(), usize::from(failed_first) + 1);
        if failed_first {
            for field in ["requestId", "responseKey", "sequence", "body"] { assert_eq!(seen[0][field], seen[1][field]); }
            assert_ne!(seen[0]["issuedAt"], seen[1]["issuedAt"]);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn audit_durable_upload_owns_the_failed_http_attempt() {
    use std::{future::Future, task::{Context, Poll, Waker}};
    let failures = [(Some(Err(TransportError::Network("unavailable".into()))), None), (Some(Err(TransportError::Http { status: 503, body: String::new() })), None)]
        .into_iter().chain([false, true].map(|confirmed| (None, Some(("TIMEOUT", confirmed)))));
    for (error, code) in failures {
    let (client, http, db) = rig(error, code);
    http.server.clone().with_sequence(&client.credential().unwrap(), 0);
    let owner = crate::transport::RequestOwner { seq: 1, item_position: 0, stage: "create".into() };
    let mut call = Box::pin(client.mutate_owned("/api/graphql", json!({}), Some(owner), false));
    assert!(matches!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(_))), "durable owner must receive the failure before another HTTP attempt");
    drop(call);
    assert_eq!(http.seen.lock().unwrap().len(), 1);
    assert!(super::state::pending(&db.shared.store.lock().connection).unwrap().is_some());
    client.resume().await.unwrap().unwrap();
    let seen = http.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    let confirmed = code.is_some_and(|(_, confirmed)| confirmed);
    for field in ["requestId", "responseKey", "sequence"] { assert_eq!(seen[0][field] == seen[1][field], !confirmed, "{field}"); }
    assert_eq!(seen[0]["body"], seen[1]["body"]);
    assert!(super::state::pending(&db.shared.store.lock().connection).unwrap().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn temporary_http_failures_retry_without_spending_sequence() {
    let errors = std::iter::once(TransportError::Network("timeout".into())).chain(std::iter::once(TransportError::Network("connection reset".into())))
        .chain([408, 429, 500, 502, 503, 599].map(|status| TransportError::Http { status, body: String::new() }));
    for error in errors {
      for route in ["social", "ticket", "read"] {
        let (client, http, db) = rig(Some(Err(error.clone())), None);
        let answer = if route == "read" {
            http.server.clone().with_sequence(&client.credential().unwrap(), 0);
            client.read("/api/graphql", json!({"query": "query { a }"})).await
        } else { client.session_call(route, json!({})).await };
        assert!(answer.is_ok(), "{error:?}: {answer:?}");
        let seen = http.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        for field in ["sequence", "body"] { assert_eq!(seen[0][field], seen[1][field], "{field}"); }
        for field in ["requestId", "responseKey"] { assert_eq!(seen[0][field] == seen[1][field], route != "ticket", "{route}: {field}"); }
        assert_eq!(super::state::session(&db.shared.store.lock().connection).unwrap().map(|session| session.sequence).unwrap_or(0), i64::from(route == "social"));
        assert_eq!(http.server.executed().len(), usize::from(route == "social"));
      }
    }
}

#[tokio::test(start_paused = true)]
async fn temporary_sealed_codes_preserve_unconfirmed_pending_and_advance_confirmed_sequence() {
    for code in ["TIMEOUT", "SERVICE_UNAVAILABLE", "INTERNAL_SERVER_ERROR", "DATABASE_ERROR"] {
        for confirmed in [false, true] {
            let (client, http, db) = rig(None, Some((code, confirmed)));
            let answer = client.session_call("social", json!({})).await.unwrap();
            assert!(answer.body.get("error").is_none(), "{code}, confirmed={confirmed}: {answer:?}");
            let seen = http.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0]["requestId"] == seen[1]["requestId"], !confirmed);
            assert_eq!(seen[1]["sequence"], if confirmed { 2 } else { 1 });
            assert_eq!(seen[0]["body"], seen[1]["body"]);
            assert_eq!(super::state::session(&db.shared.store.lock().connection).unwrap().unwrap().sequence, if confirmed { 2 } else { 1 });
        }
    }
}

#[tokio::test(start_paused = true)]
async fn fatal_http_and_security_failures_return_without_retrying() {
    let errors = [400, 401, 403, 404, 405, 413, 415, 422].map(|status| TransportError::Http { status, body: String::new() }).into_iter()
        .chain([TransportError::Security { code: SecurityCode::TlsTrust, detail: "certificate".into() }, TransportError::Config { code: ConfigCode::UnsupportedVersion, detail: "version".into() }]);
    for error in errors {
        let (client, http, _) = rig(Some(Err(error.clone())), None);
        assert_eq!(client.session_call("social", json!({})).await, Err(error));
        assert_eq!(http.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_the_call_during_backoff_stops_the_retry_and_preserves_pending() {
    use std::{future::Future, task::{Context, Waker}};
    let (client, http, db) = rig(Some(Err(TransportError::Network("offline".into()))), None);
    let mut call = Box::pin(client.session_call("social", json!({})));
    assert!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(call);
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(http.seen.lock().unwrap().len(), 1);
    assert!(super::state::pending(&db.shared.store.lock().connection).unwrap().is_some());
    assert!(client.resume().await.unwrap().is_some());
    assert_eq!(http.server.executed().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn local_credential_and_sqlite_failures_return_before_http_without_retrying() {
    use std::{future::Future, task::{Context, Poll, Waker}};
    struct BrokenCredentials(bool);
    impl CredentialSlot for BrokenCredentials {
        fn get(&self) -> Result<Option<String>, TransportError> { if self.0 { Err(TransportError::Network("credential read".into())) } else { Ok(None) } }
        fn set(&self, _: &str) -> Result<(), TransportError> { Err(TransportError::Network("credential write".into())) }
        fn clear(&self) -> Result<(), TransportError> { unreachable!() }
    }
    for case in ["credential read", "credential write", "sqlite"] {
        let (client, http, db) = rig(None, None);
        let credentials: Arc<dyn CredentialSlot> = if case == "sqlite" { Arc::new(MemoryCredentials::default()) } else { Arc::new(BrokenCredentials(case == "credential read")) };
        let client = EncryptedClient::new(&client.config, http.clone(), credentials, db.clone(), Arc::new(TestClock::default())).unwrap();
        if case == "sqlite" { db.shared.store.lock().connection.execute_batch("DROP TABLE encrypted_pending").unwrap(); }
        let mut call = Box::pin(client.session_call("social", json!({})));
        assert!(matches!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(TransportError::Network(_)))), "{case}");
        assert!(http.seen.lock().unwrap().is_empty(), "{case}");
    }
}
