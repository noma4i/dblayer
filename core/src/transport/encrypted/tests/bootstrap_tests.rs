use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;
use aes_gcm::{Aes256Gcm, Nonce, aead::{Aead, KeyInit, Payload}};
use base64::{Engine, engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}};
use rsa::{RsaPrivateKey, Oaep, pkcs1::DecodeRsaPrivateKey, pkcs8::EncodePublicKey};
use serde_json::{json, Value};
use sha1::Sha1;
use super::*;
use super::bootstrap::{fetch, request_plain, request_text, request_digest, accept};
use crate::runtime::ManualClock;

fn kat() -> Value { serde_json::from_str(include_str!("../../../../../fixtures/bootstrap-config/bootstrap_kat.json")).unwrap() }

struct ConfiguredPath;

#[tokio::test]
async fn life_bootstrap_waits_online() {
    use std::{future::Future, task::{Context, Poll, Waker}};
    struct Calls(AtomicUsize);
    #[async_trait::async_trait]
    impl HttpExchange for Calls {
        async fn post(&self, _: &str, _: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(HttpReply { status: 400, body: String::new() })
        }
    }
    let online = crate::transport::Connectivity::new(true);
    online.set_online(false);
    let clock = ManualClock::new(Duration::from_secs(1_800_000_000));
    let calls = Arc::new(Calls(AtomicUsize::new(0)));
    let config = configuration();
    let events = crate::transport::retry::RetryCalls::default();
    let mut call = Box::pin(super::bootstrap::fetch(&config, &super::bootstrap::AppRelease { platform: AppPlatform::Ios, version: "1.2", build: None }, calls.clone(), &clock, &events, &online));
    let result = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert_eq!(calls.0.load(Ordering::SeqCst), 0);
    assert!(result.is_pending());
    online.set_online(true);
    assert!(matches!(call.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(TransportError::Http { status: 400, .. }))));
    assert_eq!(calls.0.load(Ordering::SeqCst), 1);
}

#[async_trait::async_trait]
impl HttpExchange for ConfiguredPath {
    async fn post(&self, path: &str, _: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
        assert_eq!(path, "/custom/bootstrap");
        Ok(HttpReply { status: 400, body: String::new() })
    }
}

#[tokio::test]
async fn sends_bootstrap_to_the_application_path() {
    let mut config = configuration();
    config.endpoints.bootstrap = "/custom/bootstrap".into();
    let result = fetch_with(&config, AppPlatform::Ios, "1.2", None, Arc::new(ConfiguredPath), &ManualClock::new(Duration::from_secs(1))).await;
    assert!(matches!(result, Err(TransportError::Http { status: 400, .. })));
}
fn text<'a>(value: &'a Value, key: &str) -> &'a str { value[key].as_str().unwrap() }
pub(crate) fn configuration() -> EncryptedConfig {
    let k = kat();
    let private = RsaPrivateKey::from_pkcs1_pem(text(&k, "rsa_private_pem")).unwrap();
    let mut config = EncryptedConfig { origin: text(&k, "audience").into(), server_key: STANDARD.encode(private.to_public_key().to_public_key_der().unwrap().as_bytes()), pin: text(&k, "kid").into(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    config.endpoints.bootstrap = text(&k, "path").into();
    config
}
fn decrypt_request(token: &str) -> String {
    let k = kat();
    let private = RsaPrivateKey::from_pkcs1_pem(text(&k, "rsa_private_pem")).unwrap();
    let parts: Vec<_> = token.split('.').collect();
    let cek = private.decrypt(Oaep::new::<Sha1>(), &URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    let mut sealed = URL_SAFE_NO_PAD.decode(parts[3]).unwrap();
    sealed.extend(URL_SAFE_NO_PAD.decode(parts[4]).unwrap());
    String::from_utf8(Aes256Gcm::new_from_slice(&cek).unwrap().decrypt(Nonce::from_slice(&URL_SAFE_NO_PAD.decode(parts[2]).unwrap()), Payload { msg: &sealed, aad: parts[0].as_bytes() }).unwrap()).unwrap()
}
fn seal(value: &Value, key: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"dir","enc":"A256GCM"}"#);
    let iv = [7u8; 12];
    let encrypted = Aes256Gcm::new_from_slice(&URL_SAFE_NO_PAD.decode(key).unwrap()).unwrap().encrypt(Nonce::from_slice(&iv), Payload { msg: value.to_string().as_bytes(), aad: header.as_bytes() }).unwrap();
    let (body, tag) = encrypted.split_at(encrypted.len() - 16);
    format!("{header}..{}.{}.{}", URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(body), URL_SAFE_NO_PAD.encode(tag))
}

#[test]
fn published_request_and_client_request_have_the_same_plaintext_and_digest() {
    let k = kat();
    assert_eq!(decrypt_request(text(&k["request"], "token")), text(&k["request"], "plaintext"));
    let clock = ManualClock::new(Duration::from_secs(k["issued_at"].as_u64().unwrap()));
    let plain = request_plain(&configuration(), AppPlatform::Ios, "3.8.0", Some("120"), &clock, text(&k, "request_id"), text(&k, "response_key")).unwrap();
    assert_eq!(request_text(&plain), text(&k["request"], "plaintext"));
    assert_eq!(plain, serde_json::from_str::<Value>(text(&k["request"], "plaintext")).unwrap());
    assert_eq!(request_digest(&plain), text(&k, "request_digest"));
    let key = ServerKey::parse(&configuration().server_key, &configuration().pin).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&decrypt_request(&super::jwe::seal_plain(&key, request_text(&plain).as_bytes()).unwrap())).unwrap(), plain);
}

#[test]
fn published_response_opens_and_binds_to_the_request() {
    let k = kat();
    let opened = super::jwe::open_plain(text(&k["response"], "token"), text(&k, "response_key")).unwrap();
    assert_eq!(opened, serde_json::from_str::<Value>(text(&k["response"], "plaintext")).unwrap());
    let config = accept(text(&k["response"], "token"), text(&k, "response_key"), text(&k, "request_id"), text(&k, "request_digest")).unwrap();
    assert_eq!((config.config_version, config.protocol_version, config.min_app_version.as_str(), config.update_required, config.tracking_prompt_enabled), (1, 1, "3.7.2", false, true));
    assert!(config.store_url.unwrap().contains("apps.apple.com"));
}

#[test]
fn future_fields_are_accepted_and_invalid_config_is_typed() {
    let k = kat();
    let base: Value = serde_json::from_str(text(&k["response"], "plaintext")).unwrap();
    let read = |v: &Value| accept(&seal(v, text(&k, "response_key")), text(&k, "response_key"), text(&k, "request_id"), text(&k, "request_digest"));
    let mut extended = base.clone();
    extended["body"]["future"] = json!({"nested": true});
    extended["body"]["flags"] = json!({"future": [1, true]});
    extended["body"]["updateRequired"] = json!(true);
    extended["body"]["countries"] = json!([{"name":"Australia","code":"AU","flag":null,"phoneCode":"+61","phoneMask":null,"phoneFormatter":{"mobile":"04XX"},"enabled":true,"phoneAuthEnabled":false}]);
    let config = read(&extended).unwrap();
    assert!(config.update_required);
    assert_eq!(config.countries[0].phone_code.as_deref(), Some("+61"));
    for (field, value, code) in [("configVersion", json!(2), ConfigCode::UnsupportedVersion), ("updateRequired", json!("false"), ConfigCode::InvalidFormat), ("flags", json!([]), ConfigCode::InvalidFormat), ("countries", Value::Null, ConfigCode::InvalidFormat)] {
        let mut invalid = base.clone();
        invalid["body"][field] = value;
        assert!(matches!(read(&invalid), Err(TransportError::Config { code: actual, .. }) if actual == code), "{field}");
    }
    let mut refused = base;
    refused["body"] = json!({"error":"INVALID_REQUEST"});
    assert!(matches!(read(&refused), Err(TransportError::Config { code: ConfigCode::InvalidRequest, .. })));
}

struct Http { calls: AtomicUsize, status: u16 }

struct Reconnecting { calls: AtomicUsize, requests: std::sync::Mutex<Vec<Value>>, fault: &'static str }
#[async_trait::async_trait]
impl HttpExchange for Reconnecting {
    async fn post(&self, path: &str, token: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
        let plain: Value = serde_json::from_str(&decrypt_request(&token)).unwrap();
        self.requests.lock().unwrap().push(plain.clone());
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if self.fault == "network" { return Err(TransportError::Network("timeout".into())); }
            if let Ok(status) = self.fault.parse() { return Ok(HttpReply { status, body: String::new() }); }
            let response = json!({"requestId": plain["requestId"], "requestDigest": request_digest(&plain), "sequence": null, "body": {"error": self.fault}});
            return Ok(HttpReply { status: 200, body: seal(&response, text(&plain, "responseKey")) });
        }
        Http { calls: AtomicUsize::new(0), status: 200 }.post(path, token, crate::transport::NetworkPurpose::Standard, crate::transport::WireIds::bootstrap()).await
    }
}

#[test]
fn life_prop_bootstrap_bound() { crate::test_wait::paused(async {
  for fault in ["network", "408", "429", "500", "503", "599", "TIMEOUT", "SERVICE_UNAVAILABLE", "INTERNAL_SERVER_ERROR", "DATABASE_ERROR"] {
    let http = Arc::new(Reconnecting { calls: AtomicUsize::new(0), requests: std::sync::Mutex::new(Vec::new()), fault });
    let result = fetch_with(&configuration(), AppPlatform::Ios, "1.2", None, http.clone(), &crate::runtime::TestClock::new(Duration::from_secs(1_800_000_000))).await;
    assert!(result.is_ok(), "{result:?}");
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0]["requestId"], requests[1]["requestId"]);
    assert_ne!(requests[0]["responseKey"], requests[1]["responseKey"]);
    assert!(requests.iter().all(|request| request.get("credential").is_none() && request.get("sequence").is_none()));
  }
  let k = kat();
  let response: Value = serde_json::from_str(text(&k["response"], "plaintext")).unwrap();
  for field in ["requestId", "requestDigest", "sequence"] {
    let mut other = response.clone();
    other[field] = json!("other");
    assert!(matches!(accept(&seal(&other, text(&k, "response_key")), text(&k, "response_key"), text(&k, "request_id"), text(&k, "request_digest")), Err(TransportError::Security { code: SecurityCode::ResponseAuthentication, .. })), "{field}");
  }
  for (token, key) in [(text(&k["response"], "token"), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), ("broken", text(&k, "response_key"))] {
    assert!(matches!(accept(token, key, text(&k, "request_id"), text(&k, "request_digest")), Err(TransportError::Security { code: SecurityCode::ResponseAuthentication, .. })));
  }
}) }
#[async_trait::async_trait]
impl HttpExchange for Http {
    async fn post(&self, path: &str, token: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(path, text(&kat(), "path"));
        let plain: Value = serde_json::from_str(&decrypt_request(&token)).unwrap();
        assert!(plain.get("credential").is_none() && plain.get("sequence").is_none());
        if self.status != 200 { return Ok(HttpReply { status: self.status, body: String::new() }); }
        let k = kat();
        let mut response: Value = serde_json::from_str(text(&k["response"], "plaintext")).unwrap();
        response["requestId"] = plain["requestId"].clone();
        response["requestDigest"] = request_digest(&plain).into();
        Ok(HttpReply { status: 200, body: seal(&response, text(&plain, "responseKey")) })
    }
}

#[tokio::test]
async fn fetch_needs_no_database_validates_before_http_and_distinguishes_service_from_pin() {
    let clock = ManualClock::new(Duration::from_secs(1790000000));
    let http = Arc::new(Http { calls: AtomicUsize::new(0), status: 200 });
    let mut encrypted = configuration();
    encrypted.pin = "wrong".into();
    assert!(matches!(fetch_with(&encrypted, AppPlatform::Ios, "3.8.0", None, http.clone(), &clock).await, Err(TransportError::Security { code: SecurityCode::ServerKeyMismatch, .. })));
    for version in ["", "1", "1.2.3.4", "1..2", "1.2-beta", "1.2 ", "\u{0661}.\u{0662}"] {
        assert!(matches!(fetch_with(&configuration(), AppPlatform::Ios, version, None, http.clone(), &clock).await, Err(TransportError::Config { code: ConfigCode::InvalidRequest, .. })), "{version}");
    }
    assert!(matches!(fetch_with(&configuration(), AppPlatform::Ios, "1.2", Some(&"x".repeat(33)), http.clone(), &clock).await, Err(TransportError::Config { code: ConfigCode::InvalidRequest, .. })));
    assert_eq!(http.calls.load(Ordering::SeqCst), 0);
    for platform in [AppPlatform::Ios, AppPlatform::Android] {
        assert!(fetch_with(&configuration(), platform, "1.2", None, http.clone(), &clock).await.is_ok());
    }
    assert_eq!(http.calls.load(Ordering::SeqCst), 2);
    for status in [400, 413] {
        let http = Arc::new(Http { calls: AtomicUsize::new(0), status });
        assert!(matches!(fetch_with(&configuration(), AppPlatform::Ios, "1.2.3", None, http, &clock).await, Err(TransportError::Http { status: actual, .. }) if status == actual));
    }
}

async fn fetch_with(config: &EncryptedConfig, platform: AppPlatform, version: &str, build: Option<&str>, http: Arc<dyn HttpExchange>, clock: &dyn crate::runtime::Clock) -> Result<ServerConfig, TransportError> {
    fetch(config, &super::bootstrap::AppRelease { platform, version, build }, http, clock, &crate::transport::retry::RetryCalls::default(), &crate::transport::Connectivity::new(true)).await
}

type Step = Box<dyn Fn(&Value) -> Result<HttpReply, TransportError> + Send + Sync>;

struct Scripted {
    steps: std::sync::Mutex<std::collections::VecDeque<Step>>,
    calls: AtomicUsize,
    request_ids: std::sync::Mutex<Vec<String>>,
    progress: Arc<crate::transport::retry::RetryCalls>,
    seen: std::sync::Mutex<Vec<crate::transport::retry::RetrySnapshot>>,
}

/// The test clock of the world that records the retry snapshot each time a wait starts: the waiting entry is visible only while the sleep is pending.
struct Waits<'a> {
    clock: crate::runtime::TestClock,
    progress: &'a crate::transport::retry::RetryCalls,
    during: std::sync::Mutex<Vec<crate::transport::retry::RetrySnapshot>>,
}

impl crate::runtime::Clock for Waits<'_> {
    fn now(&self) -> std::time::Duration { self.clock.now() }
    fn sleep_until(&self, deadline: std::time::Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.during.lock().unwrap().push(self.progress.current());
        self.clock.sleep_until(deadline)
    }
    fn on_advance(&self, wake: Arc<dyn Fn() + Send + Sync>) { self.clock.on_advance(wake) }
    fn armed(&self, count: u64) { self.clock.armed(count) }
}

fn waits(progress: &crate::transport::retry::RetryCalls) -> Waits<'_> {
    Waits { clock: crate::test_wait::world_clock().unwrap(), progress, during: Default::default() }
}

#[async_trait::async_trait]
impl HttpExchange for Scripted {
    async fn post(&self, _: &str, token: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
        let plain: Value = serde_json::from_str(&decrypt_request(&token)).unwrap();
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.request_ids.lock().unwrap().push(text(&plain, "requestId").to_string());
        self.seen.lock().unwrap().push(self.progress.current());
        let step = self.steps.lock().unwrap().pop_front().expect("unscripted bootstrap request");
        step(&plain)
    }
}

fn scripted(steps: Vec<Step>) -> Arc<Scripted> {
    Arc::new(Scripted { steps: std::sync::Mutex::new(steps.into()), calls: AtomicUsize::new(0), request_ids: Default::default(), progress: Arc::new(Default::default()), seen: Default::default() })
}

fn answered(edit: impl Fn(&mut Value) + Send + Sync + 'static) -> Step {
    Box::new(move |plain| {
        let mut response: Value = serde_json::from_str(text(&kat()["response"], "plaintext")).unwrap();
        response["requestId"] = plain["requestId"].clone();
        response["requestDigest"] = request_digest(plain).into();
        edit(&mut response);
        Ok(HttpReply { status: 200, body: seal(&response, text(plain, "responseKey")) })
    })
}
fn good() -> Step { answered(|_| {}) }
pub(crate) fn good_reply(token: &str) -> HttpReply { let plain: Value = serde_json::from_str(&decrypt_request(token)).unwrap(); good()(&plain).unwrap() }
fn status(status: u16) -> Step { Box::new(move |_| Ok(HttpReply { status, body: String::new() })) }
fn failing(kind: crate::transport::RawFailureKind) -> Step {
    Box::new(move |_| Err(crate::transport::classify(&crate::transport::RawFailure::of_kind(kind, "x"), crate::transport::ExchangeKind::Post)))
}

async fn run(http: Arc<dyn HttpExchange>, progress: &crate::transport::retry::RetryCalls, config: &EncryptedConfig) -> Result<ServerConfig, TransportError> {
    run_with(http, progress, config, &waits(progress)).await
}

async fn run_with(http: Arc<dyn HttpExchange>, progress: &crate::transport::retry::RetryCalls, config: &EncryptedConfig, clock: &dyn crate::runtime::Clock) -> Result<ServerConfig, TransportError> {
    fetch(config, &super::bootstrap::AppRelease { platform: AppPlatform::Ios, version: "1.2", build: None }, http, clock, progress, &crate::transport::Connectivity::new(true)).await
}

#[test]
fn life_bootstrap_ok() { crate::test_wait::paused(async {
    let http = scripted(vec![good()]);
    let config = run(http.clone(), &http.progress, &configuration()).await.unwrap();
    assert_eq!((config.config_version, http.calls.load(Ordering::SeqCst)), (1, 1));
}) }

#[test]
fn life_bootstrap_invalid_config() { crate::test_wait::paused(async {
    for origin in ["http://example.com", "https://example.com/path", "ftp://example.com"] {
        let http = scripted(vec![]);
        let mut config = configuration();
        config.origin = origin.into();
        assert!(matches!(run(http.clone(), &http.progress, &config).await, Err(TransportError::Config { code: ConfigCode::InvalidRequest, .. })), "{origin}");
        assert_eq!(http.calls.load(Ordering::SeqCst), 0, "{origin}");
    }
}) }

#[test]
fn life_bootstrap_pin() { crate::test_wait::paused(async {
    let http = scripted(vec![]);
    let mut config = configuration();
    config.pin = "wrong".into();
    assert!(matches!(run(http.clone(), &http.progress, &config).await, Err(TransportError::Security { code: SecurityCode::ServerKeyMismatch, .. })));
    assert_eq!(http.calls.load(Ordering::SeqCst), 0);
}) }

#[test]
fn life_bootstrap_tampered() { crate::test_wait::paused(async {
    let wrong_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let steps: Vec<(Step, &str)> = vec![
        (Box::new(move |plain| Ok(HttpReply { status: 200, body: seal(&json!({"requestId": plain["requestId"]}), wrong_key) })), "sealed under another key"),
        (Box::new(|_| Ok(HttpReply { status: 200, body: "broken".into() })), "not a JWE"),
        (answered(|response| response["requestDigest"] = json!("other")), "digest of another request"),
        (answered(|response| response["requestId"] = json!("other")), "id of another request"),
        (answered(|response| response["sequence"] = json!(1)), "sequence present"),
    ];
    for (step, case) in steps {
        let http = scripted(vec![step]);
        assert!(matches!(run(http.clone(), &http.progress, &configuration()).await, Err(TransportError::Security { code: SecurityCode::ResponseAuthentication, .. })), "{case}");
        assert_eq!(http.calls.load(Ordering::SeqCst), 1, "{case}");
    }
}) }

#[test]
fn life_bootstrap_refused() { crate::test_wait::paused(async {
    let http = scripted(vec![answered(|response| response["body"] = json!({"error": "INVALID_REQUEST"}))]);
    assert!(matches!(run(http.clone(), &http.progress, &configuration()).await, Err(TransportError::Config { code: ConfigCode::InvalidRequest, .. })));
    assert_eq!(http.calls.load(Ordering::SeqCst), 1);
    let http = scripted(vec![answered(|response| response["body"] = json!({"error": "SERVICE_UNAVAILABLE"})), good()]);
    assert!(run(http.clone(), &http.progress, &configuration()).await.is_ok());
    assert_eq!(http.calls.load(Ordering::SeqCst), 2);
}) }

#[test]
fn life_bootstrap_config_code() { crate::test_wait::paused(async {
    for (field, value, code) in [("configVersion", json!(2), ConfigCode::UnsupportedVersion), ("updateRequired", json!("false"), ConfigCode::InvalidFormat)] {
        let http = scripted(vec![answered(move |response| response["body"][field] = value.clone())]);
        assert!(matches!(run(http.clone(), &http.progress, &configuration()).await, Err(TransportError::Config { code: actual, .. }) if actual == code), "{field}");
        assert_eq!(http.calls.load(Ordering::SeqCst), 1, "{field}");
    }
}) }

#[test]
fn life_bootstrap_http() { crate::test_wait::paused(async {
    for code in [301, 400, 404, 413] {
        let http = scripted(vec![status(code)]);
        assert!(matches!(run(http.clone(), &http.progress, &configuration()).await, Err(TransportError::Http { status, .. }) if status == code), "{code}");
        assert_eq!(http.calls.load(Ordering::SeqCst), 1, "{code}");
    }
}) }

#[test]
fn life_bootstrap_retry() { crate::test_wait::paused(async {
    use crate::transport::retry::{RetryEntry, RetryOwner, RetrySnapshot};
    let http = scripted(vec![status(500), status(408), status(429), failing(crate::transport::RawFailureKind::Unreachable), good()]);
    let clock = waits(&http.progress);
    assert!(run_with(http.clone(), &http.progress, &configuration(), &clock).await.is_ok());
    assert_eq!(http.calls.load(Ordering::SeqCst), 5);
    let ids = http.request_ids.lock().unwrap().clone();
    assert_eq!(ids.iter().collect::<std::collections::HashSet<_>>().len(), 5);
    assert!(http.seen.lock().unwrap().iter().all(|snapshot| *snapshot == RetrySnapshot::default()), "no retry waits while a request is being sent");
    let during = clock.during.lock().unwrap().clone();
    assert_eq!(during.len(), 4, "one wait per failed attempt");
    for (index, snapshot) in during.iter().enumerate() {
        let [RetryEntry { owner, kind, id: _, attempt, delay_ms }] = snapshot.waiting.as_slice() else { panic!("wait {}: one waiting entry, got {snapshot:?}", index + 1) };
        let ceiling = 1000u64 * 2u64.pow(index as u32);
        assert_eq!((*owner, *kind, *attempt), (RetryOwner::Bootstrap, crate::runtime::RetryPolicy::Bootstrap, index as u32 + 1));
        assert!((ceiling * 8 / 10..=ceiling).contains(delay_ms), "{delay_ms} outside {ceiling}");
    }
    assert_eq!(http.progress.current(), RetrySnapshot::default());
}) }

#[test]
fn life_bootstrap_timeout() { crate::test_wait::paused(async {
    let http = scripted(vec![failing(crate::transport::RawFailureKind::Timeout), failing(crate::transport::RawFailureKind::Timeout), failing(crate::transport::RawFailureKind::Io), good()]);
    let clock = waits(&http.progress);
    assert!(run_with(http.clone(), &http.progress, &configuration(), &clock).await.is_ok());
    assert_eq!(http.calls.load(Ordering::SeqCst), 4);
    let attempts: Vec<Vec<u32>> = clock.during.lock().unwrap().iter().map(|snapshot| snapshot.waiting.iter().map(|entry| entry.attempt).collect()).collect();
    assert_eq!(attempts, [vec![1], vec![2], vec![3]], "each timeout waits with the next attempt number");
}) }

struct DropFlag(Arc<AtomicUsize>);
impl Drop for DropFlag { fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); } }

struct HangsOnSecond { calls: AtomicUsize, dropped: Arc<AtomicUsize>, started: Arc<tokio::sync::Notify> }
#[async_trait::async_trait]
impl HttpExchange for HangsOnSecond {
    async fn post(&self, _: &str, _: String, _: crate::transport::NetworkPurpose, _: crate::transport::WireIds) -> Result<HttpReply, TransportError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 { return Err(TransportError::Network("x".into())); }
        let _held = DropFlag(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}

#[test]
fn life_bootstrap_cancel() { crate::test_wait::paused(async {
    let http = scripted(vec![failing(crate::transport::RawFailureKind::Cancelled)]);
    assert!(matches!(run(http.clone(), &http.progress, &configuration()).await, Err(TransportError::Interrupted(_))));
    assert_eq!(http.calls.load(Ordering::SeqCst), 1);
    let hanging = Arc::new(HangsOnSecond { calls: AtomicUsize::new(0), dropped: Arc::new(AtomicUsize::new(0)), started: Arc::new(tokio::sync::Notify::new()) });
    let progress = crate::transport::retry::RetryCalls::default();
    let config = configuration();
    tokio::select! {
        biased;
        () = hanging.started.notified() => {}
        _ = run(hanging.clone(), &progress, &config) => unreachable!(),
    }
    assert_eq!((hanging.calls.load(Ordering::SeqCst), hanging.dropped.load(Ordering::SeqCst)), (2, 1));
    assert_eq!(progress.current(), crate::transport::retry::RetrySnapshot::default());
}) }
