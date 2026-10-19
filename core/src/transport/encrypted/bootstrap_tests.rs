use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;
use aes_gcm::{Aes256Gcm, Nonce, aead::{Aead, KeyInit, Payload}};
use base64::{Engine, engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}};
use rsa::{RsaPrivateKey, Oaep, pkcs1::DecodeRsaPrivateKey, pkcs8::EncodePublicKey};
use serde_json::{json, Value};
use sha1::Sha1;
use super::*;
use super::bootstrap::{fetch_with, request_plain, request_text, request_digest, accept};
use crate::pacing::ManualClock;

fn kat() -> Value { serde_json::from_str(include_str!("../../../../fixtures/v3-config/bootstrap_kat.json")).unwrap() }
fn text<'a>(value: &'a Value, key: &str) -> &'a str { value[key].as_str().unwrap() }
fn configuration() -> EncryptedConfig {
    let k = kat();
    let private = RsaPrivateKey::from_pkcs1_pem(text(&k, "rsa_private_pem")).unwrap();
    EncryptedConfig { origin: text(&k, "audience").into(), server_key: STANDARD.encode(private.to_public_key().to_public_key_der().unwrap().as_bytes()), pin: text(&k, "kid").into() }
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
fn mismatched_digest_id_key_and_broken_jwe_are_tampering() {
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
#[async_trait::async_trait]
impl HttpExchange for Http {
    async fn post(&self, path: &str, token: String) -> Result<HttpReply, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(path, "/api/v3/bootstrap");
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
    assert!(fetch_with(&configuration(), AppPlatform::Ios, "1.2", Some(&"x".repeat(33)), http.clone(), &clock).await.is_err());
    assert_eq!(http.calls.load(Ordering::SeqCst), 0);
    for platform in [AppPlatform::Ios, AppPlatform::Android] {
        assert!(fetch_with(&configuration(), platform, "1.2", None, http.clone(), &clock).await.is_ok());
    }
    assert_eq!(http.calls.load(Ordering::SeqCst), 2);
    for status in [400, 413, 503] {
        let http = Arc::new(Http { calls: AtomicUsize::new(0), status });
        assert!(matches!(fetch_with(&configuration(), AppPlatform::Ios, "1.2.3", None, http, &clock).await, Err(TransportError::Http { status: actual, .. }) if status == actual));
    }
}
