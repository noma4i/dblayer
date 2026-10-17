use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::{Oaep, RsaPrivateKey};
use serde_json::{json, Value};
use sha1::Sha1;

use super::vectors;

pub const WINDOW: i64 = 120;
const FIELDS: [&str; 10] = ["audience", "kid", "method", "path", "requestId", "issuedAt", "responseKey", "credential", "sequence", "body"];

pub fn private_key() -> RsaPrivateKey {
    RsaPrivateKey::from_pkcs1_pem(vectors::SERVER_PRIVATE_KEY_PEM).expect("fixture key parses")
}

fn token_ok(value: &Value) -> bool {
    value.as_str().is_some_and(|text| text.len() == 43 && text.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'))
}

pub fn open_request(token: &str, path: &str, now: i64) -> Result<Value, String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 5 {
        return Err("segments".into());
    }
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    if header != json!({ "alg": "RSA-OAEP", "enc": "A256GCM" }) {
        return Err(format!("header {header}"));
    }
    let wrapped = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|e| e.to_string())?;
    let cek = private_key().decrypt(Oaep::new::<Sha1>(), &wrapped).map_err(|e| e.to_string())?;
    let iv = URL_SAFE_NO_PAD.decode(parts[2]).map_err(|e| e.to_string())?;
    let mut sealed = URL_SAFE_NO_PAD.decode(parts[3]).map_err(|e| e.to_string())?;
    sealed.extend(URL_SAFE_NO_PAD.decode(parts[4]).map_err(|e| e.to_string())?);
    let cipher = Aes256Gcm::new_from_slice(&cek).map_err(|e| e.to_string())?;
    let plain = cipher.decrypt(Nonce::from_slice(&iv), Payload { msg: &sealed, aad: parts[0].as_bytes() }).map_err(|e| e.to_string())?;
    let data: Value = serde_json::from_slice(&plain).map_err(|e| e.to_string())?;
    let object = data.as_object().ok_or("not an object")?;
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let mut expected = FIELDS.to_vec();
    expected.sort_unstable();
    if keys != expected {
        return Err(format!("fields {keys:?}"));
    }
    if object["audience"] != vectors::ORIGIN || object["kid"] != vectors::PIN || object["method"] != "POST" || object["path"] != path {
        return Err("destination".into());
    }
    let issued = object["issuedAt"].as_i64().ok_or("issuedAt")?;
    if !(-30..=WINDOW).contains(&(now - issued)) {
        return Err("timestamp".into());
    }
    let request_id = object["requestId"].as_str().ok_or("requestId")?;
    if request_id.len() != 36 || !request_id.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase() || byte == b'-') {
        return Err("requestId".into());
    }
    if !token_ok(&object["credential"]) || !token_ok(&object["responseKey"]) || !object["body"].is_object() {
        return Err("credential, responseKey or body".into());
    }
    if !(object["sequence"].is_null() || object["sequence"].as_i64().is_some_and(|sequence| sequence > 0)) {
        return Err("sequence".into());
    }
    Ok(data)
}

pub fn seal_response(request: &Value, sequence: Option<i64>, body: Value) -> String {
    let key = URL_SAFE_NO_PAD.decode(request["responseKey"].as_str().expect("responseKey")).expect("responseKey decodes");
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"dir","enc":"A256GCM"}"#);
    let iv = [7_u8; 12];
    let plain = json!({ "requestId": request["requestId"], "sequence": sequence, "body": body }).to_string();
    let sealed = Aes256Gcm::new_from_slice(&key).expect("key").encrypt(Nonce::from_slice(&iv), Payload { msg: plain.as_bytes(), aad: header.as_bytes() }).expect("seals");
    let (text, tag) = sealed.split_at(sealed.len() - 16);
    format!("{header}..{}.{}.{}", URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(text), URL_SAFE_NO_PAD.encode(tag))
}

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::{CredentialSlot, HttpExchange, HttpReply};
use crate::pacing::Clock;
use crate::transport::TransportError;

#[derive(Default)]
struct Faults {
    pub lose_next_answer: bool,
    pub drop_next_request: bool,
    pub status_next: Option<u16>,
    pub reply_to_other_request: bool,
    pub replay_next_read: bool,
    pub forget_registrations: bool,
    pub refuse_hashes: bool,
}

#[derive(Default)]
struct Session {
    digest: String,
    sequence: i64,
    request_digest: String,
    answer: Option<String>,
    ended: bool,
    reads: HashSet<String>,
}

fn document_of(registry: &mut HashSet<(String, String)>, body: &Value) -> Result<Value, &'static str> {
    let Some(persisted) = body.get("extensions").and_then(|extensions| extensions.get("persistedQuery")) else { return Ok(body["query"].clone()) };
    let hash = persisted["sha256Hash"].as_str().unwrap_or_default().to_string();
    match body["query"].as_str() {
        Some(query) if hex_of(query) == hash => {
            registry.insert((hash, query.to_string()));
            Ok(json!(query))
        }
        Some(_) => Err("INVALID_INPUT"),
        None => registry.iter().find(|(known, _)| *known == hash).map(|(_, query)| json!(query)).ok_or("PERSISTED_QUERY_NOT_FOUND"),
    }
}

fn hex_of(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn persisted_failure(code: &str) -> Value {
    json!({ "errors": [{ "message": code, "extensions": { "code": code } }] })
}

#[derive(Default)]
struct State {
    session: Option<Session>,
    faults: Faults,
    seen: Vec<Value>,
    executed: Vec<Value>,
    registry: HashSet<(String, String)>,
}

pub struct FakeServer {
    state: Mutex<State>,
    clock: Arc<dyn Clock>,
    on_request: Mutex<Option<Box<dyn Fn() + Send>>>,
    config: Mutex<HttpReply>,
    config_paths: Mutex<Vec<String>>,
}

fn digest_of(data: &Value) -> String {
    let mut copy = data.clone();
    copy.as_object_mut().expect("object").remove("issuedAt");
    Sha256::digest(copy.to_string().as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn config_body(kid: &str, der: &str, bits: u32) -> Value {
    json!({
        "protocol_version": 1, "min_app_version": "3.8.0",
        "graphql_url": "https://api.example.com/api/v3/graphql", "cable_url": "wss://api.example.com/api/v3/cable",
        "server_key": { "kid": kid, "public_key_der": der, "alg": "RSA-OAEP", "enc": "A256GCM", "bits": bits }
    })
}

impl FakeServer {
    pub fn rewind_sequence(&self, credential: &str, sequence: i64) {
        let digest: String = Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
        let mut state = self.state.lock().unwrap();
        let session = state.session.as_mut().filter(|session| session.digest == digest).expect("session of the credential");
        session.sequence = sequence;
    }

    pub fn register(&self, document: &str) {
        let hash = hex_of(document);
        self.state.lock().unwrap().registry.insert((hash, document.to_string()));
    }

    pub fn forget_registrations(&self) {
        self.state.lock().unwrap().faults.forget_registrations = true;
    }

    pub fn refuse_hashes(&self) {
        self.state.lock().unwrap().faults.refuse_hashes = true;
    }

    pub fn config_paths(&self) -> Vec<String> {
        self.config_paths.lock().unwrap().clone()
    }

    pub fn serve_config(&self, reply: HttpReply) {
        *self.config.lock().unwrap() = reply;
    }

    pub fn new(clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self { state: Mutex::default(), clock, on_request: Mutex::new(None), config: Mutex::new(HttpReply { status: 200, body: config_body(&vectors::PIN, &vectors::PUBLIC_KEY_DER_B64, 3072).to_string() }), config_paths: Mutex::default() })
    }

    pub fn with_sequence(self: Arc<Self>, credential: &str, sequence: i64) -> Arc<Self> {
        let digest = Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
        self.state.lock().unwrap().session = Some(Session { digest, sequence, ..Session::default() });
        self
    }

    pub fn on_request(&self, hook: impl Fn() + Send + 'static) {
        *self.on_request.lock().unwrap() = Some(Box::new(hook));
    }

    pub fn seen(&self) -> Vec<Value> {
        self.state.lock().unwrap().seen.clone()
    }

    pub fn executed(&self) -> Vec<Value> {
        self.state.lock().unwrap().executed.clone()
    }

    pub fn lose_next_answer(&self) {
        self.state.lock().unwrap().faults.lose_next_answer = true;
    }

    pub fn drop_next_request(&self) {
        self.state.lock().unwrap().faults.drop_next_request = true;
    }

    pub fn status_next(&self, status: u16) {
        self.state.lock().unwrap().faults.status_next = Some(status);
    }

    pub fn replay_next_read(&self) {
        self.state.lock().unwrap().faults.replay_next_read = true;
    }

    pub fn reply_to_other_request(&self) {
        self.state.lock().unwrap().faults.reply_to_other_request = true;
    }

    fn answer(&self, state: &mut State, path: &str, data: &Value) -> String {
        let credential_digest: String = Sha256::digest(data["credential"].as_str().unwrap().as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
        let auth = path.starts_with("/api/v3/auth/");
        if state.session.is_none() && auth && data["sequence"] == 1 {
            state.session = Some(Session { digest: credential_digest.clone(), ..Session::default() });
        }
        let Some(session) = state.session.as_mut().filter(|session| session.digest == credential_digest) else {
            return seal_response(data, None, json!({ "error": "UNAUTHENTICATED" }));
        };
        let (forget, refuse) = (state.faults.forget_registrations, state.faults.refuse_hashes);
        let resolved = if path == "/api/v3/graphql" && forget { Err("PERSISTED_QUERY_NOT_FOUND") } else if path == "/api/v3/graphql" && refuse { Err("INVALID_INPUT") } else if path == "/api/v3/graphql" { document_of(&mut state.registry, &data["body"]) } else { Ok(Value::Null) };
        let Some(sequence) = data["sequence"].as_i64() else {
            let document = match resolved {
                Ok(document) => document,
                Err(code) => return seal_response(data, None, persisted_failure(code)),
            };
            if !session.reads.insert(data["requestId"].as_str().unwrap().to_string()) || std::mem::take(&mut state.faults.replay_next_read) {
                return seal_response(data, None, json!({ "error": "REPLAYED_REQUEST" }));
            }
            return seal_response(data, None, json!({ "data": { "read": document } }));
        };
        let digest = digest_of(data);
        if session.sequence == sequence && session.request_digest == digest {
            return session.answer.clone().expect("stored answer");
        }
        if sequence != session.sequence + 1 {
            return seal_response(data, None, json!({ "error": "SEQUENCE_CONFLICT" }));
        }
        if session.ended {
            return seal_response(data, None, json!({ "error": "SESSION_ENDED" }));
        }
        let result = match path {
            "/api/v3/logout" => {
                session.ended = true;
                json!({ "revoked": true })
            }
            "/api/v3/auth/confirm" => json!({ "error": "INVALID_CODE" }),
            "/api/v3/graphql" => match &resolved {
                Ok(document) => json!({ "data": { "done": document } }),
                Err(code) => persisted_failure(code),
            },
            _ => json!({ "nextStep": "authenticated" }),
        };
        let answer = seal_response(data, Some(sequence), result);
        session.sequence = sequence;
        session.request_digest = digest;
        session.answer = Some(answer.clone());
        if resolved.is_ok() {
            state.executed.push(data["body"].clone());
        }
        answer
    }
}

#[async_trait]
impl HttpExchange for FakeServer {
    async fn get(&self, path: &str) -> Result<HttpReply, TransportError> {
        assert!(path == "/api/v3/config" || path.starts_with("/api/v3/config?"));
        self.config_paths.lock().unwrap().push(path.to_string());
        Ok(self.config.lock().unwrap().clone())
    }

    async fn post(&self, path: &str, body: String) -> Result<HttpReply, TransportError> {
        if let Some(hook) = self.on_request.lock().unwrap().as_ref() {
            hook();
        }
        let mut state = self.state.lock().unwrap();
        if std::mem::take(&mut state.faults.drop_next_request) {
            return Err(TransportError::Network("offline".into()));
        }
        if let Some(status) = state.faults.status_next.take() {
            return Ok(HttpReply { status, body: String::new() });
        }
        let Ok(data) = open_request(&body, path, self.clock.now().as_secs() as i64) else { return Ok(HttpReply { status: 400, body: String::new() }) };
        state.seen.push(data.clone());
        let mut answer = self.answer(&mut state, path, &data);
        if std::mem::take(&mut state.faults.reply_to_other_request) {
            let mut other = data.clone();
            other["requestId"] = json!("99999999-2222-4333-8444-555555555555");
            answer = seal_response(&other, Some(1), json!({}));
        }
        if std::mem::take(&mut state.faults.lose_next_answer) {
            return Err(TransportError::Network("the answer was lost".into()));
        }
        Ok(HttpReply { status: 200, body: answer })
    }
}

#[derive(Default)]
pub struct MemoryCredentials {
    value: Mutex<Option<String>>,
}

impl MemoryCredentials {
    pub fn value(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }
}

impl CredentialSlot for MemoryCredentials {
    fn get(&self) -> Result<Option<String>, TransportError> {
        Ok(self.value())
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        *self.value.lock().unwrap() = Some(credential.to_string());
        Ok(())
    }

    fn clear(&self) -> Result<(), TransportError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}
