use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand_core::{OsRng, RngCore};
use rsa::Oaep;
use serde_json::{json, Value};
use sha1::Sha1;

use super::key::ServerKey;

const REQUEST_HEADER: &str = r#"{"alg":"RSA-OAEP","enc":"A256GCM"}"#;
const TAG_LEN: usize = 16;
const IV_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JweError {
    #[error("jwe: {0}")]
    Invalid(String),
}

fn invalid(error: impl ToString) -> JweError {
    JweError::Invalid(error.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestPlain {
    pub audience: String,
    pub kid: String,
    pub path: String,
    pub request_id: String,
    pub issued_at: i64,
    pub response_key: String,
    pub credential: String,
    pub sequence: Option<i64>,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponsePlain {
    pub request_id: String,
    pub sequence: Option<i64>,
    pub body: Value,
}

pub fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn seal_request(key: &ServerKey, plain: &RequestPlain) -> Result<String, JweError> {
    let text = json!({
        "audience": plain.audience, "kid": plain.kid, "method": "POST", "path": plain.path, "requestId": plain.request_id,
        "issuedAt": plain.issued_at, "responseKey": plain.response_key, "credential": plain.credential,
        "sequence": plain.sequence, "body": plain.body,
    })
    .to_string();
    seal_plain(key, text.as_bytes())
}

pub(super) fn seal_plain(key: &ServerKey, text: &[u8]) -> Result<String, JweError> {
    let mut cek = zeroize::Zeroizing::new([0_u8; 32]);
    let mut iv = [0_u8; IV_LEN];
    OsRng.fill_bytes(&mut *cek);
    OsRng.fill_bytes(&mut iv);
    let header = URL_SAFE_NO_PAD.encode(REQUEST_HEADER);
    let wrapped = key.public.encrypt(&mut OsRng, Oaep::new::<Sha1>(), &*cek).map_err(invalid)?;
    let sealed = Aes256Gcm::new_from_slice(&*cek)
        .map_err(invalid)?
        .encrypt(Nonce::from_slice(&iv), Payload { msg: text, aad: header.as_bytes() })
        .map_err(invalid)?;
    let (ciphertext, tag) = sealed.split_at(sealed.len() - TAG_LEN);
    Ok([header, URL_SAFE_NO_PAD.encode(wrapped), URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(ciphertext), URL_SAFE_NO_PAD.encode(tag)].join("."))
}

pub fn open_response(token: &str, response_key: &str) -> Result<ResponsePlain, JweError> {
    let data = open_plain(token, response_key)?;
    Ok(ResponsePlain {
        request_id: data["requestId"].as_str().ok_or_else(|| invalid("the response has no requestId"))?.to_string(),
        sequence: data["sequence"].as_i64(),
        body: data["body"].clone(),
    })
}

pub(super) fn open_plain(token: &str, response_key: &str) -> Result<Value, JweError> {
    let parts: Vec<&str> = token.split('.').collect();
    let [header, wrapped, iv, ciphertext, tag] = parts[..] else { return Err(invalid("a compact JWE has 5 segments")) };
    let header_json: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).map_err(invalid)?).map_err(invalid)?;
    if header_json != json!({ "alg": "dir", "enc": "A256GCM" }) || !wrapped.is_empty() {
        return Err(invalid("the response header must be alg dir, enc A256GCM and carry no key"));
    }
    let key = URL_SAFE_NO_PAD.decode(response_key).map_err(invalid)?;
    let iv = URL_SAFE_NO_PAD.decode(iv).map_err(invalid)?;
    let tag = URL_SAFE_NO_PAD.decode(tag).map_err(invalid)?;
    if iv.len() != IV_LEN || tag.len() != TAG_LEN {
        return Err(invalid("the IV or the tag has a wrong length"));
    }
    let mut sealed = URL_SAFE_NO_PAD.decode(ciphertext).map_err(invalid)?;
    sealed.extend(tag);
    let plain = Aes256Gcm::new_from_slice(&key)
        .map_err(invalid)?
        .decrypt(Nonce::from_slice(&iv), Payload { msg: &sealed, aad: header.as_bytes() })
        .map_err(|_| invalid("the response does not authenticate"))?;
    serde_json::from_slice(&plain).map_err(invalid)
}
