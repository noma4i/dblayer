use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hkdf::Hkdf;
use serde::Serialize;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use super::*;

pub(super) struct Keys {
    pub send: Zeroizing<[u8; 32]>,
    pub receive: Zeroizing<[u8; 32]>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Hello<'a> {
    v: u8,
    purpose: &'static str,
    audience: &'a str,
    kid: &'a str,
    method: &'static str,
    path: &'static str,
    challenge: &'a str,
    client_key: String,
    credential: &'a str,
    response_key: &'a str,
}

pub(super) fn decode(value: &Value) -> Result<[u8; 32], CableError> {
    URL_SAFE_NO_PAD.decode(value.as_str().ok_or(CableError::Challenge)?).map_err(|_| CableError::Challenge)?.try_into().map_err(|_| CableError::Challenge)
}

pub(super) fn agree(config: &EncryptedConfig, challenge: &Value, credential: &str, private: [u8; 32], response: [u8; 32]) -> Result<(Keys, String), CableError> {
    let private = StaticSecret::from(private);
    let response = Zeroizing::new(response);
    let key = super::super::key::ServerKey::parse(&config.server_key, &config.pin).map_err(|_| CableError::ServerKeyMismatch)?;
    if challenge["kid"].as_str() != Some(&config.pin) { return Err(CableError::ServerKeyMismatch); }
    if challenge["type"] != "challenge" || challenge["v"] != 1 || !super::super::origin::allowed(config.origin.trim_end_matches('/')) {
        return Err(CableError::Challenge);
    }
    let salt = decode(&challenge["challenge"])?;
    let peer = PublicKey::from(decode(&challenge["x25519"])?);
    let shared = private.diffie_hellman(&peer);
    if !shared.was_contributory() { return Err(CableError::Challenge); }
    let mut ikm = Zeroizing::new([0; 64]);
    ikm[..32].copy_from_slice(shared.as_bytes());
    ikm[32..].copy_from_slice(&*response);
    let hkdf = Hkdf::<Sha256>::new(Some(&salt), &*ikm);
    let audience = config.origin.trim_end_matches('/');
    let derive = |direction| {
        let info = ["yupi-v3-ws-v1", direction, &config.pin, audience, CABLE_PATH].join("\0");
        let mut key = Zeroizing::new([0; 32]);
        hkdf.expand(info.as_bytes(), &mut *key).map_err(|_| CableError::Challenge)?;
        Ok(key)
    };
    let keys = Keys { send: derive("c2s")?, receive: derive("s2c")? };
    let response_key = Zeroizing::new(URL_SAFE_NO_PAD.encode(*response));
    let hello = Hello { v: 1, purpose: "v3.ws.hello", audience, kid: &config.pin, method: "GET", path: CABLE_PATH,
        challenge: challenge["challenge"].as_str().ok_or(CableError::Challenge)?, client_key: URL_SAFE_NO_PAD.encode(PublicKey::from(&private).as_bytes()), credential, response_key: &response_key };
    let plain = Zeroizing::new(serde_json::to_vec(&hello).map_err(|_| CableError::Challenge)?);
    let token = super::super::jwe::seal_plain(&key, &plain).map_err(|_| CableError::Challenge)?;
    Ok((keys, token))
}
