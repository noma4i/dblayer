use super::*;
use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use base64::{engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}, Engine};
use rsa::{pkcs1::DecodeRsaPrivateKey, pkcs8::EncodePublicKey, Oaep, RsaPrivateKey};
use serde_json::json;
use sha1::Sha1;

fn vectors() -> Value { serde_json::from_str(include_str!("../../../../../../fixtures/change-events/kat.json")).unwrap() }
fn hex(text: &str) -> Vec<u8> { text.as_bytes().as_chunks::<2>().0.iter().map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect() }
fn bytes(value: &Value) -> [u8; 32] { hex(value.as_str().unwrap()).try_into().unwrap() }

pub(in super::super) fn inputs() -> (Value, EncryptedConfig, Value, String) {
    let kat = vectors();
    let private = RsaPrivateKey::from_pkcs1_pem(kat["hello"]["rsa_private_pem"].as_str().unwrap()).unwrap();
    let mut config = EncryptedConfig { origin: kat["hkdf"]["audience"].as_str().unwrap().into(), server_key: STANDARD.encode(private.to_public_key().to_public_key_der().unwrap().as_bytes()), pin: kat["hkdf"]["kid"].as_str().unwrap().into(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    config.endpoints.cable = kat["hkdf"]["path"].as_str().unwrap().into();
    config.protocol_label = kat["config"]["protocol_label"].as_str().unwrap().into();
    config.cable_purpose = kat["config"]["cable_purpose"].as_str().unwrap().into();
    let challenge = json!({"type":"challenge", "v":1, "kid":config.pin, "challenge":URL_SAFE_NO_PAD.encode(bytes(&kat["hkdf"]["challenge"])), "x25519":URL_SAFE_NO_PAD.encode(bytes(&kat["x25519"]["server_public"]))});
    let credential = URL_SAFE_NO_PAD.encode(bytes(&kat["hello"]["credential"]));
    (kat, config, challenge, credential)
}

fn known_link() -> (CableLink, String, Value) {
    let (kat, config, challenge, credential) = inputs();
    let (link, hello) = CableLink::with_entropy(&config, &challenge, &credential, bytes(&kat["x25519"]["client_private"]), bytes(&kat["hkdf"]["response_key"])).unwrap();
    (link, hello, kat)
}

pub(in super::super) fn decrypt(token: &str, key: &[u8]) -> Vec<u8> {
    let parts: Vec<_> = token.split('.').collect();
    let mut data = URL_SAFE_NO_PAD.decode(parts[3]).unwrap();
    data.extend(URL_SAFE_NO_PAD.decode(parts[4]).unwrap());
    Aes256Gcm::new_from_slice(key).unwrap().decrypt(Nonce::from_slice(&URL_SAFE_NO_PAD.decode(parts[2]).unwrap()), Payload { msg: &data, aad: parts[0].as_bytes() }).unwrap()
}

#[test]
fn hello_and_directional_keys_match_server_vectors() {
    let (mut link, hello, kat) = known_link();
    let private = RsaPrivateKey::from_pkcs1_pem(kat["hello"]["rsa_private_pem"].as_str().unwrap()).unwrap();
    for token in [&hello, kat["hello"]["token"].as_str().unwrap()] {
        let wrapped = URL_SAFE_NO_PAD.decode(token.split('.').nth(1).unwrap()).unwrap();
        let key = private.decrypt(Oaep::new::<Sha1>(), &wrapped).unwrap();
        assert!(decrypt(token, &key) == kat["hello"]["plaintext"].as_str().unwrap().as_bytes(), "hello bytes differ");
    }
    for frame in kat["frames"].as_array().unwrap() {
        let direction = if frame["direction"] == "s2c" { "server_to_client" } else { "client_to_server" };
        let token = frame::encrypt(&bytes(&kat["hkdf"][direction]), frame["plaintext"].as_str().unwrap().as_bytes(), &hex(frame["iv"].as_str().unwrap()).try_into().unwrap()).unwrap();
        assert!(token == frame["token"].as_str().unwrap(), "frame bytes differ");
        let plain: Value = serde_json::from_str(frame["plaintext"].as_str().unwrap()).unwrap();
        if frame["direction"] == "s2c" {
            assert!(link.receive(frame["token"].as_str().unwrap()).unwrap() == plain, "server frame differs");
        } else {
            let mut body = plain.clone();
            body.as_object_mut().unwrap().remove("n");
            let token = link.seal(body).unwrap();
            let decoded: Value = serde_json::from_slice(&decrypt(&token, &bytes(&kat["hkdf"]["client_to_server"]))).unwrap();
            assert!(decoded == plain, "client frame differs");
        }
    }
}

#[test]
fn refuses_untrusted_pin_and_invalid_challenges() {
    let (_, config, challenge, credential) = inputs();
    for field in ["pin", "kid", "v", "challenge", "x25519", "type"] {
        let mut config = config.clone();
        let mut challenge = challenge.clone();
        if field == "pin" { config.pin = "0".repeat(64); challenge["kid"] = json!(config.pin); }
        else { challenge[field] = json!("invalid"); }
        let error = CableLink::open(&config, &challenge, &credential).err().unwrap();
        assert_eq!(error, if matches!(field, "pin" | "kid") { CableError::ServerKeyMismatch } else { CableError::Challenge }, "{field}");
    }
    let mut challenge = challenge;
    challenge["x25519"] = json!(URL_SAFE_NO_PAD.encode([0; 32]));
    assert!(matches!(CableLink::open(&config, &challenge, &credential), Err(CableError::Challenge)));
}

#[test]
fn rejects_replay_gaps_corruption_and_oversized_frames() {
    for case in ["replay", "gap", "tag", "oversized"] {
        let (mut link, _, kat) = known_link();
        let ready = kat["frames"][0]["token"].as_str().unwrap();
        let expected = match case {
            "replay" => { link.receive(ready).unwrap(); CableError::Sequence },
            "gap" => CableError::Sequence,
            "oversized" => CableError::TooLarge,
            _ => CableError::Frame,
        };
        let token = match case {
            "gap" => kat["frames"][1]["token"].as_str().unwrap().to_string(),
            "tag" => format!("{}AA", &ready[..ready.len()-2]),
            "oversized" => "x".repeat(SERVER_BYTES + 1),
            _ => ready.to_string(),
        };
        assert_eq!(link.receive(&token).unwrap_err(), expected, "{case}");
        assert_eq!(link.receive(ready).unwrap_err(), CableError::Closed, "failure must retire keys");
    }
}

#[test]
fn outbound_limits_and_close_retire_keys() {
    let (mut link, _, _) = known_link();
    assert_eq!(link.seal(json!({"t":"subscribe", "id":"one", "query":"x".repeat(CLIENT_BYTES)})).unwrap_err(), CableError::TooLarge);
    assert_eq!(link.seal(json!({"t":"unknown"})).unwrap_err(), CableError::Frame);
    link.close();
    assert_eq!(link.seal(json!({"t":"ping", "ts":1})).unwrap_err(), CableError::Closed);
    assert_eq!(link.receive("").unwrap_err(), CableError::Closed);
}

fn server_frame(kat: &Value, body: Value) -> String {
    frame::encrypt(&bytes(&kat["hkdf"]["server_to_client"]), body.to_string().as_bytes(), &[7; 12]).unwrap()
}

#[test]
fn authenticated_frames_enforce_schema_ready_and_key_budget() {
    for case in ["digest", "first", "second_ready", "unknown", "extra", "id", "result", "reason", "code", "budget"] {
        let (mut link, _, kat) = known_link();
        let mut body = json!({"n":1, "t":"next", "id":"a", "result":{}});
        if !matches!(case, "digest" | "first") { link.receive(kat["frames"][0]["token"].as_str().unwrap()).unwrap(); }
        let expected = match case {
            "digest" => { body = serde_json::from_str(kat["frames"][0]["plaintext"].as_str().unwrap()).unwrap(); body["challengeDigest"] = json!("0".repeat(64)); CableError::Challenge },
            "first" => { body["n"] = json!(0); CableError::Challenge },
            "second_ready" => { body = serde_json::from_str(kat["frames"][0]["plaintext"].as_str().unwrap()).unwrap(); body["n"] = json!(1); CableError::Frame },
            "unknown" => { body["t"] = json!("unknown"); CableError::Frame },
            "extra" => { body["extra"] = json!(true); CableError::Frame },
            "id" => { body["id"] = json!("bad id"); CableError::Frame },
            "result" => { body["result"] = json!([]); CableError::Frame },
            "reason" => { body = json!({"n":1, "t":"closing", "code":4503, "reason":"unknown"}); CableError::Frame },
            "code" => { body = json!({"n":1, "t":"error", "code":"unknown", "retryable":true}); CableError::Frame },
            _ => { link.received = FRAME_BUDGET; body["n"] = json!(FRAME_BUDGET); CableError::Exhausted },
        };
        assert_eq!(link.receive(&server_frame(&kat, body)).unwrap_err(), expected, "{case}");
        assert_eq!(link.seal(json!({"t":"close"})).unwrap_err(), CableError::Closed);
    }
    let (mut link, _, _) = known_link();
    link.sent = FRAME_BUDGET;
    assert_eq!(link.seal(json!({"t":"ping", "ts":1})).unwrap_err(), CableError::Exhausted);
    assert_eq!(link.seal(json!({"t":"close"})).unwrap_err(), CableError::Closed);
}

#[test]
fn accepts_all_server_messages_and_retires_keys_on_closing() {
    let (mut link, _, kat) = known_link();
    link.receive(kat["frames"][0]["token"].as_str().unwrap()).unwrap();
    let bodies = [json!({"t":"subscribed","id":"a"}), json!({"t":"next","id":"a","result":{}}), json!({"t":"error","code":"RATE_LIMITED","retryable":true,"id":null}), json!({"t":"complete","id":"a"}), json!({"t":"pong","ts":5}), json!({"t":"closing","code":4503,"reason":"SERVER_RESTART"})];
    for (index, mut body) in bodies.into_iter().enumerate() {
        body["n"] = json!(index + 1);
        assert!(link.receive(&server_frame(&kat, body.clone())).unwrap() == body);
    }
    assert_eq!(link.seal(json!({"t":"ping","ts":6})).unwrap_err(), CableError::Closed);
}

#[test]
fn authenticated_codes_accept_new_values_and_reject_invalid_shapes() {
    let cases = [
        (json!({"t":"error", "id":"a", "code":"REFUSED", "retryable":false}), true),
        (json!({"t":"error", "id":"a", "code":"FUTURE_2", "retryable":true}), true),
        (json!({"t":"error", "code":"A".repeat(64), "retryable":false}), true),
        (json!({"t":"error", "code":"A".repeat(65), "retryable":false}), false),
        (json!({"t":"error", "code":"", "retryable":false}), false),
        (json!({"t":"error", "code":42, "retryable":false}), false),
        (json!({"t":"error", "code":"BAD-CODE", "retryable":false}), false),
        (json!({"t":"error", "code":"REFUSED", "retryable":"false"}), false),
        (json!({"t":"closing", "code":4999, "reason":"FUTURE_REASON_2"}), true),
        (json!({"t":"closing", "code":1000, "reason":"A".repeat(64)}), true),
        (json!({"t":"closing", "code":999, "reason":"REFUSED"}), false),
        (json!({"t":"closing", "code":5000, "reason":"REFUSED"}), false),
        (json!({"t":"closing", "code":65536, "reason":"REFUSED"}), false),
        (json!({"t":"closing", "code":"4999", "reason":"REFUSED"}), false),
        (json!({"t":"closing", "code":4999, "reason":"A".repeat(65)}), false),
        (json!({"t":"closing", "code":4999, "reason":""}), false),
        (json!({"t":"closing", "code":4999, "reason":null}), false),
    ];
    for (mut body, accepted) in cases {
        let (mut link, _, kat) = known_link();
        link.receive(kat["frames"][0]["token"].as_str().unwrap()).unwrap();
        body["n"] = json!(1);
        let result = link.receive(&server_frame(&kat, body.clone()));
        assert_eq!(result.is_ok(), accepted, "{body}: {result:?}");
        if accepted { assert_eq!(result.unwrap(), body); }
        assert_eq!(link.seal(json!({"t":"ping", "ts":1})).is_ok(), accepted && body["t"] != "closing");
    }
}
