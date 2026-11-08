use std::sync::OnceLock;

fn vectors() -> &'static serde_json::Value {
    static VALUES: OnceLock<serde_json::Value> = OnceLock::new();
    VALUES.get_or_init(|| serde_json::from_str(include_str!("../../../../../fixtures/encrypted-http/kat.json")).unwrap())
}

pub fn server_private_key_pem() -> &'static str { vectors()["SERVER_PRIVATE_KEY_PEM"].as_str().unwrap() }
pub fn pin() -> &'static str { vectors()["PIN"].as_str().unwrap() }
pub fn public_key_der_b64() -> &'static str { vectors()["PUBLIC_KEY_DER_B64"].as_str().unwrap() }
pub fn origin() -> &'static str { vectors()["ORIGIN"].as_str().unwrap() }
pub fn request_jwe() -> &'static str { vectors()["REQUEST_JWE"].as_str().unwrap() }
pub fn request_plain() -> &'static str { vectors()["REQUEST_PLAIN"].as_str().unwrap() }
pub fn read_jwe() -> &'static str { vectors()["READ_JWE"].as_str().unwrap() }
pub fn response_ok() -> &'static str { vectors()["RESPONSE_OK"].as_str().unwrap() }
pub fn response_conflict() -> &'static str { vectors()["RESPONSE_CONFLICT"].as_str().unwrap() }
pub fn published_response_key() -> &'static str { vectors()["PUBLISHED_RESPONSE_KEY"].as_str().unwrap() }
pub fn published_response() -> &'static str { vectors()["PUBLISHED_RESPONSE"].as_str().unwrap() }
pub fn short_key_der_b64() -> &'static str { vectors()["SHORT_KEY_DER_B64"].as_str().unwrap() }
pub fn short_key_pin() -> &'static str { vectors()["SHORT_KEY_PIN"].as_str().unwrap() }
pub fn read_plain() -> &'static str { vectors()["READ_PLAIN"].as_str().unwrap() }
