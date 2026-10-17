use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use zeroize::Zeroizing;
use super::*;

pub(super) fn encrypt(key: &[u8; 32], text: &[u8], iv: &[u8; 12]) -> Result<String, CableError> {
    let header = URL_SAFE_NO_PAD.encode(HEADER);
    let sealed = Aes256Gcm::new_from_slice(key).map_err(|_| CableError::Frame)?
        .encrypt(Nonce::from_slice(iv), Payload { msg: text, aad: header.as_bytes() }).map_err(|_| CableError::Frame)?;
    let (cipher, tag) = sealed.split_at(sealed.len() - 16);
    Ok([header, String::new(), URL_SAFE_NO_PAD.encode(iv), URL_SAFE_NO_PAD.encode(cipher), URL_SAFE_NO_PAD.encode(tag)].join("."))
}

pub(super) fn decrypt(key: &[u8; 32], token: &str) -> Result<Value, CableError> {
    let parts: Vec<_> = token.split('.').collect();
    let [header, wrapped, iv, cipher, tag] = parts[..] else { return Err(CableError::Frame) };
    if header != URL_SAFE_NO_PAD.encode(HEADER) || !wrapped.is_empty() { return Err(CableError::Frame); }
    let decode = |text| URL_SAFE_NO_PAD.decode(text).map_err(|_| CableError::Frame);
    let iv = decode(iv)?;
    let tag = decode(tag)?;
    if iv.len() != 12 || tag.len() != 16 { return Err(CableError::Frame); }
    let mut sealed = decode(cipher)?;
    sealed.extend(tag);
    let plain = Zeroizing::new(Aes256Gcm::new_from_slice(key).map_err(|_| CableError::Frame)?
        .decrypt(Nonce::from_slice(&iv), Payload { msg: &sealed, aad: header.as_bytes() }).map_err(|_| CableError::Frame)?);
    serde_json::from_slice(&plain).map_err(|_| CableError::Frame)
}
