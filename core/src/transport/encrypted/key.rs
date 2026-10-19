use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rsa::pkcs8::DecodePublicKey;
use rsa::traits::PublicKeyParts;
use rsa::RsaPublicKey;
use sha2::{Digest, Sha256};

pub(super) const MIN_KEY_BITS: usize = 3072;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("server key: {0}")]
    Untrusted(String),
}

#[derive(Clone)]
pub struct ServerKey {
    pub(super) public: RsaPublicKey,
    pin: String,
}

impl ServerKey {
    pub fn parse(spki_der_base64: &str, pin: &str) -> Result<Self, KeyError> {
        let der = STANDARD.decode(spki_der_base64).map_err(|error| KeyError::Untrusted(error.to_string()))?;
        let public = RsaPublicKey::from_public_key_der(&der).map_err(|error| KeyError::Untrusted(error.to_string()))?;
        let digest: String = Sha256::digest(&der).iter().map(|byte| format!("{byte:02x}")).collect();
        if digest != pin {
            return Err(KeyError::Untrusted("the key does not match the pin".into()));
        }
        if public.size() * 8 < MIN_KEY_BITS {
            return Err(KeyError::Untrusted(format!("the key has {} bits, the minimum is {MIN_KEY_BITS}", public.size() * 8)));
        }
        Ok(Self { public, pin: pin.to_string() })
    }

    pub fn pin(&self) -> &str {
        &self.pin
    }

}
