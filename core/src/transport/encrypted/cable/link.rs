use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;
use super::*;

pub struct CableLink {
    keys: Option<keys::Keys>,
    challenge_digest: String,
    pub(super) sent: u64,
    pub(super) received: u64,
}

impl CableLink {
    pub fn open(config: &EncryptedConfig, challenge: &Value, credential: &str) -> Result<(Self, String), CableError> {
        let mut private = Zeroizing::new([0; 32]);
        let mut response = Zeroizing::new([0; 32]);
        OsRng.fill_bytes(&mut *private);
        OsRng.fill_bytes(&mut *response);
        Self::with_entropy(config, challenge, credential, *private, *response)
    }

    pub(super) fn with_entropy(config: &EncryptedConfig, challenge: &Value, credential: &str, private: [u8; 32], response: [u8; 32]) -> Result<(Self, String), CableError> {
        let (keys, hello) = keys::agree(config, challenge, credential, private, response)?;
        let challenge_digest = Sha256::digest(keys::decode(&challenge["challenge"])?).iter().map(|byte| format!("{byte:02x}")).collect();
        Ok((Self { keys: Some(keys), challenge_digest, sent: 0, received: 0 }, hello))
    }

    pub fn seal(&mut self, mut body: Value) -> Result<String, CableError> {
        if self.keys.is_none() { return Err(CableError::Closed); }
        if self.sent >= FRAME_BUDGET { self.close(); return Err(CableError::Exhausted); }
        let fields = body.as_object_mut().ok_or(CableError::Frame)?;
        if fields.contains_key("n") { return Err(CableError::Frame); }
        fields.insert("n".into(), self.sent.into());
        if !schema::valid(&body, false) { return Err(CableError::Frame); }
        let plain = Zeroizing::new(serde_json::to_vec(&body).map_err(|_| CableError::Frame)?);
        if plain.len() > CLIENT_BYTES { return Err(CableError::TooLarge); }
        let mut iv = [0; 12];
        OsRng.fill_bytes(&mut iv);
        let token = frame::encrypt(&self.keys.as_ref().ok_or(CableError::Closed)?.send, &plain, &iv)?;
        if token.len() > CLIENT_BYTES { return Err(CableError::TooLarge); }
        self.sent += 1;
        Ok(token)
    }

    pub fn receive(&mut self, token: &str) -> Result<Value, CableError> {
        let result = self.open_frame(token);
        if result.is_err() { self.close(); }
        result
    }

    fn open_frame(&mut self, token: &str) -> Result<Value, CableError> {
        let keys = self.keys.as_ref().ok_or(CableError::Closed)?;
        if self.received >= FRAME_BUDGET { return Err(CableError::Exhausted); }
        if token.len() > SERVER_BYTES { return Err(CableError::TooLarge); }
        let body = frame::decrypt(&keys.receive, token)?;
        if !schema::valid(&body, true) { return Err(CableError::Frame); }
        if body["n"].as_u64() != Some(self.received) { return Err(CableError::Sequence); }
        if self.received == 0 && (body["t"] != "ready" || body["challengeDigest"] != self.challenge_digest) {
            return Err(CableError::Challenge);
        }
        if self.received != 0 && body["t"] == "ready" { return Err(CableError::Frame); }
        self.received += 1;
        if body["t"] == "closing" { self.close(); }
        Ok(body)
    }

    pub fn close(&mut self) { self.keys.take(); }
}
