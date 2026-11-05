use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;
use super::*;

/// The keys of one cable connection. It holds no counter: the caller gives the counter of a frame to `seal` and reads it back from `open`.
pub struct CableKeys {
    keys: keys::Keys,
    challenge_digest: String,
}

impl CableKeys {
    pub fn agree(config: &EncryptedConfig, challenge: &Value, credential: &str) -> Result<(Self, String), CableError> {
        let mut private = Zeroizing::new([0; 32]);
        let mut response = Zeroizing::new([0; 32]);
        OsRng.fill_bytes(&mut *private);
        OsRng.fill_bytes(&mut *response);
        Self::with_entropy(config, challenge, credential, *private, *response)
    }

    pub(super) fn with_entropy(config: &EncryptedConfig, challenge: &Value, credential: &str, private: [u8; 32], response: [u8; 32]) -> Result<(Self, String), CableError> {
        let (keys, hello) = keys::agree(config, challenge, credential, private, response)?;
        let challenge_digest = Sha256::digest(keys::decode(&challenge["challenge"])?).iter().map(|byte| format!("{byte:02x}")).collect();
        Ok((Self { keys, challenge_digest }, hello))
    }

    /// Encrypts `body` as the frame number `counter`.
    pub fn seal(&self, counter: u64, mut body: Value) -> Result<String, CableError> {
        if counter >= FRAME_BUDGET { return Err(CableError::Exhausted); }
        let fields = body.as_object_mut().ok_or(CableError::Frame)?;
        if fields.contains_key("n") { return Err(CableError::Frame); }
        fields.insert("n".into(), counter.into());
        if !schema::valid(&body, false) { return Err(CableError::Frame); }
        let plain = Zeroizing::new(serde_json::to_vec(&body).map_err(|_| CableError::Frame)?);
        if plain.len() > CLIENT_BYTES { return Err(CableError::TooLarge); }
        let mut iv = [0; 12];
        OsRng.fill_bytes(&mut iv);
        let token = frame::encrypt(&self.keys.send, &plain, &iv)?;
        if token.len() > CLIENT_BYTES { return Err(CableError::TooLarge); }
        Ok(token)
    }

    /// Decrypts a frame: its number and its body. The number is checked by the owner of the counters.
    pub fn open(&self, token: &str) -> Result<(u64, Value), CableError> {
        if token.len() > SERVER_BYTES { return Err(CableError::TooLarge); }
        let body = frame::decrypt(&self.keys.receive, token)?;
        if !schema::valid(&body, true) { return Err(CableError::Frame); }
        let counter = body["n"].as_u64().ok_or(CableError::Frame)?;
        if counter >= FRAME_BUDGET { return Err(CableError::Exhausted); }
        Ok((counter, body))
    }

    /// The first frame of a connection: number 0, `ready`, the digest of the challenge.
    pub fn confirms(&self, counter: u64, body: &Value) -> Result<(), CableError> {
        if counter == 0 && body["t"] == "ready" && body["challengeDigest"] == self.challenge_digest { Ok(()) } else { Err(CableError::Challenge) }
    }
}

pub struct CableLink {
    keys: Option<CableKeys>,
    pub(super) sent: u64,
    pub(super) received: u64,
}

impl CableLink {
    pub fn open(config: &EncryptedConfig, challenge: &Value, credential: &str) -> Result<(Self, String), CableError> {
        let (keys, hello) = CableKeys::agree(config, challenge, credential)?;
        Ok((Self { keys: Some(keys), sent: 0, received: 0 }, hello))
    }

    #[cfg(test)]
    pub(super) fn with_entropy(config: &EncryptedConfig, challenge: &Value, credential: &str, private: [u8; 32], response: [u8; 32]) -> Result<(Self, String), CableError> {
        let (keys, hello) = CableKeys::with_entropy(config, challenge, credential, private, response)?;
        Ok((Self { keys: Some(keys), sent: 0, received: 0 }, hello))
    }

    pub fn seal(&mut self, body: Value) -> Result<String, CableError> {
        let keys = self.keys.as_ref().ok_or(CableError::Closed)?;
        let sealed = keys.seal(self.sent, body);
        if matches!(sealed, Err(CableError::Exhausted)) { self.close(); }
        self.sent += u64::from(sealed.is_ok());
        sealed
    }

    pub fn receive(&mut self, token: &str) -> Result<Value, CableError> {
        let result = self.open_frame(token);
        if result.is_err() { self.close(); }
        result
    }

    fn open_frame(&mut self, token: &str) -> Result<Value, CableError> {
        let keys = self.keys.as_ref().ok_or(CableError::Closed)?;
        let (counter, body) = keys.open(token)?;
        if counter != self.received { return Err(CableError::Sequence); }
        if self.received == 0 { keys.confirms(counter, &body)?; }
        if self.received != 0 && body["t"] == "ready" { return Err(CableError::Frame); }
        self.received += 1;
        if body["t"] == "closing" { self.close(); }
        Ok(body)
    }

    pub fn close(&mut self) { self.keys.take(); }
}
