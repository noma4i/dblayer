use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::owners::session::{OperationClass, PendingCall, SessionError};
use crate::runtime::Clock;
use crate::transport::encrypted::jwe::{random_token, seal_request, RequestPlain, RequestSession};
use crate::transport::encrypted::key::ServerKey;
use crate::transport::{CredentialSlot, EncryptedConfig};

pub(crate) struct SealConfig {
    key: ServerKey,
    audience: String,
    clock: Arc<dyn Clock>,
}

impl SealConfig {
    pub(crate) fn new(config: &EncryptedConfig, clock: Arc<dyn Clock>) -> Result<Self, SessionError> {
        let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| SessionError::Storage(error.to_string()))?;
        Ok(Self { key, audience: config.origin.trim_end_matches('/').into(), clock })
    }

    pub(crate) fn probe(&self, credentials: &dyn CredentialSlot, expected: Option<u64>, pending: &PendingCall) -> Result<u64, SessionError> {
        let session = if pending.class == OperationClass::Sessionless {
            None
        } else {
            let secret = credentials.get().map_err(|error| SessionError::Storage(error.to_string()))?.ok_or(SessionError::StateLost)?;
            let digest: String = Sha256::digest(secret.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
            if expected != crate::store::identity_of(&digest) { return Err(SessionError::StateLost); }
            Some(RequestSession { credential: secret, sequence: Some(i64::try_from(pending.sequence).map_err(|error| SessionError::Storage(error.to_string()))?) })
        };
        let mut body: Value = serde_json::from_str(&pending.body).map_err(|error| SessionError::Storage(error.to_string()))?;
        if body.pointer("/extensions/persistedQuery").is_some() {
            if let Some(object) = body.as_object_mut() { object.remove("query"); }
        }
        let plain = RequestPlain { audience: self.audience.clone(), kid: self.key.pin().into(), path: pending.path.clone(), request_id: uuid::Uuid::new_v4().to_string(), issued_at: self.clock.now().as_secs() as i64, response_key: random_token(), session, body };
        seal_request(&self.key, &plain).map(|token| token.len() as u64).map_err(|error| SessionError::Storage(error.to_string()))
    }
}

#[cfg(test)]
#[path = "session_seal_tests.rs"]
mod session_seal_tests;
