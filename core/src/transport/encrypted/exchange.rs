use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::jwe::{open_response, seal_request, RequestPlain, RequestSession};
use super::key::ServerKey;
use super::HttpExchange;
use crate::runtime::Clock;
use crate::transport::{SessionCode, TransportError};

const MAX_REQUEST_BYTES: usize = 256 * 1024;
const PERMANENT_HTTP_REFUSALS: [u16; 6] = [400, 404, 405, 413, 415, 422];

pub(crate) fn permanently_refused(error: &TransportError) -> bool {
    matches!(error, TransportError::Http { status, .. } if PERMANENT_HTTP_REFUSALS.contains(status))
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedConfig {
    pub origin: String,
    pub server_key: String,
    pub pin: String,
    pub endpoints: EncryptedEndpoints,
    pub protocol_label: String,
    pub cable_purpose: String,
    pub cable_channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedEndpoints {
    pub graphql: String,
    pub cable: String,
    pub logout: String,
    pub bootstrap: String,
    pub session_operations: std::collections::HashMap<String, EncryptedSessionOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct EncryptedSessionOperation {
    pub path: String,
    pub class: EncryptedOperationClass,
}

/// How the core treats a declared session operation: `Login` creates the credential, `Authenticated` signs with it, `Sessionless` sends no credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum EncryptedOperationClass {
    Login,
    Authenticated,
    Sessionless,
}

impl EncryptedConfig {
    /// The WebSocket address of the cable: the origin with `ws` or `wss` and the cable path. The platform only opens it.
    pub fn cable_url(&self) -> Result<String, TransportError> {
        let origin = self.origin.trim_end_matches('/');
        let rest = if let Some(rest) = origin.strip_prefix("https://") { format!("wss://{rest}") } else if let Some(rest) = origin.strip_prefix("http://") { format!("ws://{rest}") } else { return Err(TransportError::Decode("cable requires an HTTP origin".into())) };
        checked_path(&self.endpoints.cable)?;
        Ok(format!("{rest}{}", self.endpoints.cable))
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        for path in [&self.endpoints.graphql, &self.endpoints.cable, &self.endpoints.logout, &self.endpoints.bootstrap].into_iter().chain(self.endpoints.session_operations.values().map(|operation| &operation.path)) {
            checked_path(path)?;
        }
        if [&self.protocol_label, &self.cable_purpose, &self.cable_channel].into_iter().any(|value| value.trim().is_empty() || value.chars().any(char::is_control)) {
            return Err(TransportError::Decode("encrypted protocol labels must be nonempty and contain no control characters".into()));
        }
        if self.endpoints.session_operations.keys().any(|name| name.trim().is_empty() || name == "logout") {
            return Err(TransportError::Decode("session operation names must be nonempty and exclude logout".into()));
        }
        Ok(())
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub sequence: Option<i64>,
    pub body: Value,
}

/// The exchange of the executor of Session: it seals a request and posts it once. It keeps no state and no retry, the owner Session keeps both.
pub struct Exchange {
    config: EncryptedConfig,
    audience: String,
    key: ServerKey,
    http: Arc<dyn HttpExchange>,
    clock: Arc<dyn Clock>,
}

pub(crate) fn digest_of(credential: &str) -> String {
    Sha256::digest(credential.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn checked_path(path: &str) -> Result<(), TransportError> {
    if !path.starts_with('/') || path.starts_with("//") || !path.is_ascii() || path.contains(['?', '#', '\\']) || path.bytes().any(|byte| byte <= 32 || byte == 127) {
        return Err(TransportError::Decode("encrypted endpoints require encoded absolute paths without query or fragment".into()));
    }
    let bytes = path.as_bytes();
    if bytes.iter().enumerate().any(|(index, byte)| *byte == b'%' && !bytes.get(index + 1..index + 3).is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))) {
        return Err(TransportError::Decode("encrypted endpoints require valid percent encoding".into()));
    }
    if path.to_ascii_lowercase().replace("%2e", ".").split('/').any(|part| matches!(part, "." | "..")) {
        return Err(TransportError::Decode("encrypted endpoints cannot contain dot segments".into()));
    }
    Ok(())
}

pub(super) fn session_failure(error: crate::owners::session::SessionError) -> TransportError {
    use crate::owners::session::SessionError as Failed;
    match error {
        Failed::StateLost => TransportError::Session { code: SessionCode::StateLost, detail: "the stored credential does not match the stored session state".into() },
        Failed::Revoked => TransportError::Session { code: SessionCode::SessionEnded, detail: "the session was ended by the server".into() },
        Failed::NoCredential => TransportError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
        Failed::SequenceConflict => TransportError::Session { code: SessionCode::SequenceConflict, detail: "the sequence of the request conflicts with the server".into() },
        Failed::Invalid => TransportError::Decode("the answer does not confirm the sequence".into()),
        Failed::Unauthorized => TransportError::Http { status: 401, body: String::new() },
        Failed::Security { code, detail } => TransportError::Security { code, detail },
        Failed::Http { status } => TransportError::Http { status, body: "the request was refused".into() },
        Failed::TooLarge { size, limit } => TransportError::RequestTooLarge { size, limit },
        Failed::Rejected(crate::owners::contract::RejectReason::Storage(detail)) => TransportError::Storage(detail),
        Failed::Rejected(crate::owners::contract::RejectReason::NoSession | crate::owners::contract::RejectReason::Closed) => TransportError::Interrupted("the session ended while its answer was pending".into()),
        Failed::Rejected(reason @ (crate::owners::contract::RejectReason::StaleGeneration { .. } | crate::owners::contract::RejectReason::Invalid)) => TransportError::Internal(format!("the request was refused by the store: {reason:?}")),
        Failed::Session(code) => TransportError::Session { code, detail: code.name().to_string() },
        Failed::SessionExists => TransportError::Session { code: SessionCode::AlreadyAuthenticated, detail: "the database has a session".into() },
        Failed::Storage(detail) => TransportError::Storage(detail),
        Failed::Interrupted | Failed::NoSession | Failed::Closed => TransportError::Interrupted("the session ended while its answer was pending".into()),
    }
}

/// One request of `send_once`.
pub(crate) struct SendOnce<'a> {
    pub credential: Option<&'a str>,
    pub request_id: &'a str,
    pub response_key: &'a str,
    pub sequence: Option<i64>,
    pub path: &'a str,
    pub body: &'a Value,
    pub purpose: crate::transport::NetworkPurpose,
    pub wire: crate::transport::WireIds,
}

impl Exchange {
    pub fn new(config: &EncryptedConfig, http: Arc<dyn HttpExchange>, clock: Arc<dyn Clock>) -> Result<Self, TransportError> {
        config.validate()?;
        let audience = config.origin.trim_end_matches('/').to_string();
        if !super::origin::allowed(&audience) {
            return Err(TransportError::Decode(format!("the origin must be https, or http on a development host, without a path: {}", config.origin)));
        }
        let key = ServerKey::parse(&config.server_key, &config.pin).map_err(|error| TransportError::Decode(error.to_string()))?;
        Ok(Self { config: config.clone(), audience, key, http, clock })
    }

    pub(crate) fn config(&self) -> &EncryptedConfig { &self.config }

    pub(crate) fn graphql_path(&self) -> &str { &self.config.endpoints.graphql }

    pub(crate) fn logout_path(&self) -> &str { &self.config.endpoints.logout }

    /// One request of the executor of Session: seals `body` for `path` and posts it once. No retry and no state: the owner Session keeps both.
    pub(crate) async fn send_once(&self, request: SendOnce<'_>) -> Result<Answer, TransportError> {
        let SendOnce { credential, request_id, response_key, sequence, path, body, purpose, wire } = request;
        checked_path(path)?;
        let session = credential.map(|credential| RequestSession { credential: credential.into(), sequence });
        let token = self.request_token(session, request_id, response_key, path, body)?;
        self.post(path, token, request_id, response_key, (purpose, wire)).await
    }

    /// The declared session operations and `logout`, as the owner Session takes them.
    pub(crate) fn session_operations(&self) -> Vec<crate::owners::session::SessionOperation> {
        use crate::owners::session::OperationClass;
        let declared = self.config.endpoints.session_operations.iter().map(|(name, operation)| crate::owners::session::SessionOperation { name: name.clone(), path: operation.path.clone(), class: match operation.class { EncryptedOperationClass::Login => OperationClass::Login, EncryptedOperationClass::Authenticated => OperationClass::Authenticated, EncryptedOperationClass::Sessionless => OperationClass::Sessionless } });
        declared.chain(std::iter::once(crate::owners::session::SessionOperation { name: "logout".into(), path: self.config.endpoints.logout.clone(), class: OperationClass::Logout })).collect()
    }


    fn request_token(&self, session: Option<RequestSession>, request_id: &str, response_key: &str, path: &str, body: &Value) -> Result<String, TransportError> {
        let plain = RequestPlain {
            audience: self.audience.clone(),
            kid: self.key.pin().to_string(),
            path: path.to_string(),
            request_id: request_id.to_string(),
            issued_at: self.clock.now().as_secs() as i64,
            response_key: response_key.to_string(),
            session,
            body: body.clone(),
        };
        let token = seal_request(&self.key, &plain).map_err(|error| TransportError::Decode(error.to_string()))?;
        if token.len() > MAX_REQUEST_BYTES {
            return Err(TransportError::RequestTooLarge { size: token.len() as u64, limit: MAX_REQUEST_BYTES as u64 });
        }
        Ok(token)
    }


    async fn post(&self, path: &str, token: String, request_id: &str, response_key: &str, (purpose, wire): (crate::transport::NetworkPurpose, crate::transport::WireIds)) -> Result<Answer, TransportError> {
        crate::lock::assert_no_library_lock("HttpExchange::post");
        let reply = self.http.post(path, token, purpose, wire).await?;
        if reply.status != 200 {
            return Err(TransportError::Http { status: reply.status, body: reply.body });
        }
        let opened = open_response(&reply.body, response_key).map_err(|error| TransportError::Decode(error.to_string()))?;
        if opened.request_id != request_id {
            return Err(TransportError::Decode("the response belongs to another request".into()));
        }
        Ok(Answer { sequence: opened.sequence, body: opened.body })
    }
}
