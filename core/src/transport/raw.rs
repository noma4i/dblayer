use std::collections::HashMap;

use super::{GraphQLRequest, NetworkPurpose, SecurityCode, TransportError};

pub const CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const REQUEST_TIMEOUT_MS: u64 = 30_000;

/// What a platform executor observed when a call produced no HTTP answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum RawFailureKind {
    Cancelled,
    Tls,
    Unreachable,
    Timeout,
    Io,
    Protocol,
}

/// `code` is the platform error code, or the HTTP status of a refused cable upgrade for `Protocol`.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct RawFailure {
    pub kind: RawFailureKind,
    pub code: Option<i64>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum RawOutcome {
    Response { status: u16, etag: Option<String>, body: String },
    Failed { failure: RawFailure },
}

impl RawOutcome {
    pub fn response(status: u16, body: impl Into<String>) -> Self { Self::Response { status, etag: None, body: body.into() } }
    pub fn failed(kind: RawFailureKind, message: impl Into<String>) -> Self { Self::Failed { failure: RawFailure { kind, code: None, message: message.into() } } }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct HttpRequest {
    pub url: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub connect_ms: u64,
    pub request_ms: u64,
    pub purpose: NetworkPurpose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeKind {
    Query,
    Mutation,
    Post,
}

/// The one place where a raw platform failure gets its class. A mutation that may have reached the server is `Interrupted`, a call that never left the device is `Network`.
pub fn classify(failure: &RawFailure, kind: ExchangeKind) -> TransportError {
    let detail = failure.message.clone();
    match failure.kind {
        RawFailureKind::Cancelled => TransportError::Interrupted(detail),
        RawFailureKind::Tls => TransportError::Security { code: SecurityCode::TlsTrust, detail },
        RawFailureKind::Unreachable => TransportError::Network(detail),
        RawFailureKind::Protocol if refused_status(failure).is_some() => TransportError::Http { status: refused_status(failure).unwrap_or_default(), body: detail },
        RawFailureKind::Timeout | RawFailureKind::Io | RawFailureKind::Protocol => if kind == ExchangeKind::Mutation { TransportError::Interrupted(detail) } else { TransportError::Network(detail) },
    }
}

fn refused_status(failure: &RawFailure) -> Option<u16> { failure.code.and_then(|code| u16::try_from(code).ok()).filter(|status| (100..=599).contains(status)) }

/// A GraphQL answer is its body on 2xx and `Http` otherwise.
pub fn graphql_text(outcome: RawOutcome, kind: ExchangeKind) -> Result<String, TransportError> {
    match outcome {
        RawOutcome::Response { status, body, .. } if (200..300).contains(&status) => Ok(body),
        RawOutcome::Response { status, body, .. } => Err(TransportError::Http { status, body }),
        RawOutcome::Failed { failure } => Err(classify(&failure, kind)),
    }
}

pub fn graphql_endpoint(endpoint: &str) -> Result<String, TransportError> {
    let rest = endpoint.strip_prefix("https://").or_else(|| endpoint.strip_prefix("http://"));
    let authority = rest.and_then(|rest| rest.split(['/', '?', '#']).next()).unwrap_or_default();
    if authority.is_empty() || authority.starts_with(':') || endpoint.contains(char::is_whitespace) {
        return Err(TransportError::Decode(format!("the endpoint must be an http or https address with a host: {endpoint}")));
    }
    Ok(endpoint.to_string())
}

pub fn graphql_call(endpoint: &str, request: &GraphQLRequest) -> HttpRequest {
    let body = serde_json::json!({"query": request.document, "operationName": request.operation_name, "variables": request.variables});
    HttpRequest {
        url: endpoint.to_string(),
        method: "POST".into(),
        headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
        body: body.to_string(),
        connect_ms: CONNECT_TIMEOUT_MS,
        request_ms: REQUEST_TIMEOUT_MS,
        purpose: request.purpose,
    }
}

pub fn encrypted_call(origin: &str, path: &str, body: String, purpose: NetworkPurpose) -> Result<HttpRequest, TransportError> {
    super::encrypted::checked_path(path)?;
    Ok(HttpRequest {
        url: format!("{}{path}", origin.trim_end_matches('/')),
        method: "POST".into(),
        headers: HashMap::from([("Content-Type".into(), "application/jose".into()), ("Accept".into(), String::new()), ("Accept-Language".into(), String::new())]),
        body,
        connect_ms: CONNECT_TIMEOUT_MS,
        request_ms: REQUEST_TIMEOUT_MS,
        purpose,
    })
}

#[cfg(test)]
#[path = "raw_tests.rs"]
mod raw_tests;
