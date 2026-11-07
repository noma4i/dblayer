use std::collections::HashMap;

use super::{GraphQLRequest, NetworkPurpose, SecurityCode, TransportError};

pub const CONNECT_TIMEOUT_MS: u64 = 10_000;
pub const REQUEST_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawFailureKind {
    Cancelled,
    Tls,
    Unreachable,
    Timeout,
    Io,
}

/// The raw facts of a call that produced no HTTP answer: the platform error domain (a `NSError` domain, or the exception class name), its code and its text. The core alone derives the kind.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct RawFailure {
    pub domain: String,
    pub code: Option<i64>,
    pub message: String,
}

const URL_ERROR_DOMAIN: &str = "NSURLErrorDomain";
const URL_CANCELLED: i64 = -999;
const URL_TIMED_OUT: i64 = -1001;
const URL_TLS: [i64; 7] = [-1200, -1201, -1202, -1203, -1204, -1205, -1206];
const URL_UNREACHABLE: [i64; 6] = [-1003, -1004, -1006, -1009, -1018, -1020];
const CANCELLED_CLASSES: [&str; 4] = ["Swift.CancellationError", "java.util.concurrent.CancellationException", "kotlinx.coroutines.JobCancellationException", "kotlinx.coroutines.TimeoutCancellationException"];
const TLS_CLASSES: [&str; 2] = ["javax.net.ssl.SSLHandshakeException", "javax.net.ssl.SSLPeerUnverifiedException"];
const UNREACHABLE_CLASSES: [&str; 3] = ["java.net.UnknownHostException", "java.net.ConnectException", "java.net.NoRouteToHostException"];
const TIMEOUT_CLASSES: [&str; 2] = ["java.net.SocketTimeoutException", "java.io.InterruptedIOException"];
const CALL_CANCELLED_CLASS: &str = "java.io.IOException";
const CALL_CANCELLED_TEXT: &str = "Canceled";

impl RawFailure {
    /// The one table of known platform errors. An error it does not know is `Io`.
    pub fn kind(&self) -> RawFailureKind {
        let class = self.domain.as_str();
        let code = self.code.unwrap_or_default();
        if class == URL_ERROR_DOMAIN && self.code.is_some() {
            return match code {
                URL_CANCELLED => RawFailureKind::Cancelled,
                URL_TIMED_OUT => RawFailureKind::Timeout,
                code if URL_TLS.contains(&code) => RawFailureKind::Tls,
                code if URL_UNREACHABLE.contains(&code) => RawFailureKind::Unreachable,
                _ => RawFailureKind::Io,
            };
        }
        if CANCELLED_CLASSES.contains(&class) || (class == CALL_CANCELLED_CLASS && self.message.eq_ignore_ascii_case(CALL_CANCELLED_TEXT)) { RawFailureKind::Cancelled }
        else if TLS_CLASSES.contains(&class) { RawFailureKind::Tls }
        else if UNREACHABLE_CLASSES.contains(&class) { RawFailureKind::Unreachable }
        else if TIMEOUT_CLASSES.contains(&class) { RawFailureKind::Timeout }
        else { RawFailureKind::Io }
    }

    #[cfg(test)]
    pub fn of_kind(kind: RawFailureKind, message: impl Into<String>) -> Self {
        let (domain, code) = match kind {
            RawFailureKind::Cancelled => ("Swift.CancellationError", None),
            RawFailureKind::Tls => (URL_ERROR_DOMAIN, Some(-1202)),
            RawFailureKind::Unreachable => (URL_ERROR_DOMAIN, Some(-1009)),
            RawFailureKind::Timeout => (URL_ERROR_DOMAIN, Some(URL_TIMED_OUT)),
            RawFailureKind::Io => ("java.io.EOFException", None),
        };
        Self { domain: domain.into(), code, message: message.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum RawOutcome {
    Response { status: u16, etag: Option<String>, body: String },
    Failed { failure: RawFailure },
}

impl RawOutcome {
    pub fn response(status: u16, body: impl Into<String>) -> Self { Self::Response { status, etag: None, body: body.into() } }
    #[cfg(test)]
    pub fn failed(kind: RawFailureKind, message: impl Into<String>) -> Self { Self::Failed { failure: RawFailure::of_kind(kind, message) } }
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
    match failure.kind() {
        RawFailureKind::Cancelled => TransportError::Interrupted(detail),
        RawFailureKind::Tls => TransportError::Security { code: SecurityCode::TlsTrust, detail },
        RawFailureKind::Unreachable => TransportError::Network(detail),
        RawFailureKind::Timeout | RawFailureKind::Io => if kind == ExchangeKind::Mutation { TransportError::Interrupted(detail) } else { TransportError::Network(detail) },
    }
}

/// A cable upgrade the server answered with a status other than 101 is `Http`, any other failure is classified by its raw facts.
pub fn classify_upgrade(failure: &RawFailure, handshake_status: Option<u16>) -> TransportError {
    match handshake_status.filter(|status| (100..=599).contains(status) && *status != 101) {
        Some(status) => TransportError::Http { status, body: failure.message.clone() },
        None => classify(failure, ExchangeKind::Post),
    }
}

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

pub fn user_agent(application: Option<String>) -> String {
    let token = concat!("DBlayer/", env!("CARGO_PKG_VERSION"));
    match application.filter(|value| !value.is_empty()) {
        Some(value) => format!("{value} {token}"),
        None => token.into(),
    }
}

/// The headers of one request: the app's, then the library's own (they win), then the `User-Agent` with the library token. Field names compare without case.
pub fn merge_headers(app: HashMap<String, String>, own: HashMap<String, String>) -> HashMap<String, String> {
    let mut merged: HashMap<String, String> = HashMap::new();
    for (field, value) in app.into_iter().chain(own) {
        merged.retain(|known, _| !known.eq_ignore_ascii_case(&field));
        merged.insert(field, value);
    }
    let application = merged.iter().find(|(field, _)| field.eq_ignore_ascii_case("user-agent")).map(|(_, value)| value.clone());
    merged.retain(|field, _| !field.eq_ignore_ascii_case("user-agent"));
    merged.insert("User-Agent".into(), user_agent(application));
    merged
}

pub fn graphql_call(endpoint: &str, request: &GraphQLRequest, app_headers: HashMap<String, String>) -> HttpRequest {
    let body = serde_json::json!({"query": request.document, "operationName": request.operation_name, "variables": request.variables});
    HttpRequest {
        url: endpoint.to_string(),
        method: "POST".into(),
        headers: merge_headers(app_headers, HashMap::from([("Content-Type".into(), "application/json".into())])),
        body: body.to_string(),
        connect_ms: CONNECT_TIMEOUT_MS,
        request_ms: REQUEST_TIMEOUT_MS,
        purpose: request.purpose,
    }
}

pub fn encrypted_call(origin: &str, path: &str, body: String, purpose: NetworkPurpose, app_headers: HashMap<String, String>) -> Result<HttpRequest, TransportError> {
    super::encrypted::checked_path(path)?;
    Ok(HttpRequest {
        url: format!("{}{path}", origin.trim_end_matches('/')),
        method: "POST".into(),
        headers: merge_headers(app_headers, HashMap::from([("Content-Type".into(), "application/jose".into()), ("Accept".into(), String::new()), ("Accept-Language".into(), String::new())])),
        body,
        connect_ms: CONNECT_TIMEOUT_MS,
        request_ms: REQUEST_TIMEOUT_MS,
        purpose,
    })
}

#[cfg(test)]
#[path = "raw_tests.rs"]
mod raw_tests;
