//! The one reader of a server answer: what the answer means for the owner of the call (`specs/machines/wire.md`).

use serde_json::Value;

use super::SessionCode;

/// What came back from one send of a pair.
#[derive(Debug, Clone, PartialEq)]
pub enum WireOutcome {
    /// The send failed before the request left (DNS, connect, TLS).
    Unsent(String),
    /// The request left and no answer came (cut, read timeout).
    Lost,
    /// HTTP 2xx with a body nobody can read.
    Unreadable,
    /// An HTTP status other than 2xx.
    Status(u16),
    /// HTTP 2xx with an opened answer: its `sequence` and body.
    Answer { sequence: Option<i64>, body: Value },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AnswerKind {
    Transient { reason: String },
    Unknown,
    Revoked,
    Refused { code: SessionCode },
    DocumentMissing,
    Answered { sequence: Option<i64> },
    NotConsumed { code: SessionCode },
    Permanent { status: Option<u16> },
}

/// The name of the refusal code of a body: `error` first, then `errors[].extensions.code`, the first wins.
pub fn refusal_name(body: &Value) -> Option<String> {
    body.get("error").and_then(Value::as_str).into_iter()
        .chain(body.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(|error| error.get("extensions")?.get("code")?.as_str()))
        .next()
        .map(str::to_string)
}

/// The code of a body; an unknown name is `SessionCode::Other`, `refusal_name` keeps the name.
pub fn refusal(body: &Value) -> Option<SessionCode> {
    refusal_name(body).map(|name| SessionCode::of(&name))
}

const DOCUMENT_MISSING: &str = "PERSISTED_QUERY_NOT_FOUND";
pub(crate) const RETRYABLE_CODES: [&str; 4] = ["INTERNAL_SERVER_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE", "DATABASE_ERROR"];

fn answered(sequence: Option<i64>, body: &Value, sequenced: bool) -> AnswerKind {
    let name = refusal_name(body);
    let name = name.as_deref();
    if body.get("revoked") == Some(&Value::Bool(true)) || name == Some("SESSION_ENDED") {
        return AnswerKind::Revoked;
    }
    match name {
        Some(DOCUMENT_MISSING) => return AnswerKind::DocumentMissing,
        Some("UNAUTHENTICATED") => return AnswerKind::Refused { code: SessionCode::Unauthenticated },
        Some("REPLAYED_REQUEST") => return if sequenced { AnswerKind::Refused { code: SessionCode::ReplayedRequest } } else { AnswerKind::Transient { reason: "REPLAYED_REQUEST".into() } },
        Some("SEQUENCE_CONFLICT") if sequenced => return AnswerKind::Refused { code: SessionCode::SequenceConflict },
        _ => {}
    }
    if sequenced {
        return match (sequence, name) {
            (Some(_), _) => AnswerKind::Answered { sequence },
            (None, Some(name)) => AnswerKind::NotConsumed { code: SessionCode::of(name) },
            (None, None) => AnswerKind::NotConsumed { code: SessionCode::Other },
        };
    }
    match name {
        Some(name) if RETRYABLE_CODES.contains(&name) => AnswerKind::Transient { reason: name.into() },
        _ => AnswerKind::Answered { sequence },
    }
}

pub fn classify(outcome: &WireOutcome, sequenced: bool) -> AnswerKind {
    match outcome {
        WireOutcome::Unsent(reason) => AnswerKind::Transient { reason: reason.clone() },
        WireOutcome::Lost if sequenced => AnswerKind::Unknown,
        WireOutcome::Lost => AnswerKind::Transient { reason: "no answer".into() },
        WireOutcome::Unreadable if sequenced => AnswerKind::Unknown,
        WireOutcome::Unreadable => AnswerKind::Permanent { status: None },
        WireOutcome::Status(401) => AnswerKind::Refused { code: SessionCode::Unauthenticated },
        WireOutcome::Status(408 | 425 | 429 | 500..=599) => AnswerKind::Transient { reason: "http status".into() },
        WireOutcome::Status(status) => AnswerKind::Permanent { status: Some(*status) },
        WireOutcome::Answer { sequence, body } => answered(*sequence, body, sequenced),
    }
}

/// The failure of a body the server answered with a retryable code on a pair without a `sequence`; `None` when the answer is final.
pub fn transient_failure(body: &Value) -> Option<super::TransportError> {
    match classify(&WireOutcome::Answer { sequence: None, body: body.clone() }, false) {
        AnswerKind::Transient { reason } => Some(super::TransportError::Graphql { messages: vec![reason], extensions: None }),
        _ => None,
    }
}
