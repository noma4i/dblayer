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
pub fn refusal_name(_body: &Value) -> Option<String> { None }

/// The code of a body; an unknown name is `SessionCode::Other`, `refusal_name` keeps the name.
pub fn refusal(_body: &Value) -> Option<SessionCode> { None }

pub fn classify(_outcome: &WireOutcome, _sequenced: bool) -> AnswerKind { AnswerKind::Unknown }
