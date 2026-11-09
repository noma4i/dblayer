use crate::transport::{classify_answer, refusal, refusal_name, response_from_answer, Answer, AnswerKind, SessionCode, TransportError, WireOutcome};
use serde_json::{json, Value};

/// The classified kind of an answered body reduced to what the owner of the call acts on.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Unauthenticated,
    Session(SessionCode),
    Answered,
    Transient,
    Permanent,
}

fn from_kind(kind: &AnswerKind) -> Verdict {
    match kind {
        AnswerKind::Refused { code: SessionCode::Unauthenticated } => Verdict::Unauthenticated,
        AnswerKind::Refused { code } | AnswerKind::NotConsumed { code } | AnswerKind::PermanentSession { code } => Verdict::Session(*code),
        AnswerKind::Transient { .. } => Verdict::Transient,
        AnswerKind::Answered { .. } => Verdict::Answered,
        AnswerKind::Permanent { .. } | AnswerKind::PermanentGraphql | AnswerKind::DocumentMissing | AnswerKind::Unknown | AnswerKind::Revoked => Verdict::Permanent,
    }
}

/// wire.md answer kind table, column without `sequence`: the reader types session codes and 401, passes a GraphQL error through for the owner, and a passed code of the transient set (REPLAYED_REQUEST, the retryable list) is a Transient the owner retries by its policy (D-220).
const TRANSIENT_CODES: [&str; 5] = ["REPLAYED_REQUEST", "INTERNAL_SERVER_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE", "DATABASE_ERROR"];

fn from_reader(body: &Value) -> Verdict {
    match response_from_answer(Answer { sequence: None, body: body.clone() }) {
        Err(TransportError::Http { status: 401, .. }) => Verdict::Unauthenticated,
        Err(TransportError::Session { code, .. }) => Verdict::Session(code),
        Err(_) => Verdict::Permanent,
        Ok(response) => match response.errors.first().and_then(|error| error.code()) {
            Some(code) if TRANSIENT_CODES.contains(&code) => Verdict::Transient,
            _ => Verdict::Answered,
        },
    }
}

/// The refusal code of a body typed as the one classifier must type it: a session code only for a name of `SessionCode::from_wire`, an unknown name a GraphQL error.
#[derive(Debug, PartialEq, Eq)]
enum Code {
    Session(SessionCode),
    Graphql(String),
    None,
}

fn canonical(body: &Value) -> Code {
    match refusal_name(body) {
        None => Code::None,
        Some(name) => match SessionCode::from_wire(&name) {
            Some(code) => Code::Session(code),
            None => Code::Graphql(name),
        },
    }
}

fn from_refusal(body: &Value) -> Code {
    match refusal(body) {
        Some(code) => Code::Session(code),
        None => Code::None,
    }
}

fn reader_code(body: &Value) -> Code {
    match response_from_answer(Answer { sequence: None, body: body.clone() }) {
        Err(TransportError::Http { status: 401, .. }) => Code::Session(SessionCode::Unauthenticated),
        Err(TransportError::Session { code, .. }) => Code::Session(code),
        Ok(response) => match response.errors.first().and_then(|error| error.code()) {
            Some(code) => Code::Graphql(code.to_string()),
            None => Code::None,
        },
        Err(_) => Code::None,
    }
}

/// P-WIRE-01: the reader that turns an answered body into an outcome agrees, kind for kind, with the one classifier `classify`.
#[test]
fn wire_prop_kind_same_on_every_path() {
    let bodies = [
        json!({ "error": "UNAUTHENTICATED" }),
        json!({ "error": "REPLAYED_REQUEST" }),
        json!({ "error": "RATE_LIMITED" }),
        json!({ "error": "SEQUENCE_CONFLICT" }),
        json!({ "error": "INTERNAL_SERVER_ERROR" }),
        json!({ "error": "UNKNOWN_NAME" }),
        json!({ "errors": [{ "message": "boom", "extensions": { "code": "UNAUTHENTICATED" } }] }),
        json!({ "data": { "x": 1 } }),
    ];
    for body in bodies {
        let kind = classify_answer(&WireOutcome::Answer { sequence: None, body: body.clone() }, false, true);
        assert_eq!(from_kind(&kind), from_reader(&body), "classify {kind:?} and the reader disagree on {body}");
    }
}

/// The refusal code of a body is read by one function and every owner path yields the same session-or-GraphQL result.
#[test]
fn wire_one_refusal_classifier() {
    let bodies = [
        json!({ "error": "UNAUTHENTICATED" }),
        json!({ "error": "SEQUENCE_CONFLICT" }),
        json!({ "error": "BLOCKED" }),
        json!({ "errors": [{ "message": "boom", "extensions": { "code": "SEQUENCE_CONFLICT" } }] }),
        json!({ "errors": [{ "message": "boom", "extensions": { "code": "BLOCKED" } }] }),
        json!({ "error": "UNAUTHENTICATED", "errors": [{ "message": "boom", "extensions": { "code": "SEQUENCE_CONFLICT" } }] }),
        json!({ "data": { "x": 1 } }),
    ];
    for body in bodies {
        let want = canonical(&body);
        assert_eq!(from_refusal(&body), want, "refusal() disagrees with the one classifier on {body}");
        assert_eq!(reader_code(&body), want, "the answer reader disagrees with the one classifier on {body}");
    }
}
