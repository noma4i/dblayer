use super::answer_kind::{classify, refusal, refusal_name, AnswerKind, WireOutcome};
use super::SessionCode;
use serde_json::{json, Value};

fn code_body(name: &str) -> Value { json!({ "errors": [{ "message": name, "extensions": { "code": name } }] }) }
fn answer(sequence: Option<i64>, body: Value) -> WireOutcome { WireOutcome::Answer { sequence, body } }
fn transient() -> AnswerKind { AnswerKind::Transient { reason: "x".into() } }
fn same(kind: AnswerKind, expected: &AnswerKind) -> bool {
    match (&kind, expected) {
        (AnswerKind::Transient { .. }, AnswerKind::Transient { .. }) => true,
        _ => kind == *expected,
    }
}

/// Every row of the answer kind table of `wire.md`, with and without a `sequence` on the pair.
#[test]
fn wire_classify_table() {
    let cases: Vec<(&str, WireOutcome, bool, AnswerKind)> = vec![
        ("unsent, sequenced", WireOutcome::Unsent("dns".into()), true, AnswerKind::Unknown),
        ("unsent, plain", WireOutcome::Unsent("dns".into()), false, transient()),
        ("lost, sequenced", WireOutcome::Lost, true, AnswerKind::Unknown),
        ("lost, plain", WireOutcome::Lost, false, transient()),
        ("unreadable 2xx, sequenced", WireOutcome::Unreadable, true, AnswerKind::Unknown),
        ("unreadable 2xx, plain", WireOutcome::Unreadable, false, AnswerKind::Permanent { status: None }),
        ("revoked field", answer(Some(3), json!({ "revoked": true })), true, AnswerKind::Revoked),
        ("revoked plain", answer(None, json!({ "revoked": true })), false, AnswerKind::Revoked),
        ("session ended", answer(Some(3), json!({ "error": "SESSION_ENDED" })), true, AnswerKind::Revoked),
        ("sequence conflict", answer(None, json!({ "error": "SEQUENCE_CONFLICT" })), true, AnswerKind::Refused { code: SessionCode::SequenceConflict }),
        ("unauthenticated, sequenced", answer(None, json!({ "error": "UNAUTHENTICATED" })), true, AnswerKind::Refused { code: SessionCode::Unauthenticated }),
        ("unauthenticated, plain", answer(None, json!({ "error": "UNAUTHENTICATED" })), false, AnswerKind::Refused { code: SessionCode::Unauthenticated }),
        ("document missing, sequenced", answer(None, code_body("PERSISTED_QUERY_NOT_FOUND")), true, AnswerKind::DocumentMissing),
        ("document missing, plain", answer(None, code_body("PERSISTED_QUERY_NOT_FOUND")), false, AnswerKind::DocumentMissing),
        ("replayed, sequenced", answer(None, json!({ "error": "REPLAYED_REQUEST" })), true, AnswerKind::Refused { code: SessionCode::ReplayedRequest }),
        ("replayed, plain", answer(None, json!({ "error": "REPLAYED_REQUEST" })), false, transient()),
        ("retryable code with a sequence", answer(Some(4), code_body("INTERNAL_SERVER_ERROR")), true, AnswerKind::Answered { sequence: Some(4) }),
        ("missing document with a sequence", answer(Some(4), code_body("PERSISTED_QUERY_NOT_FOUND")), true, AnswerKind::Answered { sequence: Some(4) }),
        ("replayed with a sequence", answer(Some(4), json!({ "error": "REPLAYED_REQUEST" })), true, AnswerKind::Refused { code: SessionCode::ReplayedRequest }),
        ("unauthenticated with a sequence", answer(Some(4), json!({ "error": "UNAUTHENTICATED" })), true, AnswerKind::Refused { code: SessionCode::Unauthenticated }),
        ("any code with a sequence", answer(Some(4), code_body("FORBIDDEN")), true, AnswerKind::Answered { sequence: Some(4) }),
        ("session code with a sequence", answer(Some(4), code_body("RATE_LIMITED")), true, AnswerKind::Refused { code: SessionCode::RateLimited }),
        ("retryable code without a sequence", answer(None, code_body("SERVICE_UNAVAILABLE")), true, transient()),
        ("retryable code, plain", answer(None, code_body("SERVICE_UNAVAILABLE")), false, transient()),
        ("timeout code, plain", answer(None, code_body("TIMEOUT")), false, transient()),
        ("other code, plain", answer(None, code_body("FORBIDDEN")), false, AnswerKind::Answered { sequence: None }),
        ("code before the number check", answer(None, json!({ "error": "RATE_LIMITED" })), true, AnswerKind::NotConsumed { code: SessionCode::RateLimited }),
        ("data with a sequence", answer(Some(5), json!({ "data": {} })), true, AnswerKind::Answered { sequence: Some(5) }),
        ("data, plain", answer(None, json!({ "data": {} })), false, AnswerKind::Answered { sequence: None }),
        ("401, sequenced", WireOutcome::Status(401), true, AnswerKind::Refused { code: SessionCode::Unauthenticated }),
        ("401, plain", WireOutcome::Status(401), false, AnswerKind::Refused { code: SessionCode::Unauthenticated }),
        ("408", WireOutcome::Status(408), true, AnswerKind::Unknown),
        ("425", WireOutcome::Status(425), false, transient()),
        ("425, sequenced", WireOutcome::Status(425), true, AnswerKind::Unknown),
        ("429", WireOutcome::Status(429), true, AnswerKind::Unknown),
        ("500", WireOutcome::Status(500), true, AnswerKind::Unknown),
        ("503", WireOutcome::Status(503), false, transient()),
        ("599", WireOutcome::Status(599), false, transient()),
        ("400", WireOutcome::Status(400), true, AnswerKind::Permanent { status: Some(400) }),
        ("409", WireOutcome::Status(409), true, AnswerKind::Permanent { status: Some(409) }),
        ("410", WireOutcome::Status(410), false, AnswerKind::Permanent { status: Some(410) }),
        ("499", WireOutcome::Status(499), false, AnswerKind::Permanent { status: Some(499) }),
    ];
    for (name, outcome, sequenced, expected) in cases {
        let kind = classify(&outcome, sequenced, false);
        assert!(same(kind.clone(), &expected), "{name}: {kind:?}, expected {expected:?}");
    }
    for sequenced in [false, true] {
        assert_eq!(classify(&answer(None, code_body("PERSISTED_QUERY_NOT_FOUND")), sequenced, true), AnswerKind::PermanentGraphql);
    }
    assert_eq!(classify(&answer(None, json!({ "error": "SEQUENCE_CONFLICT" })), false, false), AnswerKind::PermanentSession { code: SessionCode::SequenceConflict });
}

#[test]
fn wire_refusal_reads_the_first_code_and_keeps_an_unknown_name() {
    assert_eq!(refusal(&json!({ "error": "SEQUENCE_CONFLICT", "errors": [{ "extensions": { "code": "RATE_LIMITED" } }] })), Some(super::answer_kind::Refusal::Session(SessionCode::SequenceConflict)));
    assert_eq!(refusal(&code_body("RATE_LIMITED")), Some(super::answer_kind::Refusal::Session(SessionCode::RateLimited)));
    assert_eq!(refusal(&code_body("BRAND_NEW")), Some(super::answer_kind::Refusal::Graphql("BRAND_NEW".into())));
    assert_eq!(refusal_name(&code_body("BRAND_NEW")).as_deref(), Some("BRAND_NEW"));
    assert_eq!(refusal(&json!({ "data": {} })), None);
    assert_eq!(refusal_name(&json!({ "data": {} })), None);
}
