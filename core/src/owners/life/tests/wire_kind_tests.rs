use crate::transport::encrypted::{response_from_answer, Answer};
use crate::transport::{classify_answer, TransportError, WireOutcome};
use serde_json::{json, Value};

fn code_body(name: &str) -> Value { json!({ "errors": [{ "message": name, "extensions": { "code": name } }] }) }

/// P-WIRE-1 on the plain path: the response reader and the classifier agree on retryable codes and on refusals of the session. The session reader joins in R2d.
#[test]
fn wire_prop_kind_same_on_every_path() {
    let bodies = [
        json!({ "error": "UNAUTHENTICATED" }),
        json!({ "error": "RATE_LIMITED" }),
        json!({ "error": "REPLAYED_REQUEST" }),
        json!({ "error": "SEQUENCE_CONFLICT" }),
        json!({ "error": "UNKNOWN_NAME" }),
        code_body("INTERNAL_SERVER_ERROR"),
        code_body("SERVICE_UNAVAILABLE"),
        code_body("TIMEOUT"),
        code_body("FORBIDDEN"),
        code_body("PERSISTED_QUERY_NOT_FOUND"),
        json!({ "data": { "x": 1 } }),
    ];
    for body in bodies {
        let kind = classify_answer(&WireOutcome::Answer { sequence: None, body: body.clone() }, false, true);
        let read_temporary = crate::transport::encrypted::response_from_answer(Answer { sequence: None, body: body.clone() }).is_ok_and(|response| response.errors.iter().any(|error| error.code().is_some_and(|code| crate::transport::answer_kind::RETRYABLE_CODES.contains(&code))));
        assert_eq!(read_temporary, matches!(kind, crate::transport::AnswerKind::Transient { .. }) && body.get("error").is_none(), "retryable code of {body}: {kind:?}");
        let expected = match crate::transport::refusal(&body) {
            Some(crate::transport::SessionCode::Unauthenticated) => "http 401",
            Some(crate::transport::SessionCode::Other) | None => "no refusal",
            Some(_) => "session",
        };
        let read = match response_from_answer(Answer { sequence: None, body: body.clone() }) {
            Err(TransportError::Http { status: 401, .. }) => "http 401",
            Err(TransportError::Session { .. }) => "session",
            _ => "no refusal",
        };
        assert_eq!(read, expected, "response reader on {body}: {kind:?}");
    }
}
