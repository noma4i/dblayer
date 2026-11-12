
#[test]
fn call_transport_network() {
    use crate::owners::calls::{CallError, CallsInput};
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::{AuthorOutcome, SessionError};
    let outcome = AuthorOutcome::Error { class: ErrorClass::Network, error: SessionError::Network, pending_retained: false };
    assert!(matches!(super::calls_runner::answer_input(5, "root", outcome), CallsInput::TransportError { call_id: 5, error: CallError::Network }));
}

#[test]
fn call_http_auth_status() {
    use crate::owners::calls::{CallError, CallsInput};
    use crate::transport::TransportError;
    for status in [401, 403] {
        let result = super::calls_runner::calls_input(5, "root", Err(TransportError::Http { status, body: "refused".into() }));
        match (status, result) {
            (401, CallsInput::TransportError { error: CallError::Unauthorized, .. }) => {},
            (403, CallsInput::TransportError { error: CallError::Http { status: 403, .. }, .. }) => {},
            (_, other) => panic!("unexpected result: {other:?}"),
        }
    }
}

#[test]
fn call_response_preserves_data_and_errors() {
    let body = serde_json::json!({"data":{"root":{"id":"1"}},"errors":[{"message":"Busy","path":["root",0],"extensions":{"code":"SERVICE_UNAVAILABLE","retry":3}}]});
    let expected: crate::transport::Response = serde_json::from_value(body.clone()).unwrap();
    let outcome = crate::owners::session::AuthorOutcome::Response { body: body.to_string(), kind: crate::transport::AnswerKind::Transient { reason: "SERVICE_UNAVAILABLE".into() }, pending_retained: false };
    let crate::owners::calls::CallsInput::Response { data, errors, kind, .. } = super::calls_runner::answer_input(5, "root", outcome) else { panic!("response missing") };
    assert_eq!((data, errors, kind), (expected.data, expected.errors, crate::transport::AnswerKind::Transient { reason: "SERVICE_UNAVAILABLE".into() }));
}

/// call_transport_error (P-BND-04, D-195): the type and the fields of a refusal of the transport or of the owner Session reach the answer of the call; a permanent refusal is no network retry.
#[test]
fn call_transport_error_keeps_the_type_and_the_fields() {
    use crate::ffi::DbError;
    use crate::owners::calls::{CallError, CallsInput};
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::{AuthorOutcome, SessionError};
    use crate::transport::SecurityCode;
    let typed = |outcome: AuthorOutcome| match super::calls_runner::answer_input(5, "root", outcome) {
        CallsInput::TransportError { call_id: 5, error } => error,
        other => panic!("not a transport error: {other:?}"),
    };
    let session = |error: SessionError| typed(AuthorOutcome::Error { class: ErrorClass::Permanent, error, pending_retained: false });
    assert_eq!(session(SessionError::Storage("disk".into())), CallError::Failed(DbError::Storage { detail: "disk".into() }));
    assert_eq!(session(SessionError::Security { code: SecurityCode::TlsTrust, detail: "pin".into() }), CallError::Failed(DbError::Security { code: SecurityCode::TlsTrust, detail: "pin".into() }));
    assert_eq!(session(SessionError::TooLarge { size: 9, limit: 4 }), CallError::Failed(DbError::RequestTooLarge { size: 9, limit: 4 }));
    assert!(matches!(session(SessionError::Rejected(crate::owners::contract::RejectReason::Storage("full".into()))), CallError::Failed(DbError::Storage { .. })));
    assert!(matches!(typed(AuthorOutcome::Response { body: "not json".into(), kind: crate::transport::AnswerKind::Answered { sequence: None }, pending_retained: false }), CallError::Failed(DbError::Invalid { .. })));
}

/// call_pair_created_network_error_is_dropped (D-343, P-CALL-06): after the attempts end, a mutate whose pair Session kept answers `Err(Dropped)` and releases the pair; `Err(Network)` only when no pair exists.
#[test]
fn call_pair_created_network_error_is_dropped() {
    use crate::owners::calls::{CallError, CallKind, Calls, CallsEffect, CallsInput, CallsState};
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::{AuthorOutcome, SessionError};
    use crate::runtime::{Effect, StateOwner, Time};
    let session_error = |class: ErrorClass, pending_retained: bool| AuthorOutcome::Error { class, error: SessionError::Network, pending_retained };
    let exhausted = |operation: &str, input: &dyn Fn() -> CallsInput| {
        let mut state = CallsState::new(1, Some(true), &[("Change", CallKind::Mutate), ("Read", CallKind::Query)]);
        Calls::step(&mut state, CallsInput::Run { epoch: 1, operation: operation.into(), variables: "{}".into(), reply: 1 }, Time(0));
        let mut released = false;
        for attempt in 1..=40 {
            let effects = Calls::step(&mut state, input(), Time(0));
            released |= effects.iter().any(|effect| matches!(effect, Effect::Run(CallsEffect::Release { request_id: 1 })));
            if let Some(answer) = effects.iter().find_map(|effect| if let Effect::Run(CallsEffect::Reply { id: 1, answer }) = effect { Some(answer.clone().map(|_| ())) } else { None }) { return (Some(answer), released); }
            Calls::step(&mut state, CallsInput::RetryDue { call_id: 1, attempt }, Time(0));
        }
        (None, released)
    };
    for class in [ErrorClass::Network, ErrorClass::Server] {
        let kept = exhausted("Change", &|| super::calls_runner::answer_input(1, "root", session_error(class, true)));
        assert_eq!(kept, (Some(Err(CallError::Dropped)), true), "mutate with a kept pair, {class:?}");
    }
    let no_pair = exhausted("Change", &|| super::calls_runner::answer_input(1, "root", session_error(ErrorClass::Network, false)));
    assert_eq!(no_pair, (Some(Err(CallError::Network)), true), "mutate without a pair");
    let query = exhausted("Read", &|| super::calls_runner::calls_input(1, "root", Err(crate::transport::TransportError::Network("down".into()))));
    assert_eq!(query, (Some(Err(CallError::Network)), false), "query");
}
