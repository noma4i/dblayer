use super::calls_runner::mutate_input;
use crate::owners::session::{MutateOwner, SessionInput, CALLS_AUTHOR};

#[test]
fn call_run_mutate() {
    let SessionInput::Mutate { author, owner, req_id, .. } = mutate_input(5, 2, "/graphql".into(), "{}".into()) else { panic!("a Mutate") };
    assert_eq!((author, owner, req_id), (CALLS_AUTHOR, Some(MutateOwner::Detached), 5), "Calls writes the owner at the first Mutate");
}

#[test]
fn call_transport_network() {
    use crate::owners::calls::{CallError, CallsInput};
    use crate::owners::contract::ErrorClass;
    use crate::owners::session::{AuthorOutcome, SessionError};
    let outcome = AuthorOutcome::Error { class: ErrorClass::Network, error: SessionError::Http { status: 0 }, pending_retained: true };
    assert!(matches!(super::calls_runner::answer_input(5, "root", outcome), CallsInput::TransportError { call_id: 5, error: CallError::Network }));
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
    assert!(matches!(session(SessionError::Protocol), CallError::Failed(DbError::Invalid { .. })));
    assert!(matches!(session(SessionError::Rejected(crate::owners::contract::RejectReason::Storage("full".into()))), CallError::Failed(DbError::Storage { .. })));
    assert!(matches!(typed(AuthorOutcome::Response("not json".into())), CallError::Failed(DbError::Invalid { .. })));
}
