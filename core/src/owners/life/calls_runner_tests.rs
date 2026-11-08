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
