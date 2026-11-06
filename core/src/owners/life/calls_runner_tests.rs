use super::calls_runner::mutate_input;
use crate::owners::session::{MutateOwner, SessionInput, CALLS_AUTHOR};

#[test]
fn call_run_mutate() {
    let SessionInput::Mutate { author, owner, req_id, .. } = mutate_input(5, 2, "/graphql".into(), "{}".into()) else { panic!("a Mutate") };
    assert_eq!((author, owner, req_id), (CALLS_AUTHOR, Some(MutateOwner::Detached), 5), "Calls writes the owner at the first Mutate");
}
