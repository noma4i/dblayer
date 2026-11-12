use super::*;
use crate::owners::contract::{EpochEnded, Published, PublishedState};
use crate::runtime::{Cause, Effect, StateOwner};

fn step(state: &mut SessionState, input: SessionInput) -> Vec<SessionEffect> {
    SessionCalls::step(state, input, crate::runtime::Time(0)).into_iter().filter_map(|effect| match effect { Effect::Run(value) => Some(value), _ => None }).collect()
}

fn setup() -> SessionState {
    let mut state = SessionState::with_operations(vec![SessionOperation { name: "read".into(), path: "/read".into(), class: OperationClass::Authenticated }], Some(true));
    step(&mut state, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
    step(&mut state, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 7 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
    state
}

fn begin(state: &mut SessionState, author: bool) -> u64 {
    let input = if author { SessionInput::Mutate { author: 5, owner: None, req_id: 8, epoch: 3, path: "/read".into(), body: "{}".into(), purpose: 0 } } else { SessionInput::SessionCall { operation: "read".into(), body: "{}".into(), reply: 9 } };
    match step(state, input).as_slice() { [SessionEffect::SealProbe { seal_id, .. }] => *seal_id, other => panic!("{other:?}") }
}

#[test]
fn sess_sealing_failed() {
    for (author, error) in [(false, SessionError::StateLost), (true, SessionError::Storage("locked".into()))] {
        let mut state = setup();
        let id = begin(&mut state, author);
        let effects = step(&mut state, SessionInput::SealFailed { seal_id: id, error });
        if author { assert!(matches!(effects.as_slice(), [SessionEffect::Answer { outcome: AuthorOutcome::Error { error: SessionError::Storage(_), .. }, .. }])); }
        else { assert!(matches!(effects.as_slice(), [SessionEffect::Reply { answer: Err(SessionError::StateLost), .. }])); }
        assert_eq!(state.credential().unwrap().sequence, 7);
    }
}

#[test]
fn sess_sealing_queue() {
    let mut state = setup();
    let id = begin(&mut state, false);
    assert!(step(&mut state, SessionInput::SessionCall { operation: "read".into(), body: "{}".into(), reply: 10 }).is_empty());
    let effects = step(&mut state, SessionInput::Sealed { seal_id: id, size: 262145 });
    assert!(matches!(effects.as_slice(), [SessionEffect::Reply { id: 9, .. }, SessionEffect::SealProbe { .. }]));
}

#[test]
fn sess_sealing_epoch_ended() {
    for author in [false, true] {
        let mut state = setup();
        let id = begin(&mut state, author);
        let effects = step(&mut state, SessionInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }));
        if author { assert!(matches!(effects.as_slice(), [SessionEffect::Answer { outcome: AuthorOutcome::Error { error: SessionError::NoSession, .. }, .. }])); }
        else { assert!(matches!(effects.as_slice(), [SessionEffect::Reply { id: 9, answer: Err(SessionError::NoSession) }]), "D-279: the author call in flight ends with the session: {effects:?}"); }
        assert!(step(&mut state, SessionInput::Sealed { seal_id: id, size: 1 }).is_empty());
    }
}

#[test]
fn sess_sealing_closing() {
    let mut state = setup();
    begin(&mut state, true);
    let effects = step(&mut state, SessionInput::Closing);
    assert!(matches!(effects.as_slice(), [SessionEffect::Answer { outcome: AuthorOutcome::Error { error: SessionError::Closed, .. }, .. }, SessionEffect::Drained(_)]));
}
