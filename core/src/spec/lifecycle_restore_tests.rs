use crate::owners::contract::CredentialCommitted;
use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleError, LifecycleInput, LifecycleState};
use crate::runtime::{Effect, StateOwner};

fn step(state: &mut LifecycleState, input: LifecycleInput) -> Vec<LifecycleEffect> {
    Lifecycle::step(state, input).into_iter().filter_map(|effect| match effect { Effect::Run(effect) => Some(effect), _ => None }).collect()
}

fn restore_ended(saved: bool, expected: Result<(), LifecycleError>) {
    for input in [
        LifecycleInput::CredentialCommitted(CredentialCommitted { identity: 10 }),
        LifecycleInput::SessionReset { reply: Some(8) },
        LifecycleInput::Close { reply: 9 },
    ] {
        let mut state = LifecycleState::new();
        step(&mut state, LifecycleInput::Open { saved_epoch: 0, saved_generation: 0, saved_identity: Some(9), reply: Some(7) });
        if saved { step(&mut state, LifecycleInput::Saved { publication_id: 1 }); }
        let effects = step(&mut state, input);
        assert_eq!(effects.iter().filter_map(|effect| match effect { LifecycleEffect::Reply { id: 7, result } => Some(result.clone()), _ => None }).collect::<Vec<_>>(), [expected.clone()]);
        assert!(state.restoring().is_none());
    }
}

#[test]
fn life_restoring_ended_saved() { restore_ended(true, Ok(())); }

#[test]
fn life_restoring_ended_unsaved() { restore_ended(false, Err(LifecycleError::Interrupted)); }

#[test]
fn life_closing_executor_failed() {
    let mut state = LifecycleState::new();
    step(&mut state, LifecycleInput::Open { saved_epoch: 0, saved_generation: 0, saved_identity: None, reply: None });
    step(&mut state, LifecycleInput::Saved { publication_id: 1 });
    step(&mut state, LifecycleInput::SessionReset { reply: Some(7) });
    step(&mut state, LifecycleInput::Close { reply: 8 });
    let effects = step(&mut state, LifecycleInput::ExecutorFailed { executor: "store".into() });
    assert_eq!(effects.iter().filter_map(|effect| match effect { LifecycleEffect::Reply { id, result } => Some((*id, result.clone())), _ => None }).collect::<Vec<_>>(), [(7, Err(LifecycleError::Runtime))]);
    assert!(matches!(state.phase(), crate::owners::lifecycle::LifecyclePhase::Closing { close_replies, .. } if close_replies == &[8]));
}
