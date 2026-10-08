use super::*;

fn state(phase: LoadingPhase) -> LoadingState {
    LoadingState { phase, ..LoadingState::default() }
}

#[test]
fn presentation_flags_follow_phase() {
    assert!(state(LoadingPhase::InitialLoading).show_skeleton());
    assert!(!LoadingState { has_data: true, ..state(LoadingPhase::InitialLoading) }.show_skeleton());
    assert!(state(LoadingPhase::Ready).show_empty_state());
    assert!(!state(LoadingPhase::InitialLoading).show_empty_state());
    assert!(!LoadingState { has_data: true, ..state(LoadingPhase::Ready) }.show_empty_state());
    assert!(state(LoadingPhase::LoadingMore).show_footer_spinner());
    assert!(!state(LoadingPhase::Ready).show_footer_spinner());
    assert!(!state(LoadingPhase::Refreshing).show_footer_spinner());
    assert!(state(LoadingPhase::Error).show_error_banner());
    assert!(!state(LoadingPhase::Ready).show_error_banner());
    assert!(!state(LoadingPhase::Error).is_retrying());
    assert!(LoadingState { retry_attempt: 2, ..state(LoadingPhase::Error) }.is_retrying());
}

#[test]
fn next_phase_follows_data() {
    assert_eq!(LoadingPhase::next(LoadingPhase::Idle, false), LoadingPhase::InitialLoading);
    assert_eq!(LoadingPhase::next(LoadingPhase::Idle, true), LoadingPhase::Ready);
    assert_eq!(LoadingPhase::next(LoadingPhase::Ready, true), LoadingPhase::Ready);
    assert_eq!(LoadingPhase::next(LoadingPhase::Refreshing, true), LoadingPhase::Refreshing);
}

#[test]
fn local_state_is_ready_and_with_phase_keeps_other_fields() {
    assert_eq!(LoadingState::LOCAL, state(LoadingPhase::Ready));
    let changed = LoadingState { phase: LoadingPhase::Refreshing, has_data: true, is_offline: true, retry_attempt: 2 }
        .with_phase(LoadingPhase::Error);
    assert_eq!(changed, LoadingState { phase: LoadingPhase::Error, has_data: true, is_offline: true, retry_attempt: 2 });
}
