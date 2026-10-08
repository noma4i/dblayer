use super::*;
use crate::identity::Generation;

#[test]
fn offline_start_fails_only_without_window() {
    assert_eq!(FetchReport::offline_start(false), Some(FetchReport::Failed(FetchError::Offline)));
    assert_eq!(FetchReport::offline_start(true), None);
}

#[test]
fn report_follows_failure_force_and_window() {
    assert_eq!(FetchReport::of(false, true, None), FetchReport::Loaded);
    assert_eq!(
        FetchReport::of(true, true, Some("boom")),
        FetchReport::Failed(FetchError::Transport("boom".into()))
    );
    assert_eq!(FetchReport::of(false, true, Some("boom")), FetchReport::KeptStale);
    assert_eq!(
        FetchReport::of(false, false, Some("boom")),
        FetchReport::Failed(FetchError::Transport("boom".into()))
    );
}

#[test]
fn attempt_is_superseded_by_later_issue_of_same_key_and_fenced_by_generation() {
    let attempt = |key: &str, issued| FetchAttempt::new(key, issued, Generation::INITIAL, 1);
    let current = attempt("feed", 2);
    assert!(current.is_superseded_by(&attempt("feed", 3)));
    assert!(!current.is_superseded_by(&attempt("feed", 1)));
    assert!(!current.is_superseded_by(&attempt("feed", 2)));
    assert!(!current.is_superseded_by(&attempt("profile", 3)));
    assert!(current.applies_to(Generation::INITIAL));
    assert!(!current.applies_to(Generation::INITIAL.next()));
}
