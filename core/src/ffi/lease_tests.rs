use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::*;

#[derive(Clone, Copy)]
enum Step {
    Opened(&'static str),
    Abandon,
    Take,
}

fn released_list() -> (Arc<Mutex<Vec<String>>>, TokenLease) {
    let released = Arc::new(Mutex::new(Vec::new()));
    let seen = released.clone();
    let lease = TokenLease::new(move |token| {
        crate::db::locked(&seen).push(token);
        Box::pin(async {})
    });
    (released, lease)
}

#[test]
fn every_order_of_open_abandon_and_take_releases_each_token_exactly_once() {
    use Step::*;
    let table: [(&str, Vec<Step>, Vec<bool>, Vec<&str>, Vec<Option<&str>>); 9] = [
        ("opened then abandoned", vec![Opened("a"), Abandon], vec![true], vec!["a"], vec![]),
        ("abandoned then opened", vec![Abandon, Opened("a")], vec![true], vec!["a"], vec![]),
        ("abandoned twice", vec![Abandon, Abandon], vec![], vec![], vec![]),
        ("opened twice", vec![Opened("a"), Opened("b"), Abandon], vec![true, false], vec!["b", "a"], vec![]),
        ("opened, taken, abandoned", vec![Opened("a"), Take, Abandon], vec![true], vec![], vec![Some("a")]),
        ("opened, taken, opened again", vec![Opened("a"), Take, Opened("b")], vec![true, false], vec!["b"], vec![Some("a")]),
        ("opened, abandoned, opened again", vec![Opened("a"), Abandon, Opened("b")], vec![true, false], vec!["a", "b"], vec![]),
        ("abandoned, opened, opened again", vec![Abandon, Opened("a"), Opened("b")], vec![true, false], vec!["a", "b"], vec![]),
        ("taken before open", vec![Take, Opened("a"), Abandon], vec![true], vec!["a"], vec![None]),
    ];
    for (name, steps, opened_ok, released_expected, taken_expected) in table {
        let (released, lease) = released_list();
        let (mut opened_seen, mut taken_seen) = (Vec::new(), Vec::new());
        for step in steps {
            match step {
                Opened(token) => opened_seen.push(lease.opened(token.into()).is_ok()),
                Abandon => lease.abandon(),
                Take => taken_seen.push(lease.take_open()),
            }
        }
        assert_eq!(opened_seen, opened_ok, "{name}: opened results");
        assert_eq!(*crate::db::locked(&released), released_expected, "{name}: released tokens");
        assert_eq!(taken_seen, taken_expected.into_iter().map(|token| token.map(String::from)).collect::<Vec<_>>(), "{name}: taken tokens");
    }
}

#[test]
fn a_second_opened_reports_an_error_and_keeps_the_first_token() {
    let (released, lease) = released_list();
    lease.opened("a".into()).unwrap();
    let error = lease.opened("b".into()).unwrap_err();
    assert!(matches!(error, DbError::Invalid { .. }));
    assert_eq!(lease.token().as_deref(), Some("a"));
    assert_eq!(*crate::db::locked(&released), vec!["b".to_string()]);
}

#[test]
fn a_release_that_calls_back_into_the_lease_does_not_deadlock() {
    let lease = Arc::new(OnceLock::<TokenLease>::new());
    let reentered = lease.clone();
    let released = Arc::new(Mutex::new(Vec::new()));
    let seen = released.clone();
    let _ = lease.set(TokenLease::new(move |token| {
        let lease = reentered.get().unwrap();
        lease.abandon();
        let _ = lease.token();
        let _ = lease.take_open();
        crate::db::locked(&seen).push(token);
        Box::pin(async {})
    }));
    let (done, finished) = std::sync::mpsc::channel();
    let driven = lease.clone();
    std::thread::spawn(move || {
        let lease = driven.get().unwrap();
        lease.abandon();
        lease.opened("a".into()).unwrap();
        lease.opened("b".into()).unwrap_err();
        done.send(()).unwrap();
    });
    finished.recv_timeout(Duration::from_secs(10)).expect("the lease deadlocked on a re-entrant release");
    assert_eq!(*crate::db::locked(&released), vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn the_release_work_runs_detached_from_the_caller() {
    let (done, finished) = std::sync::mpsc::channel();
    let lease = TokenLease::new(move |token| {
        let done = done.clone();
        Box::pin(async move { done.send(token).unwrap(); })
    });
    lease.opened("a".into()).unwrap();
    lease.abandon();
    assert_eq!(finished.recv_timeout(Duration::from_secs(10)).unwrap(), "a");
}
