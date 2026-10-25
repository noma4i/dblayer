use std::sync::{Arc, Mutex};

use super::live::{LivePhase, QueryListener};
use super::live_feed::LiveFeed;
use super::{DbError, LiveChange};
use crate::sync::{LoadError, LoadPhase, RelationState};

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

fn feed() -> (Arc<LiveFeed>, Arc<Tape>) {
    let tape = Arc::new(Tape(Mutex::new(Vec::new())));
    (LiveFeed::new(tape.clone()), tape)
}

fn heard(tape: &Tape) -> Vec<LiveChange> {
    tape.0.lock().unwrap().clone()
}

fn relation(phase: LoadPhase, revision: i64, error: Option<LoadError>) -> RelationState {
    RelationState { phase, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error, loaded_at: None, revision }
}

fn rows(revision: i64, size: u32) -> LiveChange {
    LiveChange { full: true, size, ..LiveChange::empty(revision) }
}

#[derive(Clone)]
enum Input {
    Rows(i64, u32),
    Reached(i64),
    State(LoadPhase, i64, Option<&'static str>),
}

fn send(feed: &LiveFeed, input: &Input) {
    match input {
        Input::Rows(revision, size) => feed.rows(rows(*revision, *size)),
        Input::Reached(revision) => feed.reached(*revision),
        Input::State(phase, revision, error) => feed.state(relation(*phase, *revision, error.map(|kind| LoadError::new(kind, kind)))),
    }
}

fn permutations(items: &[Input]) -> Vec<Vec<Input>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    (0..items.len())
        .flat_map(|at| {
            let mut rest = items.to_vec();
            let head = rest.remove(at);
            permutations(&rest).into_iter().map(move |mut tail| {
                tail.insert(0, head.clone());
                tail
            })
        })
        .collect()
}

fn position(order: &[Input], wanted: impl Fn(&Input) -> bool) -> Option<usize> {
    order.iter().position(wanted)
}

fn consistent(changes: &[LiveChange], landed: i64, context: &str) {
    let mut last = 0;
    for change in changes {
        assert!(change.seq > last, "{context}: seq {} after {last}", change.seq);
        last = change.seq;
        match change.phase {
            Some(LivePhase::Loaded) => assert!(change.size > 0, "{context}: Loaded with {} rows (seq {})", change.size, change.seq),
            Some(LivePhase::Empty) => assert_eq!(change.size, 0, "{context}: Empty with rows (seq {})", change.seq),
            _ => {}
        }
        if matches!(change.phase, Some(LivePhase::Loaded | LivePhase::Empty)) && landed > 0 {
            assert!(change.revision >= landed, "{context}: {:?} at rows revision {} before the landing at {landed} (seq {})", change.phase, change.revision, change.seq);
        }
    }
}

#[test]
fn inv_loadm_14_the_rows_of_a_landing_and_its_state_reach_the_subscriber_in_every_order() {
    let prelude = [Input::State(LoadPhase::Loading, 0, None), Input::Rows(1, 0)];
    let landing = [Input::State(LoadPhase::Loaded, 5, None), Input::Rows(5, 1), Input::Reached(5)];
    let mut orders = 0;
    for order in permutations(&landing) {
        let (rows_at, reached_at) = (position(&order, |input| matches!(input, Input::Rows(..))), position(&order, |input| matches!(input, Input::Reached(..))));
        if rows_at > reached_at {
            continue;
        }
        orders += 1;
        let (feed, tape) = feed();
        for input in prelude.iter().chain(&order) {
            send(&feed, input);
        }
        let changes = heard(&tape);
        let context = format!("order {:?}", order.iter().map(|input| match input { Input::State(..) => "state", Input::Rows(..) => "rows", Input::Reached(..) => "reached" }).collect::<Vec<_>>());
        consistent(&changes, 5, &context);
        let last = changes.last().unwrap();
        assert_eq!((last.phase, last.size, last.revision), (Some(LivePhase::Loaded), 1, 5), "{context}");
    }
    assert_eq!(orders, 3);
}

#[test]
fn inv_loadm_14_a_landing_of_no_rows_waits_for_the_subscriber_to_reach_its_revision() {
    let prelude = [Input::State(LoadPhase::Loading, 0, None), Input::Rows(1, 0)];
    for order in permutations(&[Input::State(LoadPhase::Empty, 5, None), Input::Reached(5)]) {
        let (feed, tape) = feed();
        for input in prelude.iter().chain(&order) {
            send(&feed, input);
        }
        let changes = heard(&tape);
        consistent(&changes, 5, "no rows");
        let last = changes.last().unwrap();
        assert_eq!((last.phase, last.size, last.revision), (Some(LivePhase::Empty), 0, 5));
    }
}

#[test]
fn inv_loadm_14_the_state_waits_while_the_rows_are_behind_and_shows_the_flight_meanwhile() {
    let (feed, tape) = feed();
    for input in [Input::State(LoadPhase::Loaded, 0, None), Input::Rows(1, 2), Input::State(LoadPhase::Loading, 0, None), Input::State(LoadPhase::Loaded, 7, None)] {
        send(&feed, &input);
    }
    let changes = heard(&tape);
    assert_eq!(changes.iter().map(|change| change.phase).collect::<Vec<_>>(), vec![Some(LivePhase::Loaded), Some(LivePhase::Loading)]);
    send(&feed, &Input::Reached(7));
    assert_eq!(heard(&tape).last().unwrap().phase, Some(LivePhase::Loaded));
}

#[test]
fn inv_loadm_13_the_state_of_a_window_does_not_decide_between_empty_and_loaded() {
    for order in permutations(&[Input::State(LoadPhase::Empty, 0, None), Input::Rows(1, 3)]) {
        let (feed, tape) = feed();
        for input in &order {
            send(&feed, input);
        }
        let changes = heard(&tape);
        consistent(&changes, 0, "stale empty");
        let last = changes.last().unwrap();
        assert_eq!((last.phase, last.size), (Some(LivePhase::Loaded), 3));
    }
}

#[test]
fn inv_loadm_15_the_first_value_waits_for_the_rows_and_carries_them_with_the_phase() {
    let (feed, tape) = feed();
    send(&feed, &Input::State(LoadPhase::Loaded, 0, None));
    send(&feed, &Input::Reached(4));
    assert!(heard(&tape).is_empty(), "nothing is told before the first read");
    send(&feed, &Input::Rows(4, 2));
    let changes = heard(&tape);
    assert_eq!(changes.len(), 1);
    assert_eq!((changes[0].phase, changes[0].size, changes[0].seq), (Some(LivePhase::Loaded), 2, 1));
}

#[test]
fn inv_loadm_15_a_read_failure_is_in_the_snapshot_once_and_ends_with_the_next_read() {
    let (feed, tape) = feed();
    send(&feed, &Input::State(LoadPhase::Loaded, 0, None));
    send(&feed, &Input::Rows(1, 2));
    feed.read_failed("disk".into());
    feed.read_failed("disk".into());
    let changes = heard(&tape);
    assert_eq!(changes.len(), 2, "the repeated failure is not told again");
    assert_eq!((changes[1].phase, changes[1].error_kind.as_deref(), changes[1].size), (Some(LivePhase::Failed), Some("database"), 2));
    assert!(matches!(changes[1].error, Some(DbError::Database { .. })));
    send(&feed, &Input::Rows(2, 2));
    let healed = heard(&tape).pop().unwrap();
    assert_eq!((healed.phase, healed.error_kind, healed.error.is_none()), (Some(LivePhase::Loaded), None, true));
}

#[test]
fn inv_loadm_15_a_failed_load_is_failed_without_rows_and_loaded_with_rows() {
    for (size, expected) in [(0, LivePhase::Failed), (2, LivePhase::Loaded)] {
        let (feed, tape) = feed();
        send(&feed, &Input::State(LoadPhase::Loading, 0, None));
        send(&feed, &Input::Rows(1, size));
        send(&feed, &Input::State(LoadPhase::Failed, 0, Some("network")));
        let last = heard(&tape).pop().unwrap();
        assert_eq!((last.phase, last.error_kind.as_deref()), (Some(expected), Some("network")), "{size} rows");
        assert!(last.error.is_some());
    }
}

#[test]
fn inv_loadm_15_a_failure_of_the_server_subscription_stays_until_it_recovers() {
    let (feed, tape) = feed();
    send(&feed, &Input::Rows(1, 1));
    feed.subscription_failed(Some(DbError::Network { detail: "cable".into() }));
    let failed = heard(&tape).pop().unwrap();
    assert_eq!((failed.error_kind.as_deref(), failed.phase), (Some("subscription"), Some(LivePhase::Loaded)));
    feed.subscription_failed(None);
    let healed = heard(&tape).pop().unwrap();
    assert!(healed.error.is_none() && healed.error_kind.is_none());
}

#[test]
fn inv_loadm_15_a_subscription_that_cannot_open_tells_its_subscriber_that_it_failed() {
    let (feed, tape) = feed();
    feed.invalid(DbError::Invalid { detail: "unknown field".into() });
    let changes = heard(&tape);
    assert_eq!(changes.len(), 1);
    assert_eq!((changes[0].phase, changes[0].error_kind.as_deref(), changes[0].seq, changes[0].size), (Some(LivePhase::Failed), Some("invalid"), 1, 0));
    assert!(matches!(changes[0].error, Some(DbError::Invalid { .. })));
}

#[test]
fn inv_loadm_14_a_listener_that_panics_does_not_stop_the_next_snapshots() {
    struct Panicky(Mutex<Vec<u64>>);
    impl QueryListener for Panicky {
        fn changed(&self, change: LiveChange) {
            let mut seen = self.0.lock().unwrap();
            seen.push(change.seq);
            if seen.len() == 1 {
                drop(seen);
                panic!("listener failed");
            }
        }
    }
    let listener = Arc::new(Panicky(Mutex::new(Vec::new())));
    let feed = LiveFeed::new(listener.clone());
    send(&feed, &Input::Rows(1, 1));
    send(&feed, &Input::Rows(2, 2));
    send(&feed, &Input::Rows(3, 3));
    assert_eq!(*listener.0.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn inv_loadm_14_snapshots_from_many_threads_arrive_in_strictly_growing_order() {
    let (feed, tape) = feed();
    send(&feed, &Input::Rows(1, 1));
    let threads: Vec<_> = (0..4)
        .map(|worker| {
            let feed = feed.clone();
            std::thread::spawn(move || {
                for step in 0..200 {
                    match (worker + step) % 3 {
                        0 => feed.rows(rows(step as i64 + 2, 1 + (step % 5) as u32)),
                        1 => feed.state(relation(if step % 2 == 0 { LoadPhase::Loading } else { LoadPhase::Loaded }, 0, None)),
                        _ => feed.subscription_failed((step % 2 == 0).then(|| DbError::Network { detail: format!("{worker}") })),
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let changes = heard(&tape);
    assert!(changes.len() > 100);
    consistent(&changes, 0, "threads");
}

#[test]
fn rows_arriving_before_the_relation_state_are_held_and_delivered_in_order_with_it() {
    let (feed, tape) = feed();
    feed.expect_load();
    feed.rows(rows(1, 1));
    feed.rows(rows(2, 2));
    assert!(heard(&tape).is_empty(), "nothing is shown before the relation state arrives");
    feed.state(relation(LoadPhase::Loaded, 0, None));
    let sizes: Vec<u32> = heard(&tape).iter().map(|change| change.size).collect();
    assert_eq!(sizes, vec![1, 2]);
    assert!(heard(&tape).iter().all(|change| change.phase == Some(LivePhase::Loaded)));
    assert!(heard(&tape).windows(2).all(|pair| pair[0].seq < pair[1].seq));
}

#[test]
fn a_load_that_ends_without_a_relation_state_releases_the_held_rows() {
    let (feed, tape) = feed();
    feed.expect_load();
    feed.rows(rows(1, 3));
    assert!(heard(&tape).is_empty());
    feed.load_finished(Some(DbError::Invalid { detail: "relation".into() }));
    let heard = heard(&tape);
    assert_eq!(heard.iter().map(|change| change.size).collect::<Vec<_>>(), vec![3]);
}
