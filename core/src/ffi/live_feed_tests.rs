use std::sync::{Arc, Mutex};

use super::live::{LivePhase, QueryListener};
use super::live_feed::LiveFeed;
use super::{DbError, LiveChange};
use crate::transport::GraphqlExtensions;
use crate::sync::{LoadError, LoadPhase, RelationState};
use crate::test_wait::join_within;

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
    RelationState { complete: false, phase, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error, loaded_at: None, revision }
}

fn wire_row(id: String) -> super::wire::WireRow {
    super::wire::WireRow { id, revision: 0, estimated_bytes: 0, layout: String::new(), cells: Vec::new(), sync: super::wire::WireSync { state: super::wire::WireSyncState::Synced, actions: Vec::new(), dirty_fields: Vec::new(), error: None, attempts: 0, since: 0 }, uploads: Vec::new() }
}

fn rows(revision: i64, size: u32) -> LiveChange {
    LiveChange { full: true, size, rows: (0..size).map(|index| wire_row(format!("r{index}"))).collect(), ..LiveChange::empty(revision) }
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
        let snapshots = folded(&changes);
        assert!(snapshots.iter().all(|(phase, ids, _)| *phase != Some(LivePhase::Loaded) || ids.len() == 1), "{context}: a Loaded snapshot without its row {snapshots:?}");
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

    let refused = |code: &str| DbError::Graphql { messages: vec!["refused".into()], extensions: Some(GraphqlExtensions { code: Some(code.into()), transaction_disposition: None, fields: Default::default() }) };
    let before = heard(&tape).len();
    feed.subscription_failed(Some(refused("A")));
    feed.subscription_failed(Some(refused("B")));
    feed.subscription_failed(Some(refused("B")));
    let codes: Vec<_> = heard(&tape)[before..].iter().filter_map(|change| match &change.error { Some(DbError::Graphql { extensions: Some(extensions), .. }) => extensions.code.clone(), _ => None }).collect();
    assert_eq!(codes, ["A", "B"]);
    feed.subscription_failed(None);
    assert!(heard(&tape).pop().unwrap().error.is_none());
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
        join_within(thread);
    }
    let changes = heard(&tape);
    assert!(changes.len() > 100);
    consistent(&changes, 0, "threads");
}

#[test]
fn a_relation_state_ahead_of_the_rows_asks_the_group_to_reach_its_revision() {
    let (feed, _tape) = feed();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let sink = asked.clone();
    feed.on_ahead(Arc::new(move |revision| sink.lock().unwrap().push(revision)));
    feed.rows(rows(3, 1));
    feed.state(relation(LoadPhase::Loaded, 3, None));
    assert!(asked.lock().unwrap().is_empty(), "rows already at the revision: nothing to ask");
    feed.state(relation(LoadPhase::Loaded, 7, None));
    assert_eq!(*asked.lock().unwrap(), vec![7]);
}

#[test]
fn a_targets_only_feed_shows_no_total_of_the_source_relation() {
    let (feed, tape) = feed();
    feed.show_targets_only();
    feed.rows(rows(1, 3));
    feed.state(RelationState { total: Some(9), ..relation(LoadPhase::Loaded, 1, None) });
    let shown = heard(&tape).pop().expect("a delivery");
    assert_eq!((shown.total, shown.size, shown.phase), (None, 3, Some(LivePhase::Loaded)));
}

pub(super) fn folded(changes: &[LiveChange]) -> Vec<(Option<LivePhase>, Vec<String>, u32)> {
    let mut ids: Vec<String> = Vec::new();
    changes
        .iter()
        .map(|change| {
            if change.full {
                ids.clear();
            }
            ids.retain(|id| !change.removed.contains(id));
            for row in &change.rows {
                if !ids.contains(&row.id) {
                    ids.push(row.id.clone());
                }
            }
            (change.phase, ids.clone(), change.size)
        })
        .collect()
}

struct Strict {
    tape: Mutex<Vec<LiveChange>>,
    feed: std::sync::OnceLock<Arc<LiveFeed>>,
}

impl QueryListener for Strict {
    fn changed(&self, change: LiveChange) {
        let refused = !change.full && change.rows.iter().any(|row| row.id.starts_with("bad"));
        self.tape.lock().unwrap().push(change);
        if refused {
            self.feed.get().unwrap().resync();
        }
    }
}

fn strict() -> (Arc<LiveFeed>, Arc<Strict>, Arc<std::sync::atomic::AtomicUsize>) {
    let listener = Arc::new(Strict { tape: Mutex::new(Vec::new()), feed: std::sync::OnceLock::new() });
    let feed = LiveFeed::new(listener.clone());
    listener.feed.set(feed.clone()).ok();
    let rejoins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = rejoins.clone();
    feed.on_rejoin(Arc::new(move || { counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }));
    feed.rows(rows(1, 2));
    (feed, listener, rejoins)
}

fn delta(revision: i64, id: &str, size: u32) -> LiveChange {
    LiveChange { size, rows: vec![wire_row(id.into())], row_indexes: vec![0], placed: vec![super::live::Placement { id: id.into(), index: 0 }], ..LiveChange::empty(revision) }
}

fn shapes(listener: &Strict) -> Vec<&'static str> {
    listener.tape.lock().unwrap().iter().map(|change| match (change.full, change.error.is_some()) {
        (true, _) => "whole",
        (false, true) => "failed",
        (false, false) => "delta",
    }).collect()
}

#[test]
fn a_refused_delta_asks_the_observer_for_the_whole_list_and_drops_deltas_until_it_arrives() {
    let (feed, listener, rejoins) = strict();
    feed.rows(delta(2, "bad1", 3));
    assert_eq!(rejoins.load(std::sync::atomic::Ordering::SeqCst), 1);
    feed.rows(delta(3, "ok1", 4));
    assert_eq!(shapes(&listener), ["whole", "delta"], "a delta for a list the reader no longer holds is not delivered");
    feed.rows(rows(4, 4));
    assert_eq!(shapes(&listener), ["whole", "delta", "whole"]);
    assert_eq!(listener.tape.lock().unwrap().last().unwrap().size, 4);
}

#[test]
fn refusals_in_a_row_stop_the_list_with_an_error_and_a_good_delta_resets_the_count() {
    let (feed, listener, rejoins) = strict();
    let refused = |revision: i64, id: &str| { feed.rows(delta(revision, id, 3)); feed.rows(rows(revision, 3)); };
    refused(2, "bad1");
    refused(3, "bad2");
    feed.rows(delta(4, "ok1", 3));
    refused(5, "bad3");
    refused(6, "bad4");
    assert_eq!(shapes(&listener), ["whole", "delta", "whole", "delta", "whole", "delta", "delta", "whole", "delta", "whole"], "a good delta in between resets the count");
    assert_eq!(rejoins.load(std::sync::atomic::Ordering::SeqCst), 4);
    feed.rows(delta(7, "bad5", 3));
    let shapes = shapes(&listener);
    assert_eq!(&shapes[shapes.len() - 2..], ["delta", "failed"]);
    assert_eq!(rejoins.load(std::sync::atomic::Ordering::SeqCst), 4, "the stopped feed asks for nothing more");
    let stopped = listener.tape.lock().unwrap().last().cloned().unwrap();
    assert!(matches!(&stopped.error, Some(DbError::Invalid { .. })) && stopped.phase == Some(LivePhase::Failed) && stopped.error_kind.as_deref() == Some("invalid"));
    assert_eq!(stopped.size, 3);
    feed.rows(rows(8, 3));
    assert_eq!(listener.tape.lock().unwrap().last().unwrap().error_kind.as_deref(), Some("invalid"), "a stopped list delivers nothing more");
}

const WAIT: std::time::Duration = std::time::Duration::from_secs(10);

fn hold_before_the_call(call_state: &crate::db::CallState) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (reached, at_the_call) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    *call_state.before_enter.lock().unwrap() = Some(Box::new(move || {
        reached.send(()).unwrap();
        released.recv_timeout(WAIT).unwrap();
    }));
    (at_the_call, release)
}

#[test]
fn inv_observe_08_a_live_call_that_passed_the_cancel_check_does_not_start_after_the_cancel_returns() {
    let (feed, tape) = feed();
    send(&feed, &Input::State(LoadPhase::Loaded, 0, None));
    send(&feed, &Input::Reached(4));
    let (at_the_call, release) = hold_before_the_call(&feed.calls);
    let delivering = {
        let feed = feed.clone();
        std::thread::spawn(move || send(&feed, &Input::Rows(4, 2)))
    };
    at_the_call.recv_timeout(WAIT).unwrap();
    feed.end();
    release.send(()).unwrap();
    join_within(delivering);
    assert!(heard(&tape).is_empty());
}

#[test]
fn ending_the_feed_while_a_delivery_wakes_the_initial_load_waiter_finishes_with_ended() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use crate::test_wait::{spin_until, ForeignWaker};
    join_within(std::thread::spawn(|| {
        let (feed, _tape) = feed();
        let foreign = Arc::new(Mutex::new(()));
        let waker = ForeignWaker::new(foreign.clone());
        let mut readiness = feed.readiness.subscribe();
        let mut waiting = Box::pin(async move { *readiness.wait_for(|readiness| *readiness != super::live_feed::Readiness::Waiting).await.unwrap() });
        assert!(waiting.as_mut().poll(&mut Context::from_waker(&Waker::from(waker.clone()))).is_pending());
        let task_lock = foreign.lock().unwrap();
        let delivering = {
            let feed = feed.clone();
            std::thread::spawn(move || send(&feed, &Input::Rows(1, 1)))
        };
        spin_until("the delivery never woke the waiter", || waker.entered());
        feed.end();
        drop(task_lock);
        join_within(delivering);
        assert!(matches!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(super::live_feed::Readiness::Ended)));
    }));
}
