use super::*;
use crate::test_wait::permutations;

// Orders of the independent inputs of a window (P-WIN-*): every order that keeps the causality is played on the twin, the observable of each trace is asserted against the table.

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Ev {
    Online,
    Offline,
    Watch,
    Call1,
    Call2,
    Stale,
    Leave1,
    Respond,
    Land,
    Reset,
    Error,
}

pub(super) fn at<T: Copy + PartialEq>(order: &[T], item: T) -> usize { order.iter().position(|found| *found == item).expect("the item is in the order") }

/// Every order of `items` where each `(a, b)` of `before` has `a` earlier than `b`.
pub(super) fn orders<T: Copy + PartialEq>(items: &[T], before: &[(T, T)]) -> Vec<Vec<T>> {
    permutations(items).into_iter().filter(|order| before.iter().all(|(first, second)| at(order, *first) < at(order, *second))).collect()
}

#[derive(Default)]
pub(super) struct Seen {
    pub(super) requests: usize,
    pub(super) cancels: usize,
    pub(super) retries: usize,
    pub(super) commits: usize,
    pub(super) answered: usize,
}

impl Seen {
    pub(super) fn add(&mut self, effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::Request { .. } => self.requests += 1,
                Effect::Cancel { .. } => self.cancels += 1,
                Effect::ScheduleRetry { .. } => self.retries += 1,
                Effect::Commit { .. } => self.commits += 1,
                _ => {}
            }
        }
    }

    pub(super) fn flying(&self) -> i64 { self.requests as i64 - self.cancels as i64 - self.answered as i64 }
}

pub(super) fn outcomes_of(twin: &Twin, id: u32) -> Vec<Result3> { twin.outcomes.iter().filter(|(found, _)| *found == id).map(|(_, result)| *result).collect() }

fn stray_answer() -> Input { Input::Response { request: 99, attempt: 99, cursor: None, rows: true } }

fn stray_commit() -> Input { Input::Committed { request: 99, revision: 1 } }

/// A reply of the network or the Store the trace plays when the machine is not in the stage that waits for it: it is a stray and changes nothing.
pub(super) fn play(twin: &mut Twin, event: Ev) -> Vec<Effect> {
    match event {
        Ev::Online => twin.step(Input::Online(true)),
        Ev::Offline => twin.step(Input::Online(false)),
        Ev::Watch => twin.watch(),
        Ev::Call1 => twin.call(1, Goal::First),
        Ev::Call2 => twin.call(2, Goal::First),
        Ev::Stale => twin.step(Input::Stale { in_keys: true }),
        Ev::Leave1 => twin.step(Input::InterestMinus { id: 1 }),
        Ev::Respond if twin.stage() == Some(WindowStage::Loading) => twin.respond(None),
        Ev::Respond => twin.step(stray_answer()),
        Ev::Land if twin.stage() == Some(WindowStage::Landing) => twin.land(),
        Ev::Land => twin.step(stray_commit()),
        Ev::Reset => unreachable!(),
        Ev::Error => twin.step(Input::TransportError { request: 1, attempt: 1, failure: Failure::Network }),
    }
}

/// Plays `event` and tallies it; an answer of the network takes its request out of the network.
pub(super) fn run(twin: &mut Twin, seen: &mut Seen, event: Ev) {
    seen.answered += usize::from(event == Ev::Respond && twin.stage() == Some(WindowStage::Loading));
    seen.add(&play(twin, event));
}

/// Answers and lands every request in flight until the machine rests.
pub(super) fn settle(twin: &mut Twin, seen: &mut Seen) {
    for _ in 0..8 {
        let effects = match twin.stage() {
            Some(WindowStage::Loading) => {
                seen.answered += 1;
                twin.respond(None)
            }
            Some(WindowStage::Landing) => twin.land(),
            _ => return,
        };
        seen.add(&effects);
    }
    panic!("the machine did not rest");
}

/// P-WIN-12, P-WIN-05, P-WIN-03: a window asks nothing before the addressed `Online`, then one request, whatever the order of the stream, the calls and `Stale`.
#[test]
fn win_orders_online_gate() {
    let all = orders(&[Ev::Online, Ev::Watch, Ev::Call1, Ev::Call2, Ev::Stale], &[]);
    assert_eq!(all.len(), 120);
    for order in all {
        let mut twin = Twin::before_online();
        let (mut seen, mut online) = (Seen::default(), false);
        for event in order.iter().copied() {
            let before = seen.requests;
            seen.add(&play(&mut twin, event));
            online |= event == Ev::Online;
            assert!(online || seen.requests == before, "{order:?}: {event:?} asked before the network was addressed");
            assert!(seen.flying() <= 1 && seen.cancels == 0, "{order:?}: {event:?} left {} requests in the network", seen.flying());
        }
        assert_eq!(seen.requests, 1, "{order:?}: the return of the network reads the window once");
        assert_eq!((twin.stage(), twin.state().waiters(&key()), twin.state().interest(&key())), (Some(WindowStage::Loading), vec![1, 2], vec![u64::from(STREAM)]), "{order:?}");
    }
}

/// P-WIN-06: a retry is scheduled and a request leaves only while the network is online, whatever the order of the loss of the network, the error and the return.
#[test]
fn win_orders_network_flap() {
    let all = orders(&[Ev::Offline, Ev::Error, Ev::Online], &[]);
    assert_eq!(all.len(), 6);
    for order in all {
        let mut twin = loading();
        let (mut seen, mut online) = (Seen { requests: 1, ..Seen::default() }, true);
        for event in order.iter().copied() {
            online = match event {
                Ev::Offline => false,
                Ev::Online => true,
                _ => online,
            };
            let (retries, requests) = (seen.retries, seen.requests);
            seen.add(&play(&mut twin, event));
            assert!(online || (seen.retries, seen.requests) == (retries, requests), "{order:?}: {event:?} scheduled or asked while offline");
        }
        let expected = if online { Some(WindowStage::Loading) } else { Some(WindowStage::Waiting) };
        assert_eq!(twin.stage(), expected, "{order:?}");
    }
}

/// P-WIN-08, P-WIN-11, P-WIN-01, P-WIN-02: `Stale` during Loading, Landing or after the landing gives one more load of a watched window, the finished waiters leave and every caller gets one outcome at a landed revision.
#[test]
fn win_orders_landing_reload() {
    let all = orders(&[Ev::Respond, Ev::Land, Ev::Stale, Ev::Call2], &[(Ev::Respond, Ev::Land)]);
    assert_eq!(all.len(), 12);
    for order in all {
        let mut twin = loading_watched();
        twin.call(1, Goal::First);
        let (mut seen, mut loaded) = (Seen { requests: 1, ..Seen::default() }, 0);
        let check = |twin: &Twin, seen: &Seen, loaded: &mut i64| {
            assert!(seen.flying() <= 1, "{order:?}: {} requests in the network", seen.flying());
            if let Some(revision) = twin.load_state().loaded {
                assert!((11..=twin.revision).contains(&revision) && revision >= *loaded, "{order:?}: Loaded at {revision} is not a landed revision in order");
                *loaded = revision;
            }
        };
        for event in order.iter().copied() {
            run(&mut twin, &mut seen, event);
            check(&twin, &seen, &mut loaded);
        }
        settle(&mut twin, &mut seen);
        check(&twin, &seen, &mut loaded);
        assert_eq!(seen.requests, 2, "{order:?}: the first load and one more load after the mark");
        for id in [1, 2] {
            assert_eq!(outcomes_of(&twin, id), [Result3::Loaded], "{order:?}: caller {id}");
        }
        assert!(twin.state().waiters(&key()).is_empty(), "{order:?}: a finished waiter stays");
        assert_eq!(twin.load_state().loaded, Some(twin.revision), "{order:?}");
    }
}

/// P-WIN-13, P-WIN-10: after each landing no waiter stays whose goal the page satisfied, and `AllPages` ends with one request per page after the first, whenever a second call joins the chain.
#[test]
fn win_orders_all_pages_landing() {
    const JOIN: u8 = 0;
    const CHAIN: [u8; 6] = [1, 2, 3, 4, 5, 6];
    let chain: Vec<(u8, u8)> = CHAIN.windows(2).map(|pair| (pair[0], pair[1])).collect();
    let all = orders(&[JOIN, 1, 2, 3, 4, 5, 6], &chain);
    assert_eq!(all.len(), 7);
    for order in all {
        let mut twin = Twin::new();
        let mut seen = Seen::default();
        seen.add(&twin.call(1, Goal::All));
        let (mut joined, mut done) = (false, false);
        for tag in order.iter().copied() {
            let effects = match tag {
                JOIN => {
                    joined = true;
                    twin.call(2, Goal::First)
                }
                1 => twin.respond(Some(2)),
                3 => twin.respond(Some(3)),
                5 => twin.respond(None),
                _ => twin.land(),
            };
            seen.add(&effects);
            done |= tag == 6;
            if matches!(tag, 2 | 4 | 6) {
                let expected = if tag == 6 { vec![] } else { vec![1] };
                assert_eq!(twin.state().waiters(&key()), expected, "{order:?}: after the landing {tag} a satisfied waiter stays (caller 2 joined: {joined})");
            }
            assert_eq!(outcomes_of(&twin, 1).len(), usize::from(done), "{order:?}: the AllPages caller got its outcome before the last page");
        }
        assert_eq!(seen.requests, 3, "{order:?}: one request per page");
        assert_eq!((outcomes_of(&twin, 1), outcomes_of(&twin, 2)), (vec![Result3::Loaded], vec![Result3::Loaded]), "{order:?}");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
}

/// P-WIN-16: a `members_complete` window reaches the end of the cursor without calls only when a reader is there at the landing; without it the chain stands.
#[test]
fn win_orders_members_reader() {
    const WATCH: u8 = 0;
    let all = orders(&[WATCH, 1, 2], &[(1, 2)]);
    assert_eq!(all.len(), 3);
    for order in all {
        let mut twin = Twin::members_complete();
        twin.call(1, Goal::First);
        let mut requests_at_landing = 0;
        let mut reader = false;
        for tag in order.iter().copied() {
            match tag {
                WATCH => {
                    reader = true;
                    twin.watch();
                }
                1 => {
                    twin.respond(Some(2));
                }
                _ => requests_at_landing = requests(&twin.land()),
            }
        }
        assert_eq!(requests_at_landing, usize::from(reader), "{order:?}: the chain goes on with a reader and stands without");
        if !reader {
            assert_eq!(twin.stage(), Some(WindowStage::Idle), "{order:?}");
            continue;
        }
        let mut pages = 1;
        while twin.stage() == Some(WindowStage::Loading) {
            pages += 1;
            assert!(!twin.load_state().settled, "{order:?}: settled before the last page");
            twin.respond((pages < 5).then_some(pages + 1));
            twin.land();
        }
        assert_eq!((pages, twin.load_state().settled, twin.load_state().has_more), (5, true, false), "{order:?}");
    }
}

/// P-WIN-07, P-WIN-27: the last `Interest-` cancels a request that is not accepted by the Store yet and never gives the leaver an outcome; a joined sibling keeps the request and no second request leaves.
#[test]
fn win_orders_cancel_before_landing() {
    let all = orders(&[Ev::Call2, Ev::Leave1, Ev::Respond, Ev::Land], &[(Ev::Respond, Ev::Land)]);
    assert_eq!(all.len(), 12);
    for order in all {
        let mut twin = loading();
        let mut seen = Seen { requests: 1, ..Seen::default() };
        for event in order.iter().copied() {
            run(&mut twin, &mut seen, event);
            assert!((0..=1).contains(&seen.flying()), "{order:?}: {event:?} left {} requests in the network", seen.flying());
        }
        let cancelled = at(&order, Ev::Leave1) < at(&order, Ev::Call2) && at(&order, Ev::Leave1) < at(&order, Ev::Respond);
        assert_eq!((seen.cancels, seen.requests), (usize::from(cancelled), 1 + usize::from(cancelled)), "{order:?}: the cancel belongs to the last leaver before the acceptance");
        if cancelled {
            assert!(twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true }).is_empty(), "{order:?}: no Commit for the cancelled request");
        }
        let landed_first: Vec<Result3> = if at(&order, Ev::Land) < at(&order, Ev::Leave1) { vec![Result3::Loaded] } else { Vec::new() };
        assert_eq!(outcomes_of(&twin, 1), landed_first, "{order:?}: the leaver gets an outcome only from a landing that ended before it left");
        settle(&mut twin, &mut seen);
        assert_eq!((outcomes_of(&twin, 2), twin.stage()), (vec![Result3::Loaded], Some(WindowStage::Idle)), "{order:?}");
        assert_eq!(seen.requests, 1 + usize::from(cancelled), "{order:?}: a second request left for the same page");
    }
}

#[test]
fn win_prop_start_triggers_one_request() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Trigger { Online, Preload, Watch, Load, Refresh }
    use Trigger::*;
    let orders = permutations(&[Online, Preload, Watch, Load, Refresh]);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let mut twin = Twin::before_online();
        let mut seen = Seen::default();
        for trigger in order.iter().copied() {
            let effects = match trigger {
                Online => twin.step(Input::Online(true)),
                Preload => twin.step(Input::InterestPlus { id: PRELOAD }),
                Watch => twin.watch(),
                Load => twin.call(1, Goal::First),
                Refresh => twin.step(Input::Load { id: 2, goal: Goal::First, force: true }),
            };
            seen.add(&effects);
            assert!(seen.requests <= 1, "{order:?}: {trigger:?} started {} requests", seen.requests);
        }
        assert_eq!(seen.requests, 1, "{order:?}: initial request");
        seen.add(&twin.respond(None));
        seen.add(&twin.land());
        assert_eq!(seen.requests, 1, "{order:?}: landing fresh data");
        seen.add(&twin.step(Input::InterestPlus { id: 23 }));
        seen.add(&twin.call(3, Goal::First));
        seen.add(&twin.step(Input::Online(true)));
        assert_eq!(seen.requests, 1, "{order:?}: fresh window started another request");
    }
}
