use super::*;

// Rows of `window.md` for `Opening` and freshness by the time of the step: the owner against the reference, the read pool answers when the test says so.

pub(super) const LIFE: i64 = 300_000;
pub(super) const EMPTY_LIFE: i64 = 60_000;

/// A twin whose reads wait for the test.
pub(super) fn opening_twin() -> Twin {
    let twin = Twin::new();
    twin.hold.store(true, Ordering::SeqCst);
    twin
}

pub(super) fn saved(landed_at: i64, stale: bool) -> Read { Read::Saved { landed_at, fresh_until: landed_at + LIFE, stale, generation_ok: true } }

/// The fresh window of a stream and a call has no interest: the next input decides by the time of its step.
pub(super) fn fresh_idle() -> Twin {
    let mut twin = fresh_watched();
    twin.step(Input::InterestMinus { id: STREAM });
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    twin
}

#[test]
fn win_opening_read() {
    let mut twin = opening_twin();
    assert_eq!(twin.call(1, Goal::First), [Effect::ReadWindow], "the saved window is read in the pool, nothing is asked");
    assert_eq!(twin.stage(), Some(WindowStage::Opening));
    assert_eq!(requests(&twin.step(Input::Read(Read::None))), 1, "the held load starts after the read");
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_created_from_saved() {
    let mut twin = opening_twin();
    twin.watch();
    let opened = twin.step(Input::Read(saved(NOW - 1_000, false)));
    assert_eq!((requests(&opened), twin.stage()), (0, Some(WindowStage::Idle)), "a saved window inside its lifetime asks nothing");
    assert_eq!(twin.call(1, Goal::First), none(), "a call on a fresh saved window is answered at once (the twin compares the outcome)");
}

#[test]
fn win_created_other_generation() {
    let mut twin = opening_twin();
    twin.watch();
    let opened = twin.step(Input::Read(Read::Saved { landed_at: NOW - 1_000, fresh_until: NOW + LIFE, stale: false, generation_ok: false }));
    assert_eq!(requests(&opened), 1, "a window of another generation is none: the first load asks the network");
}

#[test]
fn win_opening_read_failed() {
    let mut twin = opening_twin();
    twin.watch();
    assert_eq!(requests(&twin.step(Input::Read(Read::Failed))), 1, "the window loads from the network, the held input runs");
}

#[test]
fn win_opening_read_stray() {
    let mut twin = opening_twin();
    twin.watch();
    assert_eq!(twin.step(Input::Read(Read::Stray)), none());
    assert_eq!(twin.stage(), Some(WindowStage::Opening), "a read of another id changes nothing");
}

#[test]
fn win_opening_held() {
    let mut twin = opening_twin();
    twin.step(Input::InterestPassive { id: STREAM });
    assert_eq!(twin.step(Input::Online(false)), none());
    twin.call(1, Goal::First);
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), none());
    assert_eq!(twin.stage(), Some(WindowStage::Opening));
    assert_eq!((twin.state().interest(&key()), twin.state().waiters(&key())), (vec![u64::from(STREAM)], Vec::<u64>::new()), "Interest+ and Interest- change the interest at once");
    twin.step(Input::Read(Read::None));
    assert_eq!(twin.stage(), Some(WindowStage::Waiting), "the held inputs run in order: the network was away before the load began, so it waits");
}

#[test]
fn win_prop_opening_held_order() {
    let mut twin = opening_twin();
    twin.call(1, Goal::First);
    twin.call(2, Goal::First);
    twin.step(Input::InterestMinus { id: 1 });
    let opened = twin.step(Input::Read(Read::None));
    assert_eq!(requests(&opened), 1, "the inputs of the key run once, in the order they came");
    assert_eq!(twin.state().waiters(&key()), [2], "the waiter that left is gone, the other stays");
}

#[test]
fn win_opening_end() {
    let mut twin = opening_twin();
    twin.call(1, Goal::First);
    twin.step(Input::EpochEnded);
    assert_eq!(twin.stage(), Some(WindowStage::Ended), "the call that waited is interrupted (the twin compares the outcome)");
}

#[test]
fn win_idle_interest_expired() {
    let mut twin = fresh_idle();
    twin.now = NOW + LIFE - 1;
    assert_eq!(requests(&twin.watch()), 0, "inside the lifetime a stream asks nothing");
    twin.step(Input::InterestMinus { id: STREAM });
    twin.now = NOW + LIFE;
    assert_eq!(requests(&twin.watch()), 1, "at fresh_until the stream starts a load");
}

#[test]
fn win_idle_load_expired() {
    let mut twin = fresh_idle();
    twin.now = NOW + LIFE - 1;
    assert_eq!(requests(&twin.call(1, Goal::First)), 0);
    twin.now = NOW + LIFE;
    assert_eq!(requests(&twin.call(2, Goal::First)), 1, "a call at fresh_until starts a load");
}

#[test]
fn win_idle_online_expired() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    twin.now = NOW + LIFE;
    assert_eq!(requests(&twin.step(Input::Online(true))), 1, "the network returns after fresh_until: the watched window loads");
}

#[test]
fn win_idle_online_fresh() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    twin.now = NOW + LIFE - 1;
    assert_eq!(requests(&twin.step(Input::Online(true))), 0, "the network returns inside the lifetime: no request");
}

#[test]
fn win_idle_expiry_silent() {
    let mut twin = fresh_watched();
    take(&twin.log);
    twin.det.tick(Time(u64::MAX / 2));
    twin.run();
    assert!(take(&twin.log).is_empty(), "the time passed fresh_until and no input came: nothing happens");
}

#[test]
fn win_landing_fresh_until() {
    let mut twin = Twin::new();
    twin.watch();
    let (request, attempt) = twin.ref_request();
    twin.step(Input::Response { request, attempt, cursor: None, rows: false });
    assert!(twin.commit.contains(&format!("landed_at: {NOW}")), "{}", twin.commit);
    twin.now = NOW + 500;
    twin.land();
    twin.now = NOW;
    twin.step(Input::InterestMinus { id: STREAM });
    twin.now = NOW + EMPTY_LIFE - 1;
    assert_eq!(requests(&twin.call(1, Goal::First)), 0, "an empty page is fresh for empty_in");
    twin.now = NOW + EMPTY_LIFE;
    assert_eq!(requests(&twin.call(2, Goal::First)), 1);
    let mut rows = fresh_idle();
    rows.now = NOW + EMPTY_LIFE;
    assert_eq!(requests(&rows.call(1, Goal::First)), 0, "a page with rows is fresh for expires_in");
}

#[test]
fn win_landing_next_page_keeps_fresh_until() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.finish(Some(2));
    twin.now = NOW + 100_000;
    twin.call(2, Goal::Next);
    twin.now = NOW + 200_000;
    twin.finish(None);
    twin.now = NOW + LIFE;
    assert_eq!(requests(&twin.call(3, Goal::First)), 1, "page 2 did not move landed_at: the window expires from page 1");
}

#[derive(Clone, Copy, Debug)]
enum Turn {
    Watch,
    Unwatch,
    Call,
    Respond,
    Land,
}

#[derive(Default)]
struct Flight {
    request: u64,
    attempt: u32,
    batch: u64,
    revision: i64,
}

struct Trace {
    lines: Vec<String>,
    requests: Vec<usize>,
}

type Plan = Vec<(Turn, u64)>;

fn input_of(turn: Turn, flight: &Flight) -> w::WindowsInput {
    match turn {
        Turn::Watch => w::WindowsInput::InterestPlus { key: key(), id: u64::from(STREAM) },
        Turn::Unwatch => w::WindowsInput::InterestMinus { key: key(), id: u64::from(STREAM) },
        Turn::Call => w::WindowsInput::Load { key: key(), id: 1, goal: w::Goal::First, force: false, store_revision: 0 },
        Turn::Respond => w::WindowsInput::Response { request: flight.request, attempt: flight.attempt, page: AnswerPage::new(1, vec!["r1".into()], None) },
        Turn::Land => w::WindowsInput::Committed(Committed { batch: flight.batch, revision: flight.revision, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }),
    }
}

fn drive(plan: &Plan) -> Trace {
    let mut twin = Twin::new();
    take(&twin.log);
    let mut flight = Flight::default();
    let mut trace = Trace { lines: Vec::new(), requests: Vec::new() };
    for (turn, step) in plan {
        twin.det.tick(Time(*step));
        if matches!(turn, Turn::Land) {
            flight.revision += 1;
        }
        let input = input_of(*turn, &flight);
        match turn {
            Turn::Respond | Turn::Land => twin.post(input),
            _ => twin.post_call(input),
        }
        let emitted = take(&twin.log);
        let commit = emitted.iter().find_map(|entry| match entry {
            Emitted::Send { to: STORE, input } if input.contains("author: Window") => Some(input.clone()),
            _ => None,
        });
        let (effects, outcomes) = twin.split(emitted);
        for effect in &effects {
            match effect {
                Effect::Request { request, attempt, .. } => (flight.request, flight.attempt) = (*request, *attempt),
                Effect::Commit { request, .. } => flight.batch = *request,
                _ => {}
            }
        }
        trace.requests.push(requests(&effects));
        trace.lines.push(format!("{turn:?}@{step}: {effects:?} {outcomes:?} {commit:?} {:?} {:?}", twin.stage(), twin.state().load_state(&key())));
    }
    trace
}

/// A window landed at `NOW` and left idle, then inputs at growing step times: the first at or after `fresh_until` asks once, the rest wait for that answer.
fn seeded_plan(seed: u64) -> (Plan, Vec<usize>) {
    const DELTAS: [u64; 7] = [0, 1, 1_000, 150_000, 299_999, 300_000, 600_000];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut roll = |bound: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        usize::try_from(state % bound as u64).expect("a small roll")
    };
    let start = NOW as u64;
    let expired = (NOW + LIFE) as u64;
    let mut plan: Plan = vec![(Turn::Watch, start), (Turn::Respond, start), (Turn::Land, start), (Turn::Unwatch, start)];
    let mut want = vec![1, 0, 0, 0];
    let mut step = start;
    let mut asked = false;
    for _ in 0..12 {
        let turn = [Turn::Watch, Turn::Call][roll(2)];
        step += DELTAS[roll(DELTAS.len())];
        let first = !asked && step >= expired;
        asked |= first;
        want.push(usize::from(first));
        plan.push((turn, step));
        if matches!(turn, Turn::Watch) && !asked {
            plan.push((Turn::Unwatch, step));
            want.push(0);
        }
    }
    (plan, want)
}

#[test]
fn win_prop_fresh_for_any_input_times() {
    let start = NOW as u64;
    let expired = (NOW + LIFE) as u64;
    let landed = |end: Turn, step: u64| -> Plan { vec![(Turn::Watch, start), (Turn::Respond, start), (Turn::Land, start), (Turn::Unwatch, start), (end, step)] };
    let cases: [(&str, Plan, usize); 4] = [
        ("a call at fresh_until", landed(Turn::Call, expired), 1),
        ("a call inside the life", landed(Turn::Call, expired - 1), 0),
        ("a stream at fresh_until", landed(Turn::Watch, expired), 1),
        ("a stream inside the life", landed(Turn::Watch, expired - 1), 0),
    ];
    for (name, plan, asked) in cases {
        let trace = drive(&plan);
        assert_eq!(trace.requests.last(), Some(&asked), "{name}: {:?}", trace.lines.last());
    }
    let stamped = vec![(Turn::Watch, start), (Turn::Respond, start + 500), (Turn::Land, start + 500)];
    let trace = drive(&stamped);
    assert!(trace.lines[1].contains(&format!("landed_at: {}", NOW + 500)), "landed_at is the time of the step of Response: {}", trace.lines[1]);
    std::thread::scope(|scope| {
        let runs: Vec<_> = (1..=8u64)
            .map(|seed| {
                scope.spawn(move || {
                    let (plan, want) = seeded_plan(seed);
                    let trace = drive(&plan);
                    assert_eq!(trace.requests, want, "seed {seed}: the decision follows the time of the step: {:?}", plan);
                })
            })
            .collect();
        for run in runs {
            crate::test_wait::join_within(run);
        }
    });
}

#[test]
fn win_prop_clock_back() {
    for (ahead, asked) in [(1, 0), (LIFE - 1, 0), (LIFE, 1), (LIFE + 1, 1)] {
        let mut twin = opening_twin();
        twin.watch();
        assert_eq!(requests(&twin.step(Input::Read(saved(NOW + ahead, false)))), asked, "a stream: the saved window landed {ahead} ms after the step time");
        let mut twin = opening_twin();
        twin.call(1, Goal::First);
        assert_eq!(requests(&twin.step(Input::Read(saved(NOW + ahead, false)))), asked, "a call: the saved window landed {ahead} ms after the step time");
    }
}

#[test]
fn win_prop_freshness_survives_open() {
    for (read, asked) in [(saved(NOW - 10_000, false), 0), (saved(NOW - LIFE, false), 1), (saved(NOW - 10_000, true), 1)] {
        let mut twin = opening_twin();
        twin.watch();
        assert_eq!(requests(&twin.step(Input::Read(read))), asked, "{read:?}");
    }
}

#[test]
fn win_idle_stale_lost_rereads() {
    let mut twin = fresh_watched();
    take(&twin.log);
    twin.post(w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: w::StaleCause::Lost });
    let (effects, _) = twin.split(take(&twin.log));
    assert_eq!(requests(&effects), 1, "a lost event requires a new load: {effects:?}");
}
