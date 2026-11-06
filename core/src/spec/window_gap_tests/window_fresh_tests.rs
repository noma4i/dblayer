use super::*;

// Rows of `window.md` for `Opening` and freshness by the `now` of the input: the owner against the reference, the read pool answers when the test says so.

pub(super) const LIFE: i64 = 300_000;
pub(super) const EMPTY_LIFE: i64 = 60_000;

/// A twin whose reads wait for the test.
pub(super) fn opening_twin() -> Twin {
    let twin = Twin::new();
    twin.hold.store(true, Ordering::SeqCst);
    twin
}

pub(super) fn saved(landed_at: i64, stale: bool) -> Read { Read::Saved { landed_at, fresh_until: landed_at + LIFE, stale, generation_ok: true } }

/// The fresh window of a stream and a call has no interest: the next input decides by its own `now`.
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
    twin.land();
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

#[test]
fn win_prop_freshness_by_input_time() {
    for (now, asked) in [(NOW + LIFE - 1, 0), (NOW + LIFE, 1)] {
        let mut twin = fresh_idle();
        twin.now = now;
        assert_eq!(requests(&twin.watch()), asked, "now = {now}");
    }
}

#[test]
fn win_prop_clock_back() {
    let mut twin = fresh_idle();
    twin.now = NOW - 1;
    assert_eq!(requests(&twin.watch()), 1, "a clock before landed_at does not make the window fresh");
}

#[test]
fn win_prop_freshness_survives_open() {
    for (read, asked) in [(saved(NOW - 10_000, false), 0), (saved(NOW - LIFE, false), 1), (saved(NOW - 10_000, true), 1)] {
        let mut twin = opening_twin();
        twin.watch();
        assert_eq!(requests(&twin.step(Input::Read(read))), asked, "{read:?}");
    }
}
