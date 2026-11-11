use super::*;

// Loading.

#[test]
fn win_loading_join() {
    let mut twin = loading();
    assert_eq!(twin.watch(), [Effect::WindowState]);
    assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_join_waiter() {
    let mut twin = loading();
    assert!(twin.call(2, Goal::First).is_empty());
    assert!(twin.step(Input::Load { id: 3, goal: Goal::First, force: true }).is_empty());
    assert_eq!(twin.state().waiters(&key()), [1, 2, 3]);
    twin.finish(None);
    assert!(twin.state().waiters(&key()).is_empty(), "every waiter of the request was answered");
}

#[test]
fn win_loading_leave() {
    let mut twin = loading_watched();
    twin.call(2, Goal::First);
    assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin.finish(None);
}

#[test]
fn win_loading_last_leave() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_loading_load_more() {
    let mut twin = loading();
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.reference.waiters[&2], super::super::ref_window::Want::Next(2));
    twin.finish(Some(2));
    assert_eq!(twin.stage(), Some(WindowStage::Loading), "the page after the current one loads for the waiter");
}

#[test]
fn win_loading_response() {
    let mut twin = loading();
    assert_eq!(twin.respond(None), [Effect::Commit { request: 1, replace_all: false }]);
    assert_eq!(twin.stage(), Some(WindowStage::Landing));
}

#[test]
fn win_loading_stray() {
    let mut twin = loading();
    for input in [
        Input::Response { request: 9, attempt: 1, cursor: None, rows: true },
        Input::Response { request: 1, attempt: 9, cursor: None, rows: true },
        Input::TransportError { request: 1, attempt: 9, failure: Failure::Network },
        Input::Dropped { request: 9, attempt: 1 },
        Input::Retry { request: 9 },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
    }
}

#[test]
fn win_loading_temp_error() {
    let mut twin = loading();
    assert_eq!(twin.fail(Failure::Network), [Effect::ScheduleRetry { request: 1 }]);
    assert!(matches!(twin.reference.state, State::Loading { retry: true, attempts: 1, .. }));
    let mut dropped = loading();
    assert_eq!(dropped.step(Input::Dropped { request: 1, attempt: 1 }), [Effect::ScheduleRetry { request: 1 }]);
    let mut server = loading();
    assert_eq!(server.fail(Failure::Server), [Effect::ScheduleRetry { request: 1 }]);
}

#[test]
fn win_loading_attempts_exhausted() {
    let mut twin = loading_watched();
    for _ in 0..2 {
        assert_eq!(twin.fail(Failure::Network), [Effect::ScheduleRetry { request: 1 }]);
        twin.step(Input::Retry { request: 1 });
    }
    assert_eq!(twin.fail(Failure::Network), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
    let mut caller = loading();
    caller.fail(Failure::Server);
    caller.step(Input::Retry { request: 1 });
    caller.fail(Failure::Server);
    assert!(caller.state().waiters(&key()).is_empty(), "the waiter heard `Failed`");
}

#[test]
fn win_loading_invalid_cursor() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    assert_eq!(twin.fail(Failure::InvalidCursor), [Effect::Request { request: 3, attempt: 3, page: 1 }]);
    assert!(twin.reference.cursor_reset_used && twin.reference.replace_all);
    assert_eq!(twin.respond(Some(2)), [Effect::Commit { request: 3, replace_all: true }]);
    twin.land();
    let mut again = fresh_with_cursor();
    again.call(2, Goal::Next);
    again.fail(Failure::InvalidCursor);
    assert_eq!(again.fail(Failure::InvalidCursor), none(), "the second INVALID_CURSOR is permanent");
    assert_eq!(again.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_loading_error() {
    let mut twin = loading_watched();
    assert_eq!(twin.fail(Failure::Permanent), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Failed);
    let mut rows = fresh_watched();
    rows.step(Input::Stale { in_keys: true });
    rows.fail(Failure::Permanent);
    assert_eq!(rows.phase(), Phase::Loaded, "rows stay when a later load fails");
}

#[test]
fn win_loading_retry() {
    let mut twin = loading();
    twin.fail(Failure::Network);
    assert_eq!(twin.step(Input::Retry { request: 1 }), [Effect::Request { request: 1, attempt: 2, page: 1 }]);
    assert!(matches!(twin.reference.state, State::Loading { retry: false, attempt: 2, .. }));
}

#[test]
fn win_loading_retry_stray() {
    let mut twin = loading();
    assert!(twin.step(Input::Retry { request: 1 }).is_empty(), "no retry is due while the attempt is in flight");
    twin.fail(Failure::Network);
    assert!(twin.step(Input::Retry { request: 9 }).is_empty());
}

#[test]
fn win_loading_offline() {
    let mut twin = loading_watched();
    assert_eq!(twin.step(Input::Online(false)), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.phase(), Phase::Loading);
    let mut waiting_retry = loading();
    waiting_retry.fail(Failure::Network);
    assert!(waiting_retry.step(Input::Online(false)).is_empty(), "nothing is in flight to cancel");
}

#[test]
fn win_loading_online() {
    let mut twin = loading();
    assert!(twin.step(Input::Online(true)).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_loading_stale_covered() {
    let mut twin = loading();
    twin.step(Input::Online(true));
    assert!(twin.step(Input::StaleGap).is_empty());
    assert!(twin.reference.reread_due, "the request began before the return of the network");
    let mut covered = Twin::new();
    covered.step(Input::Online(false));
    covered.step(Input::Online(true));
    covered.call(1, Goal::First);
    assert!(covered.step(Input::StaleGap).is_empty());
    assert!(!covered.reference.reread_due, "the request began after the return of the network");
}

#[test]
fn win_loading_stale() {
    let mut twin = loading();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.reference.reread_due);
}

#[test]
fn win_loading_stray_store() {
    let mut twin = loading();
    assert!(twin.step(Input::Committed { request: 1, revision: 50 }).is_empty());
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::Storage }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

// Landing.

#[test]
fn win_landing_committed() {
    let mut twin = Twin::new();
    twin.watch();
    twin.call(1, Goal::First);
    twin.respond(None);
    assert_eq!(twin.land(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.phase(), Phase::Loaded);
    assert!(twin.reference.fresh && twin.load_state().loaded == Some(twin.revision));
    assert!(twin.state().waiters(&key()).is_empty());
}

#[test]
fn win_landing_committed_again() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.step(Input::Stale { in_keys: true });
    twin.watch();
    twin.respond(None);
    let effects = twin.land();
    assert_eq!(effects, [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_committed_again_offline() {
    let mut twin = landing();
    twin.watch();
    twin.step(Input::Stale { in_keys: true });
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_landing_committed_again_unwatched() {
    let mut twin = landing();
    twin.step(Input::Stale { in_keys: true });
    assert_eq!(twin.land(), none(), "the only waiter was answered, nobody asks for a reread");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_landing_committed_all_next() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    assert_eq!(twin.land(), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.state().waiters(&key()), [1], "the caller of `all` waits, the other heard `Loaded`");
    twin.finish(None);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_committed_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    assert_eq!(twin.land(), none(), "page limit reached: all heard an outcome");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_committed_all_offline() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    twin.call(2, Goal::First);
    twin.respond(Some(2));
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.state().waiters(&key()), [1]);
}

#[test]
fn win_landing_committed_next_page() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    twin.call(3, Goal::Next);
    twin.respond(Some(3));
    assert_eq!(twin.land(), [Effect::Request { request: 3, attempt: 3, page: 3 }], "the waiters of page 2 heard `Loaded`");
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_members_complete_chain() {
    let mut twin = Twin::members_complete();
    twin.watch();
    twin.call(1, Goal::First);
    twin.respond(Some(2));
    assert_eq!(twin.land(), [Effect::Request { request: 2, attempt: 2, page: 2 }, Effect::WindowState], "the next page is requested without a call of the app");
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert!(twin.state().waiters(&key()).is_empty(), "the caller of the first page heard `Loaded`");
    assert!(!twin.load_state().settled, "the window is not settled before the last page");
}

#[test]
fn win_members_complete_chain_offline() {
    let mut twin = Twin::members_complete();
    twin.watch();
    twin.call(1, Goal::First);
    twin.respond(Some(2));
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.phase(), Phase::Loaded);
    assert!(!twin.load_state().settled);
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 2 }, Effect::WindowState], "the return of the network continues the chain");
}

#[test]
fn win_members_complete_end() {
    let mut twin = Twin::members_complete();
    twin.watch();
    twin.call(1, Goal::First);
    twin.finish(Some(2));
    assert_eq!(twin.finish(None), [Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    let state = twin.load_state();
    assert!(state.settled && !state.has_more && twin.reference.fresh);
}

#[test]
fn win_landing_committed_next_page_offline() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(Some(2));
    twin.step(Input::Online(false));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.state().waiters(&key()), [2]);
}

#[test]
fn win_landing_committed_next_page_end() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(None);
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty(), "no more pages: every waiter heard `Loaded`");
}

#[test]
fn win_landing_committed_next_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::First);
    twin.call(2, Goal::Next);
    twin.respond(Some(2));
    assert_eq!(twin.land(), none());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_landing_stale_generation() {
    let mut twin = landing();
    twin.watch();
    assert_eq!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }), [Effect::Request { request: 2, attempt: 2, page: 1 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_landing_stale_generation_offline() {
    let mut twin = landing();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::Rejected { request: 1, reason: Reject::StaleGeneration }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_register_in_flight_nothing() {
    for state in ["opening", "loading", "landing", "waiting"] {
        let mut twin = match state {
            "opening" => { let mut t = Twin::new(); t.hold.store(true, Ordering::SeqCst); t.watch(); t }
            "loading" => loading(),
            "landing" => landing(),
            "waiting" => waiting(),
            _ => unreachable!(),
        };
        let before = twin.stage();
        take(&twin.log);
        twin.post(w::WindowsInput::RegisterOperations);
        let (effects, outcomes) = twin.split(take(&twin.log));
        assert!(effects.is_empty() && outcomes.is_empty(), "RegisterOperations leaves the request in flight untouched: {state}");
        assert_eq!(twin.stage(), before, "the state is unchanged: {state}");
        match state {
            "opening" => { twin.step(Input::Read(Read::None)); twin.finish(None); }
            "loading" => { twin.finish(None); }
            "landing" => { twin.land(); }
            "waiting" => { twin.step(Input::Online(true)); twin.finish(None); }
            _ => unreachable!(),
        }
        assert_eq!(twin.stage(), Some(WindowStage::Idle), "the window lands after its request finishes: {state}");
    }
}

#[test]
fn win_loading_leave_forced_waiter() {
    let mut twin = Twin::new();
    let mut sent = requests(&twin.step(Input::Load { id: 1, goal: Goal::First, force: true }));
    sent += requests(&twin.step(Input::Load { id: 2, goal: Goal::First, force: true }));
    sent += requests(&twin.step(Input::InterestMinus { id: 1 }));
    sent += requests(&twin.respond(None));
    sent += requests(&twin.land());
    assert_eq!((sent, twin.stage()), (1, Some(WindowStage::Idle)), "the second caller shares the request of the first");
    assert!(twin.state().waiters(&key()).is_empty());
}
