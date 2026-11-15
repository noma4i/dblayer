use super::*;

// Loading.

/// Waiters of a loading window share one request: a call joins, a forced call joins, a leave keeps it, the next page loads for its waiter, every waiter hears the answer once.
#[test]
fn win_loading_waiters_share_the_request() {
    {
        let mut twin = loading();
        assert!(twin.call(2, Goal::First).is_empty());
        assert!(twin.step(Input::Load { id: 3, goal: Goal::First, force: true }).is_empty());
        assert_eq!(twin.state().waiters(&key()), [1, 2, 3]);
        twin.finish(None);
        assert!(twin.state().waiters(&key()).is_empty(), "every waiter of the request was answered");
    }
    {
        let mut twin = loading_watched();
        twin.call(2, Goal::First);
        assert!(twin.step(Input::InterestMinus { id: 2 }).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        twin.finish(None);
    }
    {
        let mut twin = Twin::new();
        let mut sent = requests(&twin.step(Input::Load { id: 1, goal: Goal::First, force: true }));
        sent += requests(&twin.step(Input::Load { id: 2, goal: Goal::First, force: true }));
        sent += requests(&twin.step(Input::InterestMinus { id: 1 }));
        sent += requests(&twin.respond(None));
        sent += requests(&twin.land());
        assert_eq!((sent, twin.stage()), (1, Some(WindowStage::Idle)), "the second caller shares the request of the first");
        assert!(twin.state().waiters(&key()).is_empty());
    }
    {
        let mut twin = loading();
        assert!(twin.call(2, Goal::Next).is_empty());
        twin.finish(Some(2));
        assert_eq!(twin.stage(), Some(WindowStage::Loading), "the page after the current one loads for the waiter");
    }
}




/// A landed page answers the waiters and the stream, makes the window fresh and the loaded revision the landed one; the answer of the page goes to the Store first.
#[test]
fn win_landing_committed() {
    {
        let mut twin = loading();
        assert_eq!(twin.respond(None), [Effect::Commit { request: 1, replace_all: false }]);
        assert_eq!(twin.stage(), Some(WindowStage::Landing));
    }
    {
        let mut twin = Twin::new();
        twin.watch();
        twin.call(1, Goal::First);
        twin.respond(None);
        assert_eq!(twin.land(), [Effect::WindowState]);
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert_eq!(twin.phase(), Phase::Loaded);
        assert!(twin.load_state().loaded == Some(twin.revision));
        assert_eq!(requests(&twin.call(9, Goal::First)), 0, "the landed window is fresh");
        assert!(twin.state().waiters(&key()).is_empty());
    }
}


/// A network, server or dropped failure schedules a retry that asks again with a new attempt, the third failure fails the phase, a permanent error fails it at once and a later failed load keeps the rows.
#[test]
fn win_loading_failures_retry_then_fail() {
    {
        let mut twin = loading();
        assert_eq!(twin.fail(Failure::Network), [Effect::ScheduleRetry { request: 1 }]);
        let mut dropped = loading();
        assert_eq!(dropped.step(Input::Dropped { request: 1, attempt: 1 }), [Effect::ScheduleRetry { request: 1 }]);
        let mut server = loading();
        assert_eq!(server.fail(Failure::Server), [Effect::ScheduleRetry { request: 1 }]);
    }
    {
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
    {
        let mut twin = loading();
        twin.fail(Failure::Network);
        assert_eq!(twin.step(Input::Retry { request: 1 }), [Effect::Request { request: 1, attempt: 2, page: 1 }]);
    }
    {
        let mut twin = loading_watched();
        assert_eq!(twin.fail(Failure::Permanent), [Effect::WindowState]);
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert_eq!(twin.phase(), Phase::Failed);
        let mut rows = fresh_watched();
        rows.step(Input::Stale { in_keys: true });
        rows.fail(Failure::Permanent);
        assert_eq!(rows.phase(), Phase::Failed, "the window does not judge its rows when a later load fails");
    }
}



#[test]
fn win_loading_invalid_cursor() {
    let mut twin = fresh_with_cursor();
    twin.call(2, Goal::Next);
    assert_eq!(twin.fail(Failure::InvalidCursor), [Effect::Request { request: 3, attempt: 3, page: 1 }]);
    assert_eq!(twin.respond(Some(2)), [Effect::Commit { request: 3, replace_all: true }]);
    twin.land();
    let mut again = fresh_with_cursor();
    again.call(2, Goal::Next);
    again.fail(Failure::InvalidCursor);
    assert_eq!(again.fail(Failure::InvalidCursor), none(), "the second INVALID_CURSOR is permanent");
    assert_eq!(again.stage(), Some(WindowStage::Idle));
}



#[test]
fn win_loading_offline() {
    let mut twin = loading_watched();
    assert_eq!(twin.step(Input::Online(false)), [Effect::Cancel { request: 1, attempt: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(twin.phase(), Phase::Idle);
    let mut waiting_retry = loading();
    waiting_retry.fail(Failure::Network);
    assert!(waiting_retry.step(Input::Online(false)).is_empty(), "nothing is in flight to cancel");
}

#[test]
fn win_loading_stale_covered() {
    let mut late = Twin::new();
    late.watch();
    late.step(Input::Online(true));
    assert!(late.step(Input::StaleGap).is_empty());
    late.respond(None);
    late.advance(1_000);
    assert_eq!(requests(&late.land()), 1, "the request began before the return of the network: the landed rows are read again");
    let mut covered = Twin::new();
    covered.step(Input::Online(false));
    covered.step(Input::Online(true));
    covered.watch();
    assert!(covered.step(Input::StaleGap).is_empty());
    covered.respond(None);
    assert_eq!(requests(&covered.land()), 0, "the request began after the return of the network");
}

// Landing.


/// A Stale that came during the landing is read again only for a watched online window; a refused stale generation asks again online and waits offline.
#[test]
fn win_landing_stale_reread() {
    {
        let mut twin = Twin::new();
        twin.call(1, Goal::First);
        twin.step(Input::Stale { in_keys: true });
        twin.watch();
        twin.respond(None);
        twin.advance(1_000);
        let effects = twin.land();
        assert_eq!(effects, [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }]);
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
    }
    {
        let mut twin = landing();
        twin.watch();
        twin.step(Input::Stale { in_keys: true });
        twin.step(Input::Online(false));
        assert_eq!(twin.land(), [Effect::WindowState]);
        assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    }
    {
        let mut twin = landing();
        twin.step(Input::Stale { in_keys: true });
        assert_eq!(twin.land(), none(), "the only waiter was answered, nobody asks for a reread");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        twin.advance(1_000);
        assert_eq!(requests(&twin.call(9, Goal::First)), 1, "the window is not fresh any more");
    }
}




/// After a landing the chain asks the next page only for a waiter whose goal is not met (all pages, next page) and the network is up and the page limit is not reached; every other waiter hears Loaded.
#[test]
fn win_landing_follows_the_goal_of_the_waiters() {
    {
        let mut twin = Twin::new();
        twin.call(1, Goal::All);
        twin.call(2, Goal::First);
        twin.respond(Some(2));
        assert_eq!(twin.land(), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
        assert_eq!(twin.state().waiters(&key()), [1], "the caller of `all` waits, the other heard `Loaded`");
        twin.finish(None);
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
    {
        let mut twin = Twin::with_max_pages(1);
        twin.call(1, Goal::All);
        twin.call(2, Goal::First);
        twin.respond(Some(2));
        assert_eq!(twin.land(), none(), "page limit reached: all heard an outcome");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
    {
        let mut twin = Twin::new();
        twin.call(1, Goal::All);
        twin.call(2, Goal::First);
        twin.respond(Some(2));
        twin.step(Input::Online(false));
        assert_eq!(twin.land(), none());
        assert_eq!(twin.stage(), Some(WindowStage::Waiting));
        assert_eq!(twin.state().waiters(&key()), [1]);
    }
    {
        let mut twin = fresh_with_cursor();
        twin.call(2, Goal::Next);
        twin.call(3, Goal::Next);
        twin.respond(Some(3));
        assert_eq!(twin.land(), [Effect::Request { request: 3, attempt: 3, page: 3 }], "the waiters of page 2 heard `Loaded`");
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
    }
    {
        let mut twin = Twin::new();
        twin.call(1, Goal::First);
        twin.call(2, Goal::Next);
        twin.respond(Some(2));
        twin.step(Input::Online(false));
        assert_eq!(twin.land(), none());
        assert_eq!(twin.stage(), Some(WindowStage::Waiting));
        assert_eq!(twin.state().waiters(&key()), [2]);
    }
    {
        let mut twin = Twin::new();
        twin.call(1, Goal::First);
        twin.call(2, Goal::Next);
        twin.respond(None);
        assert_eq!(twin.land(), none());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert!(twin.state().waiters(&key()).is_empty(), "no more pages: every waiter heard `Loaded`");
    }
    {
        let mut twin = Twin::with_max_pages(1);
        twin.call(1, Goal::First);
        twin.call(2, Goal::Next);
        twin.respond(Some(2));
        assert_eq!(twin.land(), none());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
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
    assert_eq!(twin.step(Input::Online(true)), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 2 }], "the return of the network continues the chain");
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

