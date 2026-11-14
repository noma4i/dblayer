use super::window_fresh_tests::opening_twin;
use super::*;

/// A source write, the open of a saved source and RegisterOperations derive the preload keys, in every stage, and a key that has a machine is not created again.
#[test]
fn win_preload_keys_derive_from_the_source() {
    {
        let mut twin = opening_twin();
        twin.step(Input::InterestPlus { id: PRELOAD });
        assert_eq!(twin.state().interest(&key()), [w::PRELOAD]);
        take(&twin.log);
        twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()] });
        assert!(take(&twin.log).is_empty(), "a key that has a machine is not created again");
        assert_eq!(requests(&twin.step(Input::Read(Read::None))), 1);
    }
    {
        for stage in [WindowStage::Loading, WindowStage::Landing, WindowStage::Waiting, WindowStage::Idle] {
            let mut twin = build_sources(Some(stage));
            assert_eq!(twin.step(Input::SourceChanged { revision: 7 }), [Effect::DerivePreload { revision: 7 }]);
            assert_eq!(twin.stage(), Some(stage), "the state does not change");
        }
    }
    {
        let saved = Read::Saved { landed_at: NOW - 1_000, fresh_until: NOW + 300_000, stale: false, generation_ok: true };
        let mut written = build_sources(Some(WindowStage::Idle));
        assert_eq!(written.step(Input::SourceChanged { revision: 7 }), [Effect::DerivePreload { revision: 7 }], "a source write derives the RowsOf keys");
        let mut opened = Twin::with_sources();
        opened.hold.store(true, Ordering::SeqCst);
        opened.watch();
        let at_open = opened.step(Input::Read(saved));
        assert!(at_open.iter().any(|effect| matches!(effect, Effect::DerivePreload { .. })), "open derives the RowsOf keys from the saved source, not only a Store commit");
        opened.post(w::WindowsInput::RegisterOperations);
        let (at_register, _) = opened.split(take(&opened.log));
        assert!(at_register.iter().any(|effect| matches!(effect, Effect::DerivePreload { .. })), "RegisterOperations re-derives the RowsOf keys from the saved source");
    }
}



/// A twin whose key is a source of a preload declaration, in the stage `stage`.
pub(super) fn build_sources(stage: Option<WindowStage>) -> Twin {
    let mut twin = Twin::with_sources();
    match stage {
        Some(WindowStage::Waiting) => {
            twin.step(Input::Online(false));
            twin.call(1, Goal::First);
        }
        Some(WindowStage::Loading) => {
            twin.call(1, Goal::First);
        }
        Some(WindowStage::Landing) => {
            twin.call(1, Goal::First);
            twin.respond(None);
        }
        _ => {
            twin.call(1, Goal::First);
            twin.finish(None);
        }
    }
    assert_eq!(twin.stage(), stage);
    twin
}


#[test]
fn win_nodes_empty() {
    let mut twin = Twin::new();
    let nodes = WindowKey::nodes("Moment", &[]);
    twin.post_call(w::WindowsInput::Load { key: nodes, id: 1, goal: w::Goal::First, force: false, store_revision: 42 });
    let (_, outcomes) = twin.split(take(&twin.log));
    assert_eq!(outcomes, [(1, Result3::Loaded)]);
    assert_eq!(twin.state().windows(), 0, "no machine for a key without ids");
}

// Idle.

/// Interest+ on an idle window loads, before the first Online it waits, a fresh one only tells its phase; a passive interest never starts a request; has_more and settled follow the outcomes.
#[test]
fn win_interest_effects_by_stage() {
    {
        let mut twin = Twin::new();
        assert_eq!(twin.watch(), [Effect::WindowState, Effect::Request { request: 1, attempt: 1, page: 1 }]);
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
        let mut offline = Twin::before_online();
        assert_eq!(offline.watch(), [Effect::WindowState], "before the first Online the window waits");
        assert_eq!(offline.stage(), Some(WindowStage::Waiting));
        let mut known = fresh();
        assert_eq!(known.watch(), [Effect::WindowState], "a fresh window only tells its phase");
        assert_eq!(known.stage(), Some(WindowStage::Idle));
        assert_eq!(known.phase(), Phase::Loaded);
    }
    {
        let mut twin = Twin::new();
        assert_eq!(twin.step(Input::InterestPassive { id: STREAM }), [Effect::WindowState], "the phase goes to the stream and no request starts");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
    }
    {
        for (name, mut twin) in [("Loading", loading()), ("Landing", landing()), ("Waiting", waiting())] {
            take(&twin.log);
            let stage = twin.stage();
            twin.post_call(w::WindowsInput::InterestPassive { key: key(), id: u64::from(STREAM) });
            let (effects, _) = twin.split(take(&twin.log));
            assert_eq!(effects, [Effect::WindowState], "{name}: the state goes to the stream and nothing loads");
            assert_eq!(twin.stage(), stage, "{name}: the state stays");
            assert!(twin.state().interest(&key()).contains(&u64::from(STREAM)), "{name}: the interest is kept");
        }
    }
    {
        let mut twin = Twin::new();
        twin.watch();
        let loading = twin.load_state();
        assert!(!loading.settled && !loading.has_more, "the first load has no outcome yet");
        twin.finish(Some(2));
        let first = twin.load_state();
        assert!(first.settled && first.has_more, "a page with a cursor settles the window and offers more");
        assert_eq!(twin.call(2, Goal::Next), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 2 }]);
        assert!(twin.load_state().settled, "a later load does not unsettle a held window");
        twin.finish(None);
        assert!(!twin.load_state().has_more, "the last page has no cursor");
        twin.step(Input::InterestMinus { id: STREAM });
        assert!(!twin.load_state().settled, "the last stream left");
        twin.watch();
        assert!(twin.load_state().settled, "a fresh window settles when a stream joins");
        let mut failing = Twin::new();
        failing.watch();
        failing.fail(Failure::Permanent);
        let failed = failing.load_state();
        assert!(failed.settled && !failed.has_more, "a failed first load is an outcome");
    }
    {
        let mut twin = Twin::new();
        assert!(twin.call(2, Goal::First).iter().any(|effect| matches!(effect, Effect::Request { .. })), "the call starts the first page");
        twin.fail(Failure::Permanent);
        let opened = twin.watch();
        assert!(opened.iter().any(|effect| matches!(effect, Effect::Request { .. })), "a failed window is not fresh: the stream loads again: {opened:?}");
        assert!(!twin.load_state().settled, "the load of the new stream has no outcome yet");
    }
}




/// A call on an idle window asks the wanted page: first, forced, next or all pages.
#[test]
fn win_idle_call_starts_the_wanted_page() {
    {
        let mut twin = fresh_with_cursor();
        assert_eq!(twin.call(2, Goal::All), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        assert_eq!(twin.state().waiters(&key()), [2]);
    }
    {
        let mut twin = Twin::new();
        assert_eq!(twin.call(1, Goal::First), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
        assert_eq!(twin.state().waiters(&key()), [1]);
        let mut forced = fresh();
        assert_eq!(forced.step(Input::Load { id: 2, goal: Goal::First, force: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }], "force loads a fresh window");
    }
    {
        let mut twin = fresh_with_cursor();
        assert_eq!(twin.call(2, Goal::Next), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
        assert_eq!(twin.phase(), Phase::Loading);
        twin.finish(None);
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
}


/// A fresh window answers the call at once and asks nothing: a repeat, every page already there, no page after the last, the page limit.
#[test]
fn win_idle_call_answered_at_once() {
    let mut limited = Twin::with_max_pages(1);
    limited.call(1, Goal::First);
    limited.finish(Some(2));
    for (name, mut twin, input) in [
        ("first page again", fresh(), Input::Load { id: 2, goal: Goal::First, force: false }),
        ("all pages, none more", fresh(), Input::Load { id: 2, goal: Goal::All, force: false }),
        ("next page, none more", fresh(), Input::Load { id: 2, goal: Goal::Next, force: false }),
        ("next page over the page limit", limited, Input::Load { id: 2, goal: Goal::Next, force: false }),
    ] {
        assert!(twin.step(input).is_empty(), "{name}");
        assert_eq!(twin.stage(), Some(WindowStage::Idle), "{name}");
        assert!(twin.state().waiters(&key()).is_empty(), "{name}: the call was answered at once");
    }
}

/// Without the network a call waits, whatever it asks for.
#[test]
fn win_idle_call_offline_waits() {
    let offline = |mut twin: Twin| { twin.step(Input::Online(false)); twin };
    for (name, mut twin, goal) in [
        ("before the first Online", Twin::before_online(), Goal::First),
        ("first page", offline(Twin::new()), Goal::First),
        ("next page", offline(fresh_with_cursor()), Goal::Next),
        ("all pages", offline(fresh_with_cursor()), Goal::All),
    ] {
        assert!(twin.call(1, goal).is_empty(), "{name}");
        assert_eq!(twin.stage(), Some(WindowStage::Waiting), "{name}");
    }
}



/// Stale reloads a watched online window, reads a gap only when the last landing began before the return, and changes nothing offline, unwatched or for another key.
#[test]
fn win_idle_stale_decides_the_reload() {
    {
        let mut twin = fresh_watched();
        assert_eq!(twin.step(Input::Stale { in_keys: true }), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }]);
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        assert_eq!(twin.phase(), Phase::Loading);
    }
    {
        let mut twin = fresh_watched();
        twin.step(Input::Online(false));
        twin.step(Input::Online(true));
        assert_eq!(twin.step(Input::StaleGap), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }], "the last landing began before the return of the network: the gap is read");
        twin.finish(None);
        assert!(twin.step(Input::StaleGap).is_empty(), "the last landing began after the return of the network: nothing to read");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
    {
        let mut twin = fresh_watched();
        twin.step(Input::Online(false));
        assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    }
    {
        let mut twin = fresh();
        assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert!(twin.call(2, Goal::First).len() == 1, "the window is not fresh any more");
    }
    {
        let mut twin = fresh_watched();
        assert!(twin.step(Input::Stale { in_keys: false }).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
        assert_eq!(requests(&twin.call(9, Goal::First)), 0, "a stale of another key leaves the window fresh");
    }
}






/// A window with a reader that is idle and online loads again at every new return to the front, and not without a reader, for the same return, while loading or offline.
#[test]
fn win_foreground_reload_once_per_return() {
    {
        let mut twin = Twin::new();
        twin.step(Input::InterestPlus { id: 1 });
        twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
        twin.step(Input::Committed { request: 1, revision: 5 });
        assert_eq!(twin.step(Input::Foreground { seq: 1 }), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }]);
    }
    {
        let mut unread = Twin::new();
        unread.step(Input::Load { id: 1, goal: Goal::First, force: false });
        unread.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
        unread.step(Input::Committed { request: 1, revision: 5 });
        assert!(unread.step(Input::Foreground { seq: 1 }).is_empty(), "a waiter is no reader");
        let mut again = Twin::new();
        again.step(Input::InterestPlus { id: 1 });
        again.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
        again.step(Input::Committed { request: 1, revision: 5 });
        again.step(Input::Foreground { seq: 1 });
        assert!(again.step(Input::Foreground { seq: 1 }).is_empty(), "the same return counts once");
        let mut loading = Twin::new();
        loading.step(Input::InterestPlus { id: 1 });
        assert!(loading.step(Input::Foreground { seq: 1 }).is_empty(), "a window that loads already is not idle");
        let mut offline = Twin::new();
        offline.step(Input::InterestPlus { id: 1 });
        offline.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
        offline.step(Input::Committed { request: 1, revision: 5 });
        offline.step(Input::Online(false));
        assert!(offline.step(Input::Foreground { seq: 1 }).is_empty(), "no network, no request");
    }
}



/// The return of the network loads a watched window that failed or has a reload interest, and leaves a fresh, an unwatched and a failed unwatched window alone.
#[test]
fn win_idle_online_reload_by_interest() {
    {
        let mut twin = failed_watched();
        assert_eq!(twin.step(Input::Online(true)), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }]);
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
        assert!(twin.step(Input::Online(true)).is_empty(), "a second signal does not read again");
        let mut stale = fresh_watched();
        stale.step(Input::Online(false));
        stale.step(Input::Stale { in_keys: true });
        assert_eq!(stale.step(Input::Online(true)), [Effect::WindowState, Effect::Request { request: 2, attempt: 2, page: 1 }], "a stale window reads without an earlier network error");
        let mut waiting_call = waiting();
        assert_eq!(requests(&waiting_call.step(Input::Online(true))), 1, "a waiting call reads once");
    }
    {
        let mut twin = fresh_watched();
        assert!(twin.step(Input::Online(true)).is_empty(), "a fresh window is not reread");
        let mut unwatched = Twin::new();
        unwatched.step(Input::Online(false));
        assert!(unwatched.step(Input::Online(true)).is_empty());
        let mut failed = Twin::new();
        failed.call(1, Goal::First);
        failed.fail(Failure::Permanent);
        assert!(failed.step(Input::Online(true)).is_empty(), "a window with no interest is not reread");
    }
    {
        let mut twin = fresh_watched();
        twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: id64(STREAM) });
        twin.post(w::WindowsInput::Online { online: Online(false), online_seq: 0 });
        twin.post(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
        assert_eq!(twin.stage(), Some(WindowStage::Loading), "a fresh window with a reload interest loads again when the network returns");
        let mut plain = fresh_watched();
        plain.post(w::WindowsInput::Online { online: Online(false), online_seq: 0 });
        plain.post(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
        assert_eq!(plain.stage(), Some(WindowStage::Idle), "a fresh window without the interest stays");
    }
}




/// An offline refusal tells the streams of a window about the error and starts nothing; a window that loads or lands is not changed; a key without a machine ignores it.
#[test]
fn win_offline_refused_tells_the_streams() {
    {
        let mut twin = fresh_watched();
        take(&twin.log);
        twin.post(w::WindowsInput::OfflineRefused { key: key(), message: "User.all refresh needs the network".into() });
        let (effects, outcomes) = twin.split(take(&twin.log));
        assert_eq!((effects, outcomes.is_empty()), (vec![Effect::WindowState], true), "the window tells its streams about the error and starts no request");
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
    }
    for mut twin in [loading_watched(), landing()] {
        let stage = twin.stage();
        assert_eq!(twin.step(Input::OfflineRefused), none(), "a window that loads or lands is not changed");
        assert_eq!(twin.stage(), stage);
    }
    {
        let mut twin = Twin::new();
        assert!(twin.step(Input::OfflineRefused).is_empty(), "a key without a machine ignores the refusal");
        assert_eq!(twin.stage(), None, "no machine is created and no error is kept");
        assert_eq!(twin.watch(), [Effect::WindowState, Effect::Request { request: 1, attempt: 1, page: 1 }], "the next interest opens the window as on a cold key");
        assert_eq!(twin.stage(), Some(WindowStage::Loading));
    }
}



#[test]
fn win_preload_after_register() {
    enum Step { Started(Option<u64>), Online, Register }
    use Step::{Online as Net, Register, Started};
    let cases: [(&str, Vec<Step>, usize, Option<WindowStage>); 5] = [
        ("register after the session and the network", vec![Started(Some(1)), Net, Register], 1, Some(WindowStage::Loading)),
        ("register before the session", vec![Register, Started(Some(1)), Net], 1, Some(WindowStage::Loading)),
        ("register between the session and the network", vec![Started(Some(1)), Register, Net], 1, Some(WindowStage::Loading)),
        ("register twice", vec![Started(Some(1)), Net, Register, Register], 1, Some(WindowStage::Loading)),
        ("register without a session", vec![Started(None), Net, Register], 0, None),
    ];
    for (name, steps, expected, stage) in cases {
        let mut twin = Twin::awaiting_operations();
        let (mut asked, mut registered) = (0, false);
        for step in steps {
            registered |= matches!(step, Register);
            match step {
                Started(identity) => twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity, generation: 0 })),
                Net => twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1 }),
                Register => twin.post_call(w::WindowsInput::RegisterOperations),
            }
            let step_requests = requests(&twin.split(take(&twin.log)).0);
            assert!(registered || step_requests == 0, "{name}: no request before the operations are declared");
            asked += step_requests;
        }
        assert_eq!(asked, expected, "{name}: the declared preload key asks once");
        assert_eq!(twin.stage(), stage, "{name}");
    }
}


