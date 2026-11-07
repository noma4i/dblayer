use super::*;

#[test]
fn win_preload_created() {
    let mut twin = Twin::with_preload();
    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(1), generation: 0 }));
    twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    let effects = twin.reference.step(Input::InterestPlus { id: PRELOAD });
    let (got, _) = twin.split(take(&twin.log));
    assert_eq!(got, effects.iter().filter(|effect| **effect != Effect::WindowState).cloned().collect::<Vec<_>>(), "the preload key loads without a stream to tell");
    assert_eq!(twin.state().interest(&key()), [w::PRELOAD]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_preload_derived() {
    let mut twin = Twin::new();
    twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()], now: NOW });
    let effects = twin.reference.step(Input::InterestPlus { id: PRELOAD });
    let (got, _) = twin.split(take(&twin.log));
    assert_eq!(got, effects.iter().filter(|effect| **effect != Effect::WindowState).cloned().collect::<Vec<_>>());
    assert_eq!(twin.state().interest(&key()), [w::PRELOAD]);
    twin.post(w::WindowsInput::PreloadKeys { keys: vec![key()], now: NOW });
    assert!(take(&twin.log).is_empty(), "a key that has a machine is not created again");
}

#[test]
fn win_preload_source_committed() {
    for stage in [WindowStage::Loading, WindowStage::Landing, WindowStage::Waiting, WindowStage::Idle] {
        let mut twin = build_sources(Some(stage));
        assert_eq!(twin.step(Input::SourceChanged { revision: 7 }), [Effect::DerivePreload { revision: 7 }]);
        assert_eq!(twin.stage(), Some(stage), "the state does not change");
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
    twin.post_call(w::WindowsInput::Load { key: nodes, id: 1, goal: w::Goal::First, force: false, store_revision: 42, now: NOW });
    let (_, outcomes) = twin.split(take(&twin.log));
    assert_eq!(outcomes, [(1, Result3::Loaded)]);
    assert_eq!(twin.state().windows(), 0, "no machine for a key without ids");
    assert_eq!(Window::empty_nodes_outcome(1), (1, Result3::Loaded));
}

#[test]
fn win_created() {
    let mut twin = Twin::new();
    assert_eq!(twin.state().windows(), 0);
    assert_eq!(twin.watch(), [Effect::Request { request: 1, attempt: 1, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.state().windows(), 1);
    let mut called = Twin::new();
    assert_eq!(called.call(1, Goal::First), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
    assert_eq!(called.state().windows(), 1);
}

// Idle.

#[test]
fn win_idle_interest() {
    let mut twin = Twin::new();
    assert_eq!(twin.watch(), [Effect::Request { request: 1, attempt: 1, page: 1 }, Effect::WindowState]);
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

#[test]
fn win_state_has_more_settled() {
    let mut twin = Twin::new();
    twin.watch();
    let loading = twin.load_state();
    assert!(!loading.settled && !loading.has_more, "the first load has no outcome yet");
    twin.finish(Some(2));
    let first = twin.load_state();
    assert!(first.settled && first.has_more, "a page with a cursor settles the window and offers more");
    assert_eq!(twin.call(2, Goal::Next), [Effect::Request { request: 2, attempt: 2, page: 2 }, Effect::WindowState]);
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

#[test]
fn win_idle_interest_passive() {
    let mut twin = Twin::new();
    assert_eq!(twin.step(Input::InterestPassive { id: STREAM }), [Effect::WindowState], "the phase goes to the stream and no request starts");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert_eq!(twin.state().interest(&key()), [u64::from(STREAM)]);
}

#[test]
fn win_idle_interest_fresh() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::First).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty(), "the call was answered at once");
}

#[test]
fn win_idle_interest_all_done() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_interest_all_more() {
    let mut twin = fresh_with_cursor();
    assert_eq!(twin.call(2, Goal::All), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.state().waiters(&key()), [2]);
}

#[test]
fn win_idle_interest_all_offline() {
    let mut twin = fresh_with_cursor();
    twin.step(Input::Online(false));
    assert!(twin.call(2, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert!(matches!(twin.reference.state, State::Waiting { force: false, page: 2 }));
}

#[test]
fn win_idle_interest_load() {
    let mut twin = Twin::new();
    assert_eq!(twin.call(1, Goal::First), [Effect::Request { request: 1, attempt: 1, page: 1 }]);
    assert_eq!(twin.state().waiters(&key()), [1]);
    let mut forced = fresh();
    assert_eq!(forced.step(Input::Load { id: 2, goal: Goal::First, force: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }], "force loads a fresh window");
}

#[test]
fn win_idle_interest_offline() {
    let mut twin = Twin::before_online();
    assert!(twin.call(1, Goal::First).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    let mut off = Twin::new();
    off.step(Input::Online(false));
    off.call(1, Goal::First);
    assert_eq!(off.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_interest_minus() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::InterestMinus { id: STREAM }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().interest(&key()).is_empty());
}

#[test]
fn win_idle_load_more() {
    let mut twin = fresh_with_cursor();
    assert_eq!(twin.call(2, Goal::Next), [Effect::Request { request: 2, attempt: 2, page: 2 }]);
    assert_eq!(twin.phase(), Phase::Loading);
    twin.finish(None);
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_load_more_offline() {
    let mut twin = fresh_with_cursor();
    twin.step(Input::Online(false));
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert!(matches!(twin.reference.state, State::Waiting { page: 2, .. }));
}

#[test]
fn win_idle_load_more_end() {
    let mut twin = fresh();
    assert!(twin.call(2, Goal::Next).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_page_limit() {
    let mut twin = Twin::with_max_pages(1);
    twin.call(1, Goal::First);
    twin.finish(Some(2));
    assert!(twin.call(2, Goal::Next).is_empty());
    assert!(twin.call(3, Goal::All).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_stale_watched() {
    let mut twin = fresh_watched();
    assert_eq!(twin.step(Input::Stale { in_keys: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    assert_eq!(twin.phase(), Phase::Loading);
}

#[test]
fn win_idle_stale_covered() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    twin.step(Input::Online(true));
    assert_eq!(twin.step(Input::StaleGap), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState], "the last landing began before the return of the network: the gap is read");
    twin.finish(None);
    assert!(twin.step(Input::StaleGap).is_empty(), "the last landing began after the return of the network: nothing to read");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_stale_watched_offline() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_stale_unwatched() {
    let mut twin = fresh();
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
    assert!(twin.call(2, Goal::First).len() == 1, "the window is not fresh any more");
}

#[test]
fn win_idle_stale_other_key() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::Stale { in_keys: false }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.reference.fresh);
}

/// win_foreground_reload: a window with a reader that is idle and online loads its first page again at every return to the front.
#[test]
fn win_foreground_reload() {
    let mut twin = Twin::new();
    twin.step(Input::InterestPlus { id: 1 });
    twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true });
    twin.step(Input::Committed { request: 1, revision: 5 });
    assert_eq!(twin.step(Input::Foreground { seq: 1 }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
}

/// win_foreground_other: no reader, the same sequence number, a window that is not idle and no network give no request.
#[test]
fn win_foreground_other() {
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

#[test]
fn win_idle_online_retry() {
    let mut twin = failed_watched();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_idle_online() {
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

#[test]
fn win_idle_reload_on_connect() {
    let mut twin = fresh_watched();
    twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: id64(STREAM) });
    twin.post(w::WindowsInput::Online { online: Online(false), online_seq: 0, now: NOW });
    twin.post(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    assert_eq!(twin.stage(), Some(WindowStage::Loading), "a fresh window with a reload interest loads again when the network returns");
    let mut plain = fresh_watched();
    plain.post(w::WindowsInput::Online { online: Online(false), online_seq: 0, now: NOW });
    plain.post(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    assert_eq!(plain.stage(), Some(WindowStage::Idle), "a fresh window without the interest stays");
}

#[test]
fn win_idle_offline_refused() {
    let mut twin = fresh_watched();
    take(&twin.log);
    twin.post(w::WindowsInput::OfflineRefused { key: key(), message: "User.all refresh needs the network".into() });
    let (effects, outcomes) = twin.split(take(&twin.log));
    assert_eq!((effects, outcomes.is_empty()), (vec![Effect::WindowState], true), "the window tells its streams about the error and starts no request");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
}

#[test]
fn win_idle_models_reset() {
    let mut twin = fresh_watched();
    assert_eq!(twin.step(Input::ModelsReset { own_model: true }), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[test]
fn win_idle_models_reset_offline() {
    let mut twin = fresh_watched();
    twin.step(Input::Online(false));
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

#[test]
fn win_idle_models_reset_unwatched() {
    let mut twin = fresh();
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(!twin.reference.fresh);
}

#[test]
fn win_idle_models_other() {
    let mut twin = fresh_watched();
    assert!(twin.step(Input::ModelsReset { own_model: false }).is_empty());
    assert!(twin.reference.fresh);
}

#[test]
fn win_idle_stray() {
    let mut twin = fresh();
    for input in [
        Input::Response { request: 9, attempt: 9, cursor: None, rows: true },
        Input::TransportError { request: 9, attempt: 9, failure: Failure::Network },
        Input::Dropped { request: 9, attempt: 9 },
        Input::Retry { request: 9 },
        Input::Committed { request: 9, revision: 99 },
        Input::Rejected { request: 9, reason: Reject::Storage },
    ] {
        assert!(twin.step(input).is_empty());
        assert_eq!(twin.stage(), Some(WindowStage::Idle));
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
                Net => twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW }),
                Register => twin.post_call(w::WindowsInput::RegisterOperations { now: NOW }),
            }
            let step_requests = requests(&twin.split(take(&twin.log)).0);
            assert!(registered || step_requests == 0, "{name}: no request before the operations are declared");
            asked += step_requests;
        }
        assert_eq!(asked, expected, "{name}: the declared preload key asks once");
        assert_eq!(twin.stage(), stage, "{name}");

#[test]
fn win_busy_interest_passive() {
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
