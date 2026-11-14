use super::*;
use crate::spec::support::committed_at;


/// On every explored order of two callers, a Stale and the network flip: a caller gets at most one outcome (exactly one once the window does not wait), Loaded only for a landed revision, at most one request in flight.
#[test]
fn win_prop_explored_invariants() {
    {
        let report = explore(explore_rig, LIMITS, |_| {
            let log = seen();
            let events = snapshot(&log);
            for id in [1u32, 2] {
                let count = events.iter().filter(|event| matches!(event, Observed::Outcome(found, _) if *found == id)).count();
                if count > 1 {
                    return Err(format!("caller {id} got {count} outcomes"));
                }
            }
            Ok(())
        });
        assert!(report.violations.is_empty(), "{:?}", report.violations.first());
        assert!(report.traces > 10);
        let report = explore(explore_rig, Limits { depth: 40, max_traces: 3_000 }, |det| {
            if !det.ready().is_empty() {
                return Ok(());
            }
            let log = seen();
            let events = snapshot(&log);
            let windows = det.state(Address::<Spy<Windows>>::new(WINDOWS)).map(|spy| &spy.inner).ok_or("no state")?;
            if windows.stage(&key()) == Some(WindowStage::Waiting) {
                return Ok(());
            }
            for id in [1u32, 2] {
                let count = events.iter().filter(|event| matches!(event, Observed::Outcome(found, _) if *found == id)).count();
                if count != 1 && windows.windows() > 0 {
                    return Err(format!("caller {id} got {count} outcomes"));
                }
            }
            Ok(())
        });
        assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    }
    {
        let report = explore(explore_rig, LIMITS, |_| {
            let log = seen();
            let events = snapshot(&log);
            for (at, event) in events.iter().enumerate() {
                if let Observed::Outcome(_, WindowOutcome::Loaded { revision, .. }) = event {
                    let landed = events[..at].iter().any(|before| matches!(before, Observed::Landed { revision: found, .. } if found == revision));
                    if !landed {
                        return Err(format!("Loaded at {revision} without a landing"));
                    }
                }
            }
            Ok(())
        });
        assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    }
    {
        let report = explore(explore_rig, LIMITS, |_| {
            let log = seen();
            let open = open_requests(&snapshot(&log));
            if open > 1 {
                return Err(format!("{open} requests in the network"));
            }
            Ok(())
        });
        assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    }
}




/// P-WIN-08: `Stale` during Loading or Landing gives exactly one more load after the landing while interest or waiters exist.
#[test]
fn win_prop_stale_reload_once() {
    let mut twin = loading_watched();
    for _ in 0..3 {
        twin.step(Input::Stale { in_keys: true });
    }
    twin.respond(None);
    let again = twin.land();
    assert_eq!(requests(&again), 1, "three marks make one more load");
    twin.respond(None);
    let last = twin.land();
    assert_eq!(requests(&last), 0);
}


#[test]
fn win_base_revision() {
    let base_revisions = |twin: &Twin| twin.window_batches().iter().flat_map(|batch| batch.ops.iter().filter_map(|op| if let StoreOp::Window { base_revision, .. } = op { Some(*base_revision) } else { None })).collect::<Vec<_>>();
    let mut twin = Twin::new();
    twin.pin_revision.store(11, Ordering::SeqCst);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 1, goal: w::Goal::First, force: false, store_revision: 7 });
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None) });
    assert_eq!(base_revisions(&twin), [11], "the batch carries the revision from Pinned");
    twin.post(w::WindowsInput::Committed(committed_at(1, 9, &[])));
    twin.pin_revision.store(13, Ordering::SeqCst);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 2, goal: w::Goal::First, force: true, store_revision: 5 });
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None) });
    assert_eq!(base_revisions(&twin), [13], "the next batch carries the next Pinned revision");
}

/// P-WIN-11: a finished waiter does not stay: `Stale` and `Online(true)` give no request when interest and waiters are empty; the preload stays.
#[test]
fn win_prop_waiters_leave() {
    let mut twin = loading();
    twin.finish(None);
    assert!(twin.state().waiters(&key()).is_empty());
    assert!(twin.step(Input::Stale { in_keys: true }).is_empty());
    assert!(twin.step(Input::Online(true)).is_empty());
    let mut preload = Twin::with_preload();
    preload.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH, identity: Some(1), generation: 0 }));
    preload.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
    preload.step(Input::Stale { in_keys: true });
    assert_eq!(preload.state().interest(&key()), [w::PRELOAD], "the preload interest stays");
}

/// Windows of two keys are independent: an answer goes to the key that owns the request, a key answers a fresh call with its own landed revision, Closing gives one Drained.
#[test]
fn win_windows_of_keys_are_independent() {
    {
        let mut det = DetScheduler::new(5);
        let log = new_log();
        let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()).with_local_request_ids(), Reads::auto(), PinState::default());
        det.start_epoch(EPOCH);
        let other = WindowKey { model: "Chat".into(), name: "room".into() };
        for (key, id) in [(key(), 1), (other.clone(), 2)] {
            det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: 0 });
        }
        det.run_until_quiet(100).expect("quiet");
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["c1".into()], None) });
        det.run_until_quiet(100).expect("quiet");
        let state = inner(&det, windows);
        assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Loading), Some(WindowStage::Landing)), "the answer of request 2 reaches the key that owns it");
        det.close();
        det.run_until_quiet(100).expect("quiet");
        let effects = runs(take(&log));
        let drained = effects.iter().filter(|effect| matches!(effect, WindowsEffect::Drained(_))).count();
        assert_eq!(drained, 1, "one Drained for all windows");
        assert!(effects.iter().any(|effect| matches!(effect, WindowsEffect::Window { key, effect: WindowEffect::Cancel { request: 1, .. } } if *key == self::key())), "the loading window is cancelled at Closing");
        let state = inner(&det, windows);
        assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Ended), Some(WindowStage::Ended)));
    }
    {
        let mut det = DetScheduler::new(5);
        let log = new_log();
        let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()).with_local_request_ids(), Reads::auto(), PinState::default());
        det.start_epoch(EPOCH);
        let other = WindowKey { model: "Chat".into(), name: "room".into() };
        let load = |key: WindowKey, id: u64, revision: i64| w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: revision };
        for (key, id) in [(key(), 1), (other.clone(), 2)] {
            det.post(windows, EpochStamp::Epoch(EPOCH), load(key, id, 0));
        }
        det.run_until_quiet(100).expect("quiet");
        for (request, row, revision) in [(1, "r1", 5), (2, "c1", 9)] {
            det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request, attempt: u32::try_from(request).expect("a small id"), page: AnswerPage::new(1, vec![row.into()], None) });
            det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Committed(committed_at(request, revision, &[])));
        }
        det.run_until_quiet(100).expect("quiet");
        take(&log);
        det.post(windows, EpochStamp::Epoch(EPOCH), load(key(), 3, 99));
        det.run_until_quiet(100).expect("quiet");
        assert!(runs(take(&log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 3, outcome: WindowOutcome::Loaded { revision: 5, .. } }, .. })), "the fresh key answers with its own landing");
    }
}





/// win_wait_nodes: an id that holds a comma stays one id and the key keeps the order and the boundaries of the ids.
#[test]
fn win_nodes_key_keeps_the_boundaries_of_the_ids() {
    let joined = WindowKey::nodes("Moment", &["a,b"]);
    let split = WindowKey::nodes("Moment", &["a", "b"]);
    assert_ne!(joined, split);
    assert_eq!(joined.kind(), WindowKind::Nodes { ids: vec!["a,b".to_string()] });
    assert_eq!(split.kind(), WindowKind::Nodes { ids: vec!["a".to_string(), "b".to_string()] });
}

/// win_created, P-WIN-12: the dictionary lives for one epoch. The key of the old epoch is gone and the first request of the new epoch waits for the addressed `Online`.
#[test]
fn win_dictionary_and_online_belong_to_the_epoch() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin.post_call(w::WindowsInput::EpochStarted(EpochStarted { epoch: EPOCH + 1, identity: Some(1), generation: 0 }));
    assert_eq!(twin.state().windows(), 0, "the machines of the old epoch are gone");
    take(&twin.log);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 7, goal: w::Goal::First, force: false, store_revision: 0 });
    let (requests_before_online, _) = twin.split(take(&twin.log));
    assert_eq!(requests(&requests_before_online), 0, "no request before the addressed Online: {requests_before_online:?}");
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1 });
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

#[derive(Debug, PartialEq)]
struct WindowView {
    stage: Option<WindowStage>,
    interest: Vec<u64>,
    waiters: Vec<u64>,
    commit: Option<StoreBatch>,
    outcomes: Vec<(u32, Result3)>,
    load: Option<crate::owners::contract::WindowState>,
}

fn view(twin: &Twin) -> WindowView {
    let mut interest = twin.state().interest(&key());
    interest.sort_unstable();
    WindowView {
        stage: twin.stage(),
        interest,
        waiters: twin.state().waiters(&key()),
        commit: twin.commit.clone(),
        outcomes: twin.outcomes.clone(),
        load: twin.state().load_state(&key()),
    }
}

fn opening_watched() -> Twin {
    let mut twin = super::window_fresh_tests::opening_twin();
    twin.watch();
    twin
}

/// P-WIN-30: a stray input of a key leaves the addressed window's state, effects and journal unchanged; the window is the same as on the trace without the stray.
#[test]
fn win_prop_stray_input_ignored() {
    type Build = fn() -> Twin;
    let cases: Vec<(&str, Build, Vec<Input>)> = vec![
        ("opening", opening_watched, vec![Input::Read(Read::Stray)]),
        ("waiting", waiting, vec![
            Input::Response { request: 1, attempt: 1, cursor: None, rows: true },
            Input::TransportError { request: 1, attempt: 1, failure: Failure::Network },
            Input::Dropped { request: 1, attempt: 1 },
            Input::Retry { request: 1 },
            Input::Committed { request: 1, revision: 5 },
            Input::Rejected { request: 1, reason: Reject::Storage },
        ]),
        ("loading", loading, vec![
            Input::Response { request: 9, attempt: 1, cursor: None, rows: true },
            Input::Response { request: 1, attempt: 9, cursor: None, rows: true },
            Input::TransportError { request: 1, attempt: 9, failure: Failure::Network },
            Input::Dropped { request: 9, attempt: 1 },
            Input::Retry { request: 9 },
            Input::Retry { request: 1 },
            Input::Committed { request: 1, revision: 50 },
            Input::Rejected { request: 1, reason: Reject::Storage },
        ]),
        ("idle", fresh, vec![
            Input::Response { request: 9, attempt: 9, cursor: None, rows: true },
            Input::TransportError { request: 9, attempt: 9, failure: Failure::Network },
            Input::Dropped { request: 9, attempt: 9 },
            Input::Retry { request: 9 },
            Input::Committed { request: 9, revision: 99 },
            Input::Rejected { request: 9, reason: Reject::Storage },
        ]),
    ];
    for (name, build, strays) in cases {
        let baseline = view(&build());
        for stray in strays {
            let mut twin = build();
            let effects = twin.step(stray.clone());
            assert!(effects.is_empty(), "{name}: stray {stray:?} emitted {effects:?}");
            assert_eq!(view(&twin), baseline, "{name}: stray {stray:?} changed the window");
        }
    }
}
