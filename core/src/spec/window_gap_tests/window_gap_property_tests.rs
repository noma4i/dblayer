use super::*;


/// P-WIN-01: a caller gets at most one outcome on any trace and exactly one once the window is not waiting for the network.
#[test]
fn win_prop_outcome_once() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let events = snapshot(&log);
        for id in [1u32, 2] {
            let count = events.iter().filter(|event| matches!(event, Seen::Outcome(found, _) if *found == id)).count();
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
        let (log, _) = seen();
        let events = snapshot(&log);
        let windows = det.state(Address::<Spy<Windows>>::new(WINDOWS)).map(|spy| &spy.inner).ok_or("no state")?;
        if windows.stage(&key()) == Some(WindowStage::Waiting) {
            return Ok(());
        }
        for id in [1u32, 2] {
            let count = events.iter().filter(|event| matches!(event, Seen::Outcome(found, _) if *found == id)).count();
            if count != 1 && windows.windows() > 0 {
                return Err(format!("caller {id} got {count} outcomes"));
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-02: `Loaded{R}` is given only for a revision at which the rows of the answer landed.
#[test]
fn win_prop_loaded_rows() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let events = snapshot(&log);
        for (at, event) in events.iter().enumerate() {
            if let Seen::Outcome(_, WindowOutcome::Loaded { revision, .. }) = event {
                let landed = events[..at].iter().any(|before| matches!(before, Seen::Landed { revision: found, .. } if found == revision));
                if !landed {
                    return Err(format!("Loaded at {revision} without a landing"));
                }
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-03: at most one request of the key is in the network.
#[test]
fn win_prop_single_flight() {
    let report = explore(explore_rig, LIMITS, |_| {
        let (log, _) = seen();
        let open = open_requests(&snapshot(&log));
        if open > 1 {
            return Err(format!("{open} requests in the network"));
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-04: an answer issued at generation g does not land after a reset of its model; the store refuses it and the window asks again.
#[test]
fn win_prop_generation() {
    let report = explore(explore_rig, Limits { depth: 40, max_traces: 3_000 }, |det| {
        let (log, generation) = seen();
        let events = snapshot(&log);
        let current = generation.load(Ordering::SeqCst);
        if det.ready().is_empty() && events.iter().any(|event| matches!(event, Seen::Rejected(_))) {
            let landed_new = events.iter().any(|event| matches!(event, Seen::Landed { basis, .. } if *basis >= current));
            let state = det.state(Address::<Spy<Windows>>::new(WINDOWS)).map(|spy| &spy.inner);
            let waiting = state.is_some_and(|windows| windows.stage(&key()) == Some(WindowStage::Waiting));
            let idle = state.is_some_and(|windows| windows.stage(&key()) == Some(WindowStage::Idle) && windows.interest(&key()).is_empty());
            if !landed_new && !waiting && !idle {
                return Err("a refused answer was not asked again".into());
            }
        }
        Ok(())
    });
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

/// P-WIN-05: going online reads once every window that is not fresh and has interest or waiters, with no earlier network error needed.
#[test]
fn win_prop_online_once() {
    let mut twin = failed_watched();
    assert_eq!(twin.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState]);
    assert!(twin.step(Input::Online(true)).is_empty(), "a second signal does not read again");
    let mut quiet = fresh_watched();
    assert!(quiet.step(Input::Online(true)).is_empty(), "a fresh window is not reread");
    let mut stale = fresh_watched();
    stale.step(Input::Online(false));
    stale.step(Input::Stale { in_keys: true });
    assert_eq!(stale.step(Input::Online(true)), [Effect::Request { request: 2, attempt: 2, page: 1 }, Effect::WindowState], "a stale window reads without an earlier network error");
    let mut waiting_call = waiting();
    assert_eq!(requests(&waiting_call.step(Input::Online(true))), 1);
}

/// P-WIN-06: no input, no request and no schedule; a retry is scheduled only while online.
#[test]
fn win_prop_idle() {
    let twin = fresh();
    assert_eq!(twin.det.ready().len(), 0);
    assert!(take(&twin.log).is_empty());
    let mut offline = Twin::new();
    offline.call(1, Goal::First);
    offline.step(Input::Online(false));
    let effects = offline.step(Input::TransportError { request: 1, attempt: 1, failure: Failure::Network });
    assert!(effects.iter().all(|effect| !matches!(effect, Effect::ScheduleRetry { .. })), "no retry is scheduled offline");
}

/// P-WIN-07: the last `Interest-` cancels the request and the answer of the cancelled request is not committed; after the point of acceptance the landing ends.
#[test]
fn win_prop_cancel() {
    let mut twin = loading();
    assert_eq!(twin.step(Input::InterestMinus { id: 1 }), [Effect::Cancel { request: 1, attempt: 1 }]);
    assert!(twin.step(Input::Response { request: 1, attempt: 1, cursor: None, rows: true }).is_empty(), "no Commit for the cancelled request");
    let mut landed = landing();
    landed.step(Input::InterestMinus { id: 1 });
    assert!(landed.land().is_empty(), "the landing ends without a new load");
    assert_eq!(landed.stage(), Some(WindowStage::Idle));
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

/// P-WIN-09: the conditions read only the fields `online` and `generation[m]` of the dictionary.
#[test]
fn win_prop_fields_only() {
    let mut twin = Twin::new();
    twin.step(Input::Online(false));
    twin.post(w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 5, models: vec!["Moment".into()] }));
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting), "a machine created after Online(false) reads the field of the dictionary");
    take(&twin.log);
    twin.post(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None), now: NOW });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { input.contains("author: Window").then_some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Moment\": 5"), "the basis is the generation of the dictionary: {sent}");
}

#[test]
fn win_prop_landing_settles_first() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.call(2, Goal::All);
    twin.respond(Some(2));
    let effects = twin.land();
    assert_eq!(requests(&effects), 1, "the request for the next page is chosen by the AllPages waiter alone");
    assert_eq!(twin.state().waiters(&key()).len(), 1, "the waiter of the first page left with its outcome");
}

#[test]
fn win_base_revision() {
    let sent_batch = |twin: &mut Twin| take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { input.contains("author: Window").then_some(input) } else { None }).expect("a batch");
    let mut twin = Twin::new();
    twin.pin_revision.store(11, Ordering::SeqCst);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 1, goal: w::Goal::First, force: false, store_revision: 7, now: NOW });
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None), now: NOW });
    assert!(sent_batch(&mut twin).contains("base_revision: 11"), "the batch carries the revision from Pinned");
    twin.post(w::WindowsInput::Committed(Committed { batch: 1, revision: 9, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0 }));
    twin.pin_revision.store(13, Ordering::SeqCst);
    twin.post_call(w::WindowsInput::Load { key: key(), id: 2, goal: w::Goal::First, force: true, store_revision: 5, now: NOW });
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None), now: NOW });
    assert!(sent_batch(&mut twin).contains("base_revision: 13"), "the next batch carries the next Pinned revision");
}

/// P-WIN-10: the outcome `Loaded` of `AllPages` comes only at the end of the pages: one request per page after the first.
#[test]
fn win_prop_load_all_complete() {
    let mut twin = Twin::new();
    twin.call(1, Goal::All);
    let mut requests_after_first = 0;
    for page in 1..=3u32 {
        let cursor = (page < 3).then_some(page + 1);
        twin.respond(cursor);
        requests_after_first += requests(&twin.land());
    }
    assert_eq!(requests_after_first, 2, "one request per page after the first");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    assert!(twin.state().waiters(&key()).is_empty());
}

/// P-WIN-16: a `members_complete` window with a reader reaches the end of the cursor without calls of the app, settles only after the last page and ignores `max_pages`; without a reader the chain stands.
#[test]
fn win_prop_members_complete() {
    let mut twin = Twin::members_complete();
    twin.watch();
    twin.call(1, Goal::First);
    let mut requests_after_first = 0;
    for page in 1..=5u32 {
        twin.respond((page < 5).then_some(page + 1));
        requests_after_first += requests(&twin.land());
        assert_eq!(twin.load_state().settled, page == 5, "settled only after the last page");
    }
    assert_eq!(requests_after_first, 4, "one request per page after the first");
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    let mut alone = Twin::members_complete();
    alone.watch();
    alone.respond(Some(2));
    alone.step(Input::InterestMinus { id: STREAM });
    assert_eq!(requests(&alone.land()), 0, "without a reader the chain stands");
    assert_eq!(alone.stage(), Some(WindowStage::Idle));
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
    preload.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    preload.reference.step(Input::InterestPlus { id: PRELOAD });
    preload.reference.take_outcomes();
    preload.step(Input::Stale { in_keys: true });
    assert_eq!(preload.state().interest(&key()), [w::PRELOAD], "the preload interest stays");
}

/// P-WIN-12: before the first addressed `Online` a machine asks for nothing.
#[test]
fn win_prop_no_request_before_online() {
    let mut twin = Twin::before_online();
    assert!(requests(&twin.watch()) == 0);
    assert!(requests(&twin.call(1, Goal::First)) == 0);
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    assert_eq!(requests(&twin.step(Input::Online(true))), 1);
}

/// The dictionary: windows of two keys are independent, the answer goes to the key that owns the request, `Closing` gives one `Drained`.
#[test]
fn win_dictionary_owner() {
    let mut det = DetScheduler::new(5);
    let log = new_log();
    let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()), Reads::auto(), Arc::new(AtomicU64::new(0)));
    det.start_epoch(EPOCH);
    let other = WindowKey { model: "Chat".into(), name: "room".into() };
    for (key, id) in [(key(), 1), (other.clone(), 2)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: 0, now: NOW });
    }
    det.run_until_quiet(100).expect("quiet");
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["c1".into()], None), now: NOW });
    det.run_until_quiet(100).expect("quiet");
    let state = inner(&det, windows);
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Loading), Some(WindowStage::Landing)), "the answer of request 2 reaches the key that owns it");
    det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Chat".into()] }));
    det.close();
    det.run_until_quiet(100).expect("quiet");
    let effects = runs(take(&log));
    let drained = effects.iter().filter(|effect| matches!(effect, WindowsEffect::Drained(_))).count();
    assert_eq!(drained, 1, "one Drained for all windows");
    assert!(effects.iter().any(|effect| matches!(effect, WindowsEffect::Window { key, effect: WindowEffect::Cancel { request: 1, .. } } if *key == self::key())), "the loading window is cancelled at Closing");
    let state = inner(&det, windows);
    assert_eq!((state.stage(&key()), state.stage(&other)), (Some(WindowStage::Ended), Some(WindowStage::Ended)));
}

/// `landed_revision` belongs to the machine: a key answers a fresh call with its own last landing, not the last landing of any key.
#[test]
fn win_landed_revision_per_machine() {
    let mut det = DetScheduler::new(5);
    let log = new_log();
    let windows = add_windows(&mut det, &log, w::WindowsState::new(EPOCH, Some(true), peers()), Reads::auto(), Arc::new(AtomicU64::new(0)));
    det.start_epoch(EPOCH);
    let other = WindowKey { model: "Chat".into(), name: "room".into() };
    let load = |key: WindowKey, id: u64, revision: i64| w::WindowsInput::Load { key, id, goal: w::Goal::First, force: false, store_revision: revision, now: NOW };
    for (key, id) in [(key(), 1), (other.clone(), 2)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), load(key, id, 0));
    }
    det.run_until_quiet(100).expect("quiet");
    for (request, row, revision) in [(1, "r1", 5), (2, "c1", 9)] {
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Response { request, attempt: u32::try_from(request).expect("a small id"), page: AnswerPage::new(1, vec![row.into()], None), now: NOW });
        det.post(windows, EpochStamp::Epoch(EPOCH), w::WindowsInput::Committed(Committed { batch: request, revision, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0 }));
    }
    det.run_until_quiet(100).expect("quiet");
    take(&log);
    det.post(windows, EpochStamp::Epoch(EPOCH), load(key(), 3, 99));
    det.run_until_quiet(100).expect("quiet");
    assert!(runs(take(&log)).iter().any(|effect| matches!(effect, WindowsEffect::Window { effect: WindowEffect::Outcome { id: 3, outcome: WindowOutcome::Loaded { revision: 5, .. } }, .. })), "the fresh key answers with its own landing");
}

/// win_loading_response (P-WIN-04): the basis of a batch names every model of the answer with the generation it had when the request left, also a model the window did not know before.
#[test]
fn win_basis_names_every_model_of_the_answer_at_issue() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    take(&twin.log);
    twin.post(w::WindowsInput::ModelsReset(ModelsReset { epoch: EPOCH, generation: 1, models: vec!["Chat".into()] }));
    let mut page = AnswerPage::new(1, vec!["r1".into()], None);
    page.dependency_models = BTreeSet::from(["Moment".to_string(), "Chat".to_string()]);
    page.payload.related = vec![("Chat".into(), RowValue { row_id: "c1".into(), values: "{}".into() })];
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page, now: NOW });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { input.contains("author: Window").then_some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Chat\": 0") && sent.contains("\"Moment\": 0"), "the generations of the issue, not the reset that came after: {sent}");
}

/// win_landing_stale_generation: the new request carries the generations of the refusal.
#[test]
fn win_stale_generation_reissues_with_the_generations_of_the_refusal() {
    let mut twin = Twin::new();
    twin.call(1, Goal::First);
    twin.post(w::WindowsInput::Response { request: 1, attempt: 1, page: AnswerPage::new(1, vec!["r1".into()], None), now: NOW });
    twin.post(w::WindowsInput::Rejected(Rejected { batch: 1, reason: RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 3 } }));
    take(&twin.log);
    twin.post(w::WindowsInput::Response { request: 2, attempt: 2, page: AnswerPage::new(1, vec!["r1".into()], None), now: NOW });
    let sent = take(&twin.log).into_iter().find_map(|entry| if let Emitted::Send { to: STORE, input } = entry { input.contains("author: Window").then_some(input) } else { None }).expect("a batch");
    assert!(sent.contains("\"Moment\": 3"), "the basis of the new request is the generation of the refusal: {sent}");
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
    twin.post_call(w::WindowsInput::Load { key: key(), id: 7, goal: w::Goal::First, force: false, store_revision: 0, now: NOW });
    let (requests_before_online, _) = twin.split(take(&twin.log));
    assert_eq!(requests(&requests_before_online), 0, "no request before the addressed Online: {requests_before_online:?}");
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
    twin.post_call(w::WindowsInput::Online { online: Online(true), online_seq: 1, now: NOW });
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
}

/// P-WIN-27: while the interest of a second call is in the waiters before the first call leaves, no order of the second `Load`, the `Interest-` of the first and the response gives a second request or a cancel.
#[test]
fn win_prop_cancel_sibling_single_flight() {
    const JOIN: u8 = 0;
    const LEAVE: u8 = 1;
    const ANSWER: u8 = 2;
    for order in [[JOIN, LEAVE, ANSWER], [JOIN, ANSWER, LEAVE], [ANSWER, JOIN, LEAVE]] {
        let mut twin = loading();
        let mut trace = Vec::new();
        for turn in order {
            let effects = match turn {
                JOIN => twin.call(2, Goal::First),
                LEAVE => twin.step(Input::InterestMinus { id: 1 }),
                _ => twin.respond(None),
            };
            trace.push((turn, effects));
        }
        if twin.stage() == Some(WindowStage::Landing) {
            let landed = twin.land();
            trace.push((9, landed));
        }
        let again = trace.iter().flat_map(|(_, effects)| effects).filter(|effect| matches!(effect, Effect::Request { .. } | Effect::Cancel { .. })).count();
        assert_eq!(again, 0, "order {order:?} sent a second request or a cancel, trace {trace:?}");
        assert_eq!(twin.stage(), Some(WindowStage::Idle), "order {order:?}");
    }
}

/// P-WIN-28: one return of the network gives one read of a watched window in either order of `Online(true)` and `Stale{Gap}`.
#[test]
fn win_prop_reconnect_gap_one_reload() {
    const SEQ: u64 = 1;
    for (name, make) in [("fresh", fresh_watched as fn() -> Twin), ("failed", failed_watched as fn() -> Twin)] {
        for reload in [false, true] {
            for gap_first in [false, true] {
                let mut twin = make();
                if reload {
                    twin.post_call(w::WindowsInput::ReloadOnConnect { key: key(), id: u64::from(STREAM) });
                }
                take(&twin.log);
                let mut sent = Vec::new();
                let mut feed = |twin: &mut Twin, input: w::WindowsInput| {
                    twin.post(input);
                    let (effects, _) = twin.split(take(&twin.log));
                    sent.extend(effects);
                };
                feed(&mut twin, w::WindowsInput::Online { online: Online(false), online_seq: 0, now: NOW });
                let online = w::WindowsInput::Online { online: Online(true), online_seq: SEQ, now: NOW };
                let gap = w::WindowsInput::Stale { model: "Moment".into(), keys: StaleKeys::All, cause: w::StaleCause::Gap { online_seq: SEQ } };
                if gap_first {
                    feed(&mut twin, gap);
                    feed(&mut twin, online);
                } else {
                    feed(&mut twin, online);
                    feed(&mut twin, gap);
                }
                let reads = sent.iter().filter(|effect| matches!(effect, Effect::Request { .. })).count();
                assert_eq!(reads, 1, "{name} reload={reload} gap_first={gap_first}: {sent:?}");
            }
        }
    }
}
