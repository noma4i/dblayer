use super::*;

fn delivered(log: &Spied) -> Vec<d::Value> {
    snapshot(log).into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) => Some(value),
        _ => None,
    }).collect()
}

struct Rig {
    det: DetScheduler,
    log: Spied,
    deliveries: Address<Spy<Deliveries>>,
}

fn rig(kind: Kind) -> Rig {
    let mut det = DetScheduler::new(1);
    let log = new_log();
    let deliveries = add_deliveries(&mut det, &log);
    det.start_epoch(EPOCH);
    det.post(deliveries, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(kind), epoch: EPOCH, identity: true, valid: true });
    if kind == Kind::Events {
        det.post(deliveries, EpochStamp::Control, d::DeliveryInput::Joined { stream: STREAM, position: 0 });
        det.post(deliveries, EpochStamp::Control, d::DeliveryInput::ModelReady { stream: STREAM });
    }
    det.run_until_quiet(100).expect("open");
    take(&log);
    Rig { det, log, deliveries }
}

impl Rig {
    fn source(&mut self, name: &'static str, script: Vec<d::DeliveryInput>) -> Address<Source<Deliveries>> {
        let source = self.det.add::<Source<Deliveries>>(name, SourceState { to: as_owner(self.deliveries), script }, Silent);
        self.det.post(source, EpochStamp::Control, SourceInput::Go);
        source
    }
}

fn pulls(count: usize) -> Vec<d::DeliveryInput> {
    let answers = Outcomes::<PullAnswer>::new();
    (0..count).map(|_| d::DeliveryInput::Pull { stream: STREAM, outcome: answers.register(EPOCH).id() }).collect()
}

fn rows_script(rev: &[(i64, &[&str])]) -> Vec<d::DeliveryInput> {
    let mut script = Vec::new();
    for (index, (revision, list)) in rev.iter().enumerate() {
        if index > 0 {
            script.push(d::DeliveryInput::Committed(Committed { batch: 0, revision: *revision, tables: vec!["moments".into()], models: Default::default(), stale_parents: Vec::new(), position: 0 }));
        }
        let rows = list.iter().map(|id| d::StreamRow::new(id, 0)).collect();
        script.push(d::DeliveryInput::ReadDone { stream: STREAM, read_id: index as u64 + 1, revision: *revision, data: d::ReadData::Rows { rows, replaced: Vec::new(), total: None } });
    }
    script
}

fn row_sources(rig: &mut Rig) {
    rig.source("store", rows_script(&[(1, &["r1"]), (2, &["r1", "r2"])]));
    rig.source("window", vec![d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loading_after(0)) }, d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loaded_at(2)) }]);
    rig.source("reader", pulls(4));
}

fn rows_revision(value: &d::Value) -> Option<i64> {
    match value {
        d::Value::Snapshot { revision, .. } => Some(*revision),
        d::Value::Delta(delta) => Some(delta.revision),
        _ => None,
    }
}

thread_local! {
    static LOG: RefCell<Option<Rc<Spied>>> = const { RefCell::new(None) };
}

const LIMITS: Limits = Limits { depth: 4, max_traces: 20_000 };

#[test]
fn dlv_prop_first_and_base() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
            row_sources(&mut rig);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            if let Some(first) = values.first() {
                if !matches!(first, d::Value::Snapshot { .. }) {
                    return Err(format!("the first value is {first:?}"));
                }
            }
            let mut previous: Option<i64> = None;
            for value in &values {
                if let d::Value::Delta(delta) = value {
                    if previous != Some(delta.base) {
                        return Err(format!("a Delta with base {} follows revision {previous:?}", delta.base));
                    }
                }
                if let d::Value::Snapshot { rows, state, .. } | d::Value::Delta(d::Delta { rows, state, .. }) = value {
                    if state.phase == WindowPhase::Failed && !rows.is_empty() {
                        return Err("Failed with rows".into());
                    }
                }
                previous = rows_revision(value).or(previous);
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 10, "the explorer walked {} traces", report.traces);
}

#[test]
fn dlv_prop_rows_visible() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
            row_sources(&mut rig);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |det| {
            if !det.ready().is_empty() {
                return Ok(());
            }
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            let mut list: Vec<String> = Vec::new();
            for value in &values {
                match value {
                    d::Value::Snapshot { rows, .. } => list = rows.iter().map(|row| row.id.clone()).collect(),
                    d::Value::Delta(delta) => {
                        list.retain(|id| !delta.removed.contains(id));
                        for row in &delta.rows {
                            if !list.contains(&row.id) {
                                list.push(row.id.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            let last = values.iter().filter_map(rows_revision).last().unwrap_or(0);
            if last >= 2 && !(list.contains(&"r1".to_string()) && list.contains(&"r2".to_string())) {
                return Err(format!("revision {last} shows {list:?}"));
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

#[test]
fn dlv_prop_events_cover() {
    let report = explore(
        || {
            let mut rig = rig(Kind::Events);
            rig.source("feed", vec![
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 1, payload: "c1".into() },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 2, payload: "c2".into() },
                d::DeliveryInput::ModelGap { stream: STREAM },
                d::DeliveryInput::ModelLost { stream: STREAM, first: 3, last: 4 },
                d::DeliveryInput::ModelEvent { stream: STREAM, position: 6, payload: "c6".into() },
            ]);
            rig.source("reader", pulls(7));
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let mut next = 1;
            for value in delivered(&log) {
                match value {
                    d::Value::Event { position, .. } if position == next => next += 1,
                    d::Value::Lost { first, last } if first == next => next = last + 1,
                    d::Value::Gap | d::Value::Ready => {}
                    other => return Err(format!("{other:?} at position {next}")),
                }
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

#[test]
fn dlv_prop_ended_once() {
    let report = explore(
        || {
            let mut rig = rig(Kind::List);
            row_sources(&mut rig);
            let ender = rig.det.add::<Ender>("lifecycle", EnderState { epoch: EPOCH, cause: Ended::SessionReset }, Silent);
            rig.det.post(ender, EpochStamp::Control, SourceInput::Go);
            LOG.with(|slot| *slot.borrow_mut() = Some(Rc::new(rig.log.clone())));
            rig.det
        },
        LIMITS,
        |_| {
            let log = LOG.with(|slot| slot.borrow().clone()).ok_or("no log")?;
            let values = delivered(&log);
            if values.iter().filter(|value| matches!(value, d::Value::Ended(_))).count() > 1 {
                return Err("Ended twice".into());
            }
            if let Some(at) = values.iter().position(|value| matches!(value, d::Value::Ended(_))) {
                if at + 1 != values.len() {
                    return Err(format!("{:?} follows Ended", &values[at + 1..]));
                }
                if values[at] != d::Value::Ended(d::EndCause::SessionEnded) {
                    return Err(format!("{:?}", values[at]));
                }
            }
            Ok(())
        },
    );
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
}

#[test]
fn dlv_prop_epoch() {
    let mut twin = ended_by_epoch();
    for input in [Input::ReadDone { revision: 9, rows: rows(&["r9"]) }, Input::WindowState(Ws::loading_after(9)), Input::Committed { revision: 10, touches: true }] {
        twin.step(input);
    }
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_prop_same_revision() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(Ws::loading_after(1)));
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    let journal = twin.state().journal(STREAM);
    assert!(matches!(&journal[0], d::Value::State { revision: 1, state } if state.phase == WindowPhase::Loading), "{journal:?}");
    assert!(matches!(&journal[1], d::Value::Delta(delta) if delta.base == 1 && delta.revision == 2 && delta.state.loaded == Some(2)), "{journal:?}");
    twin.read_all();
}

#[test]
fn dlv_prop_failed_without_rows() {
    let mut empty = open_rows();
    let (_, values) = empty.step_read(Input::ReadFailed { revision: 1 });
    assert!(matches!(values.as_slice(), [Val::Snapshot { rows, state: Ws { phase: Phase::Failed, .. }, .. }] if rows.is_empty()));
    let mut with_rows = live_rows();
    let (_, values) = with_rows.step_read(Input::ReadFailed { revision: 2 });
    assert!(matches!(values.as_slice(), [Val::State { state: Ws { phase: Phase::Loaded, error: true, .. }, .. }]));
}

#[test]
fn dlv_prop_idle() {
    let mut twin = live_rows();
    for revision in 2..8 {
        let (effects, values) = twin.step_read(Input::Committed { revision, touches: false });
        assert!(effects.is_empty() && values.is_empty());
    }
    assert_eq!(twin.det.ready().len(), 0, "the owner has nothing to do");
}

#[test]
fn dlv_prop_bounded() {
    let mut twin = open_events();
    for position in 1..=600u64 {
        twin.post(d::DeliveryInput::ModelEvent { stream: STREAM, position, payload: format!("c{position}") });
        twin.reference.step(Input::ModelEvent { position, payload: format!("c{position}") });
        assert!(twin.state().journal_len(STREAM) <= d::JOURNAL_LIMIT);
    }
    let values = twin.read_all();
    let mut next = 1;
    for value in &values {
        match value {
            Val::Event { position, .. } if *position == next => next += 1,
            Val::Lost { first, last } if *first == next => next = last + 1,
            other => panic!("{other:?} at {next}"),
        }
    }
    assert_eq!(next, 601, "events and lost ranges cover 1..=600");
    assert!(values.iter().any(|value| matches!(value, Val::Lost { .. })));
}

#[test]
fn dlv_prop_ready_once() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::ModelEvent { position: 1, payload: "c1".into() });
    twin.step(Input::Joined { position: 0 });
    for _ in 0..3 {
        twin.step(Input::ModelReady);
        twin.step(Input::ModelEvent { position: 2, payload: "c2".into() });
    }
    let values = twin.read_all();
    assert_eq!(values.iter().filter(|value| matches!(value, Val::Ready)).count(), 1);
    assert_eq!(values.first(), Some(&Val::Ready), "Ready is before the first Event");
}

#[test]
fn dlv_prop_delta_size() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert!(matches!(values.as_slice(), [Val::Delta { upserted, .. }] if *upserted == ids(&["r2"])), "one changed row, one row in the value");
    let many: Vec<r::Row> = (0..300).map(|n| (format!("n{n}"), 0)).collect();
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: many });
    assert!(matches!(values.as_slice(), [Val::Snapshot { rows, .. }] if rows.len() == 300), "over 256 changes the whole result goes");
}

#[test]
fn dlv_prop_dictionary_bounded() {
    let mut rig = rig(Kind::List);
    for stream in 2..=40u64 {
        rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::Open { stream, view: view(Kind::Events), epoch: EPOCH, identity: true, valid: true });
        rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::Release { stream });
        rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::SinkClosed { stream });
    }
    rig.det.run_until_quiet(10_000).expect("quiet");
    assert_eq!(inner(&rig.det, rig.deliveries).streams(), 1, "only the stream with a handle is left");
    rig.det.close();
    rig.det.run_until_quiet(100).expect("quiet");
    let drained = snapshot(&rig.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 0, "the stream with a handle still waits for SinkClosed");
    rig.det.post(rig.deliveries, EpochStamp::Epoch(EPOCH), d::DeliveryInput::SinkClosed { stream: STREAM });
    rig.det.run_until_quiet(100).expect("quiet");
    let drained = snapshot(&rig.log).iter().filter(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Drained(_)))).count();
    assert_eq!(drained, 1, "one Drained for every stream");
}

#[test]
fn dlv_prop_no_available_after_cancel() {
    let paths: [(&str, Vec<Input>); 4] = [
        ("cancel", vec![Input::Cancel { reply: 1 }]),
        ("release", vec![Input::Release]),
        ("epoch", vec![Input::EpochEnded { own: true }, Input::Cancel { reply: 1 }]),
        ("closing", vec![Input::Closing, Input::Cancel { reply: 1 }]),
    ];
    for (name, inputs) in paths {
        let mut twin = live_rows();
        let mut effects = Vec::new();
        for input in inputs {
            effects.extend(twin.step(input));
        }
        assert_eq!(effects.iter().filter(|effect| **effect == Effect::CloseSink).count(), 1, "{name}: the sink closes once");
        assert!(effects.contains(&Effect::InterestMinus), "{name}: the interest goes");
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Cancelled(_))), "{name}: the reply waits for SinkClosed");
        twin.step(Input::SinkClosed);
        for input in [Input::Committed { revision: 5, touches: true }, Input::ReadDone { revision: 5, rows: rows(&["r5"]) }, Input::WindowState(Ws::loading_after(5))] {
            assert!(!twin.step(input).contains(&Effect::Available), "{name}: no Available after SinkClosed");
        }
    }
}

#[test]
fn dlv_prop_single_unread_delta() {
    let mut twin = live_rows();
    let mut previous = 1;
    for round in 2..40i64 {
        let list: Vec<String> = (0..=round % 5).map(|n| format!("r{n}")).collect();
        let list: Vec<r::Row> = list.into_iter().map(|id| (id, round)).collect();
        twin.step(Input::ReadDone { revision: round, rows: list });
        let unread = twin.state().journal(STREAM).iter().filter(|value| matches!(value, d::Value::Delta(_) | d::Value::Snapshot { .. })).count();
        assert!(unread <= 1, "at most one unread rows value");
        if round % 3 == 0 {
            for value in twin.read_all() {
                match value {
                    Val::Delta { base, revision, .. } => {
                        assert_eq!(base, previous);
                        previous = revision;
                    }
                    Val::Snapshot { revision, .. } => previous = revision,
                    _ => {}
                }
            }
        }
    }
}

#[test]
fn dlv_prop_delta_order() {
    fn apply(mut list: Vec<String>, delta: &d::Delta) -> Vec<String> {
        for (from, to) in &delta.replaced {
            for id in list.iter_mut().filter(|id| *id == from) {
                *id = to.clone();
            }
        }
        list.retain(|id| !delta.removed.contains(id));
        list.retain(|id| !delta.placed.iter().any(|(placed, _)| placed == id));
        let mut placed = delta.placed.clone();
        placed.sort_by_key(|(_, index)| *index);
        for (id, index) in placed {
            let at = (index as usize).min(list.len());
            list.insert(at, id);
        }
        list
    }
    let mut det = DetScheduler::new(3);
    let log: Spied = new_log();
    let addr = add_deliveries(&mut det, &log);
    det.start_epoch(EPOCH);
    det.post(addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::List), epoch: EPOCH, identity: true, valid: true });
    det.post(addr, EpochStamp::Epoch(EPOCH), d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::idle()) });
    let mut seed = 12_345u64;
    let mut next = move |bound: u64| {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let pool: Vec<String> = (0..12).map(|n| format!("p{n}")).collect();
    let mut platform: Vec<String> = Vec::new();
    let mut lists: std::collections::BTreeMap<i64, Vec<String>> = std::collections::BTreeMap::new();
    let answers = Outcomes::<PullAnswer>::new();
    let mut checked = 0;
    for revision in 1..120i64 {
        let mut list: Vec<String> = pool.iter().filter(|_| next(3) != 0).cloned().collect();
        for index in (1..list.len()).rev() {
            if next(4) == 0 {
                list.swap(index, next(index as u64 + 1) as usize);
            }
        }
        lists.insert(revision, list.clone());
        let read_id = inner(&det, addr).read_id(STREAM);
        let rows: Vec<d::StreamRow> = list.iter().map(|id| d::StreamRow::new(id, if next(5) == 0 { revision } else { 0 })).collect();
        det.post(addr, EpochStamp::Epoch(EPOCH), d::DeliveryInput::ReadDone { stream: STREAM, read_id, revision, data: d::ReadData::Rows { rows, replaced: Vec::new(), total: None } });
        det.post(addr, EpochStamp::Epoch(EPOCH), d::DeliveryInput::Committed(Committed { batch: 0, revision, tables: vec!["moments".into()], models: Default::default(), stale_parents: Vec::new(), position: 0 }));
        if next(2) == 0 {
            det.post(addr, EpochStamp::Control, d::DeliveryInput::Pull { stream: STREAM, outcome: answers.register(EPOCH).id() });
        }
        det.run_until_quiet(1_000).expect("quiet");
        for entry in take(&log) {
            let Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Answer { answer: PullAnswer::Value(value), .. }, .. }) = entry else { continue };
            match value {
                d::Value::Snapshot { rows, .. } => platform = rows.iter().map(|row| row.id.clone()).collect(),
                d::Value::Delta(delta) => {
                    platform = apply(platform.clone(), &delta);
                    assert_eq!(platform.len(), delta.size as usize, "the size of the result at {}", delta.revision);
                    let core: Vec<String> = lists.get(&delta.revision).cloned().expect("a list of the revision");
                    assert_eq!(platform, core, "revision {}", delta.revision);
                    checked += 1;
                }
                _ => {}
            }
        }
    }
    assert!(checked > 10, "{checked} deltas were applied");
}

#[test]
fn dlv_epoch_started_unknown_cause() {
    let mut rig = rig(Kind::List);
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochStarted(crate::owners::contract::EpochStarted { epoch: 2, identity: None, generation: 0 }));
    rig.det.post(rig.deliveries, EpochStamp::Control, d::DeliveryInput::EpochEnded(EpochEnded { epoch: 2, cause: Ended::SessionReset }));
    rig.det.run_until_quiet(100).expect("quiet");
    assert_eq!(inner(&rig.det, rig.deliveries).stage(STREAM), Some(d::DeliveryStage::Opening), "the stream of epoch 1 is not touched by the end of epoch 2");
}

/// dlv_cancelling_other: a `Pull` after `Cancel` waits for `Ended` and does not hand out the value that was unread when the reader cancelled.
#[test]
fn dlv_cancelling_drops_the_unread_values() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.step(Input::Cancel { reply: 1 });
    assert!(twin.step(Input::Pull).is_empty(), "the unread snapshot is gone: the pull waits");
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))]);
}

/// P-DLV-06 (win_loading_error): the first load of a window fails while the snapshot holds cached rows: the stream shows `Loaded` with the error, `Failed` only without rows.
#[test]
fn dlv_failed_window_with_cached_rows_shows_loaded_with_the_error() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.read_all();
    let failed = Ws { phase: Phase::Failed, error: true, loaded: None };
    let (_, values) = twin.step_read(Input::WindowState(failed.clone()));
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Loaded, error: true, loaded: None } }]);
    let mut empty = open_rows();
    empty.step(Input::ReadDone { revision: 1, rows: Vec::new() });
    empty.read_all();
    let (_, values) = empty.step_read(Input::WindowState(failed.clone()));
    assert_eq!(values, [Val::State { revision: 1, state: failed }], "no rows: the failure stays");
}

/// P-STORE-14: a snapshot read answered after the epoch ended reaches neither the stream nor the call.
#[test]
fn store_prop_read_epoch() {
    for revision in 1..=4 {
        let mut twin = ended_by_epoch();
        let (effects, values) = twin.step_read(Input::ReadDone { revision, rows: rows(&["rLate"]) });
        assert!(effects.is_empty() && values.iter().all(|value| matches!(value, Val::Ended(_))), "revision {revision}: the read of the ended epoch was delivered: {effects:?} {values:?}");
    }
}
