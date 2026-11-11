use super::*;

#[test]
fn dlv_cancelling_pull_twice() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Pull);
    let outcome = twin.answers.register(EPOCH).id();
    take(&twin.log);
    twin.call(d::DeliveryInput::Pull { stream: STREAM, outcome, reader: 1 });
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))]);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))], "delivery.md Cancelling x SinkClosed: cancels first, then the waiting Pull");
}

#[test]
fn dlv_live_committed_seen() {
    let mut twin = live_rows();
    take(&twin.log);
    twin.post(d::DeliveryInput::Committed(Committed { batch: 0, revision: 1, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
    assert!(Twin::modelled(take(&twin.log)).is_empty());
}

#[test]
fn dlv_opening_event() {
    let mut twin = open_rows();
    for input in [Input::Joined { position: 3 }, Input::ModelReady, Input::ModelEvent { position: 1, payload: "c1".into() }, Input::ModelLost { first: 2, last: 3 }, Input::ModelGap] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_opening_pull() {
    let mut twin = open_rows();
    assert!(twin.step(Input::Pull).is_empty(), "nothing to give yet");
    let effects = twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    assert_eq!(effects, [Effect::Answer(Pulled::Value(snap(1, &["r1"], Ws::idle())))], "the first value goes to the waiting Pull, no Available");
}

#[test]
fn dlv_pull_twice() {
    let mut opening = open_rows();
    opening.step(Input::Pull);
    assert_eq!(opening.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))]);
    let mut live = live_rows();
    live.step(Input::Pull);
    assert_eq!(live.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Ended(Cause::Invalid)))], "the first Pull keeps waiting");
    assert_eq!(live.step(Input::Committed { revision: 2, touches: true }), [Effect::Read(2)]);
    assert_eq!(live.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).len(), 1, "the waiting Pull is answered");
}

#[test]
fn dlv_pull_cancelled() {
    for mut twin in [open_rows(), live_rows()] {
        twin.step(Input::Pull);
        assert_eq!(twin.step(Input::PullCancelled { matched: true }), [Effect::Answer(Pulled::Interrupted), Effect::InterestMinus, Effect::CloseSink]);
        twin.step(Input::SinkClosed);
        assert_eq!(twin.read_all(), [Val::Ended(Cause::Cancelled)]);
    }
}

#[test]
fn dlv_cancelling_pull_cancelled() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::PullCancelled { matched: true }), [Effect::Answer(Pulled::Interrupted)]);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7)]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::Cancelled)]);
}

#[test]
fn dlv_pull_cancelled_stale() {
    let mut twin = live_rows();
    twin.step(Input::Pull);
    assert!(twin.step(Input::PullCancelled { matched: false }).is_empty());
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert!(twin.step(Input::PullCancelled { matched: true }).is_empty());
}

#[test]
fn dlv_epoch_closed() {
    for mut twin in [open_rows(), live_rows()] {
        twin.step(Input::EpochClosed);
        assert_eq!(twin.read_all(), [Val::Ended(Cause::Closed)]);
    }
}

#[test]
fn dlv_live_read_failed_old() {
    let mut twin = live_rows();
    assert!(twin.step(Input::ReadFailedStale).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_live_count_same() {
    let mut twin = open_as(Kind::Count);
    twin.step(Input::WindowState(Ws::idle()));
    twin.step(Input::CountDone { revision: 1, count: 1 });
    twin.read_all();
    assert!(twin.step(Input::CountDone { revision: 2, count: 1 }).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_live_read_total_only() {
    let mut twin = open_rows();
    twin.total = Some(2);
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.read_all();
    twin.total = Some(3);
    TOTALS.with(|totals| totals.borrow_mut().clear());
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1"]) });
    assert!(matches!(values.as_slice(), [Val::Delta { base: 1, revision: 2, size: 1, edits, .. }] if edits.is_empty()));
    assert_eq!(TOTALS.with(|totals| totals.borrow().clone()), [Some(3)]);
}

#[test]
fn dlv_cancel() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::Cancel { reply: 7 }), [Effect::InterestMinus, Effect::CloseSink], "the reply waits for SinkClosed");
    }
    let mut events = open_events();
    assert_eq!(events.step(Input::Cancel { reply: 7 }), [Effect::WantMinus, Effect::CloseSink]);
    let mut blob = open_as(Kind::Blob);
    assert_eq!(blob.step(Input::Cancel { reply: 7 }), [Effect::StatusInterest(false), Effect::CloseSink]);
}

#[test]
fn dlv_cancel_sink_closed() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))], "the waiting Pull gets Ended");
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Done)], "a Pull after the read end ends the stream");
    let mut first = live_rows();
    first.step(Input::Cancel { reply: 7 });
    assert_eq!(first.step(Input::SinkClosed), [Effect::Cancelled(7)]);
    assert_eq!(first.read_all(), [Val::Ended(Cause::Cancelled)], "the first Pull reads Ended");
}

#[test]
fn dlv_cancel_sink_closed_released() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 7 });
    twin.step(Input::Release);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(7)]);
    assert!(twin.reference.gone);
    assert_eq!(twin.state().streams(), 0, "the machine is out of the dictionary");
}

#[test]
fn dlv_cancelling_other() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    for input in [Input::Committed { revision: 2, touches: true }, Input::ReadDone { revision: 2, rows: rows(&["r2"]) }, Input::ReadFailed { revision: 2 }, Input::WindowState(Ws::loaded_at(2)), Input::Joined { position: 1 }, Input::ModelReady, Input::ModelGap, Input::Transfer { state: "x".into(), revision: 41 }] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.step(Input::Pull).is_empty(), "a Pull waits for Ended");
    assert_eq!(twin.step(Input::Cancel { reply: 2 }), [], "a second Cancel waits for the same SinkClosed");
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Cancelled(2), Effect::Answer(Pulled::Value(Val::Ended(Cause::Cancelled)))]);
}

#[test]
fn dlv_cancelling_end() {
    let mut epoch = live_rows();
    epoch.step(Input::Cancel { reply: 1 });
    assert!(epoch.step(Input::EpochEnded { own: true }).is_empty());
    epoch.step(Input::SinkClosed);
    assert_eq!(epoch.read_all(), [Val::Ended(Cause::SessionEnded)]);
    let mut closing = live_rows();
    closing.step(Input::Cancel { reply: 1 });
    assert!(closing.step(Input::Closing).is_empty(), "Drained waits for SinkClosed");
    assert_eq!(closing.step(Input::SinkClosed), [Effect::Cancelled(1), Effect::Drained]);
    assert_eq!(closing.read_all(), [Val::Ended(Cause::Closed)]);
}

#[test]
fn dlv_release_live() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::Release), [Effect::InterestMinus, Effect::CloseSink]);
        assert!(twin.step(Input::SinkClosed).is_empty(), "no Ended for a reader that is gone");
        assert_eq!(twin.state().streams(), 0);
    }
}

#[test]
fn dlv_release_cancelling() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 3 });
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(3)]);
    assert_eq!(twin.state().streams(), 0);
}

#[test]
fn dlv_release_ended() {
    let mut twin = ended_by_epoch();
    twin.step(Input::SinkClosed);
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.state().streams(), 0, "the machine goes at once: the sink is closed");
}

#[test]
fn dlv_release_ended_sink_open() {
    let mut twin = ended_by_epoch();
    assert!(twin.step(Input::Release).is_empty());
    assert_eq!(twin.state().streams(), 1);
    assert!(twin.step(Input::SinkClosed).is_empty());
    assert_eq!(twin.state().streams(), 0, "the machine goes after SinkClosed");
}

#[test]
fn dlv_ended_sink_closed() {
    let mut twin = ended_by_epoch();
    twin.step(Input::Cancel { reply: 9 });
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(9)]);
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
}

#[test]
fn dlv_epoch_ended() {
    for mut twin in [live_rows(), open_rows()] {
        assert_eq!(twin.step(Input::EpochEnded { own: true }), [Effect::InterestMinus, Effect::Available, Effect::CloseSink]);
        assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
    }
    let mut waiting = live_rows();
    waiting.step(Input::Pull);
    assert_eq!(waiting.step(Input::EpochEnded { own: true }), [Effect::InterestMinus, Effect::Answer(Pulled::Value(Val::Ended(Cause::SessionEnded))), Effect::CloseSink]);
}

#[test]
fn dlv_epoch_other() {
    let mut twin = live_rows();
    assert!(twin.step(Input::EpochEnded { own: false }).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_closing() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::Closing), [Effect::InterestMinus, Effect::Available, Effect::CloseSink]);
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Drained], "Drained once every sink is closed");
    assert_eq!(twin.read_all(), [Val::Ended(Cause::Closed)]);
}

#[test]
fn dlv_live_committed() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::Committed { revision: 2, touches: true }), [Effect::Read(2)]);
}

#[test]
fn dlv_live_unrelated() {
    let mut twin = live_rows();
    assert!(twin.step(Input::Committed { revision: 2, touches: false }).is_empty());
    let mut events = open_events();
    assert!(events.step(Input::Committed { revision: 2, touches: true }).is_empty());
    let mut blob = open_as(Kind::Blob);
    assert!(blob.step(Input::Committed { revision: 2, touches: true }).is_empty());
}

#[test]
fn dlv_live_read_unacked_first() {
    let mut twin = open_rows();
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1"]) });
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    twin.step(Input::Committed { revision: 2, touches: true });
    assert!(twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).is_empty(), "the journal held a value: no new Available");
    assert_eq!(twin.read_all(), [snap(2, &["r1", "r2"], Ws::loaded_at(2))], "the unread first value is replaced by the newer one");
}

#[test]
fn dlv_live_read_unread_snapshot() {
    let mut twin = live_rows();
    let many: Vec<r::Row> = (0..300).map(|n| (format!("n{n}"), 0)).collect();
    twin.step(Input::ReadDone { revision: 2, rows: many });
    twin.step(Input::ReadDone { revision: 3, rows: rows(&["n1", "r1"]) });
    assert_eq!(twin.read_all(), [snap(3, &["n1", "r1"], Ws::loaded_at(1))], "an unread recovery Snapshot gives way to a Snapshot");
}

/// P-DLV-18 (D-73): the value carries the revision of the last commit that touched the stream, whether the read saw the head of the database or an older one.
#[test]
fn dlv_prop_value_revision_relevant_commit() {
    let mut opened = open_rows();
    opened.step(Input::WindowState(Ws::loaded_at(1)));
    let (_, first) = opened.step_read(Input::ReadDone { revision: 5, rows: rows(&["r1"]) });
    assert_eq!(first, [snap(5, &["r1"], Ws::loaded_at(1))]);
    opened.step(Input::Resync);
    let (_, again) = opened.step_read(Input::ReadDone { revision: 7, rows: rows(&["r1"]) });
    assert_eq!(again, [snap(5, &["r1"], Ws::loaded_at(1))]);
    let mut pending = open_rows();
    pending.step(Input::WindowState(Ws::loaded_at(1)));
    pending.step_read(Input::ReadDone { revision: 5, rows: rows(&["r1"]) });
    pending.step(Input::Committed { revision: 6, touches: true });
    pending.step(Input::Resync);
    let (_, after_commit) = pending.step_read(Input::ReadRelevant { revision: 7, position: 0, relevant: Some((6, 0)), rows: rows(&["r1", "r2"]) });
    assert_eq!(after_commit, [snap(6, &["r1", "r2"], Ws::loaded_at(1))]);
    for seen in [3, 4] {
        let mut twin = live_rows();
        twin.step(Input::WindowState(Ws::loaded_at(2)));
        twin.step(Input::Committed { revision: 3, touches: true });
        twin.step(Input::Committed { revision: 4, touches: false });
        let (_, values) = twin.step_read(Input::ReadRelevant { revision: seen, position: 0, relevant: Some((3, 0)), rows: rows(&["r1", "r2"]) });
        assert_eq!(values, [Val::Delta { base: 1, revision: 3, size: 2, edits: vec![insert(1, "r2")], state: Ws::loaded_at(2) }], "seen {seen}");
    }
}

#[test]
fn dlv_prop_commit_during_read_is_delivered() {
    fn commit(twin: &mut Twin, revision: i64) -> Vec<Effect> {
        twin.post(d::DeliveryInput::Committed(Committed { batch: 0, revision, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }));
        Twin::modelled(take(&twin.log))
    }
    fn complete(twin: &mut Twin, read_id: u64, revision: i64, values: &[&str]) {
        twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id, revision, position: 0, relevant: Some((revision, 0)), data: d::ReadData::Rows { rows: rows_of(&rows(values)), replaced: Vec::new(), total: None } });
        take(&twin.log);
    }
    fn pull(twin: &mut Twin) -> Val {
        let outcome = twin.answers.register(EPOCH).id();
        twin.call(d::DeliveryInput::Pull { stream: STREAM, outcome, reader: 1 });
        Twin::modelled(take(&twin.log)).into_iter().find_map(|effect| match effect { Effect::Answer(Pulled::Value(value)) => Some(value), _ => None }).expect("a value is ready")
    }
    for commit_first in [false, true] {
        for first_snapshot in [5, 6] {
            let mut twin = live_rows();
            assert_eq!(commit(&mut twin, 5), [Effect::Read(5)]);
            let first_id = twin.read_id();
            if commit_first { assert_eq!(commit(&mut twin, 6), [Effect::Read(6)]); }
            complete(&mut twin, first_id, first_snapshot, if first_snapshot == 5 { &["r1", "r5"] } else { &["r1", "r5", "r6"] });
            if commit_first {
                let read_id = twin.read_id();
                complete(&mut twin, read_id, 6, &["r1", "r5", "r6"]);
            } else {
                let first = pull(&mut twin);
                assert!(matches!(&first, Val::Delta { revision, .. } if *revision == first_snapshot), "snapshot {first_snapshot}: {first:?}");
                let effects = commit(&mut twin, 6);
                assert_eq!(effects, if first_snapshot == 5 { vec![Effect::Read(6)] } else { vec![] });
                if first_snapshot == 6 { continue; }
                let read_id = twin.read_id();
                complete(&mut twin, read_id, 6, &["r1", "r5", "r6"]);
            }
            let last = pull(&mut twin);
            assert!(matches!(&last, Val::Delta { revision: 6, .. }), "commit_first={commit_first}, snapshot={first_snapshot}: {last:?}");
        }
    }
}

#[test]
fn dlv_changed_read_without_current_relevant_version_keeps_the_stream() {
    for relevant in [None, Some((0, 0))] {
        let mut twin = live_rows();
        twin.step(Input::Committed { revision: 2, touches: true });
        twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision: 2, position: 0, relevant, data: d::ReadData::Rows { rows: rows_of(&rows(&["r1", "r2"])), replaced: Vec::new(), total: None } });
        let journal = twin.state().journal(STREAM);
        assert!(!journal.iter().any(|value| matches!(value, d::Value::Ended(_))), "{relevant:?}: the stream stays open: {journal:?}");
        let labelled = match journal.last() {
            Some(d::Value::Snapshot { revision, .. }) => Some(*revision),
            Some(d::Value::Delta(delta)) => Some(delta.revision),
            _ => None,
        };
        assert_eq!(labelled, Some(2), "{relevant:?}: the changed rows carry the revision of their own read: {journal:?}");
    }
}

#[test]
fn dlv_live_read_changed() {
    let mut twin = live_rows();
    twin.step(Input::WindowState(Ws::loaded_at(2)));
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [Val::Delta { base: 1, revision: 2, size: 2, edits: vec![insert(1, "r2")], state: Ws::loaded_at(2) }]);
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: rows(&["r2"]) });
    assert_eq!(values, [Val::Delta { base: 2, revision: 3, size: 1, edits: vec![remove(0, "r1")], state: Ws::loaded_at(2) }]);
    let mut counted = open_as(Kind::Count);
    counted.step(Input::CountDone { revision: 1, count: 1 });
    counted.read_all();
    let (_, values) = counted.step_read(Input::CountDone { revision: 2, count: 2 });
    assert_eq!(values, [Val::Count { revision: 2, count: 2, state: Ws::idle() }]);
}

#[test]
fn dlv_live_read_unacknowledged() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    twin.step(Input::ReadDone { revision: 3, rows: rows(&["r1", "r2", "r3"]) });
    assert_eq!(twin.read_all(), [Val::Delta { base: 1, revision: 3, size: 3, edits: vec![insert(1, "r2"), insert(2, "r3")], state: Ws::loaded_at(1) }], "one Delta from the last read revision");
    assert_eq!(twin.state().acknowledged(STREAM), Some(3));
}

#[test]
fn dlv_live_read_same() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1"]) });
    assert!(effects.is_empty() && values.is_empty(), "the same rows: nothing");
    twin.step(Input::WindowState(Ws::loaded_at(3)));
    let (_, values) = twin.step_read(Input::ReadDone { revision: 3, rows: rows(&["r1"]) });
    assert_eq!(values, [Val::State { revision: 1, state: Ws::loaded_at(3) }], "a held state that became visible goes alone");
}

#[test]
fn dlv_live_read_old() {
    let mut twin = live_rows();
    for input in [Input::ReadStale { revision: 2, rows: rows(&["rX"]) }, Input::ReadDone { revision: 0, rows: rows(&["rZ"]) }] {
        let (effects, values) = twin.step_read(input);
        assert!(effects.is_empty() && values.is_empty());
    }
}

#[test]
fn dlv_live_read_failed() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::ReadFailed { revision: 2 });
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Loaded, error: true, loaded: Some(1) } }], "an error with rows keeps Loaded");
    let mut empty = open_rows();
    empty.step(Input::ReadDone { revision: 1, rows: Vec::new() });
    empty.read_all();
    let (_, values) = empty.step_read(Input::ReadFailed { revision: 2 });
    assert_eq!(values, [Val::State { revision: 1, state: Ws { phase: Phase::Failed, error: true, loaded: None } }]);
}

#[test]
fn dlv_live_window() {
    let mut twin = live_rows();
    let (_, values) = twin.step_read(Input::WindowState(Ws::loading_after(1)));
    assert_eq!(values, [Val::State { revision: 1, state: Ws::loading_after(1) }]);
}

#[test]
fn dlv_live_window_held() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(2)));
    assert!(values.is_empty(), "the state is visible only with revision 2");
    assert_eq!(effects, [Effect::Read(2)], "a state past the last read starts a read");
    let mut additional = 0;
    for _ in 0..20 {
        additional += twin.step(Input::WindowState(Ws::loading_after(2))).iter().filter(|effect| matches!(effect, Effect::Read(_))).count();
        additional += twin.step(Input::WindowState(Ws::loaded_at(2))).iter().filter(|effect| matches!(effect, Effect::Read(_))).count();
    }
    assert!(twin.step(Input::WindowState(Ws::loading_after(2))).is_empty());
    assert_eq!(additional, 0, "40 states at the same bound need only the initial read");
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { base: 1, revision: 2, size: 2, edits: vec![insert(1, "r2")], state: Ws::loading_after(2) }]);

    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(2)));
    assert_eq!(effects, [Effect::Read(2)]);
    assert!(values.is_empty());
    let (_, values) = twin.step_read(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(values, [Val::Delta { base: 1, revision: 2, size: 2, edits: vec![insert(1, "r2")], state: Ws::loaded_at(2) }]);
}

#[test]
fn dlv_live_window_same() {
    let mut twin = live_rows();
    let (effects, values) = twin.step_read(Input::WindowState(Ws::loaded_at(1)));
    assert!(effects.is_empty() && values.is_empty());
    twin.post(d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loaded_at(2)) });
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Read(2)]);
    twin.post(d::DeliveryInput::WindowState { stream: STREAM, state: window_state(&Ws::loaded_at(2)) });
    assert!(Twin::modelled(take(&twin.log)).is_empty(), "the same held state does not start another read");
}

#[test]
fn dlv_live_joined() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::ModelEvent { position: 5, payload: "c5".into() });
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready], "an event before Joined waits");
    twin.step(Input::Joined { position: 4 });
    assert_eq!(twin.read_all(), [Val::Event { position: 1, payload: "c5".into() }], "the held event follows Joined");
    assert!(twin.step(Input::Joined { position: 9 }).is_empty(), "a second Joined changes nothing");
}

#[test]
fn dlv_live_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    twin.step(Input::ModelEvent { position: 1, payload: "c1".into() });
    assert!(twin.read_all().is_empty(), "no event before Ready");
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Event { position: 1, payload: "c1".into() }]);
}

#[test]
fn dlv_live_ready_again() {
    let mut twin = open_events();
    assert!(twin.step(Input::ModelReady).is_empty());
}

#[test]
fn dlv_live_event_before_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    assert!(twin.step(Input::ModelEvent { position: 1, payload: "c1".into() }).is_empty());
    assert!(twin.step(Input::ModelLost { first: 2, last: 3 }).is_empty());
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Event { position: 1, payload: "c1".into() }, Val::Lost { first: 2, last: 3 }], "held inputs replay in order");
}

#[test]
fn dlv_live_event() {
    let mut twin = open_events();
    let (effects, values) = twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    assert_eq!(effects, [Effect::Available]);
    assert_eq!(values, [Val::Event { position: 1, payload: "c1".into() }]);
}

#[test]
fn dlv_live_event_gap() {
    let mut twin = open_events();
    let (_, values) = twin.step_read(Input::ModelEvent { position: 3, payload: "c3".into() });
    assert_eq!(values, [Val::Lost { first: 1, last: 2 }, Val::Event { position: 3, payload: "c3".into() }]);
}

#[test]
fn dlv_live_event_old() {
    let mut twin = open_events();
    twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    let (effects, values) = twin.step_read(Input::ModelEvent { position: 1, payload: "again".into() });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_lost() {
    let mut twin = open_events();
    let (_, values) = twin.step_read(Input::ModelLost { first: 1, last: 3 });
    assert_eq!(values, [Val::Lost { first: 1, last: 3 }]);
    let (_, values) = twin.step_read(Input::ModelLost { first: 3, last: 5 });
    assert_eq!(values, [Val::Lost { first: 4, last: 5 }], "a range that starts below the next position is cut");
}

#[test]
fn dlv_live_lost_old() {
    let mut twin = open_events();
    twin.step_read(Input::ModelLost { first: 1, last: 3 });
    let (effects, values) = twin.step_read(Input::ModelLost { first: 1, last: 2 });
    assert!(effects.is_empty() && values.is_empty());
}

#[test]
fn dlv_live_gap() {
    let mut twin = open_events();
    twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    let (_, values) = twin.step_read(Input::ModelGap);
    assert_eq!(values, [Val::Gap]);
    let (_, values) = twin.step_read(Input::ModelEvent { position: 2, payload: "c2".into() });
    assert_eq!(values, [Val::Event { position: 2, payload: "c2".into() }], "a Gap takes no position");
}

#[test]
fn dlv_live_gap_coalesced() {
    let mut twin = open_events();
    twin.step_read(Input::ModelEvent { position: 1, payload: "c1".into() });
    assert_eq!(twin.step(Input::ModelGap), [Effect::Available]);
    assert!(twin.step(Input::ModelGap).is_empty(), "a second Gap without a read has no effect");
    assert_eq!(twin.state().journal_len(STREAM), 1);
    assert_eq!(twin.read_all(), [Val::Gap]);
}

#[test]
fn dlv_live_gap_before_ready() {
    let mut twin = open_as(Kind::Events);
    twin.step(Input::Joined { position: 0 });
    assert!(twin.step(Input::ModelGap).is_empty());
    twin.step(Input::ModelReady);
    assert_eq!(twin.read_all(), [Val::Ready, Val::Gap]);
}

#[test]
fn dlv_live_event_rows_kind() {
    let mut twin = live_rows();
    for input in [Input::Joined { position: 1 }, Input::ModelReady, Input::ModelEvent { position: 1, payload: "c".into() }, Input::ModelLost { first: 1, last: 2 }, Input::ModelGap] {
        assert!(twin.step(input).is_empty());
    }
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_live_pull() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Delta { base: 1, revision: 2, size: 2, edits: vec![insert(1, "r2")], state: Ws::loaded_at(1) }))]);
    assert_eq!(twin.state().acknowledged(STREAM), Some(2), "the read of a rows value moves the acknowledged revision");
}

#[test]
fn dlv_live_pull_empty() {
    let mut twin = live_rows();
    assert!(twin.step(Input::Pull).is_empty());
    assert_eq!(twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) }).len(), 1, "the next value goes to the waiting Pull");
}

#[test]
fn dlv_ended_pull() {
    let mut twin = ended_by_epoch();
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Value(Val::Ended(Cause::SessionEnded)))]);
}

#[test]
fn dlv_ended_pull_none() {
    let mut twin = ended_by_epoch();
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::Pull), [Effect::Answer(Pulled::Done)], "a Pull after the read end ends the stream");
}

#[test]
fn dlv_ended_closing() {
    let mut open = ended_by_epoch();
    open.step(Input::SinkClosed);
    assert_eq!(open.step(Input::Closing), [Effect::Drained], "all streams are settled");
    let mut slow = ended_by_epoch();
    assert!(slow.step(Input::Closing).is_empty());
    assert_eq!(slow.step(Input::SinkClosed), [Effect::Drained], "after SinkClosed");
    let mut none = Twin::new();
    assert_eq!(none.step(Input::Closing), [Effect::Drained], "no streams");
}

#[test]
fn dlv_ended_cancel() {
    let mut twin = ended_by_epoch();
    twin.step(Input::SinkClosed);
    assert_eq!(twin.step(Input::Cancel { reply: 5 }), [Effect::Cancelled(5)]);
}

#[test]
fn dlv_ended_cancel_wait() {
    let mut twin = ended_by_epoch();
    assert!(twin.step(Input::Cancel { reply: 5 }).is_empty());
    assert_eq!(twin.step(Input::SinkClosed), [Effect::Cancelled(5)]);
}

#[test]
fn dlv_ended() {
    let mut twin = ended_by_epoch();
    for input in [Input::Committed { revision: 9, touches: true }, Input::ReadDone { revision: 9, rows: rows(&["r9"]) }, Input::WindowState(Ws::loading_after(9)), Input::ModelReady] {
        assert!(twin.step(input).is_empty());
    }
    assert_eq!(twin.read_all(), [Val::Ended(Cause::SessionEnded)]);
    let mut queue = Twin::new();
    let mut queue_view = view(Kind::List);
    queue_view.kind = d::Kind::Operations;
    queue_view.tables = vec!["op".into()];
    queue_view.local = true;
    queue_view.remote = false;
    queue.call(d::DeliveryInput::Open { stream: STREAM, view: queue_view, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0, deferred: None });
    let read_id = queue.read_id();
    queue.call(d::DeliveryInput::Cancel { stream: STREAM, reply: 5 });
    queue.call(d::DeliveryInput::SinkClosed { stream: STREAM });
    take(&queue.log);
    queue.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id, revision: 0, error: "late".into() });
    assert!(Twin::modelled(take(&queue.log)).is_empty(), "a late queue read cannot publish after the end");
    assert!(matches!(queue.state().journal(STREAM).as_slice(), [d::Value::Ended(d::EndCause::Cancelled)]));
}

#[test]
fn dlv_wait_pull() {
    let mut twin = live_rows();
    twin.step(Input::Pull);
    assert_eq!(twin.step(Input::Closing), [Effect::InterestMinus, Effect::Answer(Pulled::Value(Val::Ended(Cause::Closed))), Effect::CloseSink]);
    let mut by_epoch = live_rows();
    by_epoch.step(Input::Pull);
    assert_eq!(by_epoch.step(Input::EpochEnded { own: true }).len(), 3, "a waiting Pull is answered with Ended(SessionEnded)");
}

#[test]
fn dlv_wait_cancel() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    let effects = twin.step(Input::SinkClosed);
    assert_eq!(effects, [Effect::Cancelled(1)], "the reply comes with SinkClosed, not before");
    let mut by_epoch = live_rows();
    by_epoch.step(Input::Cancel { reply: 1 });
    by_epoch.step(Input::EpochEnded { own: true });
    assert_eq!(by_epoch.step(Input::SinkClosed), [Effect::Cancelled(1)]);
    assert_eq!(by_epoch.read_all(), [Val::Ended(Cause::SessionEnded)]);
}
