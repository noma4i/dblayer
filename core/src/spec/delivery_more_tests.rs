use super::*;

#[test]
fn dlv_force_load_uses_committed_revision() {
    let mut det = DetScheduler::new(7);
    let log = new_log();
    det.add::<Sink<Windows>>("windows", new_log(), Discard);
    det.add::<Sink<Feed>>("feed", new_log(), Discard);
    let addr = det.add::<Spy<Deliveries>>("deliveries", SpyState::new(d::DeliveriesState::new(EPOCH, peers()), &log), Discard);
    det.start_epoch(EPOCH);
    det.post(addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: view(Kind::Entity(Fetch::Force)), epoch: EPOCH, identity: true, valid: true, now: 1_000_000, revision: 37 });
    det.run_until_quiet(100).unwrap();
    assert!(snapshot(&log).iter().any(|entry| matches!(entry, Emitted::Send { to: WINDOWS, input } if input.starts_with("Load") && input.contains("store_revision: 37"))));
}

fn resync(twin: &mut Twin) -> Vec<Effect> {
    twin.step(Input::Resync)
}

#[test]
fn dlv_opening_status_committed() {
    for kind in [Kind::Blob, Kind::Retry] {
        let mut twin = open_as(kind);
        assert!(twin.step(Input::Committed { revision: 1, touches: true }).is_empty());
        assert!(twin.read_all().is_empty());
    }
}

#[test]
fn dlv_opening_gap() {
    let mut twin = open_rows();
    assert!(twin.step(Input::ModelGap).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_live_gap_rows_kind() {
    let mut twin = live_rows();
    assert!(twin.step(Input::ModelGap).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_cancelling_models_reset() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    assert!(twin.step(Input::ModelsReset { own_model: true }).is_empty());
    assert!(twin.read_all().is_empty());
}

#[test]
fn dlv_opening_resync() {
    let mut twin = open_rows();
    let before = twin.read_id();
    assert!(resync(&mut twin).is_empty());
    assert_eq!(twin.read_id(), before);
    assert!(twin.state().journal(STREAM).is_empty());
}

#[test]
fn dlv_ended_resync() {
    let mut twin = live_rows();
    twin.step(Input::Cancel { reply: 1 });
    assert!(resync(&mut twin).is_empty());
    twin.step(Input::SinkClosed);
    let journal = twin.state().journal(STREAM);
    assert!(resync(&mut twin).is_empty());
    assert_eq!(twin.state().journal(STREAM), journal);
}

#[test]
fn dlv_live_resync_other() {
    for kind in [Kind::Count, Kind::Events, Kind::Blob, Kind::Retry] {
        let mut twin = open_as(kind);
        match kind {
            Kind::Count => { twin.step(Input::CountDone { revision: 1, count: 1 }); }
            Kind::Blob => { twin.step(Input::Transfer { state: "ready".into() }); }
            Kind::Retry => { twin.step(Input::Retry { attempts: 1 }); }
            _ => {}
        }
        let before = twin.state().journal(STREAM);
        assert!(resync(&mut twin).is_empty());
        assert_eq!(twin.state().journal(STREAM), before);
    }
}

#[test]
fn dlv_live_resync() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r2"]) });
    assert!(matches!(twin.state().journal(STREAM).first(), Some(d::Value::Delta(_))));
    assert_eq!(resync(&mut twin), [Effect::Read(2)]);
    assert!(twin.state().journal(STREAM).is_empty());
    let old_id = twin.read_id();
    assert_eq!(resync(&mut twin), [Effect::Read(2)]);
    let id = twin.read_id();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: old_id, revision: 2, data: d::ReadData::Rows { rows: rows_of(&rows(&["r2"])), replaced: vec![], total: None } });
    assert!(twin.state().journal(STREAM).is_empty());
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: id, revision: 2, data: d::ReadData::Rows { rows: rows_of(&rows(&["r2"])), replaced: vec![], total: None } });
    assert!(matches!(twin.state().journal(STREAM).as_slice(), [d::Value::Snapshot { revision: 2, .. }]));
}

#[test]
fn dlv_live_resync_after_read_delta() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r2"]) });
    assert!(matches!(twin.read_all().as_slice(), [Val::Delta { base: 1, revision: 2, .. }]));
    assert_eq!(resync(&mut twin), [Effect::Read(2)]);
    let id = twin.read_id();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: id, revision: 2, data: d::ReadData::Rows { rows: rows_of(&rows(&["r2"])), replaced: vec![], total: None } });
    assert!(matches!(twin.state().journal(STREAM).as_slice(), [d::Value::Snapshot { revision: 2, .. }]), "the read Delta is followed by a Snapshot of the current revision");
}

#[test]
fn dlv_live_read_recovered() {
    for revision in [1, 2] {
        let mut twin = live_rows();
        twin.step(Input::ReadFailed { revision: 2 });
        twin.read_all();
        let id = twin.read_id();
        twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: id, revision, data: d::ReadData::Rows { rows: rows_of(&rows(&["r1"])), replaced: vec![], total: None } });
        assert!(matches!(twin.state().journal(STREAM).first(), Some(d::Value::State { state, .. }) if state.error.is_none()));
    }
    let mut twin = open_as(Kind::Count);
    twin.step(Input::WindowState(Ws::idle()));
    twin.step(Input::CountDone { revision: 1, count: 3 });
    twin.read_all();
    twin.step(Input::ReadFailed { revision: 2 });
    twin.read_all();
    let read_id = twin.read_id();
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id, revision: 1, data: d::ReadData::Count(3) });
    assert!(matches!(twin.state().journal(STREAM).first(), Some(d::Value::State { state, .. }) if state.error.is_none()));
}

#[test]
fn dlv_resync_read_failed_retry() {
    let mut twin = live_rows();
    resync(&mut twin);
    let read_id = twin.read_id();
    take(&twin.log);
    twin.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id, revision: 1, error: "offline storage".into() });
    assert!(twin.read_id() > read_id);
    assert!(Twin::modelled(take(&twin.log)).contains(&Effect::Read(1)));
    assert!(matches!(twin.state().journal(STREAM).as_slice(), [d::Value::State { state, .. }] if state.error.is_some()));
}

#[test]
fn dlv_resync_read_failed_end() {
    let mut twin = live_rows();
    resync(&mut twin);
    let read_id = twin.read_id();
    twin.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id, revision: 1, error: "first".into() });
    let read_id = twin.read_id();
    take(&twin.log);
    twin.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id, revision: 1, error: "second".into() });
    assert!(matches!(twin.state().journal(STREAM).as_slice(), [d::Value::Ended(d::EndCause::Storage { error })] if error == "second"));
    let effects = Twin::modelled(take(&twin.log));
    assert!(effects.contains(&Effect::InterestMinus));
    assert!(effects.contains(&Effect::CloseSink));
}

fn snapshot_edits(twin: &Twin) -> Vec<Ed> {
    let journal = twin.state().journal(STREAM);
    let [d::Value::Snapshot { edits, .. }] = journal.as_slice() else { panic!("one Snapshot waits: {journal:?}") };
    edits.iter().map(ed_of).collect()
}

#[test]
fn dlv_snapshot_edits_first() {
    let mut twin = open_rows();
    twin.step(Input::WindowState(Ws::loaded_at(1)));
    twin.step(Input::ReadDone { revision: 1, rows: rows(&["r1", "r2"]) });
    assert_eq!(snapshot_edits(&twin), [insert(0, "r1"), insert(1, "r2")]);
}

#[test]
fn dlv_snapshot_edits_resync() {
    let mut twin = live_rows();
    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r3"]) });
    twin.read_all();
    twin.step(Input::Resync);
    twin.step(Input::ReadDone { revision: 3, rows: rows(&["r3", "r4"]) });
    assert_eq!(snapshot_edits(&twin), [remove(0, "r1"), insert(1, "r4")]);
}
