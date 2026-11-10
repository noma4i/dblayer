use super::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

fn temporary_entity(fetch: Fetch) -> (Twin, Arc<AtomicBool>) {
    let mut twin = Twin::new();
    let resolved = Arc::new(AtomicBool::new(false));
    let ready = resolved.clone();
    let mut temporary = view(Kind::Entity(fetch));
    temporary.subject = "temp-1".into();
    let mut real = temporary.clone();
    real.subject = "real-1".into();
    real.window.name = "real-node".into();
    temporary.remote = false;
    temporary.live = false;
    let deferred = d::Deferred { temps: vec!["temp-1".into()], rebind: d::Rebind::new(move || ready.load(Ordering::SeqCst).then(|| real.clone())) };
    twin.post(d::DeliveryInput::Open { stream: STREAM, view: temporary, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0, deferred: Some(deferred) });
    (twin, resolved)
}

fn rows(twin: &mut Twin, revision: i64, rows: Vec<d::StreamRow>, replaced: Vec<(String, String)>) {
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision, data: d::ReadData::Rows { rows, replaced, total: None } });
}

fn commit(twin: &mut Twin, revision: i64) {
    twin.post(d::DeliveryInput::Committed(Committed { batch: 1, revision, tables: vec!["moments".into()], rows: None, models: Models::default(), stale_parents: Vec::new(), position: revision as u64, shown: Vec::new(), files_removed: 0 }));
}

#[test]
fn dlv_open_entity_temp() {
    let (mut twin, _) = temporary_entity(Fetch::Stale);
    assert_eq!(Twin::modelled(take(&twin.log)), [Effect::Read(0)]);
    rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
    let journal = twin.state().journal(STREAM);
    assert!(matches!(journal.as_slice(), [d::Value::Snapshot { rows, state, .. }] if rows[0].id == "temp-1" && state.phase == WindowPhase::Loaded && state.settled && state.error.is_none()), "{journal:?}");
}

#[test]
fn dlv_entity_temp_swap() {
    for fetch in [Fetch::Stale, Fetch::Force] {
        let (mut twin, resolved) = temporary_entity(fetch);
        take(&twin.log);
        rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
        take(&twin.log);
        resolved.store(true, Ordering::SeqCst);
        commit(&mut twin, 1);
        take(&twin.log);
        rows(&mut twin, 1, vec![d::StreamRow::new("real-1", 1)], vec![("temp-1".into(), "real-1".into())]);
        let effects = take(&twin.log);
        assert_eq!(effects.iter().filter(|effect| matches!(effect, Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPlus") && input.contains("real-node"))).count(), 1, "{effects:?}");
        assert_eq!(effects.iter().filter(|effect| matches!(effect, Emitted::Send { to: WINDOWS, input } if input.starts_with("Load") && input.contains("force: true"))).count(), usize::from(fetch == Fetch::Force), "{effects:?}");
        assert!(effects.iter().any(|effect| matches!(effect, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { view, .. }, .. }) if view.subject == "real-1")));
        twin.post(d::DeliveryInput::WindowState { stream: STREAM, state: WindowState::idle() });
        rows(&mut twin, 1, vec![d::StreamRow::new("real-1", 1)], vec![]);
        assert!(twin.state().journal(STREAM).iter().any(|value| matches!(value, d::Value::Snapshot { rows, .. } if rows.iter().any(|row| row.id == "real-1"))));
        take(&twin.log);
        commit(&mut twin, 2);
        rows(&mut twin, 2, vec![d::StreamRow::new("real-1", 2)], vec![("temp-1".into(), "real-1".into())]);
        assert!(!Twin::modelled(take(&twin.log)).contains(&Effect::InterestPlus));
    }
}

#[test]
fn dlv_entity_temp_rollback() {
    let (mut twin, _) = temporary_entity(Fetch::Stale);
    take(&twin.log);
    rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
    take(&twin.log);
    commit(&mut twin, 1);
    rows(&mut twin, 1, vec![], vec![]);
    let journal = twin.state().journal(STREAM);
    assert!(journal.iter().any(|value| matches!(value, d::Value::Snapshot { rows, state, .. } if rows.is_empty() && state.phase == WindowPhase::Loaded && state.settled && state.error.is_none())), "{journal:?}");
    assert!(!Twin::modelled(take(&twin.log)).contains(&Effect::InterestPlus));
    twin.post(d::DeliveryInput::Cancel { stream: STREAM, reply: 5 });
    assert!(!Twin::modelled(take(&twin.log)).contains(&Effect::InterestMinus));
}
