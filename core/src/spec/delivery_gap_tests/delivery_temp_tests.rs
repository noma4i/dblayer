use super::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

fn temporary_entity(fetch: Fetch) -> (Twin, Arc<AtomicBool>) {
    let mut twin = Twin::new();
    let resolved = Arc::new(AtomicBool::new(false));
    let ready = resolved.clone();
    let mut temporary = view(Kind::Entity(fetch));
    temporary.tables = vec![crate::store::ddl::table_name("Moment")];
    temporary.subject = "temp-1".into();
    let mut real = temporary.clone();
    real.subject = "real-1".into();
    real.window.name = "real-node".into();
    temporary.remote = false;
    temporary.live = false;
    let deferred = d::Deferred { temps: vec![(temporary.model.clone(), "temp-1".into())], rebind: d::Rebind::new(move || ready.load(Ordering::SeqCst).then(|| real.clone())) };
    twin.post(d::DeliveryInput::Open { stream: STREAM, view: temporary, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0, deferred: Some(deferred) });
    (twin, resolved)
}

fn rows(twin: &mut Twin, revision: i64, rows: Vec<d::StreamRow>, replaced: Vec<(String, String)>) {
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM, read_id: twin.read_id(), revision, data: d::ReadData::Rows { rows, replaced, total: None } });
}

fn commit(twin: &mut Twin, revision: i64) {
    twin.post(d::DeliveryInput::Committed(Committed { batch: 1, revision, tables: vec![crate::store::ddl::table_name("Moment")], rows: None, models: Models::default(), stale_parents: Vec::new(), position: revision as u64, shown: Vec::new(), files_removed: 0 }));
}

#[test]
fn dlv_deferred_index_other_model() {
    let (mut twin, first_ready) = temporary_entity(Fetch::Stale);
    let second_ready = Arc::new(AtomicBool::new(false));
    let signal = second_ready.clone();
    let mut other = view(Kind::Entity(Fetch::Stale));
    other.model = "User".into();
    other.tables = vec![crate::store::ddl::table_name("User")];
    other.window.model = "User".into();
    other.subject = "temp-1".into();
    other.remote = false;
    other.live = false;
    let mut real = other.clone();
    real.subject = "real-user".into();
    let deferred = d::Deferred { temps: vec![("User".into(), "temp-1".into())], rebind: d::Rebind::new(move || signal.load(Ordering::SeqCst).then(|| real.clone())) };
    twin.post(d::DeliveryInput::Open { stream: STREAM + 1, view: other, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0, deferred: Some(deferred) });
    take(&twin.log);
    rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
    let read_id = twin.state().read_id(STREAM + 1);
    twin.post(d::DeliveryInput::ReadDone { stream: STREAM + 1, read_id, revision: 0, data: d::ReadData::Rows { rows: vec![d::StreamRow::new("temp-1", 0)], replaced: vec![], total: None } });
    take(&twin.log);
    first_ready.store(true, Ordering::SeqCst);
    second_ready.store(true, Ordering::SeqCst);
    twin.post(d::DeliveryInput::Committed(Committed { batch: 1, revision: 1, tables: vec![crate::store::ddl::table_name("Moment")], rows: Some([(crate::store::ddl::table_name("Moment"), "temp-1".into())].into()), models: Models::default(), stale_parents: Vec::new(), position: 1, shown: Vec::new(), files_removed: 0 }));
    let effects = take(&twin.log);
    assert!(effects.iter().any(|effect| matches!(effect, Emitted::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Read { .. } }) if *stream == STREAM)), "{effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, Emitted::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Read { .. } }) if *stream == STREAM + 1)), "{effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, Emitted::Send { to: WINDOWS, input } if input.starts_with("InterestPlus") && input.contains("User"))), "{effects:?}");
}

#[test]
fn dlv_alias_restart_model_isolation() {
    use crate::identity::RowId;
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{EffectRow, IntentEffects, IntentOrigin, StoreBatch, StoreOp};
    use crate::store::fixtures::{all_specs, put};
    use crate::store::Store;
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let db = Store::open_models(Some(&path), all_specs(), None).unwrap();
    put(&db, "moments", json!({"id":"temp-1", "userId":"u1"}));
    put(&db, "users", json!({"id":"temp-1"}));
    for (op_id, model, value) in [(1, "moments", json!({"id":"temp-1", "userId":"u1"})), (2, "users", json!({"id":"temp-1"}))] {
        let effects = IntentEffects { rows: vec![EffectRow { model: model.into(), row_id: "temp-1".into(), values: value.to_string() }], ..Default::default() };
        let batch = StoreBatch { author: BatchAuthor::Outbox, id: op_id, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::Intent { attempts: 1, cycle_id: 1, op_id, model: model.into(), row_id: "temp-1".into(), action: "create".into(), payload: "{}".into(), effects, origin: IntentOrigin::Api }] };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    }
    drop(db);

    let db = Store::open_models(Some(&path), all_specs(), None).unwrap();
    let temp = RowId::new("temp-1").unwrap();
    assert_eq!(db.resolve("moments", &temp).unwrap(), temp);
    assert_eq!(db.resolve("users", &temp).unwrap(), temp);
    assert!(db.read(&["moments"], |reader| reader.point("moments", "temp-1")).unwrap().is_some());
    assert!(db.read(&["users"], |reader| reader.point("users", "temp-1")).unwrap().is_some());
    let batch = StoreBatch {
        author: BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(),
        rows: vec![("moments".into(), json!({"id":"real-1", "userId":"u1"}).to_string())], intents: 0, credential: None,
        ops: vec![StoreOp::Swap { model: "moments".into(), temp: "temp-1".into(), real: "real-1".into() }],
    };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!(db.resolve("moments", &temp).unwrap().as_str(), "real-1");
    assert_eq!(db.resolve("users", &temp).unwrap().as_str(), "temp-1");
    assert!(db.read(&["users"], |reader| reader.point("users", "temp-1")).unwrap().is_some());
    assert!(db.read(&["users"], |reader| reader.point("users", "real-1")).unwrap().is_none());
    drop(db);
    let reopened = Store::open_models(Some(&path), all_specs(), None).unwrap();
    assert_eq!(reopened.resolve("moments", &temp).unwrap().as_str(), "real-1");
    assert_eq!(reopened.resolve("users", &temp).unwrap().as_str(), "temp-1");
}

#[test]
fn dlv_deferred_index_released() {
    for ending in ["cancel", "release", "sink", "epoch", "closing"] {
        let (mut twin, ready) = temporary_entity(Fetch::Stale);
        take(&twin.log);
        rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
        take(&twin.log);
        match ending {
            "cancel" => twin.post(d::DeliveryInput::Cancel { stream: STREAM, reply: 5 }),
            "release" => twin.post(d::DeliveryInput::Release { stream: STREAM, reader: None, now: crate::runtime::Time(0) }),
            "sink" => {
                twin.post(d::DeliveryInput::Cancel { stream: STREAM, reply: 5 });
                assert!(Twin::modelled(take(&twin.log)).contains(&Effect::CloseSink));
                twin.post(d::DeliveryInput::SinkClosed { stream: STREAM });
            }
            "epoch" => { twin.det.end_epoch(EPOCH, Ended::SessionReset); twin.run(); }
            "closing" => { twin.det.close(); twin.run(); }
            _ => unreachable!(),
        }
        take(&twin.log);
        ready.store(true, Ordering::SeqCst);
        twin.post(d::DeliveryInput::Committed(Committed { batch: 1, revision: 1, tables: vec![crate::store::ddl::table_name("Moment")], rows: Some([(crate::store::ddl::table_name("Moment"), "temp-1".into())].into()), models: Models::default(), stale_parents: Vec::new(), position: 1, shown: Vec::new(), files_removed: 0 }));
        let effects = Twin::modelled(take(&twin.log));
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Read(_) | Effect::InterestPlus)), "{ending}: {effects:?}");
    }

    let (mut twin, ready) = temporary_entity(Fetch::Stale);
    take(&twin.log);
    rows(&mut twin, 0, vec![d::StreamRow::new("temp-1", 0)], vec![]);
    take(&twin.log);
    ready.store(true, Ordering::SeqCst);
    commit(&mut twin, 1);
    take(&twin.log);
    rows(&mut twin, 1, vec![d::StreamRow::new("real-1", 1)], vec![("temp-1".into(), "real-1".into())]);
    take(&twin.log);
    twin.post(d::DeliveryInput::Committed(Committed { batch: 2, revision: 2, tables: vec![crate::store::ddl::table_name("Moment")], rows: Some([(crate::store::ddl::table_name("Moment"), "temp-1".into())].into()), models: Models::default(), stale_parents: Vec::new(), position: 2, shown: Vec::new(), files_removed: 0 }));
    let effects = Twin::modelled(take(&twin.log));
    assert!(!effects.iter().any(|effect| matches!(effect, Effect::Read(_) | Effect::InterestPlus)), "{effects:?}");
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
