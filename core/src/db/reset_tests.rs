use super::fixtures::*;
use super::Db;
use crate::query::Query;

#[test]
fn inv_row_11_reset_all_wipes_the_rows_and_the_row_cache() {
    let db = booted();
    seed_tree(&db);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    db.reset_all().unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert_eq!(db.count(&Query::new("users")).unwrap(), 0);
}

#[test]
fn inv_row_11_reset_of_models_keeps_the_other_models() {
    let db = booted();
    seed_tree(&db);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    assert!(db.peek_row("moments", &rid("m1")).is_some());
    db.reset(&["moments"]).unwrap();
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert!(db.query(&Query::new("moments")).unwrap().is_empty());
    assert!(db.find("users", &rid("u1")).unwrap().is_some());
}

#[test]
fn reset_advances_the_revision() {
    let db = booted();
    seed_tree(&db);
    let before = db.revision_seq().unwrap();
    db.reset(&["moments"]).unwrap();
    assert!(db.revision_seq().unwrap() > before);
}

#[test]
fn reset_of_an_unknown_model_fails() {
    let db = booted();
    assert!(db.reset(&["ghosts"]).is_err());
}

type ResetCalls = std::sync::Arc<std::sync::Mutex<Vec<Option<Vec<String>>>>>;

type ResetCall = Box<dyn Fn(Option<&[String]>) + Send + Sync>;

struct ResetProbe {
    registration: std::sync::OnceLock<crate::ffi::legacy::Registration>,
    call: ResetCall,
}

impl crate::ffi::legacy::Holder for ResetProbe {
    fn close(&self) {}
    fn registration(&self) -> &std::sync::OnceLock<crate::ffi::legacy::Registration> { &self.registration }
    fn changed(&self, snapshot: &crate::ffi::legacy::Snapshot) {
        if let crate::ffi::legacy::Change::Cleared(models) = &snapshot.change { (self.call)(models.as_deref()); }
    }
}

fn reentrant_reset_rig() -> (Db, ResetCalls, std::sync::Arc<ResetProbe>) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let db = booted();
    seed_tree(&db);
    let calls: ResetCalls = Arc::default();
    let (inner, sink, entered) = (db.clone(), calls.clone(), Arc::new(AtomicBool::new(false)));
    let probe = Arc::new(ResetProbe { registration: Default::default(), call: Box::new(move |models| {
        sink.lock().unwrap().push(models.map(<[String]>::to_vec));
        if entered.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, inner.generation());
        envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: crate::identity::ModelKey::new("moments"), id: rid("m9"), row: obj(serde_json::json!({"id": "m9", "userId": "u2", "text": "nine"})) });
        inner.land(&inner.admit().unwrap(), envelope).unwrap();
        inner.reset(&["users"]).unwrap();
    }) });
    db.shared.store.lifecycle.enroll(&probe);
    (db, calls, probe)
}

#[test]
fn reset_listener_writes_and_resets() {
    type Outer = fn(&Db);
    let cases: [(&str, Outer, Option<Vec<String>>); 2] = [
        ("reset_all", |db| db.clear_account().unwrap(), None),
        ("reset of models", |db| db.clear_models(&["vibes"]).unwrap(), Some(vec!["vibes".to_string()])),
    ];
    for (name, outer, first) in cases {
        let (db, calls, _probe) = reentrant_reset_rig();
        let before = db.generation();
        let runner = db.clone();
        crate::test_wait::join_within(std::thread::spawn(move || outer(&runner)));
        assert_eq!(*calls.lock().unwrap(), vec![first, Some(vec!["users".to_string()])], "{name}: listener calls");
        assert_eq!(db.generation(), before.next().next(), "{name}: one generation per reset");
        assert!(db.find("moments", &rid("m9")).unwrap().is_some(), "{name}: the write made inside the callback is committed");
        assert_eq!(db.count(&Query::new("users")).unwrap(), 0, "{name}: the nested reset applied after the write");
    }
}

#[test]
fn reset_listener_sees_the_finished_reset() {
    use std::sync::{Arc, Mutex};
    let db = booted();
    seed_tree(&db);
    assert!(db.find("moments", &rid("m1")).unwrap().is_some());
    assert!(db.peek_row("moments", &rid("m1")).is_some());
    let seen: Arc<Mutex<Vec<(bool, bool, i64)>>> = Arc::default();
    let (probe, sink) = (db.clone(), seen.clone());
    let observer = Arc::new(ResetProbe { registration: Default::default(), call: Box::new(move |_| {
        let revision = probe.revision_seq().unwrap();
        sink.lock().unwrap().push((probe.find("moments", &rid("m1")).unwrap().is_none(), probe.peek_row("moments", &rid("m1")).is_none(), revision));
    }) });
    db.shared.store.lifecycle.enroll(&observer);
    db.reset(&["moments"]).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![(true, true, db.revision_seq().unwrap())]);
}
