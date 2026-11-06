use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ListKey};
use crate::pacing::ManualClock;
use crate::query::{Query, Where};
use crate::schema::{AssociationKind, AssociationSpec, EphemeralSpec, FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

const START: u64 = 1_000_000;

#[test]
fn expiry_visits_only_due_rows_and_claims_only_candidates() {
    let clock = Arc::new(ManualClock::new(seconds(0)));
    let held = super::ephemeral::Ephemerals::new(&[presence_spec()]);
    let mut state = held.lock();
    state.clock = clock.clone();
    state.track("presence", "due", 0);
    drop(state);
    clock.advance(seconds(1));
    let mut state = held.lock();
    for id in 0..1000 { state.track("presence", &id.to_string(), 0); }
    drop(state);
    clock.advance(seconds(29));
    let mut state = held.lock();
    state.due_visits.set(0);
    let due = state.take_due();
    assert_eq!(due.expired, vec![("presence".to_string(), "due".to_string())]);
    assert_eq!(state.due_visits.get(), 1);
    state.due_visits.set(0);
    assert_eq!(state.claim_expired(&due.expired), vec![(due.expired[0].clone(), 0)]);
    assert_eq!(state.due_visits.get(), 1);
}

fn people_spec() -> ModelSpec {
    ModelSpec {
        ephemeral: vec![
            EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("5.seconds"),
            EphemeralSpec::new(FieldSpec::scalar("mood", ScalarKind::Str).nullable()),
        ],
        ..ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional().indexed()])
    }
}

fn presence_spec() -> ModelSpec {
    ModelSpec {
        storage: Storage::Memory,
        expire_after: Some("30.seconds".into()),
        ..ModelSpec::new("presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("room", ScalarKind::Str).optional().indexed()])
    }
}

fn rig() -> (Db, Arc<ManualClock>) {
    let db = Db::open_in_memory(vec![people_spec(), presence_spec()], None).unwrap();
    let clock = Arc::new(ManualClock::new(Duration::from_millis(START)));
    db.set_clock(clock.clone());
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    (db, clock)
}

fn typing(db: &Db, value: bool) {
    db.set_ephemeral(&db.admit().unwrap(), "people", &rid("p1"), obj(json!({"typing": value}))).unwrap();
}

fn value_of(db: &Db, field: &str) -> serde_json::Value {
    db.find("people", &rid("p1")).unwrap().unwrap().get(field).cloned().unwrap()
}

fn seconds(count: u64) -> Duration {
    Duration::from_secs(count)
}

#[test]
fn rows_read_with_defaults_and_written_values_but_store_nothing() {
    let (db, _) = rig();
    assert_eq!((value_of(&db, "typing"), value_of(&db, "mood")), (json!(false), json!(null)));
    typing(&db, true);
    db.set_ephemeral(&db.admit().unwrap(), "people", &rid("p1"), obj(json!({"mood": "happy"}))).unwrap();
    let found = db.find("people", &rid("p1")).unwrap().unwrap();
    assert_eq!((found.get("typing"), found.get("mood")), (Some(&json!(true)), Some(&json!("happy"))));
    assert_eq!(db.query(&Query::new("people")).unwrap()[0].get("typing"), Some(&json!(true)));
    assert_eq!(db.find_many("people", &[rid("p1")]).unwrap()[0].get("typing"), Some(&json!(true)));
    let stored: i64 = db
        .shared
        .store
        .lock()
        .connection
        .query_row("SELECT COUNT(*) FROM pragma_table_info('row_people') WHERE name LIKE '%typing%' OR name LIKE '%mood%'", [], |row| row.get(0))
        .unwrap();
    assert_eq!(stored, 0);
}




#[test]
fn inv_ephemeral_04_the_position_grows_with_every_write_and_reset_while_the_revision_stays() {
    let (db, clock) = rig();
    let epoch = |db: &Db| db.shared.ephemeral.lock().epoch;
    let revision = db.revision_seq().unwrap();
    let mut seen = vec![epoch(&db)];
    typing(&db, true);
    seen.push(epoch(&db));
    typing(&db, false);
    seen.push(epoch(&db));
    typing(&db, true);
    seen.push(epoch(&db));
    clock.advance(seconds(6));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    seen.push(epoch(&db));
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "epochs {seen:?}");
    assert_eq!(db.revision_seq().unwrap(), revision);
}

#[test]
fn inv_ephemeral_08_values_for_rows_that_never_arrive_leave_no_memory_behind() {
    let (db, clock) = rig();
    for turn in 0..50 {
        db.set_ephemeral(&db.admit().unwrap(), "people", &rid(&format!("ghost{turn}")), obj(json!({"typing": true}))).unwrap();
    }
    assert_eq!(db.shared.ephemeral.lock().held_rows(), 50);
    clock.advance(seconds(6));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    assert_eq!(db.shared.ephemeral.lock().held_rows(), 0);
}

#[test]
fn inv_ephemeral_02_a_write_restarts_the_timer() {
    let (db, clock) = rig();
    typing(&db, true);
    clock.advance(seconds(3));
    db.set_ephemeral(&db.admit().unwrap(), "people", &rid("p1"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(3));
    assert_eq!(db.expire_ephemeral(&db.admit().unwrap()).unwrap(), 0);
    assert_eq!(value_of(&db, "typing"), json!(true));
    clock.advance(seconds(3));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    assert_eq!(value_of(&db, "typing"), json!(false));
}


#[test]
fn inv_ephemeral_06_destroying_a_row_removes_its_values() {
    let (db, _) = rig();
    typing(&db, true);
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("people"), id: rid("p1"), reason: DestroyReason::Server });
    db.land(&db.admit().unwrap(), envelope).unwrap();
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    assert_eq!(value_of(&db, "typing"), json!(false));
}

#[test]
fn inv_ephemeral_06_reset_forgets_every_value() {
    let (db, _) = rig();
    typing(&db, true);
    db.clear_account().unwrap();
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    assert_eq!(value_of(&db, "typing"), json!(false));
}

#[test]
fn bad_writes_are_rejected() {
    let (db, _) = rig();
    let set = |model: &str, id: &str, values: serde_json::Value| db.set_ephemeral(&db.admit().unwrap(), model, &rid(id), obj(values));
    assert!(matches!(set("people", "p1", json!({"name": "Bob"})), Err(DbError::NotEphemeralField { .. })));
    assert!(matches!(set("people", "p1", json!({"typing": "yes"})), Err(DbError::InvalidEphemeralValue { .. })));
    assert!(matches!(set("people", "p1", json!({"typing": null})), Err(DbError::InvalidEphemeralValue { .. })));
    assert!(matches!(set("ghosts", "p1", json!({})), Err(DbError::UnknownModel(_))));
}

#[test]
fn inv_ephemeral_07_ephemeral_fields_cannot_be_queried() {
    let (db, _) = rig();
    let query = Query::new("people").filtered(Where::equal("typing", json!(true)));
    assert!(matches!(db.query(&query), Err(DbError::Query(crate::query::QueryError::EphemeralField(_)))));
}


#[test]
fn inv_ephemeral_03_a_landing_before_its_expiry_is_armed_keeps_the_memory_row_alive() {
    let (db, clock) = rig();
    put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
    let _scan = db.shared.expiry_gate.lock().unwrap();
    clock.advance(seconds(31));
    let claimed = {
        let mut state = db.shared.ephemeral.lock();
        let due = state.take_due();
        state.claim_expired(&due.expired)
    };
    assert_eq!(claimed.len(), 1);
    let (rival, fired, destroyed) = (db.clone(), std::sync::atomic::AtomicBool::new(false), Arc::new(std::sync::Mutex::new(Vec::new())));
    let seen = destroyed.clone();
    *db.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |_| {
        if fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let (rival, claimed) = (rival.clone(), claimed.clone());
        let count = crate::test_wait::join_within(std::thread::spawn(move || rival.destroy_claimed(&rival.admit().unwrap(), rival.generation(), &claimed).unwrap()));
        seen.lock().unwrap().push(count);
    }));
    put(&db, "presence", json!({"id": "r1", "room": "garden"}));
    assert_eq!(*destroyed.lock().unwrap(), vec![0]);
    assert_eq!(db.find("presence", &rid("r1")).unwrap().unwrap().text("room"), Some("garden"));
}

#[test]
fn a_rewrite_keeps_a_memory_row_alive() {
    let (db, clock) = rig();
    put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
    clock.advance(seconds(20));
    put(&db, "presence", json!({"id": "r1", "room": "garden"}));
    clock.advance(seconds(20));
    assert_eq!(db.expire_ephemeral(&db.admit().unwrap()).unwrap(), 0);
    clock.advance(seconds(11));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    assert!(db.find("presence", &rid("r1")).unwrap().is_none());
}


#[test]
fn inv_ephemeral_01_values_and_memory_rows_do_not_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("a.sqlite");
    {
        let db = Db::open(&path, vec![people_spec(), presence_spec()], None).unwrap();
        put(&db, "people", json!({"id": "p1", "name": "Ann"}));
        put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
        typing(&db, true);
    }
    let db = Db::open(&path, vec![people_spec(), presence_spec()], None).unwrap();
    assert_eq!(value_of(&db, "typing"), json!(false));
    assert!(db.find("presence", &rid("r1")).unwrap().is_none());
}


#[test]
fn inv_ephemeral_06_a_value_for_a_missing_row_waits_for_the_row_and_still_resets_by_the_clock() {
    let (db, clock) = rig();
    db.set_ephemeral(&db.admit().unwrap(), "people", &rid("late"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(2));
    put(&db, "people", json!({"id": "late", "name": "Bob"}));
    assert_eq!(db.find("people", &rid("late")).unwrap().unwrap().get("typing"), Some(&json!(true)));
    clock.advance(seconds(4));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    assert_eq!(db.find("people", &rid("late")).unwrap().unwrap().get("typing"), Some(&json!(false)));
}

#[test]
fn a_value_for_a_row_that_never_arrives_lapses_by_the_clock() {
    let (db, clock) = rig();
    db.set_ephemeral(&db.admit().unwrap(), "people", &rid("ghost"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(6));
    db.expire_ephemeral(&db.admit().unwrap()).unwrap();
    put(&db, "people", json!({"id": "ghost", "name": "Bob"}));
    assert_eq!(db.find("people", &rid("ghost")).unwrap().unwrap().get("typing"), Some(&json!(false)));
}




