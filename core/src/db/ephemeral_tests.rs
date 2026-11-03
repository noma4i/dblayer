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
    db.set_ephemeral("people", &rid("p1"), obj(json!({"typing": value}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
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
    db.set_ephemeral("people", &rid("p1"), obj(json!({"mood": "happy"}))).unwrap();
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
fn inv_ephemeral_04_a_write_reaches_observers_as_an_updated_row_without_a_database_revision() {
    let (db, _) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let before = db.revision_seq().unwrap();
    typing(&db, true);
    let seen = changes(&log);
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].diff.updated, vec![rid("p1")]);
    assert_eq!(seen[1].rows[0].get("typing"), Some(&json!(true)));
    assert_eq!(db.revision_seq().unwrap(), before);
}

#[test]
fn row_observers_and_scope_observers_hear_the_write() {
    let (db, _) = rig();
    let scope = ScopeRef::new(ModelKey::new("people"), ListKey::compose(&["people", "room", "1"]));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: rid("p1"), order_key: "a".into() });
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (row_log, row_callback) = recorder();
    let (scope_log, scope_callback) = recorder();
    let _row = db.observe_row("people", &rid("p1"), row_callback).unwrap();
    let _scoped = db.observe(Query { scope: Some(scope.key.clone()), ..Query::new("people") }, scope_callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    typing(&db, true);
    assert_eq!(changes(&row_log).len(), 2);
    assert_eq!(changes(&scope_log).len(), 2);
}

#[test]
fn inv_ephemeral_02_the_value_returns_to_default_by_the_clock_and_observers_see_the_reset() {
    let (db, clock) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    typing(&db, true);
    let before = db.revision_seq().unwrap();
    clock.advance(seconds(4));
    assert_eq!(db.expire_ephemeral().unwrap(), 0);
    assert_eq!(value_of(&db, "typing"), json!(true));
    clock.advance(seconds(2));
    db.expire_ephemeral().unwrap();
    assert_eq!(value_of(&db, "typing"), json!(false));
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    assert_eq!(seen.len(), 3);
    assert_eq!((seen[2].diff.updated.clone(), seen[2].rows[0].get("typing").cloned()), (vec![rid("p1")], Some(json!(false))));
    assert_eq!(db.revision_seq().unwrap(), before);
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
    db.expire_ephemeral().unwrap();
    seen.push(epoch(&db));
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "epochs {seen:?}");
    assert_eq!(db.revision_seq().unwrap(), revision);
}

#[test]
fn inv_ephemeral_08_values_for_rows_that_never_arrive_leave_no_memory_behind() {
    let (db, clock) = rig();
    for turn in 0..50 {
        db.set_ephemeral("people", &rid(&format!("ghost{turn}")), obj(json!({"typing": true}))).unwrap();
    }
    assert_eq!(db.shared.ephemeral.lock().held_rows(), 50);
    clock.advance(seconds(6));
    db.expire_ephemeral().unwrap();
    assert_eq!(db.shared.ephemeral.lock().held_rows(), 0);
}

#[test]
fn inv_ephemeral_02_a_write_restarts_the_timer() {
    let (db, clock) = rig();
    typing(&db, true);
    clock.advance(seconds(3));
    db.set_ephemeral("people", &rid("p1"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(3));
    assert_eq!(db.expire_ephemeral().unwrap(), 0);
    assert_eq!(value_of(&db, "typing"), json!(true));
    clock.advance(seconds(3));
    db.expire_ephemeral().unwrap();
    assert_eq!(value_of(&db, "typing"), json!(false));
}

#[test]
fn writing_the_same_value_stays_silent() {
    let (db, _) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    typing(&db, true);
    typing(&db, true);
    typing(&db, false);
    typing(&db, false);
    assert_eq!(changes(&log).len(), 3);
}

#[test]
fn inv_ephemeral_06_destroying_a_row_removes_its_values() {
    let (db, _) = rig();
    typing(&db, true);
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("people"), id: rid("p1"), reason: DestroyReason::Server });
    db.land(envelope).unwrap();
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    assert_eq!(value_of(&db, "typing"), json!(false));
}

#[test]
fn inv_ephemeral_06_reset_forgets_every_value() {
    let (db, _) = rig();
    typing(&db, true);
    db.reset_all().unwrap();
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    assert_eq!(value_of(&db, "typing"), json!(false));
}

#[test]
fn bad_writes_are_rejected() {
    let (db, _) = rig();
    let set = |model: &str, id: &str, values: serde_json::Value| db.set_ephemeral(model, &rid(id), obj(values));
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
fn memory_models_read_and_observe_like_stored_ones_and_expire_by_the_clock() {
    let (db, clock) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("presence"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
    assert_eq!(db.find("presence", &rid("r1")).unwrap().unwrap().text("room"), Some("lobby"));
    clock.advance(seconds(31));
    db.expire_ephemeral().unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert!(db.find("presence", &rid("r1")).unwrap().is_none());
    let seen = changes(&log);
    assert_eq!((seen.len(), seen.last().unwrap().diff.removed.clone()), (3, vec![rid("r1")]));
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
        let count = crate::test_wait::join_within(std::thread::spawn(move || rival.destroy_claimed(rival.generation(), &claimed).unwrap()));
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
    assert_eq!(db.expire_ephemeral().unwrap(), 0);
    clock.advance(seconds(11));
    db.expire_ephemeral().unwrap();
    assert!(db.find("presence", &rid("r1")).unwrap().is_none());
}

#[test]
fn a_write_of_equal_values_renews_a_memory_row_and_stays_silent_for_observers() {
    type Writer<'a> = (&'a str, fn(&Db));
    let writers: [Writer; 2] = [
        ("landed", |db| put(db, "presence", json!({"id": "r1", "room": "lobby"}))),
        ("local update", |db| {
            db.update("presence", &rid("r1"), obj(json!({"room": "lobby"}))).unwrap();
        }),
    ];
    for (name, write) in writers {
        let (db, clock) = rig();
        let (log, callback) = recorder();
        let _subscription = db.observe(Query::new("presence"), callback).unwrap();
        put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
        db.await_revision(db.revision_seq().unwrap());
        let seen = changes(&log).len();
        clock.advance(seconds(20));
        write(&db);
        db.await_revision(db.revision_seq().unwrap());
        clock.advance(seconds(29));
        assert_eq!(db.expire_ephemeral().unwrap(), 0, "{name}");
        assert!(db.find("presence", &rid("r1")).unwrap().is_some(), "{name}");
        assert_eq!(changes(&log).len(), seen, "{name}");
        clock.advance(seconds(1));
        db.expire_ephemeral().unwrap();
        crate::test_wait::spin_until(name, || db.find("presence", &rid("r1")).unwrap().is_none());
    }
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
fn the_background_timer_resets_a_value_by_itself() {
    let db = Db::open_in_memory(vec![ModelSpec {
        ephemeral: vec![EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("1.seconds")],
        ..ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id)])
    }], None)
    .unwrap();
    put(&db, "people", json!({"id": "p1"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.set_ephemeral("people", &rid("p1"), obj(json!({"typing": true}))).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while changes(&log).len() < 3 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].rows[0].get("typing"), Some(&json!(false)));
}

#[test]
fn inv_ephemeral_06_a_value_for_a_missing_row_waits_for_the_row_and_still_resets_by_the_clock() {
    let (db, clock) = rig();
    db.set_ephemeral("people", &rid("late"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(2));
    put(&db, "people", json!({"id": "late", "name": "Bob"}));
    assert_eq!(db.find("people", &rid("late")).unwrap().unwrap().get("typing"), Some(&json!(true)));
    clock.advance(seconds(4));
    db.expire_ephemeral().unwrap();
    assert_eq!(db.find("people", &rid("late")).unwrap().unwrap().get("typing"), Some(&json!(false)));
}

#[test]
fn a_value_for_a_row_that_never_arrives_lapses_by_the_clock() {
    let (db, clock) = rig();
    db.set_ephemeral("people", &rid("ghost"), obj(json!({"typing": true}))).unwrap();
    clock.advance(seconds(6));
    db.expire_ephemeral().unwrap();
    put(&db, "people", json!({"id": "ghost", "name": "Bob"}));
    assert_eq!(db.find("people", &rid("ghost")).unwrap().unwrap().get("typing"), Some(&json!(false)));
}

#[test]
fn inv_ephemeral_05_an_ephemeral_change_of_an_included_row_updates_the_parent_observer() {
    let mut notes = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("personId", ScalarKind::Id).indexed()]);
    notes.associations.push(AssociationSpec::new("author", AssociationKind::BelongsTo, "people", "personId"));
    let db = Db::open_in_memory(vec![people_spec(), notes], None).unwrap();
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    put(&db, "notes", json!({"id": "n1", "personId": "p1"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query { include: vec!["author".into()], ..Query::new("notes") }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    typing(&db, true);
    let seen = changes(&log);
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].diff.updated, vec![rid("n1")]);
    assert_eq!(seen[1].related["author"][&rid("n1")][0].get("typing"), Some(&json!(true)));
}

#[test]
fn a_query_pinned_to_an_id_sees_the_write_and_the_reset_of_its_row() {
    let (db, clock) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people").filtered(Where::equal("id", json!("p1"))), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    typing(&db, true);
    clock.advance(seconds(6));
    db.expire_ephemeral().unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = changes(&log);
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[1].rows[0].get("typing").cloned(), Some(json!(true)));
    assert_eq!(seen[2].rows[0].get("typing").cloned(), Some(json!(false)));
}

#[test]
fn advancing_the_clock_expires_a_memory_row_and_tells_the_observer() {
    let (db, clock) = rig();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("presence"), callback).unwrap();
    put(&db, "presence", json!({"id": "r1", "room": "lobby"}));
    clock.advance(seconds(31));
    crate::test_wait::spin_until("the row expires at advance", || db.find("presence", &rid("r1")).unwrap().is_none());
    crate::test_wait::spin_until("the observer sees the removal", || changes(&log).last().is_some_and(|change| change.diff.removed == vec![rid("r1")]));
}

#[test]
fn advancing_the_clock_resets_an_ephemeral_value_and_tells_the_observer() {
    let mut people = people_spec();
    people.ephemeral[0] = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("60.seconds");
    let db = Db::open_in_memory(vec![people], None).unwrap();
    let clock = Arc::new(ManualClock::new(Duration::from_millis(START)));
    db.set_clock(clock.clone());
    put(&db, "people", json!({"id": "p1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("people"), callback).unwrap();
    typing(&db, true);
    crate::test_wait::spin_until("the write reaches the observer", || !changes(&log).is_empty());
    let before = changes(&log).len();
    clock.advance(seconds(61));
    crate::test_wait::spin_until("the observer hears the reset at advance", || {
        let seen = changes(&log);
        seen.len() > before && seen.last().is_some_and(|change| change.rows.iter().any(|row| row.get("typing") == Some(&json!(false))))
    });
}
