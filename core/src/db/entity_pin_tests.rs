use serde_json::{json, Value};

use crate::pacing::Counters;
use crate::query::{Query, Where};
use crate::schema::{AssociationKind, AssociationSpec, FieldSpec, ModelSpec};
use crate::value::ScalarKind;

use super::fixtures::*;
use super::observe::ObserverHandle;
use super::*;

fn user_spec() -> ModelSpec {
    let mut fields = vec![FieldSpec::scalar("id", ScalarKind::Id)];
    for name in ["fullName", "username", "name", "avatarUrl", "fullAvatarUrl", "description", "story", "connectionStatus", "lastInteraction", "shareUrl", "countryName", "countryCode"] {
        fields.push(FieldSpec::scalar(name, ScalarKind::Str).optional());
    }
    fields.push(FieldSpec::scalar("connectionChatId", ScalarKind::Id).nullable().optional());
    fields.push(FieldSpec::scalar("age", ScalarKind::Int).optional());
    fields.push(FieldSpec::scalar("online", ScalarKind::Bool).optional());
    fields.push(FieldSpec::scalar("distance", ScalarKind::Num).optional());
    fields.push(FieldSpec::scalar("isFriend", ScalarKind::Bool).optional().indexed());
    fields.push(FieldSpec::scalar("lastSeenAt", ScalarKind::Date).optional());
    fields.push(FieldSpec::scalar("createdAt", ScalarKind::Date).optional());
    fields.push(FieldSpec::scalar("updatedAt", ScalarKind::Date).optional());
    let mut spec = ModelSpec::new("users", fields);
    spec.associations.push(AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId"));
    spec
}

fn user_json(id: &str, online: bool) -> Value {
    json!({"id": id, "fullName": format!("Full Name {id}"), "username": format!("user_{id}"), "name": "Name", "avatarUrl": format!("https://example.test/avatars/{id}/thumb.jpg"),
        "fullAvatarUrl": format!("https://example.test/avatars/{id}/full.jpg"), "description": "A short description of the person that lives here", "story": "story",
        "connectionStatus": "CONNECTED", "connectionChatId": "c1", "lastInteraction": "GIFT", "shareUrl": format!("https://example.test/u/{id}"), "countryName": "Poland", "countryCode": "PL",
        "age": 27, "online": online, "distance": 12.5, "isFriend": true, "lastSeenAt": "2026-10-12T10:00:00Z", "createdAt": "2026-01-01T10:00:00Z", "updatedAt": "2026-10-12T10:00:00Z"})
}

fn screen_db() -> Db {
    let db = Db::open_in_memory(vec![user_spec(), moments_spec(), vibes_spec()], None).unwrap();
    for n in 0..10 {
        put(&db, "users", user_json(&format!("u{n}"), false));
    }
    put(&db, "users", user_json("me", true));
    for tab in 0..5 {
        for n in 0..10 {
            put(&db, "users", user_json(&format!("b{tab}_{n}"), false));
        }
    }
    for n in 0..50 {
        put(&db, "moments", json!({"id": format!("m{n}"), "userId": format!("u{}", n % 10), "text": "hello"}));
    }
    db.settle();
    db
}

fn find(db: &Db, id: &str) -> ObserverHandle {
    db.observe(Query::new("users").filtered(Where::equal("id", json!(id))), |_: &Change| {}).unwrap()
}

fn ids(db: &Db, ids: &[String]) -> ObserverHandle {
    db.observe(Query::new("users").filtered(Where::in_list("id", ids.iter().map(|id| json!(id)).collect())), |_: &Change| {}).unwrap()
}

fn row(db: &Db, id: &str) -> ObserverHandle {
    db.observe_row("users", &rid(id), |_: &Change| {}).unwrap()
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    FindPerView,
    IdsOnce,
    RowPerUser,
}

fn open_screen(db: &Db, mode: Mode) -> Vec<ObserverHandle> {
    let mut handles = vec![db.observe(Query::new("moments"), |_: &Change| {}).unwrap()];
    let authors: Vec<String> = (0..10).map(|n| format!("u{n}")).collect();
    match mode {
        Mode::FindPerView => handles.extend((0..50).map(|n| find(db, &format!("u{}", n % 10)))),
        Mode::IdsOnce => handles.push(ids(db, &authors)),
        Mode::RowPerUser => handles.extend((0..50).map(|n| row(db, &format!("u{}", n % 10)))),
    }
    db.settle();
    handles
}

fn background(db: &Db) -> Vec<ObserverHandle> {
    let mut handles: Vec<ObserverHandle> = (0..8).map(|_| find(db, "me")).collect();
    for tab in 0..5 {
        let set: Vec<String> = (0..10).map(|n| format!("b{tab}_{n}")).collect();
        handles.push(ids(db, &set));
    }
    db.settle();
    handles
}

fn diff(after: &Counters, before: &Counters) -> (usize, usize) {
    (after.observation_reads - before.observation_reads, after.observer_calls - before.observer_calls)
}

fn commit_user(db: &Db, id: &str, online: bool) {
    put(db, "users", user_json(id, online));
}

#[test]
fn entity_fanout_counts_stay_with_the_touched_rows() {
    for mode in [Mode::FindPerView, Mode::IdsOnce, Mode::RowPerUser] {
        let db = screen_db();
        let _screen = open_screen(&db, mode);
        let _background = background(&db);
        let before = db.counters();
        put(&db, "users", user_json("u900", true));
        db.settle();
        let (reads, calls) = diff(&db.counters(), &before);
        assert_eq!((reads, calls), (0, 0), "{mode:?}: a commit of a user that no query pins must not read or deliver");
        let before = db.counters();
        commit_user(&db, "u3", true);
        db.settle();
        let (reads, _) = diff(&db.counters(), &before);
        assert_eq!(reads, 1, "{mode:?}: only the group that pins u3 reads");
    }
}

#[test]
fn pinned_id_queries_follow_rows_that_appear_leave_or_vanish() {
    let db = screen_db();
    let (log, callback) = recorder();
    let _later = db.observe(Query::new("users").filtered(Where::equal("id", json!("late"))), callback).unwrap();
    let (anded_log, anded) = recorder();
    let both = Where::AllOf { clauses: vec![Where::in_list("id", vec![json!("u1"), json!("u2")]), Where::equal("online", json!(true))] };
    let _anded = db.observe(Query::new("users").filtered(both), anded).unwrap();
    db.settle();
    assert_eq!(calls(&log), vec![0]);
    put(&db, "users", user_json("late", false));
    assert_eq!(calls(&log), vec![0, 1], "a pinned id that did not exist lands later");
    commit_user(&db, "u1", true);
    commit_user(&db, "u2", true);
    commit_user(&db, "u1", false);
    assert_eq!(calls(&anded_log), vec![0, 1, 2, 1], "a pinned row enters and leaves the narrower filter");
    assert_eq!(db.keyed_rows(), 3);
    drop(_later);
    drop(_anded);
    db.settle();
    assert_eq!(db.keyed_rows(), 0, "a dropped group leaves no row in the reverse index");
}

#[test]
fn queries_whose_result_can_gain_rows_outside_the_id_set_keep_the_model_pin() {
    let db = screen_db();
    let id_u1 = || Where::equal("id", json!("u1"));
    let shapes = [
        ("any-of", Where::AnyOf { clauses: vec![id_u1(), Where::equal("online", json!(true))] }),
        ("not", Where::Not { clause: Box::new(id_u1()) }),
        ("not-in-list", Where::NotInList { field: "id".into(), values: vec![json!("u1")] }),
    ];
    for (name, clause) in shapes {
        let (log, callback) = recorder();
        let _handle = db.observe(Query::new("users").filtered(clause), callback).unwrap();
        db.settle();
        let before = calls(&log).len();
        commit_user(&db, "u7", true);
        assert_eq!(calls(&log).len(), before + 1, "{name}: a row outside the id set changes the result, the group must wake");
        commit_user(&db, "u7", false);
    }
}
