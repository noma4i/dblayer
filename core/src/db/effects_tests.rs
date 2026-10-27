use serde_json::json;

use super::spec_fixtures::{join, members, scope_key, specs, ROOM_ORDER};
use super::fixtures::{obj, put, rid};
use super::Db;

fn open() -> Db {
    Db::open_in_memory(specs(), None).unwrap()
}

fn act(db: &Db, model: &str, action: &str, id: &str, args: serde_json::Value) -> i64 {
    db.act(model, action, &rid(id), obj(json!({})), obj(args)).unwrap().operation
}

fn blocked(db: &Db) -> Vec<String> {
    members(db, "people", &scope_key(db, "people", "blocked", json!({})))
}

#[test]
fn a_selection_effect_joins_the_owner_at_once_and_a_rollback_gives_back_only_its_own_membership() {
    let db = open();
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    put(&db, "people", json!({"id": "p2", "name": "b"}));
    let first = act(&db, "people", "block", "p1", json!({}));
    act(&db, "people", "block", "p2", json!({}));
    assert_eq!(blocked(&db), ["p2", "p1"]);
    db.roll_back(first).unwrap();
    assert_eq!(blocked(&db), ["p2"]);
}

#[test]
fn rolling_back_an_older_membership_change_hands_its_before_state_to_the_newer_one() {
    let db = open();
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    let block = act(&db, "people", "block", "p1", json!({}));
    let unblock = act(&db, "people", "unblock", "p1", json!({}));
    assert!(blocked(&db).is_empty());
    db.roll_back(block).unwrap();
    assert!(blocked(&db).is_empty());
    db.roll_back(unblock).unwrap();
    assert!(blocked(&db).is_empty());
}

#[test]
fn a_selection_effect_without_parameters_leaves_the_selection_for_every_parameter_value_and_comes_back_at_its_place() {
    let db = open();
    put(&db, "rooms", json!({"id": "r1", "title": "t"}));
    put(&db, "rooms", json!({"id": "r2", "title": "u"}));
    let (primary, secondary) = (scope_key(&db, "rooms", "open", json!({"folder": "PRIMARY"})), scope_key(&db, "rooms", "open", json!({"folder": "SECONDARY"})));
    join(&db, "rooms", &primary, "r1", "a");
    join(&db, "rooms", &primary, "r2", "b");
    join(&db, "rooms", &secondary, "r1", "c");
    let leave = act(&db, "rooms", "leave", "r1", json!({}));
    assert_eq!(members(&db, "rooms", &primary), ["r2"]);
    assert!(members(&db, "rooms", &secondary).is_empty());
    db.roll_back(leave).unwrap();
    assert_eq!(members(&db, "rooms", &primary), ["r1", "r2"]);
    assert_eq!(members(&db, "rooms", &secondary), ["r1"]);
}

#[test]
fn a_selection_effect_with_rows_and_bound_parameters_edits_the_membership_of_other_rows() {
    let db = open();
    put(&db, "rooms", json!({"id": "r1", "title": "t"}));
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    put(&db, "people", json!({"id": "p2", "name": "b"}));
    let roster = scope_key(&db, "people", "roster", json!({"roomId": "r1", "order": ROOM_ORDER}));
    let add = act(&db, "rooms", "add_members", "r1", json!({"personIds": ["p1", "p2", "stranger"]}));
    assert_eq!(members(&db, "people", &roster), ["p1", "p2"]);
    let remove = act(&db, "rooms", "remove_members", "r1", json!({"personIds": ["p1"]}));
    assert_eq!(members(&db, "people", &roster), ["p2"]);
    db.roll_back(remove).unwrap();
    assert_eq!(members(&db, "people", &roster), ["p1", "p2"]);
    db.roll_back(add).unwrap();
    assert!(members(&db, "people", &roster).is_empty());
}

#[test]
fn a_landing_keeps_the_rows_an_open_operation_joined_and_withholds_the_rows_it_left() {
    let db = open();
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    put(&db, "rooms", json!({"id": "r1", "title": "t"}));
    let primary = scope_key(&db, "rooms", "open", json!({"folder": "PRIMARY"}));
    join(&db, "rooms", &primary, "r1", "a");
    act(&db, "people", "block", "p1", json!({}));
    act(&db, "rooms", "leave", "r1", json!({}));
    let blocked_scope = crate::commit::ScopeRef::new(crate::identity::ModelKey::new("people"), scope_key(&db, "people", "blocked", json!({})));
    let open_scope = crate::commit::ScopeRef::new(crate::identity::ModelKey::new("rooms"), primary.clone());
    db.replace(&super::ReplaceRequest::new(blocked_scope, crate::schema::Coverage::Complete), vec![]).unwrap();
    db.replace(&super::ReplaceRequest::new(open_scope, crate::schema::Coverage::Complete), vec![obj(json!({"id": "r1", "title": "t"}))]).unwrap();
    assert_eq!(blocked(&db), ["p1"]);
    assert!(members(&db, "rooms", &primary).is_empty());
    assert_eq!(db.find("rooms", &rid("r1")).unwrap().unwrap().text("title"), Some("t"));
}

#[test]
fn a_landing_keeps_a_row_another_model_operation_added_to_the_selection() {
    let db = open();
    put(&db, "rooms", json!({"id": "r1", "title": "t"}));
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    let roster = scope_key(&db, "people", "roster", json!({"roomId": "r1", "order": ROOM_ORDER}));
    act(&db, "rooms", "add_members", "r1", json!({"personIds": ["p1"]}));
    let scope = crate::commit::ScopeRef::new(crate::identity::ModelKey::new("people"), roster.clone());
    db.replace(&super::ReplaceRequest::new(scope, crate::schema::Coverage::Complete), vec![]).unwrap();
    assert_eq!(members(&db, "people", &roster), ["p1"]);
}

#[test]
fn a_created_row_of_another_model_joins_the_lists_of_its_parameters_and_a_rollback_removes_only_it() {
    let db = open();
    put(&db, "notes", json!({"id": "n1", "roomId": "a", "body": "hello"}));
    let target = scope_key(&db, "notes", "thread", json!({"roomId": "b"}));
    let forward = act(&db, "notes", "forward", "n1", json!({"targetRoom": "b"}));
    let copies = members(&db, "notes", &target);
    assert_eq!(copies.len(), 1);
    assert!(rid(&copies[0]).is_temp());
    let copy = db.find("notes", &rid(&copies[0])).unwrap().unwrap();
    assert_eq!((copy.text("body"), copy.get("roomId")), (Some("hello"), Some(&json!("b"))));
    assert_eq!(db.find("notes", &rid("n1")).unwrap().unwrap().text("body"), Some("hello"));
    db.roll_back(forward).unwrap();
    assert!(members(&db, "notes", &target).is_empty());
    assert!(db.find("notes", &rid(&copies[0])).unwrap().is_none());
    assert!(db.find("notes", &rid("n1")).unwrap().is_some());
}

#[test]
fn a_change_effect_shifts_a_number_of_the_singleton_row_and_skips_an_unknown_price() {
    let db = open();
    put(&db, "people", json!({"id": "p1", "name": "a"}));
    put(&db, "wallets", json!({"id": "w1", "balance": 100}));
    join(&db, "wallets", &scope_key(&db, "wallets", "current", json!({})), "w1", "a");
    let balance = |db: &Db| db.find("wallets", &rid("w1")).unwrap().unwrap().get("balance").cloned();
    let gift = act(&db, "people", "send_gift", "p1", json!({"giftId": "g", "roomId": "a", "price": 30}));
    assert_eq!(balance(&db), Some(json!(70)));
    act(&db, "people", "send_gift", "p1", json!({"giftId": "g", "roomId": "a"}));
    assert_eq!(balance(&db), Some(json!(70)));
    db.roll_back(gift).unwrap();
    assert_eq!(balance(&db), Some(json!(100)));
}

#[test]
fn a_reopened_database_keeps_the_row_a_queued_operation_created_in_another_model() {
    let directory = std::env::temp_dir().join(format!("dblayer-effects-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("db.sqlite");
    let created = {
        let db = Db::open_with(Some(&path), specs(), None, super::DbConfig::default()).unwrap();
        put(&db, "notes", json!({"id": "n1", "roomId": "a", "body": "hello"}));
        act(&db, "notes", "forward", "n1", json!({"targetRoom": "b"}));
        let target = scope_key(&db, "notes", "thread", json!({"roomId": "b"}));
        members(&db, "notes", &target)
    };
    let db = Db::open_with(Some(&path), specs(), None, super::DbConfig::default()).unwrap();
    assert!(db.find("notes", &rid(&created[0])).unwrap().is_some());
    drop(db);
    std::fs::remove_dir_all(&directory).unwrap();
}

fn authored_notes() -> Db {
    let mut notes = super::spec_fixtures::notes_spec();
    notes.fields.push(crate::schema::FieldSpec::scalar("authorId", crate::value::ScalarKind::Id).optional());
    notes.fields.push(crate::schema::FieldSpec::scalar("regionId", crate::value::ScalarKind::Id).optional());
    let mut create = crate::schema::ActionDeclaration::new("create", crate::schema::RootShape::Upsert);
    create.mutation = Some("CreateNote".to_string());
    create.local = vec!["authorId".to_string()];
    create.local_from = std::collections::BTreeMap::from([("authorId".to_string(), crate::schema::EffectSource::Row { model: "wallets".to_string(), row: None, field: "id".to_string() })]);
    create.local.push("regionId".into());
    create.local_from.insert("regionId".into(), crate::schema::EffectSource::Row { model: "regions".into(), row: None, field: "id".into() });
    notes.actions.push(create);
    let mut regions = super::spec_fixtures::wallets_spec();
    regions.key = crate::identity::ModelKey::new("regions");
    Db::open_in_memory(vec![super::spec_fixtures::people_spec(), super::spec_fixtures::rooms_spec(), notes, super::spec_fixtures::wallets_spec(), regions], None).unwrap()
}

#[test]
fn inv_dsl_09_a_create_fills_a_local_attribute_from_the_singleton_row_and_skips_it_without_one() {
    let db = authored_notes();
    let author = |db: &Db, values: serde_json::Value| db.create("notes", obj(values)).unwrap().get("authorId").cloned().filter(|value| !value.is_null());
    assert_eq!(author(&db, json!({"roomId": "a"})), None);
    put(&db, "wallets", json!({"id": "w1", "balance": 1}));
    join(&db, "wallets", &scope_key(&db, "wallets", "current", json!({})), "w1", "a");
    assert_eq!(author(&db, json!({"roomId": "a"})), Some(json!("w1")));
    assert_eq!(author(&db, json!({"roomId": "a", "authorId": "w9"})), Some(json!("w9")));
    put(&db, "regions", json!({"id": "r1", "balance": 0}));
    join(&db, "regions", &scope_key(&db, "regions", "current", json!({})), "r1", "a");
    for (arguments, expected) in [(json!({"roomId":"a"}), json!(["w1","r1"])), (json!({"roomId":"a", "authorId":null, "regionId":null}), json!(["w1","r1"])), (json!({"roomId":"a", "authorId":"w9", "regionId":"r9"}), json!(["w9","r9"]))] {
        let row = db.create("notes", obj(arguments)).unwrap();
        assert_eq!(json!([row.get("authorId"), row.get("regionId")]), expected);
    }
}

fn reaction_patch() -> serde_json::Map<String, serde_json::Value> {
    obj(json!({
        "visitorIds": {"$flag": {"when": "reacted", "include": {"of": "wallets", "row": null, "field": "id"}}},
        "visitsCount": {"$flag": {"when": "reacted", "at_least": 1}},
    }))
}

fn reactions() -> Db {
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let posts = crate::schema::ModelSpec::new("posts", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::list("visitorIds", ScalarKind::Id).optional(), FieldSpec::scalar("visitsCount", ScalarKind::Int).optional()]);
    let db = Db::open_in_memory(vec![posts, super::spec_fixtures::wallets_spec()], None).unwrap();
    put(&db, "wallets", json!({"id": "me", "balance": 0}));
    join(&db, "wallets", &scope_key(&db, "wallets", "current", json!({})), "me", "a");
    db
}

#[test]
fn a_flagged_reaction_adds_or_removes_the_current_user_and_raises_the_count_only_when_set() {
    let db = reactions();
    put(&db, "posts", json!({"id": "p1", "visitorIds": ["u9"], "visitsCount": 5}));
    put(&db, "posts", json!({"id": "p2", "visitorIds": [], "visitsCount": 0}));
    let state = |id: &str| { let row = db.find("posts", &rid(id)).unwrap().unwrap(); (row.get("visitorIds").cloned().unwrap(), row.get("visitsCount").cloned().unwrap()) };
    let react = |id: &str, reacted: bool| db.act("posts", "react", &rid(id), reaction_patch(), obj(json!({"reacted": reacted}))).unwrap().operation;
    react("p1", true);
    react("p1", true);
    assert_eq!(state("p1"), (json!(["me", "u9"]), json!(5)));
    react("p2", true);
    assert_eq!(state("p2"), (json!(["me"]), json!(1)));
    let removal = react("p1", false);
    assert_eq!(state("p1"), (json!(["u9"]), json!(5)));
    db.roll_back(removal).unwrap();
    assert_eq!(state("p1"), (json!(["me", "u9"]), json!(5)));
}

#[test]
fn a_refused_flagged_reaction_gives_back_the_ids_and_the_count() {
    let db = reactions();
    put(&db, "posts", json!({"id": "p1", "visitorIds": ["u9"], "visitsCount": 0}));
    let operation = db.act("posts", "react", &rid("p1"), reaction_patch(), obj(json!({"reacted": true}))).unwrap().operation;
    assert_eq!(db.find("posts", &rid("p1")).unwrap().unwrap().get("visitsCount"), Some(&json!(1)));
    db.roll_back(operation).unwrap();
    let row = db.find("posts", &rid("p1")).unwrap().unwrap();
    assert_eq!((row.get("visitorIds"), row.get("visitsCount")), (Some(&json!(["u9"])), Some(&json!(0))));
}
