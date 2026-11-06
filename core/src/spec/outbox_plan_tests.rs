//! Owner-level tests of the `outbox` machine: the optimistic effects of an action, planned from a view of the base. Names are the `Test` column.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::owners::write::outbox_plan::{optimistic_row, Fields, receiver_tombstones, scope_changes, ListChange, PlanError, PlanView};
use crate::schema::keyed::{take, take_flags};
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, ScopeEffect, ScopeEffectKind};
use crate::value::ScalarKind;

type Row = Map<String, Value>;

fn object(value: Value) -> Row { value.as_object().cloned().expect("an object") }

#[derive(Default)]
struct Base {
    rows: BTreeMap<(String, String), Row>,
    /// `(model, list)` -> members `(id, order key)`.
    lists: BTreeMap<(String, String), Vec<(String, String)>>,
    dependents: BTreeMap<(String, String), Vec<(String, String)>>,
}

impl Base {
    fn row(mut self, model: &str, value: Value) -> Self {
        let row = object(value);
        self.rows.insert((model.into(), row["id"].as_str().expect("id").into()), row);
        self
    }

    fn member(mut self, list: &crate::identity::ListKey, id: &str, key: &str) -> Self {
        self.lists.entry(("users".into(), list.to_string())).or_default().push((id.into(), key.into()));
        self
    }
}

impl PlanView for Base {
    fn row(&self, model: &str, id: &str) -> Result<Option<Row>, PlanError> { Ok(self.rows.get(&(model.into(), id.into())).cloned()) }

    fn member_key(&self, model: &str, list: &str, id: &str) -> Result<Option<String>, PlanError> {
        Ok(self.lists.get(&(model.into(), list.into())).and_then(|members| members.iter().find(|(member, _)| member == id)).map(|(_, key)| key.clone()))
    }

    fn last_key(&self, model: &str, list: &str) -> Result<Option<String>, PlanError> {
        Ok(self.lists.get(&(model.into(), list.into())).and_then(|members| members.iter().map(|(_, key)| key.clone()).max()))
    }

    fn singleton(&self, _: &str) -> Result<Option<String>, PlanError> { Ok(None) }

    fn removed_with(&self, model: &str, id: &str) -> Result<Vec<(String, String)>, PlanError> {
        let mut removed = vec![(model.to_string(), id.to_string())];
        removed.extend(self.dependents.get(&(model.into(), id.into())).cloned().unwrap_or_default());
        Ok(removed)
    }
}

fn users() -> ModelSpec {
    let mut users = ModelSpec::new("users", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut members = RelationSpec::new("members", &[]);
    members.text_params = vec!["chatId".to_string(), "order".to_string()];
    members.remote = Some(crate::schema_fixtures::remote("ChatMembers", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    users.relations.push(members);
    users
}

fn effect(kind: ScopeEffectKind) -> ScopeEffect {
    ScopeEffect { kind, model: "users".into(), scope: "members".into(), rows: "userIds".into(), with: [("chatId".to_string(), "id".to_string())].into_iter().collect(), order_param: "order".into(), orders: vec!["A".into(), "B".into()] }
}

fn list(order: &str) -> crate::identity::ListKey { users().selection_key("members", &object(json!({"chatId": "c1", "order": order}))).unwrap() }

fn order_keys() -> (String, String) {
    let first = crate::value::fractional_between(None, None).unwrap();
    let second = crate::value::fractional_between(Some(&first), None).unwrap();
    (first, second)
}

fn seeded() -> Base {
    let (first, second) = order_keys();
    let mut base = Base::default();
    for user in ["u1", "u2", "u3"] {
        base = base.row("users", json!({"id": user, "name": user}));
    }
    for order in ["A", "B"] {
        base = base.member(&list(order), "u1", &first).member(&list(order), "u2", &second);
    }
    base
}

fn changes(base: &Base, kind: ScopeEffectKind, ids: Value) -> Vec<ListChange> {
    scope_changes(base, &users(), &effect(kind), "c1", &object(json!({"userIds": ids}))).unwrap()
}

#[test]
fn out_submit_removes_receiver() {
    let mut base = seeded().row("chats", json!({"id": "c1"})).row("messages", json!({"id": "m1"}));
    base.dependents.insert(("chats".into(), "c1".into()), vec![("messages".into(), "m1".into())]);
    assert_eq!(receiver_tombstones(&base, "chats", "c1").unwrap(), [("chats".to_string(), "c1".to_string()), ("messages".to_string(), "m1".to_string())]);
    assert_eq!(receiver_tombstones(&base, "users", "u3").unwrap(), [("users".to_string(), "u3".to_string())], "a row nothing depends on is removed alone");
}

#[test]
fn out_removes_receiver_every_window() {
    let base = seeded();
    let leaving = changes(&base, ScopeEffectKind::Leave, json!(["u1"]));
    let windows: Vec<_> = leaving.iter().map(|change| match change { ListChange::Leave { list, id, before, .. } => (list.to_string(), id.clone(), before.clone()), other => panic!("{other:?}") }).collect();
    assert_eq!(windows, [(list("A").to_string(), "u1".to_string(), Some(order_keys().0)), (list("B").to_string(), "u1".to_string(), Some(order_keys().0))], "a Leave for each window the row is a member of, with the place to give back");
}

#[test]
fn out_submit_leaves_scope() {
    let base = seeded();
    let leaving = changes(&base, ScopeEffectKind::Leave, json!(["u2", "u3"]));
    let seen: Vec<_> = leaving.iter().map(|change| match change { ListChange::Leave { id, before, .. } => (id.as_str(), before.clone()), other => panic!("{other:?}") }).collect();
    assert_eq!(seen, [("u2", Some(order_keys().1)), ("u3", None), ("u2", Some(order_keys().1)), ("u3", None)], "the member keeps its place for the rollback, a row that is no member has none");
}

#[test]
fn out_submit_join_missing_row() {
    let base = seeded();
    let joining = changes(&base, ScopeEffectKind::Join, json!(["u7", "u3", "u3", "u1"]));
    let joined: Vec<_> = joining.iter().map(|change| match change { ListChange::Join { id, order_key, before, .. } => (id.as_str(), order_key.is_some(), before.is_some()), other => panic!("{other:?}") }).collect();
    assert_eq!(joined, [("u7", false, false), ("u3", true, false), ("u1", false, true), ("u7", false, false), ("u3", true, false), ("u1", false, true)], "no row, no join and no error · one join per row · a member stays");
    let keys: Vec<_> = joining.iter().filter_map(|change| match change { ListChange::Join { order_key: Some(key), list: found, .. } if *found == list("A") => Some(key.clone()), _ => None }).collect();
    assert!(keys.iter().all(|key| key.as_str() > order_keys().1.as_str()), "a joining row goes behind the last member: {keys:?}");
}

fn reacting() -> (ModelSpec, Base) {
    let messages = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("count", ScalarKind::Int).nullable().optional(), FieldSpec::array("reactions").optional(), FieldSpec::array("recent").optional(), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    (messages, seeded().row("messages", json!({"id": "m1", "count": 1, "recent": ["u9"], "reactions": [{"userId": "u2", "body": "x"}], "text": "one"})))
}

fn plan(spec: &ModelSpec, base: &Base, current: &Row, mut patch: Row, args: Value) -> Row {
    let args = object(args);
    crate::schema::keyed::resolve_maps(&mut patch, &args);
    let keyed = take(&mut patch);
    let flagged = take_flags(&mut patch);
    optimistic_row(spec, base, (current, &patch), Fields { keyed: &keyed, flagged: &flagged }, &args, ("messages", "m1")).unwrap()
}

fn react(_: &str, _: &str) -> Row { object(json!({"reactions": {"$keyed": {"by": "userId", "element": {"userId": {"$arg": "user"}, "body": {"$arg": "body"}}}}})) }

#[test]
fn out_optimistic_keyed_element() {
    let (spec, base) = reacting();
    let current = base.rows[&("messages".into(), "m1".into())].clone();
    let step = |current: &Row, user: &str, body: &str| plan(&spec, &base, current, react(user, body), json!({"user": user, "body": body}));
    let added = step(&current, "u1", "a");
    assert_eq!(added["reactions"], json!([{"userId": "u2", "body": "x"}, {"userId": "u1", "body": "a"}]), "a missing key is appended, the other element stays");
    let replaced = step(&added, "u1", "b");
    assert_eq!(replaced["reactions"], json!([{"userId": "u2", "body": "x"}, {"userId": "u1", "body": "b"}]));
    let removed = step(&replaced, "u1", "");
    assert_eq!(removed["reactions"], json!([{"userId": "u2", "body": "x"}]), "all fields but the key blank: the element goes");
}

#[test]
fn out_optimistic_keyed_independent() {
    let (spec, base) = reacting();
    let current = base.rows[&("messages".into(), "m1".into())].clone();
    let first = plan(&spec, &base, &current, react("u1", "a"), json!({"user": "u1", "body": "a"}));
    let second = plan(&spec, &base, &first, react("u3", "c"), json!({"user": "u3", "body": "c"}));
    assert_eq!(second["reactions"], json!([{"userId": "u2", "body": "x"}, {"userId": "u1", "body": "a"}, {"userId": "u3", "body": "c"}]), "both operations show");
    let first_taken_back = plan(&spec, &base, &second, react("u1", ""), json!({"user": "u1", "body": ""}));
    assert_eq!(first_taken_back["reactions"], json!([{"userId": "u2", "body": "x"}, {"userId": "u3", "body": "c"}]), "taking back one element leaves the element of the other operation");
}

#[test]
fn out_optimistic_map() {
    let (spec, base) = reacting();
    let current = base.rows[&("messages".into(), "m1".into())].clone();
    let patch = || object(json!({"text": {"$map": {"from": "decision", "map": {"ACCEPT": "yes", "REJECT": "no"}}}}));
    assert_eq!(plan(&spec, &base, &current, patch(), json!({"decision": "REJECT"}))["text"], "no");
    assert_eq!(plan(&spec, &base, &current, patch(), json!({"decision": "ACCEPT"}))["text"], "yes");
    assert_eq!(plan(&spec, &base, &current, patch(), json!({"decision": "OTHER"}))["text"], "one", "a value outside the map leaves the field");
}

#[test]
fn out_optimistic_include_at_least() {
    let (spec, base) = reacting();
    let current = base.rows[&("messages".into(), "m1".into())].clone();
    let patch = || object(json!({"recent": {"$flag": {"include": {"of": "users", "row": "who", "field": "id"}, "when": "on"}}, "count": {"$flag": {"at_least": 3, "when": "on"}}}));
    let on = plan(&spec, &base, &current, patch(), json!({"on": true, "who": "u1"}));
    assert_eq!((on["recent"].clone(), on["count"].clone()), (json!(["u1", "u9"]), json!(3)));
    let again = plan(&spec, &base, &on, patch(), json!({"on": true, "who": "u1"}));
    assert_eq!(again["recent"], json!(["u1", "u9"]), "the id is not repeated");
    let off = plan(&spec, &base, &on, patch(), json!({"on": false, "who": "u1"}));
    assert_eq!((off["recent"].clone(), off["count"].clone()), (json!(["u9"]), json!(3)), "false takes the id out and keeps the number");
    let missing = plan(&spec, &base, &current, patch(), json!({"on": true, "who": "u8"}));
    assert_eq!(missing["recent"], json!(["u9"]), "a row that is not in the base is not included");
}

#[test]
fn inv_merge_keyed_nested_path() {
    let messages = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::array("reactions").optional()]);
    let base = seeded().row("messages", json!({"id": "m1", "reactions": [{"body": "bare"}, {"user": {"id": "u2"}, "body": "x"}]}));
    let current = base.rows[&("messages".into(), "m1".into())].clone();
    let patch = || object(json!({"reactions": {"$keyed": {"by": "user.id", "element": {"user": {"$arg": "user"}, "body": {"$arg": "body"}}}}}));
    let step = |current: &Row, user: &str, body: &str| plan(&messages, &base, current, patch(), json!({"user": {"id": user}, "body": body}));
    let added = step(&current, "u1", "a");
    assert_eq!(added["reactions"], json!([{"body": "bare"}, {"user": {"id": "u2"}, "body": "x"}, {"user": {"id": "u1"}, "body": "a"}]), "an element without the path is a different element, a missing key is appended");
    let replaced = step(&added, "u2", "y");
    assert_eq!(replaced["reactions"], json!([{"body": "bare"}, {"user": {"id": "u2"}, "body": "y"}, {"user": {"id": "u1"}, "body": "a"}]), "the element is matched by the nested field");
    let removed = step(&replaced, "u2", "");
    assert_eq!(removed["reactions"], json!([{"body": "bare"}, {"user": {"id": "u1"}, "body": "a"}]), "every field but the key blank: the element goes");
}
