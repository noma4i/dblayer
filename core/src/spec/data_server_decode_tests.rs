//! Cells of `data.md` P-DATA-63, P-DATA-64, P-DATA-66 through the public API of `Database`: a page answer whose row does not decode lands nothing.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::harness_life::Held;
use super::rig::{moment_models, within, Server, Wire, OPERATIONS};
use crate::ffi::{Cell, Config, Database, DbError, EncryptedReply, FakeHttp, OpenPorts, TestClock, WireRow};
use crate::schema::{resolve_types, Coverage, FieldSpec, ModelSpec, OnFailure, RelationSpec, RemoteShape, RemoteSpec};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response, MemoryCredentials};
use crate::transport::{EncryptedOperationClass, NetworkPurpose};
use crate::value::ScalarKind;

struct Items { server: Arc<Server>, answer: Arc<Mutex<Value>> }

#[async_trait]
impl FakeHttp for Items {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        if request["body"]["operationName"] != "Items" {
            return Wire(self.server.clone()).post(path, body, purpose).await;
        }
        let answer = json!({"data": {"items": self.answer.lock().unwrap().clone()}});
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

fn listed(mut model: ModelSpec) -> ModelSpec {
    let mut all = RelationSpec::global("all");
    all.remote = Some(RemoteSpec { operation: "Items".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None });
    model.relations.push(all);
    model
}

fn item_model() -> ModelSpec {
    let note = FieldSpec::variant("note", "Note", vec![FieldSpec::scalar("body", ScalarKind::Str), FieldSpec::scalar("tag", ScalarKind::Str).with_default(json!("t"))]);
    let fields = vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("name", ScalarKind::Str),
        FieldSpec::scalar("kind", ScalarKind::Str).with_default(json!("plain")),
        FieldSpec::scalar("count", ScalarKind::Int).nullable(),
        FieldSpec::union("attachment", "Attachment", vec![note]).nullable(),
    ];
    listed(ModelSpec::new("Item", fields))
}

/// Loads a page that holds `node` and gives the outcome of the load and the stored row.
async fn land_in(model: ModelSpec, node: Value) -> (Result<(), DbError>, Option<WireRow>) { land_twice(model, node, None).await.0 }

/// `land_in` with a good node after the first one: the outcome and row of the first, then of the second, and the revision before and after the first.
async fn land_twice(model: ModelSpec, node: Value, next: Option<Value>) -> ((Result<(), DbError>, Option<WireRow>), Option<(Result<(), DbError>, Option<WireRow>)>, (i64, i64)) {
    let mut models: Vec<ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    let name = model.key.to_string();
    models.push(model);
    resolve_types(&mut models);
    let server = Server::new();
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let answer = Arc::new(Mutex::new(json!([node])));
    let http = Items { server: server.clone(), answer: answer.clone() };
    let ports = OpenPorts { partial_operations: true, transport: Some(Arc::new(Wire(server))), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(Arc::new(MemoryCredentials::default())))), http: Some(Arc::new(http)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let db = within(Database::open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), ports, TestClock::new(1_000_000))).await.unwrap();
    db.register_operations(OPERATIONS.into()).await.unwrap();
    db.register_operations(r#"[{"name":"Items","document":"query Items { items { id name } }","rootPath":"items"}]"#.into()).await.unwrap();
    db.set_online(true).await.unwrap();
    within(db.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
    let epoch = db.session_epoch();
    let before = db.store_revision().unwrap();
    let load = db.begin_load(epoch, name.clone(), Some("all".into()), "{}".into()).unwrap();
    let outcome = within(load.result()).await;
    let row = within(db.read_row(epoch, name.clone(), "i1".into())).await.unwrap();
    let after = db.store_revision().unwrap();
    let then = match next {
        Some(next) => {
            *answer.lock().unwrap() = json!([next]);
            let load = db.begin_load(epoch, name.clone(), Some("all".into()), "{}".into()).unwrap();
            let outcome = within(load.result()).await;
            Some((outcome, within(db.read_row(epoch, name, "i1".into())).await.unwrap()))
        }
        None => None,
    };
    within(db.close_database()).await.unwrap();
    ((outcome, row), then, (before, after))
}

async fn land(node: Value) -> (Result<(), DbError>, Option<WireRow>) { land_in(item_model(), node).await }

/// P-DATA-64: a top-level scalar that does not read as its type refuses the row. Entry: page answer. Observed: the load fails and no row is stored. Catches: the field dropped and the row stored without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_nested_decode_error_lands_nothing() {
    let good = || json!({"id": "i1", "name": "n", "count": 5});
    for (case, count) in [("an object", json!({})), ("an array", json!([1]))] {
        let ((outcome, row), then, (before, after)) = land_twice(item_model(), json!({"id": "i1", "name": "n", "count": count}), Some(good())).await;
        assert!(outcome.is_err(), "{case}: the window ended without an error");
        assert!(row.is_none(), "{case}: the row is stored");
        assert_eq!(after, before, "{case}: the revision moved");
        let (outcome, row) = then.unwrap();
        assert!(outcome.is_ok() && row.is_some_and(|row| row.cells[2] == Cell::Int { value: 5 }), "{case}: the next good page does not land");
    }
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "count": "12"})).await;
    assert!(outcome.is_ok() && row.is_some_and(|row| row.cells[2] == Cell::Int { value: 12 }), "a text that reads as the type is coerced");
    let (outcome, row) = land(json!({"id": true, "name": "n"})).await;
    assert!(outcome.is_err() && row.is_none(), "an id of another type lands");
    let (outcome, row) = land(json!({"name": "n"})).await;
    assert!(outcome.is_ok() && row.is_none(), "a node without an id is a row or a failure");
}

/// Behaviour: null in a required top-level field. Entry: page answer. Observed: refused without a default, the default stored with one. Catches: null written into a required field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_null_in_a_required_field_is_refused_or_takes_the_default() {
    let (outcome, row) = land(json!({"id": "i1", "name": null})).await;
    assert!(outcome.is_err() && row.is_none(), "null in a required field without a default lands");
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "kind": null})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(row.unwrap().cells[1], Cell::Text { value: "plain".into() });
}

/// Behaviour: a union value decodes its variant members. Entry: page answer. Observed: refused for a wrong or missing required member, default stored, unknown typename keeps only itself. Catches: members of a variant stored undecoded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_union_members_decode_by_variant() {
    for (case, attachment) in [("a member of another type", json!({"__typename": "Note", "body": 5})), ("a missing required member", json!({"__typename": "Note"}))] {
        let (outcome, row) = land(json!({"id": "i1", "name": "n", "attachment": attachment})).await;
        assert!(outcome.is_err() && row.is_none(), "{case}: the row landed");
    }
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "attachment": {"__typename": "Note", "body": "b"}})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    let text = |value: &str| Cell::Text { value: value.into() };
    assert_eq!(row.unwrap().cells[3], Cell::Seq { items: vec![text("Note"), Cell::Seq { items: vec![text("b"), text("t")] }] });
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "attachment": {"__typename": "Future", "size": 3}})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(row.unwrap().cells[3], Cell::Seq { items: vec![text("Future"), Cell::Null] });
}

fn typed(mut field: FieldSpec, name: &str, members: Vec<FieldSpec>) -> FieldSpec {
    field.app_type = Some(name.into());
    field.fields = members;
    field
}

fn reading_model() -> ModelSpec {
    let inner = typed(FieldSpec::object("inner"), "Inner", vec![FieldSpec::scalar("v", ScalarKind::Str)]);
    let members = vec![
        FieldSpec::scalar("name", ScalarKind::Str).nullable(),
        FieldSpec::scalar("tier", ScalarKind::Int).nullable().with_default(json!(1)),
        FieldSpec::scalar("rank", ScalarKind::Int).with_default(json!(2)),
        FieldSpec::scalar("code", ScalarKind::Str),
        FieldSpec::scalar("ref", ScalarKind::Id).nullable(),
        FieldSpec::scalar("flag", ScalarKind::Bool).nullable(),
        FieldSpec::scalar("ratio", ScalarKind::Num).nullable(),
        FieldSpec::scalar("when", ScalarKind::Date).nullable(),
        FieldSpec::enumeration("mood", &["calm", "loud"]).nullable(),
        inner,
    ];
    let frame = typed(FieldSpec::object("frame"), "Frame", vec![FieldSpec::scalar("v", ScalarKind::Str)]);
    listed(ModelSpec::new("Item", vec![FieldSpec::scalar("id", ScalarKind::Id), typed(FieldSpec::object("box"), "Box", members).nullable(), frame]))
}

fn boxed(members: Value) -> Value {
    let mut full = json!({"code": "c", "inner": {"v": "x"}});
    full.as_object_mut().unwrap().extend(members.as_object().unwrap().clone());
    json!({"id": "i1", "box": full, "frame": {"v": "f"}})
}

/// P-DATA-63: a nested value decodes by one table at any depth: the stored member or an error of the row. Entry: page answer. Observed: the member cell, or a failed load with no row. Catches: a nested member dropped, defaulted wrongly or coerced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_nested_decode_table() {
    let text = |value: &str| Cell::Text { value: value.into() };
    let ok: &[(&str, Value, (usize, Cell))] = &[
        ("an unknown key is ignored", json!({"extra": 1}), (3, text("c"))),
        ("an absent nullable is null", json!({}), (0, Cell::Null)),
        ("an absent field with a default is the default", json!({}), (2, Cell::Int { value: 2 })),
        ("an absent nullable with a default is the default", json!({}), (1, Cell::Int { value: 1 })),
        ("a null nullable is null at any default", json!({"tier": null}), (1, Cell::Null)),
        ("a null required with a default is the default", json!({"rank": null}), (2, Cell::Int { value: 2 })),
        ("an id of text", json!({"ref": "a1"}), (4, text("a1"))),
        ("an id of an integer is text", json!({"ref": 42}), (4, text("42"))),
        ("a bool", json!({"flag": true}), (5, Cell::Bool { value: true })),
        ("a number with a fraction", json!({"ratio": 1.5}), (6, Cell::Real { value: 1.5 })),
        ("a date is text", json!({"when": "2026-11-07T10:00:00Z"}), (7, text("2026-11-07T10:00:00Z"))),
        ("a declared enum value", json!({"mood": "loud"}), (8, text("loud"))),
    ];
    for (case, members, (at, want)) in ok {
        let (outcome, row) = land_in(reading_model(), boxed(members.clone())).await;
        assert!(outcome.is_ok(), "{case}: {outcome:?}");
        let Some(Cell::Seq { items }) = row.map(|row| row.cells[0].clone()) else { panic!("{case}: no box") };
        assert_eq!(items[*at], *want, "{case}");
    }
    let bad: &[(&str, Value)] = &[
        ("a required without a default is absent", json!({"id": "i1", "box": {"inner": {"v": "x"}}, "frame": {"v": "f"}})),
        ("a required without a default is null", boxed(json!({"code": null}))),
        ("a wrong type", boxed(json!({"tier": "one"}))),
        ("an id that is a bool", boxed(json!({"ref": true}))),
        ("an id that is a fraction", boxed(json!({"ref": 1.5}))),
        ("an id that is an object", boxed(json!({"ref": {"a": 1}}))),
        ("an id outside i64", boxed(json!({"ref": 18446744073709551615u64}))),
        ("a bool that is text", boxed(json!({"flag": "true"}))),
        ("a required nested object is absent", json!({"id": "i1", "box": {"code": "c"}, "frame": {"v": "f"}})),
        ("a required nested object is null", boxed(json!({"inner": null}))),
        ("a required object of the row is null", json!({"id": "i1", "frame": null})),
    ];
    for (case, node) in bad {
        let (outcome, row) = land_in(reading_model(), node.clone()).await;
        assert!(outcome.is_err() && row.is_none(), "{case}: the row landed");
    }
}

/// P-DATA-63: a list with an element of another type refuses the row like a scalar of another type, the list is not stored shortened. Entry: page answer. Observed: failed load and no row, a list element of text that reads as the type is coerced. Catches: a shortened list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_list_element_of_another_type_refuses_row() {
    let blocks = FieldSpec::union_array("blocks", "Block", vec![FieldSpec::variant("text", "Text", vec![FieldSpec::scalar("body", ScalarKind::Str)])]);
    let model = || listed(ModelSpec::new("Item", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::list("tags", ScalarKind::Int), blocks.clone(), FieldSpec::list("dates", ScalarKind::Date).optional(), typed(FieldSpec::object("box"), "Box", vec![FieldSpec::list("nums", ScalarKind::Int)]).optional()]));
    for (case, node) in [
        ("a list element of another type", json!({"id": "i1", "tags": [1, "x", 3], "blocks": []})),
        ("a nested list element that is text", json!({"id": "i1", "tags": [], "blocks": [], "box": {"nums": [1, "2"]}})),
        ("a union element without a typename", json!({"id": "i1", "tags": [], "blocks": [{"__typename": "Text", "body": "a"}, {"body": "b"}]})),
    ] {
        let (outcome, row) = land_in(model(), node).await;
        assert!(outcome.is_err() && row.is_none(), "{case}: the row landed");
    }
    let (outcome, row) = land_in(model(), json!({"id": "i1", "tags": [1, "2", 3], "blocks": [{"__typename": "Text", "body": "a"}], "dates": ["2026-10-14T10:00:00+10:00", 1700000000000i64], "box": {"nums": [1, 2]}})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    let cells = row.unwrap().cells;
    assert_eq!(cells[0], Cell::Seq { items: vec![Cell::Int { value: 1 }, Cell::Int { value: 2 }, Cell::Int { value: 3 }] });
    let text = |value: &str| Cell::Text { value: value.into() };
    assert_eq!(cells[2], Cell::Seq { items: vec![text("2026-10-14T00:00:00.000Z"), text("2023-11-14T22:13:20.000Z")] }, "a date list of the row is canonical");
}
