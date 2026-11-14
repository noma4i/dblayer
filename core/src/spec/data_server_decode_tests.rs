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

struct Items { server: Arc<Server>, answer: Mutex<Value> }

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

fn item_model() -> ModelSpec {
    let note = FieldSpec::variant("note", "Note", vec![FieldSpec::scalar("body", ScalarKind::Str), FieldSpec::scalar("tag", ScalarKind::Str).with_default(json!("t"))]);
    let fields = vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("name", ScalarKind::Str),
        FieldSpec::scalar("kind", ScalarKind::Str).with_default(json!("plain")),
        FieldSpec::scalar("count", ScalarKind::Int).nullable(),
        FieldSpec::union("attachment", "Attachment", vec![note]).nullable(),
    ];
    let mut model = ModelSpec::new("Item", fields);
    let mut all = RelationSpec::global("all");
    all.remote = Some(RemoteSpec { operation: "Items".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None });
    model.relations.push(all);
    model
}

/// Loads a page that holds `node` and gives the outcome of the load and the stored row.
async fn land(node: Value) -> (Result<(), DbError>, Option<WireRow>) {
    let mut models: Vec<ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models.push(item_model());
    resolve_types(&mut models);
    let server = Server::new();
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let http = Items { server: server.clone(), answer: Mutex::new(json!([node])) };
    let ports = OpenPorts { partial_operations: true, transport: Some(Arc::new(Wire(server))), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(Arc::new(MemoryCredentials::default())))), http: Some(Arc::new(http)), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let db = within(Database::open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), ports, TestClock::new(1_000_000))).await.unwrap();
    db.register_operations(OPERATIONS.into()).await.unwrap();
    db.register_operations(r#"[{"name":"Items","document":"query Items { items { id name } }","rootPath":"items"}]"#.into()).await.unwrap();
    db.set_online(true).await.unwrap();
    within(db.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
    let epoch = db.session_epoch();
    let load = db.begin_load(epoch, "Item".into(), Some("all".into()), "{}".into()).unwrap();
    let outcome = within(load.result()).await;
    let row = within(db.read_row(epoch, "Item".into(), "i1".into())).await.unwrap();
    within(db.close_database()).await.unwrap();
    (outcome, row)
}

/// Behaviour: a top-level scalar that does not read as its type refuses the row. Entry: page answer. Observed: the load fails and no row is stored. Catches: the field dropped and the row stored without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_top_level_scalar_that_does_not_decode_refuses_the_row() {
    for (case, count) in [("an object", json!({})), ("an array", json!([1]))] {
        let (outcome, row) = land(json!({"id": "i1", "name": "n", "count": count})).await;
        assert!(outcome.is_err(), "{case}: the window ended without an error");
        assert!(row.is_none(), "{case}: the row is stored");
    }
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "count": "12"})).await;
    assert!(outcome.is_ok() && row.is_some_and(|row| row.cells[2] == Cell::Int { value: 12 }), "a text that reads as the type is coerced");
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
    let entries = |row: WireRow| match &row.cells[3] {
        Cell::Keyed { entries } => entries.iter().map(|entry| (entry.key.clone(), entry.value.clone())).collect::<Vec<_>>(),
        other => panic!("{other:?}"),
    };
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "attachment": {"__typename": "Note", "body": "b"}})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    let stored = entries(row.unwrap());
    assert!(stored.contains(&("tag".into(), Cell::Text { value: "t".into() })), "{stored:?}");
    let (outcome, row) = land(json!({"id": "i1", "name": "n", "attachment": {"__typename": "Future", "size": 3}})).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(entries(row.unwrap()), [("__typename".to_string(), Cell::Text { value: "Future".into() })]);
}
