use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::rig::{moment_models, within, Server, Wire, OPERATIONS};
use super::harness_life::Held;
use crate::ffi::{Config, Database, DbError, FakeHttp, EncryptedReply, OpenPorts, TestClock};
use crate::schema::{ActionLanding, Coverage, FieldSpec, ModelSpec, OnFailure, RelationSpec, RemoteShape, RemoteSpec};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response, MemoryCredentials};
use crate::transport::{EncryptedOperationClass, NetworkPurpose};
use crate::value::ScalarKind;

struct LandingHttp(Arc<Server>);

const MAKE_MOMENT: &str = "mutation MakeMoment { makeMoment { id text extras { id text } } }";

#[async_trait]
impl FakeHttp for LandingHttp {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        if request["body"]["operationName"] == "Annotations" {
            return Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), json!({"data":{"annotations":[]}})) });
        }
        if request["body"]["operationName"] != "MakeMoment" {
            return Wire(self.0.clone()).post(path, body, purpose).await;
        }
        let hash: String = Sha256::digest(MAKE_MOMENT.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(request["body"]["extensions"]["persistedQuery"]["sha256Hash"], hash);
        if let Some(query) = request["body"]["query"].as_str() { assert_eq!(query, MAKE_MOMENT); }
        let answer = json!({"data": {"makeMoment": {"id": "m1", "text": "root", "extras": [{"id": "a1", "text": "first"}, {"id": "a2", "text": "second"}]}}});
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_upsert_lands() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lands.sqlite").to_str().unwrap().to_string();
    let mut models: Vec<ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    let action = models[0].actions.iter_mut().find(|action| action.name == "create").unwrap();
    action.lands.push(ActionLanding { model: "Annotation".into(), root: "makeMoment.extras".into() });
    action.lands.push(ActionLanding { model: "Absent".into(), root: "missing".into() });
    for model in ["Annotation", "Absent"] {
        models.push(ModelSpec::new(model, vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]));
    }
    let mut list = RelationSpec::global("all");
    list.joins_events = true;
    list.remote = Some(RemoteSpec { operation: "Annotations".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None });
    models[1].relations.push(list);
    let server = Server::new();
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let ports = OpenPorts { partial_operations: true, transport: Some(Arc::new(Wire(server.clone()))), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(Arc::new(MemoryCredentials::default())))), http: Some(Arc::new(LandingHttp(server))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let db = within(Database::open_with_clock(Config { path: Some(path), data_version: None }, crate::ffi::specs(serde_json::to_string(&models).unwrap()), ports, TestClock::new(1_000_000))).await.unwrap();
    let mut operations: serde_json::Value = serde_json::from_str(OPERATIONS).unwrap();
    operations.as_array_mut().unwrap().iter_mut().find(|operation| operation["name"] == "MakeMoment").unwrap()["document"] = json!(MAKE_MOMENT);
    db.register_operations(operations.to_string()).await.unwrap();
    db.register_operations(r#"[{"name":"Annotations","document":"query Annotations { annotations { id text } }","rootPath":"annotations"}]"#.into()).await.unwrap();
    db.set_online(true).await.unwrap();
    within(db.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
    let epoch = db.session_epoch();
    let load = db.begin_load(epoch, "Annotation".into(), Some("all".into()), "{}".into()).unwrap();
    within(load.result()).await.unwrap();
    let receipt = within(db.send_create("Moment", &json!({"text": "root"}).to_string())).await;
    assert!(within(db.confirmed(receipt, false)).await.is_ok());
    let root = within(db.read_row(epoch, "Moment".into(), "m1".into())).await.unwrap().unwrap();
    for (id, text) in [("a1", "first"), ("a2", "second")] {
        let row = within(db.read_row(epoch, "Annotation".into(), id.into())).await.unwrap().unwrap();
        assert_eq!(row.revision, root.revision);
        assert_eq!(row.cells, vec![crate::ffi::Cell::Text { value: text.into() }]);
        assert!(within(db.read_row(epoch, "Absent".into(), id.into())).await.unwrap().is_none());
    }
    let query = json!({"model":"Annotation", "relation":"all", "params":{}}).to_string();
    let ids: Vec<_> = within(db.read_query(epoch, crate::ffi::QuerySpec::json(query))).await.unwrap().into_iter().map(|row| row.id).collect();
    assert_eq!(ids, ["a2", "a1"]);
    within(db.close_database()).await.unwrap();
}
