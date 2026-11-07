use std::sync::Arc;
use async_trait::async_trait;
use super::harness_life::Held;
use super::rig::{moment_models, within, Kind, Rig, Server, Wire, OPERATIONS};
use crate::ffi::{Config, Database, DbError, EncryptedHttp, EncryptedReply, OpenPorts, TestClock, WireOperationOutcome};
use crate::schema::{Coverage, Membership, OnFailure, RelationSpec, RemoteShape, RemoteSpec, RowRule, RowTest};
use crate::transport::encrypted::fake_server_tests::{decrypt_request, seal_response, MemoryCredentials};
use crate::transport::{CredentialSlot, EncryptedOperationClass, NetworkPurpose};
use serde_json::{json, Value};

struct WindowHttp(Arc<Server>);

struct ThresholdHttp(Arc<Server>);

#[async_trait]
impl EncryptedHttp for ThresholdHttp {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        let name = request["body"]["operationName"].as_str().unwrap_or_default();
        if name != "MakeMoment" && name != "UpdateMoment" {
            return WindowHttp(self.0.clone()).post(path, body, purpose).await;
        }
        let text = request["body"]["variables"]["text"].as_str().unwrap();
        let score: i64 = text.parse().unwrap();
        let row = json!({"id":"m1", "text":text, "score":score});
        let answer = if name == "MakeMoment" { json!({"data":{"makeMoment":row}}) } else { json!({"data":{"updateMoment":row}}) };
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), answer) })
    }
}

#[async_trait]
impl EncryptedHttp for WindowHttp {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        if request["body"]["operationName"] == "Profile" {
            return Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), json!({"data":{"profile":{"id":"u1"}}})) });
        }
        if request["body"]["operationName"] != "Feed" {
            return Wire(self.0.clone()).post(path, body, purpose).await;
        }
        Ok(EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), json!({"data":{"moments":[]}})) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inv_dsl_09_a_create_fills_a_local_attribute_from_the_singleton_row_and_skips_it_without_one() {
    use crate::schema::{ActionDeclaration, EffectSource, FieldSpec, MergeRule, ModelSpec};
    use crate::value::ScalarKind;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("singleton.sqlite").to_str().unwrap().to_string();
    let mut models: Vec<ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    let mut owner = FieldSpec::scalar("ownerId", ScalarKind::Id).nullable();
    owner.merge = Some(MergeRule::Local);
    models[0].fields.push(owner);
    let create: &mut ActionDeclaration = models[0].actions.iter_mut().find(|action| action.name == "create").unwrap();
    create.local_from.insert("ownerId".into(), EffectSource::Row { model: "User".into(), row: None, field: "id".into() });
    let mut user = ModelSpec::new("User", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut profile = RelationSpec::global("profile");
    profile.remote = Some(RemoteSpec { operation: "Profile".into(), shape: RemoteShape::Single, stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None });
    user.relations.push(profile);
    models.push(user);
    let server = Server::new();
    let rig = open_models(serde_json::to_string(&models).unwrap(), path, server.clone(), Arc::default()).await;
    rig.db.register_operations(r#"[{"name":"Profile","document":"query Profile { profile { id } }","rootPath":"profile"}]"#.into()).await.unwrap();
    server.hold(Kind::Mutate, true);
    let without = rig.create("before").await;
    assert_eq!(without.optimistic_value.unwrap().cells[1], crate::ffi::Cell::Null);
    let request = rig.db.begin_load(rig.epoch(), "User".into(), "profile".into(), "{}".into()).unwrap();
    within(request.result()).await.unwrap();
    let query = json!({"model":"User", "relation":"profile", "params":{}}).to_string();
    let rows = within(rig.db.read_query(rig.epoch(), query)).await.unwrap();
    assert_eq!(rows.into_iter().map(|row| row.id).collect::<Vec<_>>(), ["u1"]);
    let with = rig.create("after").await;
    assert_eq!(with.optimistic_value.unwrap().cells[1], crate::ffi::Cell::Text { value: "u1".into() });
}

fn relation(name: &str, created: bool, response: bool) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    relation.joins_created = created;
    relation.joins_events = response;
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false,
        page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    relation
}

async fn open(relations: Vec<RelationSpec>, server: std::sync::Arc<Server>, path: String) -> Rig {
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].relations.extend(relations);
    let rig = open_models(serde_json::to_string(&models).unwrap(), path, server, Arc::default()).await;
    rig.db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).await.unwrap();
    rig
}

async fn open_models(models: String, path: String, server: Arc<Server>, credentials: Arc<MemoryCredentials>) -> Rig {
    let http: Arc<dyn EncryptedHttp> = Arc::new(WindowHttp(server.clone()));
    open_models_with_http(models, path, server, credentials, http).await
}

async fn open_models_with_http(models: String, path: String, server: Arc<Server>, credentials: Arc<MemoryCredentials>, http: Arc<dyn EncryptedHttp>) -> Rig {
    let signed = credentials.get().unwrap().is_some();
    let clock = TestClock::new(1_000_000);
    let mut encrypted = crate::transport::encrypted::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let ports = OpenPorts { transport: Some(Arc::new(Wire(server.clone()))), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(credentials))), http: Some(http) };
    let db = within(Database::open_with_clock(Config { path: Some(path), data_version: None }, models, ports, clock.clone())).await.unwrap();
    db.register_operations(OPERATIONS.into()).await.unwrap();
    db.set_online(true).unwrap();
    if !signed { within(db.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap(); }
    Rig { db, server, clock }
}

async fn load(rig: &Rig, name: &str) {
    load_params(rig, name, json!({})).await;
}

async fn load_params(rig: &Rig, name: &str, params: serde_json::Value) {
    let request = rig.db.begin_load(rig.epoch(), "Moment".into(), name.into(), params.to_string()).unwrap();
    within(request.result()).await.unwrap();
}

async fn ids(rig: &Rig, name: &str) -> Vec<String> {
    ids_params(rig, name, json!({})).await
}

async fn ids_params(rig: &Rig, name: &str, params: serde_json::Value) -> Vec<String> {
    let query = json!({"model":"Moment", "relation":name, "params":params}).to_string();
    within(rig.db.read_query(rig.epoch(), query)).await.unwrap().into_iter().map(|row| row.id).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_insert_on_response() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("response.sqlite").to_str().unwrap().to_string();
    let server = Server::new();
    let mut selected = relation("selected", false, true);
    selected.row_rules.push(RowRule { field: "text".into(), test: RowTest::Equals { value: json!("second") } });
    let mut allowed = relation("allowed", false, true);
    allowed.row_rules.push(RowRule { field: "text".into(), test: RowTest::In { values: vec![json!("second"), json!("third")] } });
    let mut bound = relation("bound", false, true);
    bound.text_params.push("filter".into());
    bound.membership.insert("filter".into(), Membership::Equals("text".into()));
    let mut unbound = relation("unbound", false, true);
    unbound.text_params.push("filter".into());
    let rig = open(vec![relation("response", false, true), relation("other", false, false), relation("unloaded", false, true), selected, allowed, bound, unbound], server.clone(), path).await;
    load(&rig, "response").await;
    load(&rig, "other").await;
    load(&rig, "selected").await;
    load(&rig, "allowed").await;
    load_params(&rig, "bound", json!({"filter":"second"})).await;
    load_params(&rig, "unbound", json!({"filter":"second"})).await;
    server.hold(Kind::Mutate, true);
    let receipt = rig.create("answer").await;
    crate::test_wait::yield_until("first response is held", || server.held(Kind::Mutate) >= 1).await;
    assert!(ids(&rig, "response").await.is_empty());
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(ids(&rig, "response").await, ["srv1"]);
    assert!(ids(&rig, "other").await.is_empty());
    assert!(ids(&rig, "unloaded").await.is_empty());
    assert!(ids(&rig, "selected").await.is_empty());
    assert!(ids(&rig, "allowed").await.is_empty());
    let second = rig.create("second").await;
    crate::test_wait::yield_until("second response is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(matches!(rig.outcome(&second).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(ids(&rig, "response").await, ["srv2", "srv1"]);
    assert_eq!(ids(&rig, "selected").await, ["srv2"]);
    assert_eq!(ids(&rig, "allowed").await, ["srv2"]);
    assert_eq!(ids_params(&rig, "bound", json!({"filter":"second"})).await, ["srv2"]);
    assert!(ids_params(&rig, "unbound", json!({"filter":"second"})).await.is_empty());
    let update = rig.update("srv1", "changed").await.unwrap();
    crate::test_wait::yield_until("update response is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(matches!(rig.outcome(&update).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(ids(&rig, "response").await, ["srv2", "srv1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_insert_on_create() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("create.sqlite").to_str().unwrap().to_string();
    let server = Server::new();
    let rig = open(vec![relation("created", true, false), relation("other", false, false), relation("unloaded", true, false)], server.clone(), path).await;
    load(&rig, "created").await;
    load(&rig, "other").await;
    server.hold(Kind::Mutate, true);
    let first = rig.create("first").await;
    let first_id = first.optimistic_value.as_ref().unwrap().id.clone();
    let second = rig.create("second").await;
    let second_id = second.optimistic_value.as_ref().unwrap().id.clone();
    assert_eq!(ids(&rig, "created").await, [second_id.clone(), first_id.clone()]);
    assert!(ids(&rig, "other").await.is_empty());
    assert!(ids(&rig, "unloaded").await.is_empty());
    crate::test_wait::yield_until("first create is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Refuse);
    assert!(matches!(rig.outcome(&first).await, WireOperationOutcome::Rejected { .. }));
    assert_eq!(ids(&rig, "created").await, [second_id]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_joins_loaded_window() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("create-call.sqlite").to_str().unwrap().to_string();
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].relations.push(relation("created", true, false));
    let server = Server::new();
    let rig = open_models(serde_json::to_string(&models).unwrap(), path, server.clone(), Arc::default()).await;
    rig.db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).await.unwrap();
    load(&rig, "created").await;
    server.hold(Kind::Mutate, true);
    let receipt = within(rig.db.create(rig.epoch(), "Moment".into(), json!({"text":"created"}).to_string(), "{}".into())).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    assert_eq!(ids(&rig, "created").await, [id]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_threshold_updates_loaded_windows_through_database() {
    use crate::schema::FieldSpec;
    use crate::value::ScalarKind;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("threshold.sqlite").to_str().unwrap().to_string();
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].fields.push(FieldSpec::scalar("score", ScalarKind::Int).nullable());
    let mut selected = relation("selected", false, true);
    selected.text_params.push("minimum".into());
    selected.scalar_types.insert("minimum".into(), ScalarKind::Int);
    selected.row_rules.push(RowRule { field: "score".into(), test: RowTest::GreaterThan { parameter: "minimum".into() } });
    models[0].relations.push(selected);
    let server = Server::new();
    let http = Arc::new(ThresholdHttp(server.clone()));
    let rig = open_models_with_http(serde_json::to_string(&models).unwrap(), path, server, Arc::default(), http).await;
    rig.db.register_operations(r#"[
        {"name":"Feed","document":"query Feed { moments { id text score } }","rootPath":"moments"},
        {"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text score } }","rootPath":"makeMoment"},
        {"name":"UpdateMoment","document":"mutation UpdateMoment { updateMoment { id text score } }","rootPath":"updateMoment"}
    ]"#.into()).await.unwrap();
    for minimum in [5, 10] { load_params(&rig, "selected", json!({"minimum":minimum})).await; }
    let first = rig.create("8").await;
    assert!(matches!(rig.outcome(&first).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(ids_params(&rig, "selected", json!({"minimum":5})).await, ["m1"]);
    assert!(ids_params(&rig, "selected", json!({"minimum":10})).await.is_empty());
    let update = rig.update("m1", "12").await.unwrap();
    assert!(matches!(rig.outcome(&update).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(ids_params(&rig, "selected", json!({"minimum":10})).await, ["m1"]);
    let update = rig.update("m1", "3").await.unwrap();
    assert!(matches!(rig.outcome(&update).await, WireOperationOutcome::Confirmed { .. }));
    for minimum in [5, 10] { assert!(ids_params(&rig, "selected", json!({"minimum":minimum})).await.is_empty()); }
}

#[test]
fn created_window_membership_uses_the_committed_window() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{StoreBatch, StoreOp};
    use crate::store::Store;

    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].relations.push(relation("created", true, false));
    let db = Store::open_models(None, models, None).unwrap();
    let planned = db.plan_create("Moment", json!({"text":"created"}).as_object().unwrap().clone(), Default::default()).unwrap();
    let key = WindowKey::relation("Moment", "created", &Default::default());
    let commit = |author, ops| {
        let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    };
    commit(BatchAuthor::Window, vec![StoreOp::Window {
        key: key.name, model: key.model, page: 1, replace_all: false, cursor: None,
        roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap(),
    }]);
    let row_id = planned.row_id.to_string();
    commit(BatchAuthor::Outbox, vec![StoreOp::Intent {
        op_id: 1, model: "Moment".into(), row_id: row_id.clone(), action: "create".into(),
        payload: "{}".into(), effects: planned.effects, origin: crate::owners::store::IntentOrigin::Api,
    }]);
    let query = db.remote_relation_query("Moment", "created", &Default::default()).unwrap().unwrap();
    let rows = db.query(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&json!(row_id)));
}

#[test]
fn data_matches_window() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreBatch, StoreOp};
    use crate::schema::FieldSpec;
    use crate::store::Store;
    use crate::value::ScalarKind;

    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].fields.push(FieldSpec::scalar("score", ScalarKind::Int).nullable());
    let mut selected = relation("selected", false, true);
    selected.joins_events = true;
    selected.text_params.push("minimum".into());
    selected.scalar_types.insert("minimum".into(), ScalarKind::Int);
    selected.row_rules.push(RowRule { field: "score".into(), test: RowTest::GreaterThan { parameter: "minimum".into() } });
    models[0].relations.push(selected);
    let db = Store::open_models(None, models, None).unwrap();
    let commit = |author, ops| {
        let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    };
    for minimum in [5, 10] {
        let key = WindowKey::relation("Moment", "selected", json!({"minimum":minimum}).as_object().unwrap());
        commit(BatchAuthor::Window, vec![StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
    }
    let ids = |minimum| {
        let query = db.remote_relation_query("Moment", "selected", json!({"minimum":minimum}).as_object().unwrap()).unwrap().unwrap();
        db.query(&query).unwrap().into_iter().map(|row| row.get("id").unwrap().clone()).collect::<Vec<_>>()
    };
    for (score, expected) in [(json!(8), [true, false]), (json!(12), [true, true]), (json!(3), [false, false]), (Value::Null, [false, false])] {
        let row = RowValue { row_id: "m1".into(), values: json!({"id":"m1", "score":score}).to_string() };
        commit(BatchAuthor::Outbox, vec![StoreOp::Rows { model: "Moment".into(), rows: vec![row] }]);
        assert_eq!([!ids(5).is_empty(), !ids(10).is_empty()], expected);
    }
    let row = RowValue { row_id: "m1".into(), values: json!({"id":"m1", "score":12}).to_string() };
    commit(BatchAuthor::Outbox, vec![StoreOp::Rows { model: "Moment".into(), rows: vec![row] }]);
    let row = RowValue { row_id: "m1".into(), values: json!({"id":"m1", "score":8}).to_string() };
    commit(BatchAuthor::LocalWrites, vec![StoreOp::LocalRows { model: "Moment".into(), rows: vec![row] }]);
    assert_eq!([!ids(5).is_empty(), !ids(10).is_empty()], [true, false]);
    let event = json!({"op":"upsert", "id":"m1", "row":{"id":"m1", "score":12}}).to_string();
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 2, epoch: 1, basis: Default::default(), rows: vec![("Moment".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!([!ids(5).is_empty(), !ids(10).is_empty()], [true, true]);
}
