use crate::ffi::{FakeHttp};
use std::sync::Arc;
use async_trait::async_trait;
use super::harness_life::Held;
use super::rig::{moment_models, within, Kind, Rig, Server, Wire, OPERATIONS};
use crate::ffi::{Config, Database, DbError, EncryptedHttp, EncryptedReply, OpenPorts, TestClock};
use crate::schema::{Coverage, Membership, OnFailure, RelationSpec, RemoteShape, RemoteSpec, RowRule, RowTest};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response, MemoryCredentials};
use crate::transport::{CredentialSlot, EncryptedOperationClass, NetworkPurpose};
use serde_json::{json, Value};

struct WindowHttp(Arc<Server>);

struct ThresholdHttp(Arc<Server>);

#[async_trait]
impl FakeHttp for ThresholdHttp {
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
impl FakeHttp for WindowHttp {
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
    let request = rig.db.begin_load(rig.epoch(), "User".into(), Some("profile".into()), "{}".into()).unwrap();
    within(request.result()).await.unwrap();
    let query = json!({"model":"User", "relation":"profile", "params":{}}).to_string();
    let rows = within(rig.db.read_query(rig.epoch(), crate::ffi::QuerySpec::json(query))).await.unwrap();
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
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), crate::ffi::EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let ports = OpenPorts { transport: Some(Arc::new(Wire(server.clone()))), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(credentials))), http: Some(http), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let db = within(Database::open_with_clock(Config { path: Some(path), data_version: None }, crate::ffi::specs(models), ports, clock.clone())).await.unwrap();
    db.register_operations(OPERATIONS.into()).await.unwrap();
    db.set_online(true).unwrap();
    if !signed { within(db.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap(); }
    Rig { db, server }
}

async fn load(rig: &Rig, name: &str) {
    load_params(rig, name, json!({})).await;
}

async fn load_params(rig: &Rig, name: &str, params: serde_json::Value) {
    let request = rig.db.begin_load(rig.epoch(), "Moment".into(), Some(name.into()), params.to_string()).unwrap();
    within(request.result()).await.unwrap();
}

async fn ids(rig: &Rig, name: &str) -> Vec<String> {
    ids_params(rig, name, json!({})).await
}

async fn ids_params(rig: &Rig, name: &str, params: serde_json::Value) -> Vec<String> {
    let query = json!({"model":"Moment", "relation":name, "params":params}).to_string();
    within(rig.db.read_query(rig.epoch(), crate::ffi::QuerySpec::json(query))).await.unwrap().into_iter().map(|row| row.id).collect()
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
    crate::test_wait::settled(&rig, "first response is held", || server.held(Kind::Mutate) >= 1).await;
    assert!(ids(&rig, "response").await.is_empty());
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(rig.confirmed(&receipt).await.is_ok());
    assert_eq!(ids(&rig, "response").await, ["srv1"]);
    assert!(ids(&rig, "other").await.is_empty());
    assert!(ids(&rig, "unloaded").await.is_empty());
    assert!(ids(&rig, "selected").await.is_empty());
    assert!(ids(&rig, "allowed").await.is_empty());
    let second = rig.create("second").await;
    crate::test_wait::settled(&rig, "second response is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(rig.confirmed(&second).await.is_ok());
    assert_eq!(ids(&rig, "response").await, ["srv2", "srv1"]);
    assert_eq!(ids(&rig, "selected").await, ["srv2"]);
    assert_eq!(ids(&rig, "allowed").await, ["srv2"]);
    assert_eq!(ids_params(&rig, "bound", json!({"filter":"second"})).await, ["srv2"]);
    assert!(ids_params(&rig, "unbound", json!({"filter":"second"})).await.is_empty());
    let update = rig.update("srv1", "changed").await.unwrap();
    crate::test_wait::settled(&rig, "update response is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Ok);
    assert!(rig.confirmed(&update).await.is_ok());
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
    crate::test_wait::settled(&rig, "first create is held", || server.held(Kind::Mutate) >= 1).await;
    server.answer(Kind::Mutate, super::rig::Reply::Refuse);
    assert!(matches!(rig.confirmed(&first).await, Err(DbError::Rejected { kept: false, .. })));
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
    assert!(rig.confirmed(&first).await.is_ok());
    assert_eq!(ids_params(&rig, "selected", json!({"minimum":5})).await, ["m1"]);
    assert!(ids_params(&rig, "selected", json!({"minimum":10})).await.is_empty());
    let update = rig.update("m1", "12").await.unwrap();
    assert!(rig.confirmed(&update).await.is_ok());
    assert_eq!(ids_params(&rig, "selected", json!({"minimum":10})).await, ["m1"]);
    let update = rig.update("m1", "3").await.unwrap();
    assert!(rig.confirmed(&update).await.is_ok());
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
    let db = Store::open_models(None, models, None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let planned = db.plan_create("Moment", json!({"text":"created"}).as_object().unwrap().clone(), Default::default()).unwrap();
    let key = WindowKey::relation("Moment", "created", &Default::default());
    let commit = |author, ops| {
        let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    };
    commit(BatchAuthor::Window, vec![StoreOp::Window { landed_at: 0,
        key: key.name, model: key.model, page: 1, replace_all: false, cursor: None,
        roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap(),
    }]);
    let row_id = planned.row_id.to_string();
    commit(BatchAuthor::Outbox, vec![StoreOp::Intent { created_at: 0, attempts: 1, cycle_id: 1,
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
    let db = Store::open_models(None, models, None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let commit = |author, ops| {
        let batch = StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    };
    for minimum in [5, 10] {
        let key = WindowKey::relation("Moment", "selected", json!({"minimum":minimum}).as_object().unwrap());
        commit(BatchAuthor::Window, vec![StoreOp::Window { landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: Vec::new(), related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() }]);
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
    let batch = StoreBatch { author: BatchAuthor::Feed, id: 2, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: vec![("Moment".into(), event)], intents: 0, credential: None, ops: Vec::new() };
    db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
    assert_eq!([!ids(5).is_empty(), !ids(10).is_empty()], [true, true]);
}

#[test]
fn data_membership_window_leave() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreBatch, StoreOp};
    use crate::schema::FieldSpec;
    use crate::store::Store;
    use crate::value::ScalarKind;

    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
    models[0].fields.push(FieldSpec::list("shelves", ScalarKind::Str));
    let mut shelf = relation("shelf", false, false);
    shelf.text_params.push("shelf".into());
    shelf.membership.insert("shelf".into(), Membership::Contains("shelves".into()));
    models[0].relations.push(shelf);
    let db = Store::open_models(None, models, None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let ids = |shelf: &str| {
        let query = db.remote_relation_query("Moment", "shelf", json!({"shelf":shelf}).as_object().unwrap()).unwrap().unwrap();
        db.query(&query).unwrap().into_iter().map(|row| row.get("id").unwrap().clone()).collect::<Vec<_>>()
    };
    for (shelf, held) in [("A", [json!(["m1"]), json!([])]), ("B", [json!([]), json!(["m1"])])] {
        let key = WindowKey::relation("Moment", "shelf", json!({"shelf":shelf}).as_object().unwrap());
        let row = RowValue { row_id: "m1".into(), values: json!({"id":"m1", "shelves":[shelf]}).to_string() };
        let page = StoreOp::Window { landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row], related: Vec::new(), total: None, base_revision: db.revision_seq().unwrap() };
        let batch = StoreBatch { author: BatchAuthor::Window, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: vec![page] };
        db.write_batch(&batch, db.files(), db.pointer_guards(), None).unwrap();
        assert_eq!([json!(ids("A")), json!(ids("B"))], held, "after the page of window {shelf}");
    }
}

mod by_source {
    use super::*;
    use crate::owners::contract::BatchAuthor;
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{IntentOrigin, RowValue, StoreBatch, StoreOp};
    use crate::schema::FieldSpec;
    use crate::store::Store;
    use crate::value::ScalarKind;
    use std::cell::Cell;

    pub(super) const COLUMNS: [&str; 5] = ["plain", "event", "created", "node", "cols"];

    pub(super) struct Bench {
        pub(super) db: Store,
        batch: Cell<u64>,
    }

    fn params_of(name: &str) -> serde_json::Map<String, Value> {
        if name == "cols" { json!({"groupId":"g"}).as_object().unwrap().clone() } else { Default::default() }
    }

    pub(super) fn moment_models_with(columns: Vec<RelationSpec>, extra: Vec<FieldSpec>) -> Vec<crate::schema::ModelSpec> {
        let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&moment_models(OnFailure::Rollback)).unwrap();
        models[0].fields.push(FieldSpec::scalar("groupId", ScalarKind::Str).nullable().indexed());
        models[0].fields.extend(extra);
        models[0].relations.extend(columns);
        models
    }

    impl Bench {
        pub(super) fn open(models: Vec<crate::schema::ModelSpec>) -> Bench {
            Bench { db: Store::open_models(None, models, None, Arc::new(crate::runtime::SystemClock)).unwrap(), batch: Cell::new(0) }
        }

        pub(super) fn commit(&self, author: BatchAuthor, rows: Vec<(String, String)>, ops: Vec<StoreOp>) {
            self.batch.set(self.batch.get() + 1);
            let batch = StoreBatch { author, id: self.batch.get(), stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows, intents: 0, credential: None, ops };
            self.db.write_batch(&batch, self.db.files(), self.db.pointer_guards(), None).unwrap();
        }

        pub(super) fn page(&self, name: &str, params: &serde_json::Map<String, Value>, roots: Vec<Value>) {
            let key = WindowKey::relation("Moment", name, params);
            let roots = roots.into_iter().map(|row| RowValue { row_id: row["id"].as_str().unwrap().into(), values: row.to_string() }).collect();
            let op = StoreOp::Window { landed_at: 0, key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots, related: Vec::new(), total: None, base_revision: self.db.revision_seq().unwrap() };
            self.commit(BatchAuthor::Window, Vec::new(), vec![op]);
        }

        pub(super) fn ids(&self, name: &str, params: &serde_json::Map<String, Value>) -> Vec<String> {
            let query = self.db.remote_relation_query("Moment", name, params).unwrap().unwrap();
            self.db.query(&query).unwrap().into_iter().map(|row| row.get("id").unwrap().as_str().unwrap().to_string()).collect()
        }

        pub(super) fn event(&self, row: Value) {
            let event = json!({"op":"upsert", "id":row["id"], "row":row}).to_string();
            self.commit(BatchAuthor::Feed, vec![("Moment".into(), event)], Vec::new());
        }

        fn write(&self, source: &str, row: Value) -> String {
            let value = |row: &Value| vec![RowValue { row_id: row["id"].as_str().unwrap().into(), values: row.to_string() }];
            match source {
                "page" => self.page("pager", &Default::default(), vec![row.clone()]),
                "event" => self.event(row.clone()),
                "response" => self.commit(BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "Moment".into(), rows: value(&row) }]),
                "local" => self.commit(BatchAuthor::LocalWrites, Vec::new(), vec![StoreOp::LocalRows { model: "Moment".into(), rows: value(&row) }]),
                "create" => {
                    let planned = self.db.plan_create("Moment", json!({"text":"t", "groupId":"g"}).as_object().unwrap().clone(), Default::default()).unwrap();
                    let id = planned.row_id.to_string();
                    let op = StoreOp::Intent { attempts: 1, cycle_id: 1, op_id: self.batch.get() + 1, model: "Moment".into(), row_id: id.clone(), action: "create".into(), payload: "{}".into(), effects: planned.effects, origin: IntentOrigin::Api };
                    self.commit(BatchAuthor::Outbox, Vec::new(), vec![op]);
                    return id;
                }
                other => panic!("no source {other}"),
            }
            row["id"].as_str().unwrap().to_string()
        }

        pub(super) fn matrix_write(&self, source: &str, id: &str, group: &str) -> String {
            self.write(source, json!({"id":id, "text":"t", "groupId":group}))
        }
    }

    pub(super) fn matrix_bench() -> Bench {
        let mut node = relation("node", false, false);
        node.joins_node = true;
        let mut cols = relation("cols", false, false);
        cols.is_global = false;
        cols.by_columns = vec!["groupId".into()];
        let columns = vec![relation("plain", false, false), relation("event", false, true), relation("created", true, false), node, cols, relation("pager", false, false), relation("unloaded", true, true)];
        let bench = Bench::open(moment_models_with(columns, Vec::new()));
        for name in COLUMNS.into_iter().chain(["pager"]) {
            bench.page(name, &params_of(name), vec![json!({"id":"s1", "text":"t", "groupId":"g"})]);
        }
        bench
    }

    pub(super) fn column_ids(bench: &Bench) -> Vec<(String, Vec<String>)> {
        COLUMNS.into_iter().map(|name| (name.to_string(), bench.ids(name, &params_of(name)))).collect()
    }

    /// P-DATA-100 cells: the declared `insert_on` of the window against the source of the batch.
    pub(super) fn entering(source: &str, cached: bool) -> [bool; 5] {
        match source {
            "event" | "response" => [false, true, false, false, cached],
            "create" => [false, false, true, false, true],
            _ => [false, false, false, false, cached],
        }
    }
}

/// P-DATA-100 (P-DATA-84, P-DATA-85, P-DATA-92, M24): the window declaration and the source of the batch decide the entry of a row into a loaded window, at the head, once; a member keeps its place; a window not loaded is not made.
#[test]
fn data_membership_by_source_matrix() {
    use by_source::*;
    let mut red = Vec::new();
    for source in ["page", "event", "response", "create", "local"] {
        for cached in [false, true] {
            if cached && source == "create" { continue; }
            let bench = matrix_bench();
            if cached { bench.matrix_write("local", "n1", "x"); }
            let id = bench.matrix_write(source, "n1", "g");
            let expected = entering(source, cached);
            for (index, (name, ids)) in column_ids(&bench).into_iter().enumerate() {
                let want: Vec<String> = if expected[index] { vec![id.clone(), "s1".into()] } else { vec!["s1".into()] };
                if ids != want { red.push(format!("source {source}, cached {cached}, window {name}: {ids:?}, spec {want:?}")); }
            }
            if !bench.ids("unloaded", &Default::default()).is_empty() { red.push(format!("source {source}: a window that is not loaded was made")); }
            if source == "create" { continue; }
            let entered = column_ids(&bench);
            bench.matrix_write(source, "n1", "g");
            if column_ids(&bench) != entered { red.push(format!("source {source}, cached {cached}: the same row again moved: {:?}", column_ids(&bench))); }
            bench.matrix_write(source, "s1", "g");
            if column_ids(&bench) != entered { red.push(format!("source {source}, cached {cached}: a member lost its place: {:?}", column_ids(&bench))); }
        }
    }
    assert!(red.is_empty(), "cells against P-DATA-100:\n{}", red.join("\n"));
}

/// P-DATA-84: an event `upsert` for a window with `insert_on: [:event, :response]` puts the row at the head when the row passes `matches` or the `membership` of the window, once for a repeated event; a member keeps its place.
#[test]
fn data_event_join_goes_through_membership() {
    use by_source::*;
    let mut matching = relation("matching", false, true);
    matching.row_rules.push(RowRule { field: "text".into(), test: RowTest::Equals { value: json!("wanted") } });
    let mut shelf = relation("shelf", false, true);
    shelf.text_params.push("shelf".into());
    shelf.membership.insert("shelf".into(), Membership::Contains("shelves".into()));
    let bench = Bench::open(moment_models_with(vec![matching, shelf], vec![crate::schema::FieldSpec::list("shelves", crate::value::ScalarKind::Str)]));
    let a = json!({"shelf":"A"}).as_object().unwrap().clone();
    let b = json!({"shelf":"B"}).as_object().unwrap().clone();
    let none = Default::default();
    bench.page("matching", &none, Vec::new());
    bench.page("shelf", &a, vec![json!({"id":"s1", "text":"t", "shelves":["A"]})]);
    bench.page("shelf", &b, vec![json!({"id":"s2", "text":"t", "shelves":["B"]})]);
    assert_eq!([bench.ids("matching", &none), bench.ids("shelf", &a), bench.ids("shelf", &b)], [Vec::<String>::new(), vec!["s1".to_string()], vec!["s2".to_string()]], "the pages landed");
    let seen = |bench: &Bench| [bench.ids("matching", &none), bench.ids("shelf", &a), bench.ids("shelf", &b)];
    bench.event(json!({"id":"e1", "text":"wanted", "shelves":["A"]}));
    bench.event(json!({"id":"e2", "text":"other", "shelves":["B"]}));
    let spec = |rows: [&[&str]; 3]| rows.map(|ids| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>());
    let entered = seen(&bench);
    bench.event(json!({"id":"e1", "text":"wanted", "shelves":["A"]}));
    bench.event(json!({"id":"s1", "text":"t", "shelves":["A"]}));
    let again = seen(&bench);
    let want = spec([&["e1"], &["e1", "s1"], &["e2", "s2"]]);
    assert_eq!((entered, again), (want.clone(), want), "the rows that pass the window rules enter at the head; a repeated event enters once and a member keeps its place");
}
