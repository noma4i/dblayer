use super::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use async_trait::async_trait;
use serde_json::{json, Value};
use crate::db::Db;
use crate::operation::OperationStatus;
use crate::pacing::ManualClock;
use crate::schema::ModelSpec;
use crate::sync::rig::{keeping_specs, params, register_operations};
use crate::sync::SyncConfig;
use crate::transport::{GraphQLRequest, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError};
use crate::transport::{EncryptedClient, EncryptedConfig, EncryptedTransport, HttpExchange, HttpReply};
use crate::transport::encrypted::fake_server_tests::{open_request, seal_response, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};

struct Exchange {
    server: Arc<Mutex<Server>>,
    requests: Mutex<Vec<(Value, String)>>,
}

#[async_trait]
impl HttpExchange for Exchange {
    async fn post(&self, path: &str, body: String, _: crate::transport::NetworkPurpose) -> Result<HttpReply, TransportError> {
        let request = open_request(&body, path, 1_800_000_000).unwrap();
        let reply = if path == "/api/graphql" {
            self.requests.lock().unwrap().push((request.clone(), body));
            let response = self.server.lock().unwrap().mutate(GraphQLRequest::new("", "CreateMoment", request["body"]["variables"].clone()))?;
            serde_json::to_value(response).unwrap()
        } else { json!({"ok": true}) };
        Ok(HttpReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), reply) })
    }
}

fn encrypted(db: &Db, exchange: Arc<Exchange>, credentials: Arc<MemoryCredentials>) -> (Sync, Arc<EncryptedClient>) {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let config = EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::protocol_tests::config() };
    let client = Arc::new(EncryptedClient::new(&config, exchange.clone(), credentials, db.clone(), clock.clone()).unwrap());
    let transport = Arc::new(EncryptedTransport::new(client.clone(), exchange.server.clone()));
    let sync = Sync::new(db.clone(), transport, clock, SyncConfig::default());
    register_operations(&sync);
    (sync, client)
}

#[derive(Default)]
struct Server {
    results: HashMap<String, Response>,
    calls: Vec<Value>,
    effects: usize,
    lose_answer: bool,
    refuse: Option<&'static str>,
}

impl Server {
    fn mutate(&mut self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let server = self;
        server.calls.push(request.variables.clone());
        if let Some(code) = server.refuse.take() {
            return Ok(serde_json::from_value(json!({"errors": [{"message": code, "extensions": {"code": code, "retryable": code == "IN_PROGRESS"}}]})).unwrap());
        }
        let input = &request.variables["input"];
        let key = input["clientMutationId"].as_str().unwrap_or_default().to_string();
        let answer = if let Some(answer) = server.results.get(&key).filter(|_| !key.is_empty()) {
            answer.clone()
        } else {
            server.effects += 1;
            let answer = Response::data(json!({"createMoment": {"id": format!("m{}", server.effects), "text": input["text"]}}));
            if !key.is_empty() { server.results.insert(key, answer.clone()); }
            answer
        };
        if std::mem::take(&mut server.lose_answer) { return Err(TransportError::Interrupted("lost response".into())); }
        Ok(answer)
    }
}

#[async_trait]
impl Transport for Mutex<Server> {
    async fn mutate(&self, _admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> {
        self.lock().unwrap().mutate(request)
    }
    async fn query(&self, _admitted: &crate::lifecycle::Admitted, _: GraphQLRequest) -> Result<Response, TransportError> { unreachable!() }
    async fn subscribe(&self, _admitted: &crate::lifecycle::Admitted, _: GraphQLRequest) -> Result<ServerSubscription, TransportError> { unreachable!() }
    async fn put(&self, _admitted: &crate::lifecycle::Admitted, _: PutRequest) -> Result<PutResponse, TransportError> { unreachable!() }
}

fn models(accepts: bool) -> Vec<ModelSpec> {
    let mut models = keeping_specs();
    let action = &mut models[1].actions[0];
    action.wrap = Some("input".into());
    let mut encoded = serde_json::to_value(&action).unwrap();
    encoded["client_mutation_id"] = json!(accepts);
    *action = serde_json::from_value(encoded).unwrap();
    models
}

fn connect(db: &Db, server: Arc<Mutex<Server>>) -> Sync {
    let sync = Sync::new(db.clone(), server, Arc::new(ManualClock::new(Duration::ZERO)), SyncConfig::default());
    register_operations(&sync);
    sync
}

#[tokio::test(start_paused = true)]
async fn idempotency_survives_reopen_and_reuses_the_successful_answer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("outbox.sqlite");
    let server = Arc::new(Mutex::new(Server { lose_answer: true, ..Server::default() }));
    let exchange = Arc::new(Exchange { server: server.clone(), requests: Mutex::new(Vec::new()) });
    let credentials = Arc::new(MemoryCredentials::default());
    let (id, key) = {
        let db = Db::open(&path, models(true), None).unwrap();
        let row = db.create(&db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
        let key = db.outbox().unwrap()[0].operation.client_mutation_id.clone();
        let (sync, client) = encrypted(&db, exchange.clone(), credentials.clone());
        client.mutate(&db.admit().unwrap(), "/api/auth/social", json!({"provider": "test"})).await.unwrap();
        sync.commit_session_for_test();
        crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.unwrap();
        assert_eq!(db.outbox().unwrap()[0].operation.status, OperationStatus::DeliveryUnknown);
        (row.id.clone(), key)
    };
    let db = Db::open(&path, models(true), None).unwrap();
    assert_eq!(db.outbox().unwrap()[0].operation.client_mutation_id, key);
    let (sync, client) = encrypted(&db, exchange.clone(), credentials);
    client.clear_credential_for_test().unwrap();
    client.mutate(&db.admit().unwrap(), "/api/auth/social", json!({"provider": "test"})).await.unwrap();
    sync.commit_session_for_test();
    crate::sync::rig::app(&sync, sync.retry(&sync.inner.db.admit().unwrap(), "moments", &id, None)).await.unwrap();
    assert!(db.outbox().unwrap().is_empty());
    assert!(db.find("moments", &id).unwrap().is_none());
    assert!(db.find("moments", &RowId::new("m1").unwrap()).unwrap().is_some());
    let server = server.lock().unwrap();
    assert_eq!(server.effects, 1);
    assert_eq!(server.calls.len(), 2);
    assert_eq!(server.calls[0]["input"]["clientMutationId"], server.calls[1]["input"]["clientMutationId"]);
    assert!(server.calls[0]["input"]["clientMutationId"].as_str().is_some());
    assert_eq!(server.calls[0]["input"]["clientMutationId"], key);
    let requests = exchange.requests.lock().unwrap();
    assert_ne!(requests[0].0["requestId"], requests[1].0["requestId"]);
    assert_ne!(requests[0].1, requests[1].1);
}

#[tokio::test(start_paused = true)]
async fn idempotency_selects_distinct_or_explicit_keys_only_for_supported_inputs() {
    for (accepts, explicit) in [(true, None), (true, Some("app-key")), (false, None)] {
        let db = Db::open_in_memory(models(accepts), None).unwrap();
        let server = Arc::new(Mutex::new(Server::default()));
        let sync = connect(&db, server.clone());
        for text in ["one", "two"] {
            let args = explicit.map_or(json!({}), |key| json!({"clientMutationId": key}));
            db.create_with(&db.admit().unwrap(), "moments", params(json!({"text": text})), params(args)).unwrap();
        }
        crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.unwrap();
        let server = server.lock().unwrap();
        let keys: Vec<_> = server.calls.iter().map(|call| call["input"]["clientMutationId"].clone()).collect();
        match (accepts, explicit) {
            (_, Some(key)) => assert_eq!(keys, [json!(key), json!(key)]),
            (true, None) => { assert!(keys.iter().all(|key| key.as_str().is_some_and(|key| uuid::Uuid::parse_str(key).is_ok()))); assert_ne!(keys[0], keys[1]); }
            (false, None) => assert!(server.calls.iter().all(|call| call["input"].get("clientMutationId").is_none())),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn idempotency_refusals_keep_the_row_and_manual_retry_keeps_the_key() {
    for code in ["IN_PROGRESS", "DUPLICATE_CLIENT_KEY"] {
        let db = Db::open_in_memory(models(true), None).unwrap();
        let server = Arc::new(Mutex::new(Server { refuse: Some(code), ..Server::default() }));
        let sync = connect(&db, server.clone());
        let row = db.create(&db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
        crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.unwrap();
        assert_eq!(db.outbox().unwrap()[0].operation.status, OperationStatus::Failed);
        assert!(db.find("moments", &row.id).unwrap().is_some());
        crate::sync::rig::app(&sync, sync.retry_all(&sync.inner.db.admit().unwrap())).await.unwrap();
        assert!(db.outbox().unwrap().is_empty());
        let server = server.lock().unwrap();
        assert!(server.calls[0]["input"]["clientMutationId"].as_str().is_some());
        assert_eq!(server.calls[0], server.calls[1]);
        assert_eq!(server.effects, 1);
    }
}
