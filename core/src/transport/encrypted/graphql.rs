use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::client::{Answer, EncryptedClient};
use crate::transport::{GraphQLError, GraphQLRequest, PutRequest, PutResponse, Response, Subscription, Transport, TransportError};
use crate::transport::{CableConnect, CableSocket, CableSubscriptions, Connectivity};

const GRAPHQL_PATH: &str = "/api/v3/graphql";
const PERSISTED_QUERY_VERSION: u32 = 1;
const PERSISTED_QUERY_NOT_FOUND: &str = "PERSISTED_QUERY_NOT_FOUND";

pub struct EncryptedTransport {
    client: Arc<EncryptedClient>,
    rest: Arc<dyn Transport>,
    cable: CableSubscriptions,
}

struct TransportCable(Arc<dyn Transport>);

#[async_trait]
impl CableConnect for TransportCable {
    async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> { self.0.open_cable().await }
}

impl EncryptedTransport {
    pub fn new(client: Arc<EncryptedClient>, rest: Arc<dyn Transport>) -> Self {
        let credentials = client.clone();
        let cable = CableSubscriptions::new(client.config.clone(), Arc::new(move || credentials.credential()), Arc::new(TransportCable(rest.clone())), Connectivity::new());
        Self { client, rest, cable }
    }
}

pub fn graphql_body(request: &GraphQLRequest, with_document: bool) -> Value {
    let hash: String = Sha256::digest(request.document.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
    let mut body = Map::new();
    if with_document {
        body.insert("query".into(), json!(request.document));
    }
    body.insert("extensions".into(), json!({ "persistedQuery": { "version": PERSISTED_QUERY_VERSION, "sha256Hash": hash } }));
    if !request.operation_name.is_empty() {
        body.insert("operationName".into(), json!(request.operation_name));
    }
    if !request.variables.is_null() {
        body.insert("variables".into(), request.variables.clone());
    }
    Value::Object(body)
}

pub fn repeats(request: &GraphQLRequest, path: &str, body: &Value) -> bool {
    path == GRAPHQL_PATH && (*body == graphql_body(request, false) || *body == graphql_body(request, true))
}

pub(crate) fn pending_matches(db: &crate::db::Db, seq: i64, position: i64, stage: &str) -> bool {
    let stored = db.shared.store.lock();
    super::state::pending(&stored.connection).ok().flatten().is_some_and(|pending| {
        pending.owner.is_some_and(|owner| owner.seq == seq && owner.stage == stage && (owner.item_position == position || owner.item_position == -1 && stage == "mutation"))
    })
}

pub fn response_of(answer: Answer) -> Result<Response, TransportError> {
    if let Some(code) = answer.body.get("error").and_then(Value::as_str) {
        if let session @ TransportError::Session { .. } = TransportError::refusal(code) {
            return Err(session);
        }
        let error = GraphQLError { message: code.to_string(), path: None, extensions: json!({ "code": code }).as_object().cloned() };
        return Ok(Response { data: None, errors: vec![error] });
    }
    serde_json::from_value(answer.body).map_err(|error| TransportError::Decode(error.to_string()))
}

pub fn missing_from_registry(response: &Response) -> bool {
    response.errors.iter().any(|error| error.code() == Some(PERSISTED_QUERY_NOT_FOUND))
}

#[async_trait]
impl Transport for EncryptedTransport {
    fn set_online(&self, online: bool) { self.cable.set_online(online); }
    fn resume_subscriptions(&self) { self.cable.resume(); }

    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let response = response_of(self.client.read(GRAPHQL_PATH, graphql_body(&request, false)).await?)?;
        if !missing_from_registry(&response) {
            return Ok(response);
        }
        response_of(self.client.read(GRAPHQL_PATH, graphql_body(&request, true)).await?)
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let stored = graphql_body(&request, true);
        if self.client.pending_call()?.is_some_and(|(path, body)| path == GRAPHQL_PATH && body == stored) {
            return response_of(self.client.mutate_owned(GRAPHQL_PATH, stored, request.owner).await?);
        }
        let response = response_of(self.client.mutate_owned(GRAPHQL_PATH, graphql_body(&request, false), request.owner.clone()).await?)?;
        if !missing_from_registry(&response) {
            return Ok(response);
        }
        response_of(self.client.mutate_owned(GRAPHQL_PATH, graphql_body(&request, true), request.owner.clone()).await?)
    }

    async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> {
        self.cable.subscribe(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
        self.rest.put(request).await
    }
}
