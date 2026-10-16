use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::client::{Answer, EncryptedClient};
use crate::transport::{GraphQLError, GraphQLRequest, PutRequest, PutResponse, Response, Subscription, Transport, TransportError};

const GRAPHQL_PATH: &str = "/api/v3/graphql";
const PERSISTED_QUERY_VERSION: u32 = 1;

pub struct EncryptedTransport {
    client: Arc<EncryptedClient>,
    rest: Arc<dyn Transport>,
}

impl EncryptedTransport {
    pub fn new(client: Arc<EncryptedClient>, rest: Arc<dyn Transport>) -> Self {
        Self { client, rest }
    }
}

pub fn graphql_body(request: &GraphQLRequest) -> Value {
    let hash: String = Sha256::digest(request.document.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
    let mut body = Map::new();
    body.insert("query".into(), json!(request.document));
    body.insert("extensions".into(), json!({ "persistedQuery": { "version": PERSISTED_QUERY_VERSION, "sha256Hash": hash } }));
    if !request.operation_name.is_empty() {
        body.insert("operationName".into(), json!(request.operation_name));
    }
    if !request.variables.is_null() {
        body.insert("variables".into(), request.variables.clone());
    }
    Value::Object(body)
}

pub fn response_of(answer: Answer) -> Result<Response, TransportError> {
    if let Some(code) = answer.body.get("error").and_then(Value::as_str) {
        let error = GraphQLError { message: code.to_string(), path: None, extensions: json!({ "code": code }).as_object().cloned() };
        return Ok(Response { data: None, errors: vec![error] });
    }
    serde_json::from_value(answer.body).map_err(|error| TransportError::Decode(error.to_string()))
}

#[async_trait]
impl Transport for EncryptedTransport {
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        response_of(self.client.read(GRAPHQL_PATH, graphql_body(&request)).await?)
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        response_of(self.client.mutate(GRAPHQL_PATH, graphql_body(&request)).await?)
    }

    async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> {
        self.rest.subscribe(request).await
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
        self.rest.put(request).await
    }
}
