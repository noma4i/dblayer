use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::client::{Answer, EncryptedClient};
use crate::transport::{GraphQLError, GraphQLRequest, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError};
use crate::transport::{CableConnect, CableSocket, CableSubscriptions};

const PERSISTED_QUERY_VERSION: u32 = 1;
const PERSISTED_QUERY_NOT_FOUND: &str = "PERSISTED_QUERY_NOT_FOUND";

pub struct EncryptedTransport {
    client: Arc<EncryptedClient>,
    rest: Arc<dyn Transport>,
    cable: CableSubscriptions,
}

struct TransportCable(Arc<dyn Transport>, super::client::EncryptedConfig);

#[async_trait]
impl CableConnect for TransportCable {
    async fn open(&self) -> Result<Arc<dyn CableSocket>, TransportError> { crate::db::lock::assert_no_library_lock("Transport::open_cable"); self.0.open_cable(&self.1.cable_url()?).await }
}

impl EncryptedTransport {
    pub fn new(client: Arc<EncryptedClient>, rest: Arc<dyn Transport>) -> Self {
        let credentials = client.clone();
        let cable = CableSubscriptions::new(client.config.clone(), Arc::new(move |admitted| credentials.cable_credential(admitted)), Arc::new(TransportCable(rest.clone(), client.config.clone())), client.online.clone(), client.clock.clone());
        Self { client, rest, cable }
    }

    #[cfg(test)]
    pub(super) fn test_admission(&self) -> Result<crate::ffi::legacy::Admitted, TransportError> { self.client.admit("test transport") }
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

pub fn repeats(config: &super::EncryptedConfig, request: &GraphQLRequest, path: &str, body: &Value) -> bool {
    path == config.endpoints.graphql && (*body == graphql_body(request, false) || *body == graphql_body(request, true))
}

pub(crate) fn pending_matches(db: &crate::db::Db, owner: &crate::transport::RequestOwner) -> Result<bool, crate::store::StoreError> {
    Ok(db.shared.store.read_locked(super::state::pending)?.is_some_and(|pending| {
        pending.owner.as_ref() == Some(owner)
    }))
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
    fn retries_queries(&self) -> bool { true }
    fn resume_subscriptions(&self) { self.cable.resume(); }
    fn session(&self) -> Result<Option<String>, TransportError> { self.client.signed_in() }

    async fn query(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> {
        let response = response_of(self.client.read(admitted, &self.client.config.endpoints.graphql, graphql_body(&request, false)).await?)?;
        if !missing_from_registry(&response) {
            return Ok(response);
        }
        response_of(self.client.read(admitted, &self.client.config.endpoints.graphql, graphql_body(&request, true)).await?)
    }

    async fn mutate(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> {
        response_of(self.client.mutate_owned(admitted, &self.client.config.endpoints.graphql, graphql_body(&request, true), request.owner, request.purpose).await?)
    }

    async fn subscribe(&self, admitted: &crate::ffi::legacy::Admitted, request: GraphQLRequest) -> Result<ServerSubscription, TransportError> {
        self.client.validate(admitted)?;
        self.cable.subscribe(admitted, request).await
    }

    async fn put(&self, admitted: &crate::ffi::legacy::Admitted, request: PutRequest) -> Result<PutResponse, TransportError> {
        self.client.validate(admitted)?;
        crate::db::lock::assert_no_library_lock("Transport::put");
        self.rest.put(admitted, request).await
    }
}
