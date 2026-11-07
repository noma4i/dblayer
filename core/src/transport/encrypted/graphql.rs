use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use std::sync::{OnceLock, Weak};

use super::credential::Secret;
use super::exchange::{session_failure, temporary_error, Answer, Exchange};
use super::jwe::random_token;
use crate::runtime::{Clock, RetryPolicy};
use crate::store::Store;
use crate::transport::retry::{RetryCalls, Retrying};
use crate::transport::Connectivity;
use crate::transport::{GraphQLError, GraphQLRequest, PutRequest, PutResponse, Response, Transport, TransportError};

const PERSISTED_QUERY_VERSION: u32 = 1;
const PERSISTED_QUERY_NOT_FOUND: &str = "PERSISTED_QUERY_NOT_FOUND";

pub struct EncryptedTransport {
    exchange: Arc<Exchange>,
    credential: Arc<Secret>,
    store: Arc<Store>,
    online: Arc<Connectivity>,
    clock: Arc<dyn Clock>,
    progress: Arc<RetryCalls>,
    life: OnceLock<Weak<crate::owners::life::Life>>,
    rest: Arc<dyn Transport>,
}

impl EncryptedTransport {
    pub(crate) fn new(exchange: Arc<Exchange>, credential: Arc<Secret>, store: Arc<Store>, clock: Arc<dyn Clock>, progress: Arc<RetryCalls>, rest: Arc<dyn Transport>) -> Self {
        let online = store.connectivity();
        Self { exchange, credential, store, online, clock, progress, life: OnceLock::new(), rest }
    }

    /// Joins the owners: from now on a request with a `sequence` goes through the owner Session.
    pub(crate) fn attach(&self, life: &Arc<crate::owners::life::Life>) { let _ = self.life.set(Arc::downgrade(life)); }

    /// The cable of the owner Feed: it opens sockets through `rest` and signs in with the stored credential.
    pub(crate) fn cable_lines(&self) -> Arc<dyn crate::transport::CableLines> {
        let (credential, store) = (self.credential.clone(), self.store.clone());
        Arc::new(crate::transport::EncryptedLines::new(self.rest.clone(), self.exchange.config().clone(), Arc::new(move || credential.read(&store))))
    }

    async fn read(&self, path: &str, body: Value) -> Result<Answer, TransportError> {
        let credential = self.credential.read(&self.store)?;
        let mut answer = self.exchange_once(&credential, path, &body).await?;
        if answer.body.get("error").and_then(Value::as_str) == Some("REPLAYED_REQUEST") {
            answer = self.exchange_once(&credential, path, &body).await?;
        }
        Ok(answer)
    }

    async fn exchange_once(&self, credential: &str, path: &str, body: &Value) -> Result<Answer, TransportError> {
        let (request_id, response_key) = (uuid::Uuid::new_v4().to_string(), random_token());
        let mut retry = Retrying::new(self.clock.as_ref(), &self.progress, &self.online, RetryPolicy::Http);
        loop {
            self.online.online().await;
            let result = self.exchange.send_once(super::SendOnce { credential: Some(credential), request_id: &request_id, response_key: &response_key, sequence: None, path, body, purpose: crate::transport::NetworkPurpose::Standard }).await.and_then(|answer| match temporary_error(&answer.body) { Some(error) => Err(error), None => Ok(answer) });
            match result { Ok(answer) => return Ok(answer), Err(error) => retry.failed(error).await? }
        }
    }

    /// A request with a `sequence`: the owner Session sends it, in order with every other request of the session.
    async fn mutate_owned(&self, path: &str, body: Value, owner: Option<crate::transport::RequestOwner>, purpose: crate::transport::NetworkPurpose) -> Result<Answer, TransportError> {
        let life = self.life.get().and_then(Weak::upgrade).ok_or_else(|| TransportError::Interrupted("the database closed".into()))?;
        let waiter = life.mutate_for_call(owner, path.to_string(), body.to_string(), u8::from(purpose == crate::transport::NetworkPurpose::Transfer));
        match waiter.wait().await {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Mutated(outcome)) => match outcome {
                crate::owners::session::AuthorOutcome::Response(text) => Ok(Answer { sequence: None, body: serde_json::from_str(&text).map_err(|error| TransportError::Decode(error.to_string()))? }),
                crate::owners::session::AuthorOutcome::Error { error, .. } => Err(session_failure(error)),
                crate::owners::session::AuthorOutcome::Dropped => Err(TransportError::Network("the answer was not received".into())),
            },
            _ => Err(TransportError::Interrupted("the session ended while its answer was pending".into())),
        }
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

pub fn repeats(config: &super::EncryptedConfig, request: &GraphQLRequest, path: &str, body: &Value) -> bool {
    path == config.endpoints.graphql && (*body == graphql_body(request, false) || *body == graphql_body(request, true))
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
    fn session(&self) -> Result<Option<String>, TransportError> {
        Ok(super::credential::stored_session(&self.store)?.filter(|session| session.sequence > 0).map(|session| session.credential_digest))
    }

    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let path = &self.exchange.config().endpoints.graphql;
        let response = response_of(self.read(path, graphql_body(&request, false)).await?)?;
        if !missing_from_registry(&response) {
            return Ok(response);
        }
        response_of(self.read(path, graphql_body(&request, true)).await?)
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        response_of(self.mutate_owned(&self.exchange.config().endpoints.graphql, graphql_body(&request, true), request.owner, request.purpose).await?)
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
        crate::lock::assert_no_library_lock("Transport::put");
        self.rest.put(request).await
    }
}
