use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use std::sync::{OnceLock, Weak};

use super::credential::Secret;
use super::exchange::{session_failure, Answer, Exchange};
use super::jwe::random_token;
use crate::store::Store;
use crate::transport::{GraphQLError, GraphQLRequest, PutRequest, PutReply, Response, Upstream, TransportError};

const PERSISTED_QUERY_VERSION: u32 = 1;

pub struct EncryptedTransport {
    exchange: Arc<Exchange>,
    credential: Arc<Secret>,
    store: Arc<Store>,
    life: OnceLock<Weak<crate::owners::life::Life>>,
    rest: Arc<dyn Upstream>,
}

impl EncryptedTransport {
    pub(crate) fn new(exchange: Arc<Exchange>, credential: Arc<Secret>, store: Arc<Store>, rest: Arc<dyn Upstream>) -> Self {
        Self { exchange, credential, store, life: OnceLock::new(), rest }
    }

    /// Joins the owners: from now on a request with a `sequence` goes through the owner Session.
    pub(crate) fn attach(&self, life: &Arc<crate::owners::life::Life>) {
        if self.life.set(Arc::downgrade(life)).is_err() { life.fault("the encrypted transport is attached twice".into()); }
    }

    /// The cable of the owner Feed: it opens sockets through `rest` and signs in with the stored credential.
    pub(crate) fn cable_lines(&self) -> Arc<dyn crate::transport::CableLines> {
        let (credential, store) = (self.credential.clone(), self.store.clone());
        Arc::new(crate::transport::EncryptedLines::new(self.rest.clone(), self.exchange.config().clone(), Arc::new(move || credential.read(&store))))
    }

    async fn read(&self, path: &str, body: Value, wire: crate::transport::WireIds) -> Result<Answer, TransportError> {
        let credential = self.credential.read(&self.store)?;
        let (request_id, response_key) = (uuid::Uuid::new_v4().to_string(), random_token());
        self.exchange.send_once(super::SendOnce { credential: Some(&credential), request_id: &request_id, response_key: &response_key, sequence: None, path, body: &body, purpose: crate::transport::NetworkPurpose::Standard, wire }).await
    }

    /// A request with a `sequence`: the owner Session sends it, in order with every other request of the session.
    async fn mutate_owned(&self, path: &str, body: Value, (owner, wire_id): (Option<crate::transport::RequestOwner>, Option<u64>), purpose: crate::transport::NetworkPurpose) -> Result<(Answer, crate::transport::AnswerKind), TransportError> {
        let life = self.life.get().and_then(Weak::upgrade).ok_or_else(|| TransportError::Interrupted("the database closed".into()))?;
        let waiter = life.mutate_for_call(owner, wire_id, path.to_string(), body.to_string(), u8::from(purpose == crate::transport::NetworkPurpose::Transfer));
        match waiter.wait().await {
            crate::runtime::Outcome::Value(crate::owners::life::LifeReply::Mutated(outcome)) => match outcome {
                crate::owners::session::AuthorOutcome::Response { body, kind, .. } => Ok((Answer { sequence: None, body: serde_json::from_str(&body).map_err(|error| TransportError::Decode(error.to_string()))? }, kind)),
                crate::owners::session::AuthorOutcome::Error { error: crate::owners::session::SessionError::ResponseTooLarge { limit }, pending_retained: true, .. } => Err(TransportError::SentAnswerTooLarge { limit }),
                crate::owners::session::AuthorOutcome::Error { class: crate::owners::contract::ErrorClass::Network | crate::owners::contract::ErrorClass::Server, pending_retained: true, .. } => Err(TransportError::Network("the server asked to retry".into())),
                crate::owners::session::AuthorOutcome::Error { error, .. } => Err(session_failure(error)),
                crate::owners::session::AuthorOutcome::Dropped => Err(TransportError::Network("the answer was not received".into())),
            },
            crate::runtime::Outcome::Internal => Err(TransportError::Internal("the answer of the session was lost".into())),
            crate::runtime::Outcome::Value(_) => Err(TransportError::Internal("the owner Session answered with a reply the call does not wait for".into())),
            crate::runtime::Outcome::Closing | crate::runtime::Outcome::EpochEnded(_) => Err(TransportError::Interrupted("the session ended while its answer was pending".into())),
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

pub fn hash_only(body: &str) -> Option<String> {
    let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(body) else { return None };
    fields.get("extensions")?.get("persistedQuery")?;
    fields.remove("query")?;
    Some(Value::Object(fields).to_string())
}

pub fn repeats(config: &super::EncryptedConfig, request: &GraphQLRequest, path: &str, body: &Value) -> bool {
    path == config.endpoints.graphql && (*body == graphql_body(request, false) || *body == graphql_body(request, true))
}


pub fn response_from_answer(answer: Answer) -> Result<Response, TransportError> {
    let outcome = crate::transport::WireOutcome::Answer { sequence: answer.sequence, body: answer.body.clone() };
    let kind = crate::transport::classify_answer(&outcome, false, true);
    response_from_answer_with_kind(answer, kind)
}

pub(crate) fn response_from_answer_with_kind(answer: Answer, kind: crate::transport::AnswerKind) -> Result<Response, TransportError> {
    match &kind {
        crate::transport::AnswerKind::Refused { code: crate::transport::SessionCode::Unauthenticated } => return Err(TransportError::Http { status: 401, body: answer.body.to_string() }),
        crate::transport::AnswerKind::Refused { code } | crate::transport::AnswerKind::NotConsumed { code } | crate::transport::AnswerKind::PermanentSession { code } => return Err(TransportError::Session { code: *code, detail: answer.body.to_string() }),
        crate::transport::AnswerKind::Revoked => return Err(TransportError::Session { code: crate::transport::SessionCode::SessionEnded, detail: answer.body.to_string() }),
        _ => {}
    }
    if let Some(code) = answer.body.get("error").and_then(Value::as_str) {
        let error = GraphQLError { message: code.to_string(), path: None, extensions: json!({ "code": code }).as_object().cloned() };
        let mut response: Response = serde_json::from_value(answer.body).map_err(|error| TransportError::Decode(error.to_string()))?;
        response.errors.insert(0, error);
        response.kind = Some(kind);
        return Ok(response);
    }
    let mut response: Response = serde_json::from_value(answer.body).map_err(|error| TransportError::Decode(error.to_string()))?;
    response.kind = Some(kind);
    Ok(response)
}

fn response_from_read(answer: Answer, has_document: bool) -> Result<Response, TransportError> {
    let outcome = crate::transport::WireOutcome::Answer { sequence: answer.sequence, body: answer.body.clone() };
    let kind = crate::transport::classify_answer(&outcome, false, has_document);
    response_from_answer_with_kind(answer, kind)
}

#[async_trait]
impl Upstream for EncryptedTransport {
    fn session(&self) -> Result<Option<String>, TransportError> {
        Ok(super::credential::stored_session(&self.store)?.filter(|session| session.sequence > 0).map(|session| session.credential_digest))
    }

    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let path = &self.exchange.config().endpoints.graphql;
        let wire = request.authored()?;
        let response = response_from_read(self.read(path, graphql_body(&request, false), wire).await?, false)?;
        if !matches!(&response.kind, Some(crate::transport::AnswerKind::DocumentMissing)) {
            return Ok(response);
        }
        response_from_read(self.read(path, graphql_body(&request, true), crate::transport::WireIds::new(wire.author, wire.request_id, crate::transport::WireIds::next_attempt())).await?, true)
    }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        let (answer, kind) = self.mutate_owned(&self.exchange.config().endpoints.graphql, graphql_body(&request, true), (request.owner, request.wire.map(|wire| wire.request_id)), request.purpose).await?;
        response_from_answer_with_kind(answer, kind)
    }

    async fn put(&self, request: PutRequest) -> Result<PutReply, TransportError> {
        crate::lock::assert_no_library_lock("Upstream::put");
        self.rest.put(request).await
    }
}
