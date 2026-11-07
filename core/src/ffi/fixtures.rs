use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use crate::transport::{self as core, RawFailureKind, RawOutcome};
use crate::transport::encrypted::cable::peer_tests::CablePeer;
use super::{ByteProgress, CableEvents, CableOutcome, DbError, EncryptedHttp, Transport};

pub(crate) const FAKE_ENDPOINT: &str = "https://graphql.test/graphql";

#[derive(Clone)]
pub(crate) struct TransportRequest {
    pub document: String,
    pub operation_name: String,
    pub variables_json: String,
    pub purpose: core::NetworkPurpose,
}

pub(crate) struct EncryptedReply { pub status: u16, pub body: String }

/// Scripted GraphQL and PUT answers as values and errors; the blanket impl turns them into the raw outcomes of the platform port.
#[async_trait]
pub(crate) trait FakeTransport: Send + Sync {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!("no scripted query") }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!("no scripted mutation") }
    async fn put(&self, _: String, _: HashMap<String, String>, _: core::PutFile, _: core::TransferIdentity, _: Arc<ByteProgress>) -> Result<core::PutReply, DbError> { unreachable!("no scripted put") }
    fn cable(&self) -> Option<&CablePeer> { None }
    fn cable_opened(&self, _: &str) {}
}

/// Scripted encrypted POST answers addressed by path.
#[async_trait]
pub(crate) trait FakeHttp: Send + Sync {
    async fn post(&self, path: String, body: String, purpose: core::NetworkPurpose) -> Result<EncryptedReply, DbError>;
}

fn failed(error: DbError) -> RawOutcome {
    let message = match &error {
        DbError::Network { detail } | DbError::Offline { detail } | DbError::Interrupted { detail } | DbError::Security { detail, .. } | DbError::Http { detail, .. } => detail.clone(),
        other => other.to_string(),
    };
    match error {
        DbError::Http { status, detail } => RawOutcome::response(status, detail),
        DbError::Network { .. } | DbError::Offline { .. } => RawOutcome::failed(RawFailureKind::Unreachable, message),
        DbError::Interrupted { .. } => RawOutcome::failed(RawFailureKind::Cancelled, message),
        DbError::Security { code: core::SecurityCode::TlsTrust, .. } => RawOutcome::failed(RawFailureKind::Tls, message),
        DbError::Graphql { messages, .. } => RawOutcome::response(200, serde_json::json!({"errors": messages.iter().map(|text| serde_json::json!({"message": text, "extensions": {"code": text}})).collect::<Vec<_>>()}).to_string()),
        _ => RawOutcome::failed(RawFailureKind::Io, message),
    }
}

fn path_of(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.find('/').map_or_else(String::new, |at| rest[at..].to_string())
}

#[async_trait]
impl<T: FakeTransport> Transport for T {
    fn endpoint(&self) -> String { FAKE_ENDPOINT.into() }

    async fn send(&self, request: core::HttpRequest) -> RawOutcome {
        let body: Value = serde_json::from_str(&request.body).expect("GraphQL body");
        let scripted = TransportRequest {
            document: body["query"].as_str().unwrap_or_default().to_string(),
            operation_name: body["operationName"].as_str().unwrap_or_default().to_string(),
            variables_json: body["variables"].to_string(),
            purpose: request.purpose,
        };
        let answer = if matches!(scripted.document.split_whitespace().next(), Some("mutation")) { self.mutate(scripted).await } else { self.query(scripted).await };
        answer.map_or_else(failed, |text| RawOutcome::response(200, text))
    }

    async fn open_cable(&self, _: String, _: u64, events: Arc<dyn CableEvents>) -> CableOutcome {
        match self.cable() {
            Some(peer) => {
                let (token, outcome) = peer.connect(events);
                self.cable_opened(&token);
                outcome
            }
            None => CableOutcome::Failed { failure: core::RawFailure { kind: RawFailureKind::Unreachable, code: None, message: "no cable".into() } },
        }
    }

    async fn put(&self, url: String, headers: HashMap<String, String>, file: core::PutFile, identity: core::TransferIdentity, progress: Arc<ByteProgress>) -> RawOutcome {
        match FakeTransport::put(self, url, headers, file, identity, progress).await {
            Ok(reply) => RawOutcome::Response { status: reply.status, etag: reply.etag, body: String::new() },
            Err(error) => failed(error),
        }
    }
}

#[async_trait]
impl<T: FakeHttp> EncryptedHttp for T {
    async fn send(&self, request: core::HttpRequest) -> RawOutcome {
        match self.post(path_of(&request.url), request.body, request.purpose).await {
            Ok(reply) => RawOutcome::response(reply.status, reply.body),
            Err(error) => failed(error),
        }
    }
}
