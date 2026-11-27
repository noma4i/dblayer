use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::response::ResponseShape;

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NetworkPurpose {
    Standard,
    Transfer,
}

/// Why a request leaves for the network: the author of the request names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum RequestReason {
    Load,
    Refresh,
    Event,
    Reconnect,
    Retry,
    Send,
}

/// One request that passed the gate, as the `requests` stream shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRecord {
    pub operation: String,
    pub variables_json: String,
    pub reason: RequestReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOwner {
    Operation { seq: i64 },
    Transfer { operation_id: i64, item_id: String, step: crate::owners::write::upload::UploadStage },
    /// The abort of a multipart debt: the pair lives after the transfer is gone (D-280).
    Orphan { upload_id: String },
    Unmatched { operation_id: i64 },
    Detached,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphQLRequest {
    pub document: String,
    pub operation_name: String,
    pub variables: Value,
    pub owner: Option<RequestOwner>,
    pub purpose: NetworkPurpose,
    pub wire: Option<super::WireIds>,
    pub reason: RequestReason,
    pub(crate) epoch: Option<crate::runtime::Epoch>,
}

impl GraphQLRequest {
    pub fn new(document: impl Into<String>, operation_name: impl Into<String>, variables: Value) -> Self {
        Self { document: document.into(), operation_name: operation_name.into(), variables, owner: None, purpose: NetworkPurpose::Standard, wire: None, reason: RequestReason::Send, epoch: None }
    }

    pub fn why(self, reason: RequestReason) -> Self { Self { reason, ..self } }

    /// The author and the id the author gave the request: a request that reaches the platform carries them in `HttpRequest`.
    pub fn by(self, wire: super::WireIds) -> Self { Self { wire: Some(wire), ..self } }

    pub fn authored(&self) -> Result<super::WireIds, super::TransportError> {
        self.wire.clone().ok_or_else(|| super::TransportError::Config { code: super::ConfigCode::InvalidRequest, detail: "the request has no author".into() })
    }

}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphQLError {
    pub message: String,
    #[serde(default)]
    pub path: Option<Vec<Value>>,
    #[serde(default)]
    pub extensions: Option<Map<String, Value>>,
}

impl GraphQLError {
    pub fn code(&self) -> Option<&str> {
        self.extensions.as_ref()?.get("code")?.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Response {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub errors: Vec<GraphQLError>,
    #[serde(skip)]
    pub kind: Option<super::AnswerKind>,
}

impl Response {
    /// An error `NOT_FOUND` on path `node` is the answer "no such row" of a node read, no failure.
    pub fn absent_node(&self) -> bool {
        !self.errors.is_empty() && self.errors.iter().all(|error| error.code() == Some("NOT_FOUND") && error.path.as_deref() == Some(&[Value::String("node".into())]))
    }

    pub fn data(value: Value) -> Self {
        Self { data: Some(value), errors: Vec::new(), kind: None }
    }

    pub fn answer_kind(&self) -> Result<super::AnswerKind, serde_json::Error> {
        if let Some(kind) = &self.kind { return Ok(kind.clone()); }
        let body = serde_json::to_value(self)?;
        Ok(super::classify_answer(&super::WireOutcome::Answer { sequence: None, body }, false, true))
    }

    pub fn shape(&self) -> ResponseShape {
        ResponseShape::of(self.data.is_some(), self.errors.len(), false)
    }
}
