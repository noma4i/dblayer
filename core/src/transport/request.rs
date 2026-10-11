use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::response::ResponseShape;

#[derive(Debug, Clone, PartialEq)]
pub struct GraphQLRequest {
    pub document: String,
    pub operation_name: String,
    pub variables: Value,
}

impl GraphQLRequest {
    pub fn new(document: impl Into<String>, operation_name: impl Into<String>, variables: Value) -> Self {
        Self { document: document.into(), operation_name: operation_name.into(), variables }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Response {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub errors: Vec<GraphQLError>,
}

impl Response {
    pub fn data(value: Value) -> Self {
        Self { data: Some(value), errors: Vec::new() }
    }

    pub fn shape(&self) -> ResponseShape {
        ResponseShape::of(self.data.is_some(), self.errors.len(), false)
    }
}
