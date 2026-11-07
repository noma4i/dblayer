use serde::de::Error;

use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationSpec {
    pub name: String,
    pub returns: FieldSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Specs {
    pub models: Vec<ModelSpec>,
    #[serde(default)]
    pub operations: Vec<OperationSpec>,
}

impl Specs {
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        match serde_json::from_str::<Value>(text)? {
            Value::Array(models) => Ok(Self { models: serde_json::from_value(Value::Array(models))?, operations: Vec::new() }),
            Value::Object(fields) => serde_json::from_value(Value::Object(fields)),
            _ => Err(serde_json::Error::custom("the specs are a list of models or an object with models and operations")),
        }
    }
}
