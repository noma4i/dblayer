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
        let value: Value = serde_json::from_str(text)?;
        if !value.is_object() { return Err(serde_json::Error::custom("the specs are an object with models and operations")); }
        serde_json::from_value(value)
    }
}
