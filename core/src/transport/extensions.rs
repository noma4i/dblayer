use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum TransactionDisposition {
    Finish,
    Keep,
    Unknown { value: String },
}

impl TransactionDisposition {
    pub fn from_text(value: &str) -> Self {
        match value {
            "FINISH" => Self::Finish,
            "KEEP" => Self::Keep,
            _ => Self::Unknown { value: value.to_string() },
        }
    }

    pub fn as_text(&self) -> &str {
        match self {
            Self::Finish => "FINISH",
            Self::Keep => "KEEP",
            Self::Unknown { value } => value,
        }
    }
}

impl Serialize for TransactionDisposition {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_text())
    }
}

impl<'de> Deserialize<'de> for TransactionDisposition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_text(&String::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct GraphqlExtensions {
    pub code: Option<String>,
    pub transaction_disposition: Option<TransactionDisposition>,
    pub fields: std::collections::HashMap<String, String>,
}

impl super::Response {
    pub fn extensions(&self) -> Option<GraphqlExtensions> {
        let fields = self.errors.first()?.extensions.as_ref()?;
        Some(GraphqlExtensions {
            code: fields.get("code").and_then(serde_json::Value::as_str).map(str::to_string),
            transaction_disposition: fields.get("transactionDisposition").and_then(serde_json::Value::as_str).map(TransactionDisposition::from_text),
            fields: fields.iter().filter(|(key, _)| !matches!(key.as_str(), "code" | "transactionDisposition")).map(|(key, value)| (key.clone(), value.to_string())).collect(),
        }).filter(|extensions| extensions.code.is_some() || extensions.transaction_disposition.is_some() || !extensions.fields.is_empty())
    }
}

#[uniffi::export]
pub fn graphql_extensions(response_json: String) -> Result<Option<GraphqlExtensions>, crate::ffi::DbError> {
    Ok(serde_json::from_str::<super::Response>(&response_json)?.extensions())
}

#[uniffi::export]
pub fn transaction_disposition(value: String) -> TransactionDisposition {
    TransactionDisposition::from_text(&value)
}

#[uniffi::export]
pub fn transaction_disposition_text(disposition: TransactionDisposition) -> String {
    disposition.as_text().to_string()
}
