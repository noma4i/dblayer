use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDescriptor {
    pub id: String,
    pub size: u64,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub sha256: Option<String>,
}

impl FileDescriptor {
    pub fn to_value(&self) -> Value {
        json!({"id": self.id, "name": self.name, "mime": self.mime, "size": self.size, "sha256": self.sha256})
    }

    pub fn from_value(value: &Value) -> Option<Self> {
        let object: &Map<String, Value> = value.as_object()?;
        let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
        Some(Self { id: text("id")?, size: object.get("size")?.as_u64()?, name: text("name"), mime: text("mime"), sha256: text("sha256") })
    }
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}
