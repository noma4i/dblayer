use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{CloudBlob, CloudChange, CloudError};

const FORMAT: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Record {
    v: u32,
    model: String,
    row_id: String,
    field: String,
    clock_ms: i64,
    clock_count: u32,
    replica: String,
    value: Value,
}

impl CloudChange {
    pub fn blob(&self) -> CloudBlob {
        let name = [self.replica.as_str(), &self.model, &self.row_id, &self.field].join("\u{1f}");
        let key = Sha256::digest(name.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
        let record = Record {
            v: FORMAT,
            model: self.model.clone(),
            row_id: self.row_id.clone(),
            field: self.field.clone(),
            clock_ms: self.clock_ms,
            clock_count: self.clock_count,
            replica: self.replica.clone(),
            value: self.value.clone(),
        };
        CloudBlob { key, bytes: serde_json::to_vec(&record).unwrap_or_default() }
    }

    pub fn from_blob(bytes: &[u8]) -> Result<CloudChange, CloudError> {
        let record: Record = serde_json::from_slice(bytes).map_err(|error| CloudError::Rejected { detail: format!("malformed cloud record: {error}") })?;
        if record.v != FORMAT {
            return Err(CloudError::Rejected { detail: format!("unknown cloud record format {}", record.v) });
        }
        Ok(CloudChange { model: record.model, row_id: record.row_id, field: record.field, clock_ms: record.clock_ms, clock_count: record.clock_count, replica: record.replica, value: record.value })
    }
}
