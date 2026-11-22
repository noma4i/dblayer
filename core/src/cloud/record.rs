use super::StampedValue;
use crate::identity::RowId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_RECORD_BYTES: usize = 900_000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CloudRecord {
    pub(crate) model: String,
    pub(crate) row_id: String,
    pub(crate) server_revision: Option<i64>,
    pub(crate) deleted: bool,
    pub(crate) fields: Vec<CloudField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudField {
    pub(crate) path: Vec<String>,
    pub(crate) data: StampedValue,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordError {
    TooLarge,
    Invalid,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    format: u32,
    model: String,
    row_id: String,
    server_revision: Option<i64>,
    deleted: bool,
    fields: Vec<CloudField>,
}

pub(crate) fn record_key(model: &str, row_id: &str) -> String {
    let mut hash = Sha256::new();
    for component in [model, row_id] {
        hash.update((component.len() as u64).to_be_bytes());
        hash.update(component.as_bytes());
    }
    hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn canonicalize(record: &mut CloudRecord) -> Result<(), RecordError> {
    let id = RowId::new(&record.row_id).map_err(|_| RecordError::Invalid)?;
    if record.model.trim().is_empty() || id.is_temp() || id.as_str() != record.row_id || (record.deleted && !record.fields.is_empty()) {
        return Err(RecordError::Invalid);
    }
    record.fields.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    if record.fields.iter().any(|field| field.path.is_empty() || field.path.iter().any(|segment| segment.is_empty()))
        || record.fields.windows(2).any(|pair| pair[0].path == pair[1].path) {
        return Err(RecordError::Invalid);
    }
    Ok(())
}

pub(crate) fn encode(record: &CloudRecord) -> Result<Vec<u8>, RecordError> {
    let mut record = record.clone();
    canonicalize(&mut record)?;
    let wire = WireRecord { format: 1, model: record.model, row_id: record.row_id, server_revision: record.server_revision, deleted: record.deleted, fields: record.fields };
    let bytes = serde_json::to_vec(&wire).map_err(|_| RecordError::Invalid)?;
    if bytes.len() > MAX_RECORD_BYTES { return Err(RecordError::TooLarge); }
    Ok(bytes)
}

pub(crate) fn decode(key: &str, bytes: &[u8]) -> Result<CloudRecord, RecordError> {
    if bytes.len() > MAX_RECORD_BYTES { return Err(RecordError::TooLarge); }
    let wire: WireRecord = serde_json::from_slice(bytes).map_err(|_| RecordError::Invalid)?;
    if wire.format != 1 || key != record_key(&wire.model, &wire.row_id) { return Err(RecordError::Invalid); }
    let mut record = CloudRecord { model: wire.model, row_id: wire.row_id, server_revision: wire.server_revision, deleted: wire.deleted, fields: wire.fields };
    canonicalize(&mut record)?;
    Ok(record)
}
