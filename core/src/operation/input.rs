use serde_json::{Map, Value};

use super::status::{Operation, OperationStatus};
use crate::identity::RowId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InputFault {
    #[error("action input must be a JSON object")]
    NotAnObject,
    #[error("action input must not be empty")]
    EmptyInput,
}

pub fn encode_input(fields: &Map<String, Value>) -> Result<Vec<u8>, InputFault> {
    if fields.is_empty() {
        return Err(InputFault::EmptyInput);
    }
    serde_json::to_vec(fields).map_err(|_| InputFault::NotAnObject)
}

pub fn decode_input(data: &[u8]) -> Result<Map<String, Value>, InputFault> {
    match serde_json::from_slice(data) {
        Ok(Value::Object(fields)) => Ok(fields),
        _ => Err(InputFault::NotAnObject),
    }
}

pub fn input_round_trips(fields: &Map<String, Value>) -> bool {
    encode_input(fields).and_then(|data| decode_input(&data)).is_ok_and(|decoded| &decoded == fields)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedOperation {
    pub operation: Operation,
    pub input: Vec<u8>,
}

impl EncodedOperation {
    pub fn make(action_key: &str, fields: &Map<String, Value>, temp_id: Option<RowId>, created_at: i64) -> Result<Self, InputFault> {
        let input = encode_input(fields)?;
        let mut operation = Operation::new(action_key, temp_id.iter().cloned().collect(), 0, created_at);
        operation.temp_id = temp_id;
        Ok(Self { operation, input })
    }

    pub fn is_resumable(&self) -> bool {
        matches!(self.operation.status, OperationStatus::Pending | OperationStatus::DeliveryUnknown)
    }
}
