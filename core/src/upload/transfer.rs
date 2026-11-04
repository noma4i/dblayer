use crate::transport::{GraphQLRequest, RequestOwner, TransferIdentity, TransferStep};
use serde_json::Value;

use super::UploadStage;

#[derive(Clone)]
pub(crate) struct Transfer {
    pub operation_id: i64,
    pub item_id: String,
}

pub(crate) struct AbortTransfer {
    pub id: i64,
    pub transfer: Transfer,
    pub operation: String,
    pub key: String,
    pub upload_id: String,
}

impl AbortTransfer {
    pub fn variables(&self) -> Value {
        serde_json::json!({"key": &self.key, "uploadId": &self.upload_id})
    }
}

impl Transfer {
    pub fn owner(&self, step: UploadStage) -> RequestOwner {
        RequestOwner::Transfer { operation_id: self.operation_id, item_id: self.item_id.clone(), step }
    }

    pub fn request(&self, document: &str, name: &str, variables: Value, step: UploadStage) -> GraphQLRequest {
        GraphQLRequest { document: document.into(), operation_name: name.into(), variables, owner: Some(self.owner(step)), purpose: crate::transport::NetworkPurpose::Transfer }
    }

    pub fn put(&self, step: TransferStep) -> TransferIdentity {
        TransferIdentity { operation_id: self.operation_id, item_id: self.item_id.clone(), step }
    }
}
