use crate::transport::{GraphQLRequest, RequestOwner, TransferIdentity, TransferStep};
use serde_json::Value;

use super::UploadStage;

#[derive(Clone)]
pub(crate) enum Transfer {
    Active { operation_id: i64, item_id: String },
    Abort { operation_id: i64, item_id: String, id: i64, operation: String, key: String, upload_id: String },
}

impl Transfer {
    pub fn owner(&self, step: UploadStage) -> RequestOwner {
        let (operation_id, item_id) = match self {
            Self::Active { operation_id, item_id } | Self::Abort { operation_id, item_id, .. } => (*operation_id, item_id.clone()),
        };
        RequestOwner::Transfer { operation_id, item_id, step }
    }

    pub fn request(&self, document: &str, name: &str, variables: Value, step: UploadStage) -> GraphQLRequest {
        GraphQLRequest { document: document.into(), operation_name: name.into(), variables, owner: Some(self.owner(step)), purpose: crate::transport::NetworkPurpose::Transfer }
    }

    pub fn put(&self, step: TransferStep) -> TransferIdentity {
        let (operation_id, item_id) = match self {
            Self::Active { operation_id, item_id } | Self::Abort { operation_id, item_id, .. } => (*operation_id, item_id.clone()),
        };
        TransferIdentity { operation_id, item_id, step }
    }
}
