use crate::db::DbError;
use crate::store::StoreError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    #[error("model {0} is not registered")]
    UnknownModel(String),
    #[error("relation {relation} of {model} does not exist")]
    UnknownRelation { model: String, relation: String },
    #[error("relation {relation} of {model} has no remote")]
    NotRemote { model: String, relation: String },
    #[error("relation {relation} of {model} is not paged")]
    NotPaged { model: String, relation: String },
    #[error("operation {0} is not registered")]
    UnknownOperation(String),
    #[error("freshness policy {0} is unknown")]
    UnknownFreshness(String),
    #[error("params are invalid: {0}")]
    Params(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("graphql: {0}")]
    Graphql(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("event is invalid: {0}")]
    Event(String),
    #[error("store: {0}")]
    Store(String),
    #[error("load was cancelled")]
    Cancelled,
}

impl From<DbError> for SyncError {
    fn from(error: DbError) -> Self {
        SyncError::Store(error.to_string())
    }
}

impl From<StoreError> for SyncError {
    fn from(error: StoreError) -> Self {
        SyncError::Store(error.to_string())
    }
}
