use crate::db::DbError;
use crate::store::StoreError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    #[error("no session: {0}")]
    NoSession(String),
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
    Transport(crate::transport::TransportError),
    #[error("offline")]
    Offline,
    #[error("graphql: {}", messages.join("\n"))]
    Graphql { messages: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("event is invalid: {0}")]
    Event(String),
    #[error("store: {0}")]
    Store(String),
    #[error("load was cancelled")]
    Cancelled,
    #[error("loading all of {relation} of {model} stopped early: {reason}")]
    Incomplete { model: String, relation: String, reason: String },
}

impl From<DbError> for SyncError {
    fn from(error: DbError) -> Self {
        match error {
            DbError::NoSession { detail } => SyncError::NoSession(detail),
            other => SyncError::Store(other.to_string()),
        }
    }
}

impl From<StoreError> for SyncError {
    fn from(error: StoreError) -> Self {
        SyncError::Store(error.to_string())
    }
}
