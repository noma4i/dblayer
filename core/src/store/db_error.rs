use crate::identity::IdentityError;
use crate::store::query::QueryError;
use crate::schema::{ModelFault, SchemaError};
use crate::store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("no session: {detail}")]
    NoSession { detail: String },
    #[error("invalid upload: {0}")]
    InvalidUpload(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("invalid models: {0:?}")]
    Invalid(Vec<ModelFault>),
    #[error("model {0} is not registered")]
    UnknownModel(String),
    #[error("model {model} declares no action {action}")]
    UnknownAction { model: String, action: String },
    #[error("model {model} has no association {name}")]
    UnknownAssociation { model: String, name: String },
    #[error("model {model} declares no field {field}")]
    UnknownField { model: String, field: String },
    #[error("field {field} of {model} is not a file field")]
    NotFileField { model: String, field: String },
    #[error("field {field} of {model} is not an ephemeral field")]
    NotEphemeralField { model: String, field: String },
    #[error("value for ephemeral field {field} of {model} is invalid")]
    InvalidEphemeralValue { model: String, field: String },
    #[error("{path} of {model} is not a local field")]
    NotLocalField { model: String, path: String },
    #[error("value for local field {path} of {model} is invalid")]
    LocalValue { model: String, path: String },
    #[error("row {id} of {model} does not exist")]
    NotFound { model: String, id: String },
    #[error("row {id} of {model} already exists")]
    AlreadyExists { model: String, id: String },
    #[error("row {id} of {model} has an operation in flight")]
    InFlight { model: String, id: String },
    #[error("operation {0} is not in the outbox")]
    UnknownOperation(i64),
    #[error("thread {name} did not start: {detail}")]
    ThreadSpawn { name: &'static str, detail: String },
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Landing(#[from] crate::read::LandingError),
    #[error(transparent)]
    Selection(#[from] crate::schema::SelectionKeyError),
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        DbError::Store(error.into())
    }
}
