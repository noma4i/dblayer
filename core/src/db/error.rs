use crate::commit::WriteOrigin;
use crate::identity::IdentityError;
use crate::query::QueryError;
use crate::schema::{ModelFault, SchemaError};
use crate::store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
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
    #[error("model {model} has no association {name}")]
    UnknownAssociation { model: String, name: String },
    #[error("association {name} of {model} has another kind")]
    WrongAssociation { model: String, name: String },
    #[error("field {field} of {model} is not a file field")]
    NotFileField { model: String, field: String },
    #[error("field {field} of {model} is not an ephemeral field")]
    NotEphemeralField { model: String, field: String },
    #[error("value for ephemeral field {field} of {model} is invalid")]
    InvalidEphemeralValue { model: String, field: String },
    #[error("row {id} of {model} does not exist")]
    NotFound { model: String, id: String },
    #[error("origin {0:?} cannot be landed")]
    OriginNotLandable(WriteOrigin),
    #[error("row {id} of {model} already exists")]
    AlreadyExists { model: String, id: String },
    #[error("row {id} of {model} has an operation in flight")]
    InFlight { model: String, id: String },
    #[error("operation {0} is not in the outbox")]
    UnknownOperation(i64),
    #[error(transparent)]
    Landing(#[from] crate::read::LandingError),
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        DbError::Store(error.into())
    }
}
