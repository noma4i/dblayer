use crate::owners::contract::RejectReason;
use crate::store::PlanFault;
use crate::store::ddl::DdlError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(rusqlite::Error),
    #[error(transparent)]
    Ddl(#[from] DdlError),
    #[error(transparent)]
    Plan(#[from] PlanFault),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("model {0} is not registered")]
    UnknownModel(String),
    #[error("store is poisoned: {0}")]
    Poisoned(String),
    #[error("the row does not decode: {0}")]
    Invalid(String),
    #[error("constraint violated: {0}")]
    Constraint(String),
    #[error("the database is closed or its state moved")]
    Refused,
    #[error("{0} is already attached")]
    AlreadyAttached(&'static str),
    #[error("corrupt manifest: {0}")]
    CorruptManifest(String),
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        match &error {
            rusqlite::Error::SqliteFailure(failure, _)
                if matches!(failure.code, rusqlite::ErrorCode::ConstraintViolation | rusqlite::ErrorCode::TypeMismatch | rusqlite::ErrorCode::TooBig | rusqlite::ErrorCode::ParameterOutOfRange) =>
            {
                StoreError::Constraint(error.to_string())
            }
            _ => StoreError::Sqlite(error),
        }
    }
}

impl StoreError {
    pub(crate) fn reject_reason(&self) -> RejectReason {
        match self {
            StoreError::Constraint(_) | StoreError::Plan(_) | StoreError::Json(_) | StoreError::UnknownModel(_) | StoreError::Invalid(_) => RejectReason::Invalid,
            StoreError::Sqlite(_)
            | StoreError::Ddl(_)
            | StoreError::Io(_)
            | StoreError::Poisoned(_)
            | StoreError::Refused
            | StoreError::AlreadyAttached(_)
            | StoreError::CorruptManifest(_) => RejectReason::Storage(self.to_string()),
        }
    }
    pub(crate) fn is_apply_failure(&self) -> bool {
        match self {
            StoreError::Sqlite(rusqlite::Error::SqliteFailure(failure, _)) => !matches!(
                failure.code,
                rusqlite::ErrorCode::DiskFull
                    | rusqlite::ErrorCode::SystemIoFailure
                    | rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::DatabaseLocked
                    | rusqlite::ErrorCode::ReadOnly
            ),
            StoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(_)) => false,
            StoreError::Sqlite(_) => true,
            _ => false,
        }
    }
}
