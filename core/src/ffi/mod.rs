mod database;
mod encrypted;
mod entities;
mod live;
mod retention;
mod state;
mod transport;
pub mod wire;

pub use database::{Config, Database, SubscriptionCensus, TestClock};
pub use encrypted::{CredentialStore, EncryptedHttp, EncryptedReply};
pub use crate::transport::SessionCode;
pub use crate::transport::{EncryptedConfig, ServerConfig, ServerKeyInfo};
pub use entities::{EntityDelivery, EntityListener, LiveEntities};
pub use live::{LiveChange, LiveSubscription, QueryListener};
pub use retention::{RetentionChange, RetentionPool};
pub use state::{RelationStateChange, StateListener, StateSubscription};
pub use wire::{Cell, CellEntry, WireFailure, WireModel, WireRow, WireSync, WireSyncState, WireUpload, WireUploadStatus};
pub use transport::{ByteProgress, OnlineSink, PutReply, SubscriptionSink, Transport, TransportRequest};

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum DbError {
    #[error("invalid: {detail}")]
    Invalid { detail: String },
    #[error("database: {detail}")]
    Database { detail: String },
    #[error("sync: {detail}")]
    Sync { detail: String },
    #[error("network: {detail}")]
    Network { detail: String },
    #[error("interrupted: {detail}")]
    Interrupted { detail: String },
    #[error("http {status}: {detail}")]
    Http { status: u16, detail: String },
    #[error("graphql: {}", messages.join("\n"))]
    Graphql { messages: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    #[error("session {code:?}: {detail}")]
    Session { code: SessionCode, detail: String },
}

impl From<crate::db::DbError> for DbError {
    fn from(error: crate::db::DbError) -> Self { Self::Database { detail: error.to_string() } }
}

impl From<crate::sync::SyncError> for DbError {
    fn from(error: crate::sync::SyncError) -> Self {
        use crate::sync::SyncError;
        use crate::transport::TransportError;
        match error {
            SyncError::Transport(TransportError::Http { status, body }) => Self::Http { status, detail: body },
            SyncError::Transport(TransportError::Network(detail)) => Self::Network { detail },
            SyncError::Transport(TransportError::Interrupted(detail)) => Self::Interrupted { detail },
            SyncError::Transport(TransportError::Session { code, detail }) => Self::Session { code, detail },
            SyncError::Transport(TransportError::Graphql { messages, extensions }) | SyncError::Graphql { messages, extensions } => Self::Graphql { messages, extensions },
            other => Self::Sync { detail: other.to_string() },
        }
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self { Self::Invalid { detail: error.to_string() } }
}

#[cfg(test)]
pub(super) mod encrypted_tests;
#[cfg(test)]
mod lost_session_recovery_tests;
#[cfg(test)]
mod queued_detail_tests;
#[cfg(test)]
mod sealed_errors_tests;
#[cfg(test)]
mod session_guards_tests;
#[cfg(test)]
mod session_resume_effect_tests;
#[cfg(test)]
mod upload_resume_tests;
#[cfg(test)]
mod upload_process_tests;
#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod failure_tests;
#[cfg(test)]
mod file_calls_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod stable_order_tests;
#[cfg(test)]
mod local_open_tests;
#[cfg(test)]
mod write_calls_tests;
#[cfg(test)]
mod retention_tests;
