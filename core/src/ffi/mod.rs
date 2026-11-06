mod database;
mod receipt;
pub(crate) mod admission;
#[cfg(test)]
pub(crate) use database::NEEDS_NETWORK;
#[cfg(test)]
pub(crate) async fn settled(database: &Database) -> Result<(), DbError> {
    let (lifecycle, db) = (database.streams.clone(), database.db.clone());
    let quiet = async {
        loop {
            lifecycle.settled().await;
            database.operations_quiet().await;
            let owners = database.life.clone();
            database::blocking(move || { owners.barrier(std::time::Duration::from_secs(5)); Ok(()) }).await?;
            let listeners = db.clone();
            database::blocking(move || { listeners.await_revision(listeners.revision_seq()?); Ok(()) }).await?;
            if lifecycle.quiet() { return Ok::<(), DbError>(()); }
        }
    };
    match tokio::time::timeout(std::time::Duration::from_secs(10), quiet).await {
        Ok(result) => result,
        Err(_) => Err(DbError::Invalid { detail: format!("settled timed out: {}", lifecycle.held().join("; ")) }),
    }
}
#[cfg(test)]
mod close_tests;
mod foreign_future;
mod lease;
mod open_slot;
mod opening;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod selftest;
#[cfg(test)]
pub(crate) mod database_tests;
mod encrypted;
mod query;
#[cfg(test)]
#[path = "tape_tests.rs"]
pub(crate) mod tape;
mod command;
mod loading;
mod write;
mod read;
#[cfg(test)]
mod read_tests;
mod transport;
mod upload;
mod upload_rounds;
mod transfer_status;
pub use transfer_status::{WireTransferState, WireTransferStatus};
mod event;
pub use event::WireModelEvent;
pub mod upload_wire;
pub mod wire;

#[uniffi::export]
pub fn user_agent(application: Option<String>) -> String {
    let token = concat!("DBlayer/", env!("CARGO_PKG_VERSION"));
    match application.filter(|value| !value.is_empty()) {
        Some(value) => format!("{value} {token}"),
        None => token.into(),
    }
}

pub use database::{Config, Database, SubscriptionCensus, TestClock};
pub use receipt::{ItemId, WireBatchReceipt, WireItemReceipt, WireOperationOutcome, WireOperationReceipt, WireReceiptTicket};
pub use opening::{OpenDatabase, OpenOptions};
pub use encrypted::{fetch_server_config, CredentialStore, EncryptedHttp, EncryptedReply, RetryProgress, RetryProgressListener, RetrySubscription};
pub use crate::transport::SessionCode;
pub use crate::transport::{AppPlatform, ConfigCode, EncryptedConfig, EncryptedEndpoints, EncryptedOperationClass, EncryptedSessionOperation, SecurityCode, ServerConfig};
pub use loading::{LoadMoreResult, LoadRequest};
pub use command::CommandRequest;
pub use read::{ReadEnd, ReadFetch, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, RowPlace, RowRename};
pub use upload_rounds::RoundJobsSink;
pub use wire::{Cell, CellEntry, WireFailure, WireModel, WireRow, WireSync, WireSyncState, WireProcessing, WireUpload, WireUploadStatus};
pub use crate::transport::PutFile;
pub use transport::{ByteProgress, CableLease, EncryptedSocketEvent, OnlineSink, PutReply, SubscriptionSink, Transport, TransportRequest};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, uniffi::Error)]
pub enum DbError {
    #[error("no session: {detail}")]
    NoSession { detail: String },
    #[error("security {code:?}: {detail}")]
    Security { code: SecurityCode, detail: String },
    #[error("config {code:?}: {detail}")]
    Config { code: ConfigCode, detail: String },
    #[error("invalid: {detail}")]
    Invalid { detail: String },
    #[error("not found: {detail}")]
    NotFound { detail: String },
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
    #[error("request is {size} bytes, exceeding the local limit of {limit} bytes")]
    RequestTooLarge { size: u64, limit: u64 },
    #[error("graphql: {}", messages.join("\n"))]
    Graphql { messages: Vec<String>, extensions: Option<crate::transport::GraphqlExtensions> },
    #[error("session {code:?}: {detail}")]
    Session { code: SessionCode, detail: String },
    /// The database is closed.
    #[error("closed: {detail}")]
    Closed { detail: String },
    #[error("corrupt manifest: {detail}")]
    CorruptManifest { detail: String },
}

impl From<crate::db::DbError> for DbError {
    fn from(error: crate::db::DbError) -> Self {
        match error {
            crate::db::DbError::NoSession { detail } => Self::NoSession { detail },
            crate::db::DbError::Store(crate::store::StoreError::CorruptManifest(detail)) => Self::CorruptManifest { detail },
            error @ (crate::db::DbError::InvalidUpload(_)
                | crate::db::DbError::Schema(_)
                | crate::db::DbError::Identity(_)
                | crate::db::DbError::Invalid(_)
                | crate::db::DbError::UnknownModel(_)
                | crate::db::DbError::UnknownAction { .. }
                | crate::db::DbError::UnknownAssociation { .. }
                | crate::db::DbError::UnknownField { .. }
                | crate::db::DbError::NotFileField { .. }
                | crate::db::DbError::NotEphemeralField { .. }
                | crate::db::DbError::InvalidEphemeralValue { .. }
                | crate::db::DbError::NotLocalField { .. }
                | crate::db::DbError::LocalValue { .. }
                | crate::db::DbError::Selection(_)
                | crate::db::DbError::Store(crate::store::StoreError::UnknownModel(_))) => Self::Invalid { detail: error.to_string() },
            crate::db::DbError::Query(crate::query::QueryError::Sqlite(detail)) => Self::Database { detail },
            error @ crate::db::DbError::Query(_) => Self::Invalid { detail: error.to_string() },
            other => Self::Database { detail: other.to_string() },
        }
    }
}

impl From<crate::transport::TransportError> for DbError {
    fn from(error: crate::transport::TransportError) -> Self {
        use crate::transport::TransportError;
        match error {
            TransportError::Security { code, detail } => Self::Security { code, detail },
            TransportError::Config { code, detail } => Self::Config { code, detail },
            TransportError::Http { status, body } => Self::Http { status, detail: body },
            TransportError::RequestTooLarge { size, limit } => Self::RequestTooLarge { size, limit },
            TransportError::Network(detail) => Self::Network { detail },
            TransportError::Interrupted(detail) => Self::Interrupted { detail },
            TransportError::Decode(detail) => Self::Invalid { detail },
            TransportError::Session { code, detail } => Self::Session { code, detail },
            TransportError::Graphql { messages, extensions } => Self::Graphql { messages, extensions },
            other => Self::Sync { detail: other.to_string() },
        }
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self {
        #[cfg(test)]
        offload_tests::note_parse_failure(&error);
        Self::Invalid { detail: error.to_string() }
    }
}

#[cfg(test)]
pub(super) mod encrypted_tests;
#[cfg(test)]
mod lost_session_recovery_tests;
#[cfg(test)]
mod online_tests;
#[cfg(test)]
mod queued_detail_tests;
#[cfg(test)]
mod sealed_errors_tests;
#[cfg(test)]
mod session_guards_tests;
#[cfg(test)]
mod open_layout_tests;
#[cfg(test)]
#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod failure_tests;
#[cfg(test)]
mod offload_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod session_restore_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
#[cfg(test)]
mod local_open_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod write_calls_tests;
#[cfg(test)]
#[cfg(test)]
mod read_load_tests;
#[cfg(test)]
mod signed_session_tests;
#[cfg(test)]
mod load_all_cancel_tests;
