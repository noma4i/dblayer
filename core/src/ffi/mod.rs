mod database;
mod cable;
mod receipt;
#[cfg(test)]
#[cfg(test)]
pub(crate) async fn settled(database: &Database) -> Result<(), DbError> {
    loop {
        database.operations_quiet().await;
        let owners = database.life.clone();
        database::blocking(move || { owners.barrier(); Ok(()) }).await?;
        if database.life.requests_in_flight() == 0 { return Ok(()); }
        database.life.requests_none().await;
    }
}
#[cfg(test)]
mod close_tests;
mod call_result;
#[cfg(test)]
pub(crate) const EMPTY_SPECS: &str = r#"{"models":[]}"#;
#[cfg(test)]
pub(crate) fn specs(models: impl AsRef<str>) -> String { format!(r#"{{"models":{}}}"#, models.as_ref()) }
mod call_state;
pub(crate) use call_state::CallState;
mod foreign_future;
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
mod query_spec;
#[cfg(test)]
mod query_spec_tests;
pub use query_spec::{QueryCondition, QueryOrder, QuerySpec};
#[cfg(test)]
#[path = "tape_tests.rs"]
pub(crate) mod tape;
mod command;
mod load_order;
mod loading;
mod write;
mod read;
mod queue;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod read_cancel_tests;
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
pub fn user_agent(application: Option<String>) -> String { crate::transport::user_agent(application) }

pub use database::{Config, Database, EdgeRow, SubscriptionCensus, TestClock};
pub use receipt::{ItemId, WireBatchReceipt, WireItemReceipt, WireOperationOutcome, WireOperationReceipt, WireReceiptTicket};
pub use opening::{OpenDatabase, OpenOptions, OpenPorts, OperationDefinition};
pub use write::FileBytes;
pub use encrypted::{fetch_server_config, CredentialStore, EncryptedHttp, RetryProgress, RetryProgressListener, RetrySubscription};
pub use crate::transport::SessionCode;
pub use crate::transport::{AppPlatform, ConfigCode, EncryptedConfig, EncryptedEndpoints, EncryptedOperationClass, EncryptedSessionOperation, SecurityCode, ServerConfig};
pub use loading::{LoadMoreResult, LoadRequest};
pub use command::CommandRequest;
pub use read::{ReadEnd, ReadFetch, ReadPhase, ReadRequest, ReadState, ReadStream, ReadValue, RowEdit};
pub use upload_rounds::{RoundJob, RoundJobsSink, RoundRefusal};
pub use wire::{Cell, CellEntry, WireFailure, WireFile, WireModel, WireRow, WireSync, WireSyncState, WireProcessing, WireUpload, WireUploadStatus};
pub(crate) use queue::queue_of;
pub use queue::{OperationInfo, OperationQueueStatus};
pub use crate::transport::{PutFile, PutReply};
#[cfg(test)]
mod fixtures;
#[cfg(test)]
pub(crate) use fixtures::{EncryptedReply, FakeHttp, FakeTransport, TransportRequest, FAKE_ENDPOINT};
pub use cable::{PlatformSocket, CableEvents, CableOutcome};
pub use crate::transport::{HttpRequest, RawFailure, RawOutcome};
pub use transport::{ByteProgress, OnlineSink, PlatformObserver, PutTarget, Transport};

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
    #[error("row {id} of {model} already exists")]
    AlreadyExists { model: String, id: String },
    #[error("storage: {detail}")]
    Storage { detail: String },
    #[error("offline: {detail}")]
    Offline { detail: String },
    #[error("dropped: {detail}")]
    Dropped { detail: String },
    #[error("unauthorized: {detail}")]
    Unauthorized { detail: String },
    #[error("in flight: {detail}")]
    InFlight { detail: String },
    #[error("runtime: {detail}")]
    Runtime { detail: String },
    #[error("internal: {detail}")]
    Internal { detail: String },
    #[error("copy: {detail}")]
    Copy { detail: String },
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
    /// No database was opened yet.
    #[error("not open: {detail}")]
    NotOpen { detail: String },
}

impl From<crate::store::CoreError> for DbError {
    fn from(error: crate::store::CoreError) -> Self {
        match error {
            crate::store::CoreError::NoSession { detail } => Self::NoSession { detail },
            crate::store::CoreError::AlreadyExists { model, id } => Self::AlreadyExists { model, id },
            crate::store::CoreError::Store(crate::store::StoreError::CorruptManifest(detail)) => Self::CorruptManifest { detail },
            error @ (crate::store::CoreError::InvalidUpload(_)
                | crate::store::CoreError::Schema(_)
                | crate::store::CoreError::Identity(_)
                | crate::store::CoreError::Invalid(_)
                | crate::store::CoreError::UnknownModel(_)
                | crate::store::CoreError::UnknownAction { .. }
                | crate::store::CoreError::UnknownAssociation { .. }
                | crate::store::CoreError::UnknownField { .. }
                | crate::store::CoreError::NotFileField { .. }
                | crate::store::CoreError::NotEphemeralField { .. }
                | crate::store::CoreError::InvalidEphemeralValue { .. }
                | crate::store::CoreError::NotLocalField { .. }
                | crate::store::CoreError::LocalValue { .. }
                | crate::store::CoreError::Selection(_)
                | crate::store::CoreError::Store(crate::store::StoreError::UnknownModel(_))) => Self::Invalid { detail: error.to_string() },
            crate::store::CoreError::InFlight { model, id } => Self::InFlight { detail: format!("row {id} of {model} has an operation in flight") },
            crate::store::CoreError::ThreadSpawn { name, detail } => Self::Runtime { detail: format!("thread {name} did not start: {detail}") },
            crate::store::CoreError::Internal(detail) => Self::Internal { detail },
            error @ (crate::store::CoreError::NotFound { .. } | crate::store::CoreError::UnknownOperation(_)) => Self::NotFound { detail: error.to_string() },
            crate::store::CoreError::Query(crate::store::query::QueryError::Sqlite(detail)) => Self::Storage { detail },
            error @ crate::store::CoreError::Query(_) => Self::Invalid { detail: error.to_string() },
            other => Self::Storage { detail: other.to_string() },
        }
    }
}

impl From<crate::store::StoreError> for DbError {
    fn from(error: crate::store::StoreError) -> Self { crate::store::CoreError::from(error).into() }
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
            TransportError::Unscripted(detail) => Self::Internal { detail },
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
pub(super) mod queue_view_tests;
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
mod input_encoding_tests;
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
mod call_result_tests;
#[cfg(test)]
#[cfg(test)]
mod read_load_tests;
#[cfg(test)]
mod signed_session_tests;
#[cfg(test)]
mod load_all_cancel_tests;
