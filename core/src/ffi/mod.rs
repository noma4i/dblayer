mod database;
#[cfg(test)]
pub(crate) async fn settled(database: &Database) -> Result<(), DbError> {
    let (lifecycle, db) = (database.streams.clone(), database.db.clone());
    let quiet = async {
        loop {
            lifecycle.settled().await;
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
#[cfg(test)]
mod pending_registration_tests;
mod foreign_future;
mod lease;
mod open_slot;
mod opening;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod selftest;
#[cfg(test)]
mod database_tests;
mod encrypted;
pub(crate) mod stream;
mod command;
mod related;
#[cfg(test)]
mod related_tests;
mod transport;
mod upload_accepted;
mod blob;
mod event;
pub use blob::WireBlobSnapshot;
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

pub use database::{Config, Database, ItemId, SubscriptionCensus, TestClock, WireBatchOutcome, WireBatchReceipt, WireBatchResult, WireBatchTicket, WireItemOutcome, WireItemReceipt, WireOperationOutcome, WireOperationReceipt, WireReceiptTicket};
pub use opening::{OpenDatabase, OpenOptions};
pub use encrypted::{fetch_server_config, CredentialStore, EncryptedHttp, EncryptedReply, RetryProgress, RetryProgressListener, RetrySubscription};
pub use crate::transport::SessionCode;
pub use crate::transport::{AppPlatform, ConfigCode, EncryptedConfig, EncryptedEndpoints, EncryptedSessionOperation, SecurityCode, ServerConfig};
pub use stream::{CachedQuery, EntityFetch, LivePhase, LoadMoreResult, Placement, Replacement, StreamCensus, StreamDelivery, StreamEnd, StreamRequest, StreamState, StreamValue, ValueSink};
pub use command::CommandRequest;
pub use upload_accepted::{UploadAcceptedSink, WireAcceptedUpload, WireRoundStep, WireUploadAccepted};
pub use wire::{Cell, CellEntry, WireFailure, WireModel, WireRow, WireSync, WireSyncState, WireUpload, WireUploadStatus};
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
}

impl From<crate::db::DbError> for DbError {
    fn from(error: crate::db::DbError) -> Self {
        match error {
            crate::db::DbError::NoSession { detail } => Self::NoSession { detail },
            other => Self::Database { detail: other.to_string() },
        }
    }
}

impl From<crate::sync::SyncError> for DbError {
    fn from(error: crate::sync::SyncError) -> Self {
        use crate::sync::SyncError;
        use crate::transport::TransportError;
        match error {
            SyncError::NoSession(detail) => Self::NoSession { detail },
            SyncError::Transport(TransportError::Security { code, detail }) => Self::Security { code, detail },
            SyncError::Transport(TransportError::Config { code, detail }) => Self::Config { code, detail },
            SyncError::Transport(TransportError::Http { status, body }) => Self::Http { status, detail: body },
            SyncError::Transport(TransportError::RequestTooLarge { size, limit }) => Self::RequestTooLarge { size, limit },
            SyncError::Transport(TransportError::Network(detail)) => Self::Network { detail },
            SyncError::Transport(TransportError::Interrupted(detail)) => Self::Interrupted { detail },
            SyncError::Transport(TransportError::Decode(detail)) => Self::Invalid { detail },
            SyncError::Cancelled => Self::Interrupted { detail: error.to_string() },
            SyncError::Offline => Self::Network { detail: error.to_string() },
            SyncError::Transport(TransportError::Session { code, detail }) => Self::Session { code, detail },
            SyncError::Transport(TransportError::Graphql { messages, extensions }) | SyncError::Graphql { messages, extensions } => Self::Graphql { messages, extensions },
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
mod offload_tests;
#[cfg(test)]
mod load_tests;
#[cfg(test)]
mod session_restore_tests;
#[cfg(test)]
#[cfg(test)]
mod reject_tests;
#[cfg(test)]
#[cfg(test)]
mod resync_tests;
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
mod load_more_tests;
#[cfg(test)]
mod load_all_cancel_tests;
#[cfg(test)]
#[cfg(test)]
mod follow_failures_tests;
