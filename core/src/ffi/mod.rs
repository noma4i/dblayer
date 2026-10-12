mod database;
mod entities;
mod live;
mod state;
mod transport;

pub use database::{Config, Database};
pub use entities::{EntityDelivery, EntityListener, LiveEntities};
pub use live::{LiveChange, LiveSubscription, QueryListener};
pub use state::{RelationStateChange, StateListener, StateSubscription};
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
}

impl From<crate::db::DbError> for DbError {
    fn from(error: crate::db::DbError) -> Self { Self::Database { detail: error.to_string() } }
}

impl From<crate::sync::SyncError> for DbError {
    fn from(error: crate::sync::SyncError) -> Self { Self::Sync { detail: error.to_string() } }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self { Self::Invalid { detail: error.to_string() } }
}

fn row_json(row: &crate::db::Row) -> String {
    let mut data = row.data.clone();
    data.insert("id".into(), serde_json::json!(row.id.as_str()));
    data.insert("sync".into(), serde_json::json!({
        "state": row.sync.state, "actions": row.sync.actions, "dirtyFields": row.sync.dirty_fields,
        "error": row.sync.error, "attempts": row.sync.attempts, "since": row.sync.since,
    }));
    data.insert("upload".into(), serde_json::to_value(&row.sync.upload).expect("upload serializes"));
    serde_json::Value::Object(data).to_string()
}
