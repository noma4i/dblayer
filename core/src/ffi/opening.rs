use std::sync::Arc;
use super::open_slot::OpenSlot;
use super::upload_wire::WireUploadOperations;
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, TestClock, Transport};

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub operations: String,
    pub uploads: Option<WireUploadOperations>,
}

#[derive(uniffi::Object)]
pub struct OpenDatabase {
    slot: Arc<OpenSlot>,
}

#[uniffi::export]
impl OpenDatabase {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { slot: OpenSlot::new() }) }

    pub async fn open(&self, config: Config, models_json: String, options: OpenOptions, transport: Option<Arc<dyn Transport>>, encrypted: Option<EncryptedConfig>, credentials: Option<Arc<dyn CredentialStore>>, http: Option<Arc<dyn EncryptedHttp>>) -> Result<Arc<Database>, DbError> {
        serde_json::from_str::<serde_json::Value>(&options.operations)?;
        let lease = self.slot.acquire()?;
        let remote = transport.is_some();
        let database = Database::open(config, models_json, transport, encrypted, credentials, http, options.uploads.map(Into::into)).await?;
        database.hold(lease);
        if remote {
            if let Err(error) = database.register_operations(options.operations).and_then(|()| Ok(database.sync()?.check_uploads()?)) {
                database.shutdown();
                return Err(error);
            }
        }
        Ok(database)
    }

    pub async fn open_with_clock(&self, config: Config, models_json: String, transport: Option<Arc<dyn Transport>>, uploads: Option<WireUploadOperations>, clock: Arc<TestClock>) -> Result<Arc<Database>, DbError> {
        let lease = self.slot.acquire()?;
        let database = Database::open_with_clock(config, models_json, transport, uploads.map(Into::into), clock).await?;
        database.hold(lease);
        Ok(database)
    }
}

#[cfg(test)]
#[path = "opening_tests.rs"]
mod tests;
