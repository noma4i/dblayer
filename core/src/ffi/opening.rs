use std::sync::Arc;
use super::open_slot::OpenSlot;
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, RetryProgress, TestClock, Transport};

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub operations: String,
    pub power_saving: bool,
    pub low_ram: bool,
}

/// The ports of the platform an open hands to the core: the network, the encrypted session and what it stores and reports.
#[derive(Default, uniffi::Record)]
pub struct OpenPorts {
    pub transport: Option<Arc<dyn Transport>>,
    pub encrypted: Option<EncryptedConfig>,
    pub credentials: Option<Arc<dyn CredentialStore>>,
    pub http: Option<Arc<dyn EncryptedHttp>>,
    pub retries: Option<Arc<RetryProgress>>,
}

#[derive(uniffi::Object)]
pub struct OpenDatabase {
    slot: Arc<OpenSlot>,
}

#[uniffi::export]
impl OpenDatabase {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { slot: OpenSlot::new() }) }

    pub async fn open(&self, config: Config, models_json: String, options: OpenOptions, ports: OpenPorts) -> Result<Arc<Database>, DbError> {
        let options = super::database::blocking(move || { super::database::parse_json::<serde_json::Value>(&options.operations)?; Ok(options) }).await?;
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open(config, models_json, ports).await?;
        database.hold(lease);
        if remote {
            drop(database.life.set_power_saving(options.power_saving));
            if let Err(error) = database.register_operations(options.operations).and_then(|()| database.check_uploads()) {
                let _ = database.close_database().await;
                return Err(error);
            }
        }
        Ok(database)
    }

    pub async fn open_with_clock(&self, config: Config, models_json: String, ports: OpenPorts, clock: Arc<TestClock>) -> Result<Arc<Database>, DbError> {
        let lease = self.slot.acquire()?;
        let database = Database::open_with_clock(config, models_json, ports, clock).await?;
        database.hold(lease);
        Ok(database)
    }
}

#[cfg(test)]
#[path = "opening_tests.rs"]
mod tests;
