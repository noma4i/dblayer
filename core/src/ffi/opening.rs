use std::sync::Arc;
use super::open_slot::OpenSlot;
use super::transport::ObserverStop;
use super::{Config, CredentialStore, Database, DbError, EncryptedConfig, EncryptedHttp, PlatformObserver, TestClock, Transport};

/// A GraphQL operation the app declares: its name and its document.
#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OperationDefinition {
    pub name: String,
    pub document: String,
}

#[derive(Clone, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub operations: Vec<OperationDefinition>,
}

/// The ports of the platform an open hands to the core: the network, the encrypted session and what it stores and reports.
#[derive(Default, uniffi::Record)]
pub struct OpenPorts {
    pub transport: Option<Arc<dyn Transport>>,
    pub encrypted: Option<EncryptedConfig>,
    pub credentials: Option<Arc<dyn CredentialStore>>,
    pub http: Option<Arc<dyn EncryptedHttp>>,
}

#[derive(uniffi::Object)]
pub struct OpenDatabase {
    slot: Arc<OpenSlot>,
}

#[uniffi::export]
impl OpenDatabase {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self { slot: OpenSlot::new() }) }

    pub async fn open(&self, config: Config, models_json: String, options: OpenOptions, ports: OpenPorts, observers: Option<Arc<dyn PlatformObserver>>) -> Result<Arc<Database>, DbError> {
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open(config, models_json, ports).await?;
        database.hold(lease);
        let opened = match database.start_observers(observers).await {
            Ok(()) if remote => database.declare_operations(options.operations).await.and_then(|()| database.check_uploads()),
            other => other,
        };
        if let Err(error) = opened {
            let _ = database.close_database().await;
            return Err(error);
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

#[uniffi::export]
impl Database {
    /// The observers of the platform start with the database and stop in `Closing`: `close` returns after their `stop`. `open` calls this; a database opened on a test clock starts its observers here.
    pub async fn start_observers(&self, observers: Option<Arc<dyn PlatformObserver>>) -> Result<(), DbError> {
        let Some(observers) = observers else { return Ok(()) };
        if self.db.observers().set(Arc::new(ObserverStop(observers.clone()))).is_err() {
            return Err(DbError::Invalid { detail: "the observers are already set".into() });
        }
        let sink = super::OnlineSink::of_life(&self.life);
        super::database::blocking(move || { observers.start(sink); Ok(()) }).await
    }
}

#[cfg(test)]
#[path = "opening_tests.rs"]
mod tests;
