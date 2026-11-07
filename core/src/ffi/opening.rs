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

#[derive(Clone, Default, PartialEq, Eq, uniffi::Record)]
pub struct OpenOptions {
    pub operations: Vec<OperationDefinition>,
    /// Android below API 34 runs the upload round in a foreground service of type `dataSync`: `Some(declared)` says whether the manifest of the app declares its permission. `None` where the platform needs no declaration.
    pub data_sync_declared: Option<bool>,
}

/// The ports of the platform an open hands to the core: the network, the encrypted session and what it stores and reports.
#[derive(Default, uniffi::Record)]
pub struct OpenPorts {
    pub transport: Option<Arc<dyn Transport>>,
    pub encrypted: Option<EncryptedConfig>,
    pub credentials: Option<Arc<dyn CredentialStore>>,
    pub http: Option<Arc<dyn EncryptedHttp>>,
    /// The GraphQL address of a database without an encrypted session.
    #[uniffi(default = None)]
    pub endpoint: Option<String>,
}

impl OpenOptions {
    fn check_data_sync(&self) -> Result<(), DbError> {
        if self.data_sync_declared == Some(false) {
            return Err(DbError::Config { code: super::ConfigCode::InvalidRequest, detail: "the manifest of the app lacks android.permission.FOREGROUND_SERVICE_DATA_SYNC".into() });
        }
        Ok(())
    }
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
        options.check_data_sync()?;
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open(config, models_json, ports).await?;
        Self::start(database, lease, remote, options, observers, true).await
    }

    pub async fn open_with_clock(&self, config: Config, models_json: String, ports: OpenPorts, clock: Arc<TestClock>, options: OpenOptions, observers: Option<Arc<dyn PlatformObserver>>) -> Result<Arc<Database>, DbError> {
        options.check_data_sync()?;
        let lease = self.slot.acquire()?;
        let remote = ports.transport.is_some();
        let database = Database::open_with_clock(config, models_json, ports, clock).await?;
        Self::start(database, lease, remote, options, observers, false).await
    }
}

impl OpenDatabase {
    async fn start(database: Arc<Database>, lease: Arc<super::open_slot::OpenLease>, remote: bool, options: OpenOptions, observers: Option<Arc<dyn PlatformObserver>>, verify_uploads: bool) -> Result<Arc<Database>, DbError> {
        database.hold(lease);
        let opened = match database.start_observers(observers).await {
            Ok(()) if remote => database.declare_operations(options.operations).await.and_then(|()| if verify_uploads { database.check_uploads() } else { Ok(()) }),
            other => other,
        };
        if let Err(error) = opened {
            let _ = database.close_database().await;
            return Err(error);
        }
        Ok(database)
    }
}

impl Database {
    pub(super) async fn start_observers(&self, observers: Option<Arc<dyn PlatformObserver>>) -> Result<(), DbError> {
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
